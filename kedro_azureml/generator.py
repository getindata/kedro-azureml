import logging
import re
from typing import Any, Dict, Optional, Type, Union
from uuid import uuid4

from azure.ai.ml import (
    Input,
    MpiDistribution,
    Output,
    PyTorchDistribution,
    TensorFlowDistribution,
    command,
)
from azure.ai.ml.dsl import pipeline as azure_pipeline
from azure.ai.ml.entities import Environment, Job
from azure.ai.ml.entities._builders import Command
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node

from kedro_azureml.config import (
    ComputeConfig,
    KedroAzureMLConfig,
    KedroAzureRunnerConfig,
)
from kedro_azureml.constants import (
    DISTRIBUTED_CONFIG_FIELD,
    KEDRO_AZURE_RUNNER_CONFIG,
    PARAMS_PREFIX,
)
from kedro_azureml.datasets import AzureMLAssetDataset
from kedro_azureml.distributed import DistributedNodeConfig
from kedro_azureml.distributed.config import Framework

logger = logging.getLogger(__name__)


class ConfigException(BaseException):
    pass


class AzureMLPipelineGenerator:
    def __init__(
        self,
        pipeline_name: str,
        kedro_environment: str,
        config: KedroAzureMLConfig,
        kedro_params: Dict[str, Any],
        catalog: DataCatalog,
        aml_env: Optional[str] = None,
        docker_image: Optional[str] = None,
        params: Optional[str] = None,
        storage_account_key: Optional[str] = "",
        extra_env: Dict[str, str] = {},
        load_versions: Dict[str, str] = {},
    ):
        self.storage_account_key = storage_account_key
        self.kedro_environment = kedro_environment

        self.params = params
        self.kedro_params = kedro_params
        self.catalog = catalog
        self.aml_env = aml_env
        self.docker_image = docker_image
        self.config = config
        self.pipeline_name = pipeline_name
        self.extra_env = extra_env
        self.load_versions = load_versions

    def generate(self) -> Job:
        pipeline = self.get_kedro_pipeline()
        kedro_azure_run_id = uuid4().hex

        logger.info(f"Translating {self.pipeline_name} to Azure ML Pipeline")

        def kedro_azure_pipeline_fn():
            commands = {}

            for node in pipeline.nodes:
                azure_command = self._construct_azure_command(
                    pipeline, node, kedro_azure_run_id
                )

                commands[node.name] = azure_command

            # wire the commands into execution graph
            invoked_components = self._connect_commands(pipeline, commands)

            # pipeline outputs
            azure_pipeline_outputs = self._gather_pipeline_outputs(
                pipeline, invoked_components
            )
            return azure_pipeline_outputs

        kedro_azure_pipeline = azure_pipeline(name=self.pipeline_name)(
            kedro_azure_pipeline_fn
        )

        azure_pipeline_job: Job = kedro_azure_pipeline()
        return azure_pipeline_job

    def get_kedro_pipeline(self) -> Pipeline:
        from kedro.framework.project import pipelines

        pipeline: Pipeline = pipelines[self.pipeline_name]
        return pipeline

    def get_target_resource_from_node_tags(self, node: Node) -> ComputeConfig:
        resource_tags = set(node.tags).intersection(
            set(self.config.azure.compute.keys())
        )
        if len(resource_tags) > 1:
            raise ConfigException(
                "Node tags contain two values that are in defined in the resource config,"
                "a node can only have a maximum of 1 resource"
            )
        elif len(resource_tags) == 1:
            return self.config.azure.compute[resource_tags.pop()]
        else:
            return self.config.azure.compute["__default__"]

    def _sanitize_param_name(self, param_name: str) -> str:
        return re.sub(r"[^a-z0-9_]", "_", param_name.lower())

    def _sanitize_azure_name(self, name: str) -> str:
        return name.lower().replace(".", "__")

    def _get_kedro_param(
        self, param_name: str, params: Optional[Dict[str, Any]] = None
    ):
        if "." in param_name:
            name, remainder = param_name.split(".", 1)
            return self._get_kedro_param(remainder, (params or self.kedro_params)[name])
        else:
            return (params or self.kedro_params)[param_name]

    def _resolve_azure_environment(self) -> Union[Environment, str]:
        if image := (
            self.docker_image
            or (self.config.docker.image if self.config.docker else None)
        ):
            logger.info(f"Using docker image: {image} to run the pipeline.")
            return Environment(image=image)
        else:
            return self.aml_env or self.config.azure.environment_name

    def _get_versioned_azureml_dataset_name(
        self, catalog_name: str, azureml_dataset_name: str
    ):
        version = self.load_versions.get(catalog_name)
        if version is None or version == "latest":
            suffix = "@latest"
        else:
            suffix = ":" + version
        return azureml_dataset_name + suffix

    def _get_input(self, dataset_name: str, pipeline: Pipeline) -> Input:
        if self._is_param_or_root_non_azureml_asset_dataset(dataset_name, pipeline):
            return Input(type="string")
        elif dataset_name in self.catalog.list() and isinstance(
            ds := self.catalog._get_dataset(dataset_name), AzureMLAssetDataset
        ):
            if ds._azureml_type == "uri_file" and dataset_name not in pipeline.inputs():
                raise ValueError(
                    "AzureMLAssetDatasets with azureml_type 'uri_file' can only be used as pipeline inputs"
                )
            return Input(type=ds._azureml_type)
        else:
            return Input(type="uri_folder")

    def _get_output(self, name):
        if name in self.catalog.list() and isinstance(
            ds := self.catalog._get_dataset(name), AzureMLAssetDataset
        ):
            if ds._azureml_type == "uri_file":
                raise ValueError(
                    "AzureMLAssetDatasets with azureml_type 'uri_file' cannot be used as outputs"
                )
            # TODO: add versioning
            return Output(type=ds._azureml_type, name=ds._azureml_dataset)
        else:
            return Output(type="uri_folder")

    def _from_params_or_value(
        self,
        namespace: Optional[str],
        value_to_parse,
        hint,
        expected_value_type: Type = int,
    ):
        if isinstance(value_to_parse, str) and value_to_parse.startswith(PARAMS_PREFIX):
            prefix = f"{namespace}." if namespace else ""
            return self._get_kedro_param(
                prefix + value_to_parse.replace(PARAMS_PREFIX, "", 1)
            )
        elif (
            type(value_to_parse) is expected_value_type
        ):  # this is not isinstance() because isinstance(False, int) returns True...
            return value_to_parse
        else:
            msg = f"Expected either `params:` or actual value of type {expected_value_type}"
            msg += f" while parsing: {hint}"
            msg += f", got {value_to_parse}"
            raise ValueError(msg)

    def _is_param_or_root_non_azureml_asset_dataset(
        self, dataset_name: str, pipeline: Pipeline
    ) -> bool:
        return dataset_name.startswith(PARAMS_PREFIX) or (
            dataset_name in pipeline.inputs()
            and dataset_name in self.catalog.list()
            and not isinstance(
                self.catalog._get_dataset(dataset_name), AzureMLAssetDataset
            )
        )

    def _construct_azure_command(
        self,
        pipeline: Pipeline,
        node: Node,
        kedro_azure_run_id: str,
    ):
        command_kwargs = {}
        command_kwargs.update(self._get_distributed_azure_command_kwargs(node))
        pipeline_data_passing = (
            self.config.azure.pipeline_data_passing is not None
            and self.config.azure.pipeline_data_passing.enabled
        )

        return command(
            name=self._sanitize_azure_name(node.name),
            display_name=node.name,
            command=self._prepare_command(node, pipeline),
            compute=self.get_target_resource_from_node_tags(node).cluster_name,
            environment_variables={
                KEDRO_AZURE_RUNNER_CONFIG: KedroAzureRunnerConfig(
                    temporary_storage=self.config.azure.temporary_storage,
                    run_id=kedro_azure_run_id,
                    storage_account_key=self.storage_account_key,
                ).json()
                if not pipeline_data_passing
                else "",
                **self.extra_env,
            },
            environment=self._resolve_azure_environment(),  # TODO: check whether Environment exists
            inputs={
                self._sanitize_param_name(name): self._get_input(name, pipeline)
                for name in node.inputs
            },
            outputs={
                self._sanitize_param_name(name): self._get_output(name)
                for name in node.outputs
            },
            code=self.config.azure.code_directory,
            is_deterministic=("deterministic" in node.tags),
            **command_kwargs,
        )

    def _get_distributed_azure_command_kwargs(self, node) -> dict:
        azure_command_kwargs = {}
        if hasattr(node.func, DISTRIBUTED_CONFIG_FIELD) and isinstance(
            distributed_config := getattr(node.func, DISTRIBUTED_CONFIG_FIELD),
            DistributedNodeConfig,
        ):
            distributed_config: DistributedNodeConfig
            logger.info(
                f"Using distributed configuration for node {node.name}: {distributed_config}"
            )

            num_nodes: int = self._from_params_or_value(
                node.namespace, distributed_config.num_nodes, hint="num_nodes"
            )

            processes_per_instance: int = (
                self._from_params_or_value(
                    node.namespace,
                    distributed_config.processes_per_node,
                    hint="processes_per_node",
                )
                if distributed_config.processes_per_node is not None
                else 1
            )

            azure_command_kwargs["instance_count"] = num_nodes
            azure_command_kwargs["distribution"] = {
                Framework.PyTorch: PyTorchDistribution(
                    process_count_per_instance=processes_per_instance
                ),
                # TODO: test tensorflow
                Framework.TensorFlow: TensorFlowDistribution(worker_count=num_nodes),
                Framework.MPI: MpiDistribution(
                    process_count_per_instance=processes_per_instance
                ),
            }[distributed_config.framework]
        return azure_command_kwargs

    def _gather_pipeline_outputs(self, pipeline: Pipeline, invoked_components):
        azure_pipeline_outputs = {}
        for pipeline_output in pipeline.outputs():
            sanitized_output_name = self._sanitize_param_name(pipeline_output)
            source_node = next(
                (n for n in pipeline.nodes if pipeline_output in n.outputs), None
            )
            assert (
                source_node is not None
            ), f"There is no node which outputs `{pipeline_output}` dataset"
            azure_pipeline_outputs[sanitized_output_name] = invoked_components[
                source_node.name
            ].outputs[sanitized_output_name]
        return azure_pipeline_outputs

    def _connect_commands(self, pipeline: Pipeline, commands: Dict[str, Command]):
        """
        So far, only standalone commands were constructed, this method
        connects command inputs with command outputs, to build the actual execution graph.
        Connection is done by "invoking" the commands, so the Azure's DSL builds them here
        :param pipeline:
        :param commands:
        :return:
        """
        node_deps = pipeline.node_dependencies
        invoked_components = {}
        for node in pipeline.nodes:  # pipeline.nodes are sorted topologically
            dependencies = node_deps[node]
            azure_inputs = {}
            for node_input in node.inputs:
                # 1. try to find output in dependencies
                sanitized_input_name = self._sanitize_param_name(node_input)
                output_from_deps = next(
                    (d for d in dependencies if node_input in d.outputs), None
                )
                if output_from_deps:
                    parent_outputs = invoked_components[output_from_deps.name].outputs
                    azure_output = parent_outputs[sanitized_input_name]
                    azure_inputs[sanitized_input_name] = azure_output
                # 2. try to find AzureMLAssetDataset in catalog
                elif node_input in self.catalog.list() and isinstance(
                    ds := self.catalog._get_dataset(node_input), AzureMLAssetDataset
                ):
                    azure_inputs[sanitized_input_name] = Input(
                        type=ds._azureml_type,
                        path=self._get_versioned_azureml_dataset_name(
                            node_input, ds._azureml_dataset
                        ),
                    )
                # 3. if not found, provide dummy input
                else:
                    azure_inputs[sanitized_input_name] = node_input
            invoked_components[node.name] = commands[node.name](**azure_inputs)
        return invoked_components

    def _prepare_command(self, node, pipeline):
        input_data_paths = (
            [
                f"--az-input={name} "
                + "${{inputs."
                + self._sanitize_param_name(name)
                + "}}"
                for name in node.inputs
                if not self._is_param_or_root_non_azureml_asset_dataset(name, pipeline)
            ]
            if node.inputs
            else []
        )
        output_data_paths = (
            [
                f"--az-output={name} "
                + "${{outputs."
                + self._sanitize_param_name(name)
                + "}}"
                for name in node.outputs
            ]
            if node.outputs
            else []
        )
        return (
            (
                f"cd {self.config.azure.working_directory} && "
                if self.config.azure.working_directory is not None
                and self.config.azure.code_directory is None
                else ""
            )
            + f"kedro azureml -e {self.kedro_environment} execute --pipeline={self.pipeline_name} --node={node.name} "  # noqa
            + " ".join(input_data_paths + output_data_paths)
            + (f" --params='{self.params}'" if self.params else "")
        ).strip()

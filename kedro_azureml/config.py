from collections import defaultdict
from typing import Dict, Optional, Type

import yaml
from pydantic import BaseModel, validator

from kedro_azureml.utils import update_dict


class DefaultConfigDict(defaultdict):
    def __getitem__(self, key):
        defaults: BaseModel = super().__getitem__("__default__")
        this: BaseModel = super().__getitem__(key)
        return defaults.copy(update=this.dict(exclude_none=True)) if defaults else this


class AzureTempStorageConfig(BaseModel):
    account_name: Optional[str] = None
    container: Optional[str] = None


class ComputeConfig(BaseModel):
    cluster_name: str


class DockerConfig(BaseModel):
    image: Optional[str] = None


class PipelineDataPassingConfig(BaseModel):
    enabled: bool = False


class AzureMLConfig(BaseModel):
    @staticmethod
    def _create_default_dict_with(
        value: dict, default, dict_cls: Type = DefaultConfigDict
    ):
        default_value = (value := value or {}).get("__default__", default)
        return dict_cls(lambda: default_value, value)

    @validator("compute", always=True)
    def _validate_compute(cls, value):
        return AzureMLConfig._create_default_dict_with(
            value, ComputeConfig(cluster_name="{cluster_name}")
        )

    subscription_id: str
    resource_group: str
    workspace_name: str
    experiment_name: str
    compute: Optional[Dict[str, ComputeConfig]]
    temporary_storage: Optional[AzureTempStorageConfig]
    environment_name: Optional[str]
    code_directory: Optional[str]
    working_directory: Optional[str]
    pipeline_data_passing: Optional[PipelineDataPassingConfig] = None


class KedroAzureMLConfig(BaseModel):
    azure: AzureMLConfig
    docker: Optional[DockerConfig] = None


class KedroAzureRunnerConfig(BaseModel):
    # Class for use only in the runner
    temporary_storage: AzureTempStorageConfig
    run_id: str
    storage_account_key: str


CONFIG_TEMPLATE_YAML = """
azure:
  # Azure subscription ID to use
  subscription_id: "{subscription_id}"
  # Azure ML Experiment Name
  experiment_name: "{experiment_name}"
  # Azure resource group to use
  resource_group: "{resource_group}"
  # Azure ML Workspace name
  workspace_name: "{workspace_name}"
  # Azure ML Environment to use during pipeline execution
  environment_name: {environment_name}
  # Path to directory to upload, or null to disable code upload
  code_directory: {code_directory}
  # Path to the directory in the Docker image to run the code from
  # Ignored when code_directory is set
  working_directory: /home/kedro_docker
  # Use Azure ML pipeline data passing instead of temporary storage
  pipeline_data_passing:
    enabled: {pipeline_data_passing} # disabled by default

  # Temporary storage settings - this is used to pass some data between steps
  # if the data is not specified in the catalog directly
  temporary_storage:
    # Azure Storage account name, where the temp data should be stored
    # It's recommended to set Lifecycle management rule for storage container, to avoid costs of long-term storage
    # of the temporary data. Temporary data will be stored under abfs://<containter>/kedro-azureml-temp path
    # See https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-policy-configure?tabs=azure-portal
    account_name: {storage_account_name}
    # Name of the storage container
    container: {storage_container}
  compute:
    # Azure compute used for running kedro jobs.
    # Additional compute cluster can be defined here. Individual nodes can reference specific compute clusters by adding
    # the section title (e.g. <your_node_tag>) as a node_tag to their tags list. Nodes without a tag will run on
    # __default__ cluster.
    __default__:
      cluster_name: "{cluster_name}"
    # <your_node_tag>:
    #   cluster_name: "<your_cluster_name>"
docker:
  # This option is for backward compatibility and will be removed in the future versions
  # We suggest using the Azure environment instead
  # See https://kedro-azureml.readthedocs.io/en/0.2.1/source/03_quickstart.html
  # Docker image to use during pipeline execution
  image: {docker_image}
""".strip()

# This auto-validates the template above during import
_CONFIG_TEMPLATE = KedroAzureMLConfig.parse_obj(
    update_dict(
        yaml.safe_load(CONFIG_TEMPLATE_YAML),
        ("azure.pipeline_data_passing.enabled", False),
        ("azure.temporary_storage.container", ""),
        ("azure.temporary_storage.account_name", ""),
        ("azure.code_directory", None),
        ("azure.environment_name", None),
        ("docker.image", None),
    )
)

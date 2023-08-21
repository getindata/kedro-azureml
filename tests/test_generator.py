from unittest.mock import MagicMock, patch

import pytest
from azure.ai.ml.entities import Job

from kedro_azureml.config import ComputeConfig
from kedro_azureml.generator import AzureMLPipelineGenerator, ConfigException


@pytest.mark.parametrize(
    "pipeline_name",
    [
        ("dummy_pipeline"),
        ("dummy_pipeline_compute_tag"),
    ],
)
@pytest.mark.parametrize(
    "generator_kwargs",
    [
        {"aml_env": "unit_test/aml_env@latest"},
        {"docker_image": "unit/tests/docker/image:latest"},
    ],
)
@pytest.mark.parametrize(
    "pipeline_data_passing_enabled",
    (False, True),
    ids=("temporary storage", "pipeline data passing"),
)
def test_can_generate_azure_pipeline(
    pipeline_name,
    dummy_plugin_config,
    generator_kwargs: dict,
    pipeline_data_passing_enabled,
    multi_catalog,
    request,
):
    pipeline = request.getfixturevalue(pipeline_name)
    if pipeline_data_passing_enabled:
        dummy_plugin_config.azure.pipeline_data_passing.enabled = True
    with patch.object(
        AzureMLPipelineGenerator, "get_kedro_pipeline", return_value=pipeline
    ):
        env_name = "unit_test_env"
        generator = AzureMLPipelineGenerator(
            pipeline_name,
            env_name,
            dummy_plugin_config,
            {},
            catalog=multi_catalog,
            **generator_kwargs,
        )

        az_pipeline = generator.generate()
        assert (
            isinstance(az_pipeline, Job) and az_pipeline.display_name == pipeline_name
        ), "Invalid basic pipeline data"
        assert all(
            f"kedro azureml -e {env_name} execute" in node.command
            for node in az_pipeline.jobs.values()
        ), "Commands seems invalid"

        if "aml_env" in generator_kwargs:
            assert all(
                node.environment == generator_kwargs["aml_env"]
                for node in az_pipeline.jobs.values()
            ), "Invalid Azure ML Environment name set on commands"
        else:
            # For backward compatibility
            assert all(
                node.environment.image == generator_kwargs["docker_image"]
                for node in az_pipeline.jobs.values()
            ), "Invalid docker image set on commands"


def test_azure_pipeline_with_different_compute(
    dummy_pipeline_compute_tag, dummy_plugin_config, multi_catalog
):
    """
    Test that when a Node in an Azure Pipeline is tagged with a compute tag
    this gets passed through to the generated azure pipeline
    """
    dummy_plugin_config.azure.compute["compute-2"] = ComputeConfig(
        **{"cluster_name": "cpu-cluster-2"}
    )
    with patch.object(
        AzureMLPipelineGenerator,
        "get_kedro_pipeline",
        return_value=dummy_pipeline_compute_tag,
    ):
        env_name = "unit_test_env"
        aml_env = "unit_test/aml_env@latest"
        generator = AzureMLPipelineGenerator(
            "dummy_pipeline_compute_tag",
            env_name,
            dummy_plugin_config,
            {},
            catalog=multi_catalog,
            aml_env=aml_env,
        )

        az_pipeline = generator.generate()
        for node in dummy_pipeline_compute_tag.nodes:
            if node.tags:
                assert all(
                    [
                        dummy_plugin_config.azure.compute[tag].cluster_name
                        == az_pipeline.jobs[node.name]["compute"]
                        for tag in node.tags
                    ]
                ), "compute settings don't match"


def test_can_get_pipeline_from_kedro(
    dummy_plugin_config, dummy_pipeline, multi_catalog
):
    pipeline_name = "unit_test_pipeline"
    with patch.dict(
        "kedro.framework.project.pipelines", {pipeline_name: dummy_pipeline}
    ):
        generator = AzureMLPipelineGenerator(
            pipeline_name, "local", dummy_plugin_config, {}, catalog=multi_catalog
        )
        p = generator.get_kedro_pipeline()
        assert p == dummy_pipeline


def test_get_target_resource_from_node_tags_raises_exception(
    dummy_plugin_config, dummy_pipeline, multi_catalog
):
    pipeline_name = "unit_test_pipeline"
    node = MagicMock()
    node.tags = ["compute-2", "compute-3"]
    for t in node.tags:
        dummy_plugin_config.azure.compute[t] = ComputeConfig(**{"cluster_name": t})
    with patch.dict(
        "kedro.framework.project.pipelines", {pipeline_name: dummy_pipeline}
    ):
        generator = AzureMLPipelineGenerator(
            pipeline_name, "local", dummy_plugin_config, {}, catalog=multi_catalog
        )
        with pytest.raises(ConfigException):
            generator.get_target_resource_from_node_tags(node)


def test_azure_pipeline_with_custom_env_vars(
    dummy_plugin_config, dummy_pipeline, multi_catalog
):
    pipeline_name = "unit_test_pipeline"
    node = MagicMock()
    node.tags = ["compute-2", "compute-3"]
    for t in node.tags:
        dummy_plugin_config.azure.compute[t] = ComputeConfig(**{"cluster_name": t})
    with patch.dict(
        "kedro.framework.project.pipelines", {pipeline_name: dummy_pipeline}
    ):
        generator = AzureMLPipelineGenerator(
            pipeline_name,
            "local",
            dummy_plugin_config,
            {},
            extra_env={"ABC": "def"},
            catalog=multi_catalog,
        )

        for node in generator.generate().jobs.values():
            assert "ABC" in node.environment_variables
            assert node.environment_variables["ABC"] == "def"


def test_azure_pipeline_with_deterministic_node_tag(
    dummy_pipeline_deterministic_tag, dummy_plugin_config, multi_catalog
):
    """
    Test that when a Node in an Azure Pipeline is tagged with a deterministic tag
    this gets passed through to the generated azure pipeline
    """

    with patch.object(
        AzureMLPipelineGenerator,
        "get_kedro_pipeline",
        return_value=dummy_pipeline_deterministic_tag,
    ):
        env_name = "unit_test_env"
        aml_env = "unit_test/aml_env@latest"
        generator = AzureMLPipelineGenerator(
            "dummy_pipeline_deterministic_tag",
            env_name,
            dummy_plugin_config,
            {},
            catalog=multi_catalog,
            aml_env=aml_env,
        )

        az_pipeline = generator.generate()
        for node in dummy_pipeline_deterministic_tag.nodes:
            assert az_pipeline.jobs[node.name].component.is_deterministic == (
                "deterministic" in node.tags
            ), "is_deterministic property does not match node tag"

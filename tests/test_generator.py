from unittest.mock import patch

import pytest
from azure.ai.ml.entities import Job

from kedro_azureml.config import ComputeConfig
from kedro_azureml.generator import AzureMLPipelineGenerator


@pytest.mark.parametrize(
    "pipeline_name",
    [
        ("dummy_pipeline"),
        ("dummy_pipeline_compute_tag"),
    ],
)
def test_can_generate_azure_pipeline(pipeline_name, dummy_plugin_config, request):
    pipeline = request.getfixturevalue(pipeline_name)
    with patch.object(
        AzureMLPipelineGenerator, "get_kedro_pipeline", return_value=pipeline
    ):
        env_name = "unit_test_env"
        docker_image = "unit_test/docker_image:latest"
        generator = AzureMLPipelineGenerator(
            pipeline_name,
            env_name,
            dummy_plugin_config,
            {},
            docker_image=docker_image,
        )

        az_pipeline = generator.generate()
        assert (
            isinstance(az_pipeline, Job) and az_pipeline.display_name == pipeline_name
        ), "Invalid basic pipeline data"
        assert all(
            f"kedro azureml -e {env_name} execute" in node.command
            for node in az_pipeline.jobs.values()
        ), "Commands seems invalid"
        assert all(
            node.environment.image == docker_image for node in az_pipeline.jobs.values()
        ), "Invalid docker image set on commands"


def test_azure_pipeline_with_different_compute(
    dummy_pipeline_compute_tag, dummy_plugin_config
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
        docker_image = "unit_test/docker_image:latest"
        generator = AzureMLPipelineGenerator(
            "dummy_pipeline_compute_tag",
            env_name,
            dummy_plugin_config,
            docker_image=docker_image,
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


def test_can_get_pipeline_from_kedro(dummy_plugin_config, dummy_pipeline):
    pipeline_name = "unit_test_pipeline"
    with patch.dict(
        "kedro.framework.project.pipelines", {pipeline_name: dummy_pipeline}
    ):
        generator = AzureMLPipelineGenerator(
            pipeline_name, "local", dummy_plugin_config, {}
        )
        p = generator.get_kedro_pipeline()
        assert p == dummy_pipeline

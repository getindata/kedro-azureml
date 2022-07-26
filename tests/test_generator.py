from unittest.mock import patch

from azure.ai.ml.entities import Job
from kedro.pipeline import Pipeline

from kedro_azureml.generator import AzureMLPipelineGenerator


def test_can_generate_azure_pipeline(dummy_pipeline, dummy_plugin_config):
    with patch.object(
        AzureMLPipelineGenerator, "get_kedro_pipeline", return_value=dummy_pipeline
    ):
        env_name = "unit_test_env"
        docker_image = "unit_test/docker_image:latest"
        generator = AzureMLPipelineGenerator(
            "dummy_pipeline",
            env_name,
            dummy_plugin_config,
            docker_image=docker_image,
        )

        az_pipeline = generator.generate()
        assert (
            isinstance(az_pipeline, Job)
            and az_pipeline.display_name == "dummy_pipeline"
        ), "Invalid basic pipeline data"
        assert all(
            f"kedro azureml -e {env_name} execute" in node.command
            for node in az_pipeline.jobs.values()
        ), "Commands seems invalid"
        assert all(
            node.environment.image == docker_image for node in az_pipeline.jobs.values()
        ), "Invalid docker image set on commands"


def test_can_get_pipeline_from_kedro(dummy_plugin_config, dummy_pipeline):
    pipeline_name = "unit_test_pipeline"
    with patch.dict(
        "kedro.framework.project.pipelines", {pipeline_name: dummy_pipeline}
    ):
        generator = AzureMLPipelineGenerator(
            pipeline_name,
            "local",
            dummy_plugin_config,
        )
        p = generator.get_kedro_pipeline()
        assert p == dummy_pipeline

from pathlib import Path
from tempfile import TemporaryFile, TemporaryDirectory

from azure.ai.ml import MLClient
from azure.ai.ml.entities import Job
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
import json
from kedro_azureml.config import AzureMLConfig
from contextlib import contextmanager


@contextmanager
def _get_azureml_client(config: AzureMLConfig):
    client_config = {
        "subscription_id": config.subscription_id,
        "resource_group": config.resource_group,
        "workspace_name": config.workspace_name,
    }

    try:
        credential = DefaultAzureCredential()
        # Check if given credential can get token successfully.
        credential.get_token("https://management.azure.com/.default")
    except Exception as ex:
        # Fall back to InteractiveBrowserCredential in case DefaultAzureCredential not work
        credential = InteractiveBrowserCredential()

    with TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir) / "config.json"
        config_path.write_text(json.dumps(client_config))
        ml_client = MLClient.from_config(
            credential=credential, path=str(config_path.absolute())
        )
        yield ml_client


class AzureMLPipelinesClient:
    def __init__(self, azure_pipeline: Job):
        self.azure_pipeline = azure_pipeline

    def compile(self, output_path: Path):
        output_path.write_text(str(self.azure_pipeline))

    def run(self, config: AzureMLConfig, wait_for_completion=False) -> bool:
        with _get_azureml_client(config) as ml_client:
            assert ml_client.compute.get(
                config.cluster_name
            ), f"Cluster {config.cluster_name} does not exist"

            pipeline_job = ml_client.jobs.create_or_update(
                self.azure_pipeline,
                experiment_name=config.experiment_name,
                compute=config.cluster_name,
            )

            if wait_for_completion:
                try:
                    ml_client.jobs.stream(pipeline_job.name)
                    return True
                except Exception:
                    return False
            else:
                return True

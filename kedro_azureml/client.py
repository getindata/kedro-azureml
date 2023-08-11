import json
import logging
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Optional

from azure.ai.ml import MLClient
from azure.ai.ml.entities import Job

from kedro_azureml.auth.utils import get_azureml_credentials
from kedro_azureml.config import AzureMLConfig

logger = logging.getLogger(__name__)


@contextmanager
def _get_azureml_client(subscription_id: Optional[str], config: AzureMLConfig):
    client_config = {
        "subscription_id": subscription_id or config.subscription_id,
        "resource_group": config.resource_group,
        "workspace_name": config.workspace_name,
    }

    credential = get_azureml_credentials()

    with TemporaryDirectory() as tmp_dir:
        config_path = Path(tmp_dir) / "config.json"
        config_path.write_text(json.dumps(client_config))
        ml_client = MLClient.from_config(
            credential=credential, path=str(config_path.absolute())
        )
        yield ml_client


class AzureMLPipelinesClient:
    def __init__(self, azure_pipeline: Job, subscription_id: str):
        self.subscription_id = subscription_id
        self.azure_pipeline = azure_pipeline

    def run(
        self,
        config: AzureMLConfig,
        wait_for_completion=False,
        on_job_scheduled: Optional[Callable[[Job], None]] = None,
    ) -> bool:
        with _get_azureml_client(self.subscription_id, config) as ml_client:
            assert (
                cluster := ml_client.compute.get(
                    config.compute["__default__"].cluster_name
                )
            ), f"Cluster {config.compute['__default__'].cluster_name} does not exist"

            logger.info(
                f"Creating job on cluster {cluster.name} ({cluster.size}, min instances: {cluster.min_instances}, "
                f"max instances: {cluster.max_instances})"
            )

            pipeline_job = ml_client.jobs.create_or_update(
                self.azure_pipeline,
                experiment_name=config.experiment_name,
                compute=cluster,
            )

            if on_job_scheduled:
                on_job_scheduled(pipeline_job)

            if wait_for_completion:
                try:
                    ml_client.jobs.stream(pipeline_job.name)
                    return True
                except Exception:
                    logger.exception("Error while running the pipeline", exc_info=True)
                    return False
            else:
                return True

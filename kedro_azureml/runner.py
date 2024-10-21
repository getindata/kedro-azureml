import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from kedro.io import AbstractDataset, DataCatalog
from kedro.pipeline import Pipeline
from kedro.runner import SequentialRunner
from kedro_datasets.pickle import PickleDataset
from pluggy import PluginManager

from kedro_azureml.config import KedroAzureRunnerConfig
from kedro_azureml.constants import KEDRO_AZURE_RUNNER_CONFIG
from kedro_azureml.datasets import (
    AzureMLPipelineDataset,
    KedroAzureRunnerDataset,
    KedroAzureRunnerDistributedDataset,
)
from kedro_azureml.datasets.asset_dataset import AzureMLAssetDataset
from kedro_azureml.distributed.utils import is_distributed_environment

logger = logging.getLogger(__name__)


class AzurePipelinesRunner(SequentialRunner):
    def __init__(
        self,
        is_async: bool = False,
        data_paths: Optional[Dict[str, str]] = None,
        pipeline_data_passing: bool = False,
    ):
        super().__init__(is_async)
        self.pipeline_data_passing = pipeline_data_passing
        self.runner_config_raw = os.environ.get(KEDRO_AZURE_RUNNER_CONFIG)
        self.runner_config: KedroAzureRunnerConfig = (
            KedroAzureRunnerConfig.parse_raw(self.runner_config_raw)
            if not self.pipeline_data_passing
            else None
        )
        self.data_paths = data_paths if data_paths is not None else {}

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        session_id: str = None,
    ) -> Dict[str, Any]:
        catalog = catalog.shallow_copy()
        catalog_set = set(catalog.list())

        # Loop over datasets in arguments to set their paths
        for ds_name, azure_dataset_path in self.data_paths.items():
            if ds_name in catalog_set:
                ds = catalog._get_dataset(ds_name)
                if isinstance(ds, AzureMLPipelineDataset):
                    if (
                        isinstance(ds, AzureMLAssetDataset)
                        and ds._azureml_type == "uri_file"
                    ):
                        ds.root_dir = str(Path(azure_dataset_path).parent)
                    else:
                        ds.root_dir = azure_dataset_path
                    catalog.add(ds_name, ds, replace=True)
            else:
                catalog.add(ds_name, self.create_default_data_set(ds_name))

        # Loop over remaining input datasets to add them to the catalog
        unsatisfied = pipeline.inputs() - set(catalog.list())
        for ds_name in unsatisfied:
            catalog.add(ds_name, self.create_default_data_set(ds_name))

        return super().run(pipeline, catalog, hook_manager, session_id)

    def create_default_data_set(self, ds_name: str) -> AbstractDataset:
        if self.pipeline_data_passing:
            return AzureMLPipelineDataset(
                {
                    "type": PickleDataset,
                    "backend": "cloudpickle",
                    "filepath": f"{ds_name}.pickle",
                },
                root_dir=self.data_paths[ds_name],
            )
        else:
            # TODO: handle credentials better (probably with built-in Kedro credentials
            #  via ConfigLoader (but it's not available here...)
            dataset_cls = KedroAzureRunnerDataset
            if is_distributed_environment():
                logger.info("Using distributed dataset class as a default")
                dataset_cls = KedroAzureRunnerDistributedDataset

            return dataset_cls(
                self.runner_config.temporary_storage.account_name,
                self.runner_config.temporary_storage.container,
                self.runner_config.storage_account_key,
                ds_name,
                self.runner_config.run_id,
            )

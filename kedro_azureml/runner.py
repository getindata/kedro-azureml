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
            KedroAzureRunnerConfig.model_validate_json(self.runner_config_raw)
            if not self.pipeline_data_passing
            else None
        )
        self.data_paths = data_paths if data_paths is not None else {}

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        run_id: str = None,
        only_missing_outputs: bool = False,
    ) -> Dict[str, Any]:
        # In Kedro 1.0, we need to create a new catalog with additional datasets
        # since we can't modify the existing catalog directly
        
        # Get the current catalog configuration
        catalog_config, credentials, load_versions, save_version = catalog.to_config()
        
        # Add missing datasets to the configuration
        for ds_name, azure_dataset_path in self.data_paths.items():
            if ds_name not in catalog_config:
                if self.pipeline_data_passing:
                    catalog_config[ds_name] = {
                        "type": "kedro_azureml.datasets.pipeline_dataset.AzureMLPipelineDataset",
                        "dataset": {
                            "type": "kedro_datasets.pickle.PickleDataset",
                            "filepath": f"{ds_name}.pickle",
                            "backend": "cloudpickle",
                        },
                        "root_dir": azure_dataset_path,
                    }
                else:
                    # TODO: handle credentials better (probably with built-in Kedro credentials
                    #  via ConfigLoader (but it's not available here...)
                    dataset_cls = "kedro_azureml.datasets.runner_dataset.KedroAzureRunnerDataset"
                    if is_distributed_environment():
                        logger.info("Using distributed dataset class as a default")
                        dataset_cls = "kedro_azureml.datasets.runner_dataset.KedroAzureRunnerDistributedDataset"
                    
                    catalog_config[ds_name] = {
                        "type": dataset_cls,
                        "storage_account_name": self.runner_config.temporary_storage.account_name,
                        "storage_container": self.runner_config.temporary_storage.container,
                        "storage_account_key": self.runner_config.storage_account_key,
                        "dataset_name": ds_name,
                        "run_id": self.runner_config.run_id,
                    }
            else:
                # Update existing dataset paths if needed
                ds = catalog.get(ds_name)
                if isinstance(ds, AzureMLPipelineDataset):
                    if (
                        isinstance(ds, AzureMLAssetDataset)
                        and ds._azureml_type == "uri_file"
                    ):
                        catalog_config[ds_name]["root_dir"] = str(Path(azure_dataset_path).parent)
                    else:
                        catalog_config[ds_name]["root_dir"] = azure_dataset_path

        # Add remaining unsatisfied input datasets
        unsatisfied = pipeline.inputs() - set(catalog_config.keys())
        for ds_name in unsatisfied:
            if self.pipeline_data_passing:
                catalog_config[ds_name] = {
                    "type": "kedro_azureml.datasets.pipeline_dataset.AzureMLPipelineDataset",
                    "dataset": {
                        "type": "kedro_datasets.pickle.PickleDataset",
                        "filepath": f"{ds_name}.pickle",
                        "backend": "cloudpickle",
                    },
                    "root_dir": self.data_paths.get(ds_name, ""),
                }
            else:
                dataset_cls = "kedro_azureml.datasets.runner_dataset.KedroAzureRunnerDataset"
                if is_distributed_environment():
                    logger.info("Using distributed dataset class as a default")
                    dataset_cls = "kedro_azureml.datasets.runner_dataset.KedroAzureRunnerDistributedDataset"
                
                catalog_config[ds_name] = {
                    "type": dataset_cls,
                    "storage_account_name": self.runner_config.temporary_storage.account_name,
                    "storage_container": self.runner_config.temporary_storage.container,
                    "storage_account_key": self.runner_config.storage_account_key,
                    "dataset_name": ds_name,
                    "run_id": self.runner_config.run_id,
                }

        # Create a new catalog with the updated configuration
        new_catalog = DataCatalog.from_config(catalog_config, credentials, load_versions, save_version)
        
        return super().run(pipeline, new_catalog, hook_manager, run_id, only_missing_outputs)

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

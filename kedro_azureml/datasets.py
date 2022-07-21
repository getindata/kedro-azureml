from functools import lru_cache
from typing import Any, Dict

import fsspec
from cloudpickle import cloudpickle
from kedro.io import AbstractDataSet

from kedro_azureml.constants import KEDRO_AZURE_BLOB_TEMP_DIR_NAME


class KedroAzureRunnerDataset(AbstractDataSet):
    def __init__(
        self,
        storage_account_name,
        storage_container,
        storage_account_key,
        dataset_name,
        run_id,
    ):
        self.storage_container = storage_container
        self.run_id = run_id
        self.dataset_name = dataset_name
        self.storage_account_key = storage_account_key
        self.storage_account_name = storage_account_name

    @lru_cache()
    def _get_target_path(self):
        return f"abfs://{self.storage_container}/{KEDRO_AZURE_BLOB_TEMP_DIR_NAME}/{self.run_id}/{self.dataset_name}.bin"

    @lru_cache()
    def _get_storage_options(self):
        return {
            "account_name": self.storage_account_name,
            "account_key": self.storage_account_key,
        }

    def _load(self):
        with fsspec.open(
            self._get_target_path(), "rb", **self._get_storage_options()
        ) as f:
            return cloudpickle.load(f)

    def _save(self, data: Any) -> None:
        with fsspec.open(
            self._get_target_path(), "wb", **self._get_storage_options()
        ) as f:
            cloudpickle.dump(data, f)

    def _describe(self) -> Dict[str, Any]:
        return {"KedroAzureRunnerDataset": "for use only within Azure ML Pipelines"}

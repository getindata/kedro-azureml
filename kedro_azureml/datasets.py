import bz2  # TODO: consider zstandard?
from functools import lru_cache
from sys import version_info
from typing import Any, Dict

import cloudpickle
import fsspec
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
        self.pickle_protocol = None if version_info[:2] > (3, 8) else 4

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
            with bz2.open(f, "rb") as stream:
                return cloudpickle.load(stream)

    def _save(self, data: Any) -> None:
        with fsspec.open(
            self._get_target_path(), "wb", **self._get_storage_options()
        ) as f:
            with bz2.open(f, "wb") as stream:
                cloudpickle.dump(data, stream, protocol=self.pickle_protocol)

    def _describe(self) -> Dict[str, Any]:
        return {
            "info": "for use only within Azure ML Pipelines",
            "dataset_name": self.dataset_name,
            "path": self._get_target_path(),
        }

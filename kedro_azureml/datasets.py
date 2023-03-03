import bz2  # TODO: consider zstandard?
import logging
import os
from copy import deepcopy
from functools import lru_cache
from sys import version_info
from typing import Any, Dict, Type, Union
from warnings import warn

import backoff
import cloudpickle
import fsspec
from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataSet,
    DataSetError,
    parse_dataset_definition,
)

from kedro_azureml.constants import (
    KEDRO_AZURE_BLOB_TEMP_DIR_NAME,
    KEDRO_AZURE_RUNNER_DATASET_TIMEOUT,
)
from kedro_azureml.distributed.utils import is_distributed_master_node

logger = logging.getLogger(__name__)


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


class KedroAzureRunnerDistributedDataset(KedroAzureRunnerDataset):
    @backoff.on_exception(
        backoff.fibo,
        Exception,
        max_time=lambda: int(os.environ.get(KEDRO_AZURE_RUNNER_DATASET_TIMEOUT, "300")),
        raise_on_giveup=False,
    )
    def _load(self):
        return super()._load()

    def _save(self, data: Any) -> None:
        if is_distributed_master_node():
            super()._save(data)
        else:
            logger.warning(
                f"DataSet {self.dataset_name} will not be saved on a distributed node"
            )


# TODO: First make work in AML, then also locally
# TODO: Switch to File dataset?
class AzureMLFolderDataset(AbstractDataSet):
    def __init__(  # pylint: disable=too-many-arguments
        self,
        path: str,
        dataset: Union[str, Type[AbstractDataSet], Dict[str, Any]],
        filepath_arg: str = "filepath",
    ):
        """Creates a new instance of ``PartitionedDataSet``.

        Args:
            path: Path to the folder containing partitioned data.
                If path starts with the protocol (e.g., ``s3://``) then the
                corresponding ``fsspec`` concrete filesystem implementation will
                be used. If protocol is not specified,
                ``fsspec.implementations.local.LocalFileSystem`` will be used.
                **Note:** Some concrete implementations are bundled with ``fsspec``,
                while others (like ``s3`` or ``gcs``) must be installed separately
                prior to usage of the ``PartitionedDataSet``.
            dataset: Underlying dataset definition.
                Accepted formats are:
                a) object of a class that inherits from ``AbstractDataSet``
                b) a string representing a fully qualified class name to such class
                c) a dictionary with ``type`` key pointing to a string from b),
                other keys are passed to the Dataset initializer.
                Credentials for the dataset can be explicitly specified in
                this configuration.
            filepath_arg: Underlying dataset initializer argument that will
                contain a path to each corresponding partition file.
                If unspecified, defaults to "filepath".

        Raises:
            DataSetError: If versioning is enabled for the underlying dataset.
        """

        super().__init__()

        self.path = path

        dataset = dataset if isinstance(dataset, dict) else {"type": dataset}
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)

        # TODO: remove and disable versioning?
        if VERSION_KEY in self._dataset_config:
            raise DataSetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

        self._filepath_arg = filepath_arg
        if self._filepath_arg in self._dataset_config:
            warn(
                f"'{self._filepath_arg}' key must not be specified in the dataset "
                f"definition as it will be overwritten by path argument"
            )

    def _construct_dataset(self):
        kwargs = deepcopy(self._dataset_config)
        kwargs[self._filepath_arg] = self.path
        dataset = self._dataset_type(**kwargs)
        return dataset

    def _load(self) -> Any:
        return self._construct_dataset().load()

    def _save(self, data: Any) -> None:
        self._construct_dataset().save(data)

    def _describe(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "dataset_type": self._dataset_type.__name__,
            "dataset_config": self._dataset_config,
        }

    def _exists(self) -> bool:
        return self._construct_dataset().exists()


# TODO: Add distributed dataset version

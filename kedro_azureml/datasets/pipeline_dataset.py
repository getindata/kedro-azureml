import logging
from copy import deepcopy
from typing import Any, Dict, Type, Union
from warnings import warn

from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataSet,
    DataSetError,
    parse_dataset_definition,
)

from kedro_azureml.distributed.utils import is_distributed_master_node

logger = logging.getLogger(__name__)


class AzureMLPipelineDataSet(AbstractDataSet):
    def __init__(
        self,
        path: str,
        dataset: Union[str, Type[AbstractDataSet], Dict[str, Any]],
        filepath_arg: str = "filepath",
    ):
        """Creates a new instance of ``AzureMLPipelineDataSet``.

        Args:
            path: Path to the file containing the data.
            dataset: Underlying dataset definition.
                Accepted formats are:
                a) object of a class that inherits from ``AbstractDataSet``
                b) a string representing a fully qualified class name to such class
                c) a dictionary with ``type`` key pointing to a string from b),
                other keys are passed to the Dataset initializer.
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

        # TODO: remove and disable versioning in Azure ML runner?
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


class AzureMLFolderDistributedDataset(AzureMLPipelineDataSet):
    def _save(self, data: Any) -> None:
        if is_distributed_master_node():
            super()._save(data)
        else:
            logger.warning(
                f"DataSet {self.dataset_name} will not be saved on a distributed node"
            )

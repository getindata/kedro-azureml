import logging
from typing import Any, Dict, Optional, Type, Union

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
        dataset: Union[str, Type[AbstractDataSet], Dict[str, Any]],
        path: Optional[str] = None,
        filepath_arg: str = "filepath",
    ):
        """Creates a new instance of ``AzureMLPipelineDataSet``.

        Args:
            dataset: Underlying dataset definition.
                Accepted formats are:
                a) object of a class that inherits from ``AbstractDataSet``
                b) a string representing a fully qualified class name to such class
                c) a dictionary with ``type`` key pointing to a string from b),
                other keys are passed to the Dataset initializer.
            path: Path to override the path of the underlying dataset with.
            filepath_arg: Underlying dataset initializer argument that will
                contain a path to each corresponding partition file.
                If unspecified, defaults to "filepath".

        Raises:
            DataSetError: If versioning is enabled for the underlying dataset.
        """

        super().__init__()

        dataset = dataset if isinstance(dataset, dict) else {"type": dataset}
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)

        self._filepath_arg = filepath_arg

        if path is not None:
            self._dataset_config[self._filepath_arg] = path

        # TODO: remove and disable versioning in Azure ML runner?
        if VERSION_KEY in self._dataset_config:
            raise DataSetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

    @property
    def path(self) -> str:
        return self._dataset_config[self._filepath_arg]

    @path.setter
    def path(self, path: str) -> None:
        self._dataset_config[self._filepath_arg] = path

    def _construct_dataset(self) -> AbstractDataSet:
        return self._dataset_type(**self._dataset_config)

    def _load(self) -> Any:
        return self._construct_dataset().load()

    def _save(self, data: Any) -> None:
        self._construct_dataset().save(data)

    def _describe(self) -> Dict[str, Any]:
        return {
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
            logger.warning(f"DataSet {self} will not be saved on a distributed node")

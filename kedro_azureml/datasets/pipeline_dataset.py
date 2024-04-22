import logging
from pathlib import Path
from typing import Any, Dict, Type, Union

from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataset,
    DatasetError,
    parse_dataset_definition,
)

from kedro_azureml.distributed.utils import (
    is_distributed_environment,
    is_distributed_master_node,
)

logger = logging.getLogger(__name__)


class AzureMLPipelineDataset(AbstractDataset):
    """
    Dataset to support pipeline data passing in Azure ML between nodes, using `kedro.io.AbstractDataset` as base class.
    Wraps around an underlying dataset, which can be any dataset supported by Kedro, and adds the ability to modify the
    file path of the underlying dataset, to point to the mount paths on the Azure ML compute where the node is run.

    Args
    ----

     | - ``dataset``: Underlying dataset definition.
            Accepted formats are:
            a) object of a class that inherits from ``AbstractDataset``
            b) a string representing a fully qualified class name to such class
            c) a dictionary with ``type`` key pointing to a string from b),
            other keys are passed to the Dataset initializer.
     | - ``root_dir``: Folder (path) to prepend to the filepath of the underlying dataset.
            If unspecified, defaults to "data".
     | - ``filepath_arg``: Underlying dataset initializer argument that will
            set the filepath.
            If unspecified, defaults to "filepath".

    Example
    -------

    Example of a catalog.yml entry:

    .. code-block:: yaml

        processed_images:
          type: kedro_azureml.datasets.AzureMLPipelineDataset
          root_dir: 'data/01_raw'
          dataset:
            type: pillow.ImageDataset
            filepath: 'images.png'

    """

    def __init__(
        self,
        dataset: Union[str, Type[AbstractDataset], Dict[str, Any]],
        root_dir: str = "data",
        filepath_arg: str = "filepath",
        metadata: Dict[str, Any] = None,
    ):
        """Creates a new instance of ``AzureMLPipelineDataset``.

        Args:
            dataset: Underlying dataset definition.
                Accepted formats are:
                a) object of a class that inherits from ``AbstractDataset``
                b) a string representing a fully qualified class name to such class
                c) a dictionary with ``type`` key pointing to a string from b),
                other keys are passed to the Dataset initializer.
            filepath_arg: Underlying dataset initializer argument that will
                set the filepath.
                If unspecified, defaults to "filepath".
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: If versioning is enabled for the underlying dataset.
        """

        dataset = dataset if isinstance(dataset, dict) else {"type": dataset}
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)

        self.root_dir = root_dir
        self._filepath_arg = filepath_arg
        self.metadata = metadata
        try:
            # Convert filepath to relative path
            self._dataset_config[self._filepath_arg] = str(
                Path(self._dataset_config[self._filepath_arg]).relative_to(Path.cwd())
            )
        except ValueError:
            # If the path is not relative to the current working directory, do not modify it
            pass

        # TODO: remove and disable versioning in Azure ML runner?
        if VERSION_KEY in self._dataset_config:
            raise DatasetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

    @property
    def path(self) -> str:
        return Path(self.root_dir) / Path(self._dataset_config[self._filepath_arg])

    @property
    def _filepath(self) -> str:
        """
        Fixes compatibility with kedro-mlflow https://github.com/getindata/kedro-azureml/issues/53
        :return:
        """
        return self.path

    def _construct_dataset(self) -> AbstractDataset:
        dataset_config = self._dataset_config.copy()
        dataset_config[self._filepath_arg] = str(self.path)
        return self._dataset_type(**dataset_config)

    def _load(self) -> Any:
        return self._construct_dataset().load()

    def _save(self, data: Any) -> None:
        if is_distributed_environment() and not is_distributed_master_node():
            logger.warning(f"Dataset {self} will not be saved on a distributed node")
        else:
            self._construct_dataset().save(data)

    def _describe(self) -> Dict[str, Any]:
        return {
            "dataset_type": self._dataset_type.__name__,
            "dataset_config": self._dataset_config,
            "root_dir": self.root_dir,
            "filepath_arg": self._filepath_arg,
        }

    def _exists(self) -> bool:
        return self._construct_dataset().exists()

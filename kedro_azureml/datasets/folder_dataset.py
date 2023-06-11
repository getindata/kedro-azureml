import logging
from typing import Any, Dict, Optional, Type, Union

from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataSet,
    DataSetError,
    Version,
)

from kedro_azureml.datasets.pipeline_dataset import AzureMLPipelineDataSet

logger = logging.getLogger(__name__)


class AzureMLFolderDataSet(AzureMLPipelineDataSet):
    def __init__(
        self,
        azureml_dataset: str,
        dataset: Union[str, Type[AbstractDataSet], Dict[str, Any]],
        version: Optional[Version] = None,
        folder: str = "data",
        filepath_arg: str = "filepath",
    ):
        super().__init__(dataset=dataset, folder=folder, filepath_arg=filepath_arg)

        self._azureml_dataset = azureml_dataset
        self._version = version

        # TODO: remove and disable versioning in Azure ML runner?
        if VERSION_KEY in self._dataset_config:
            raise DataSetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

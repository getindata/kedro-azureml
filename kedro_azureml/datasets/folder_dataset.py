import logging
from typing import Any, Dict, Literal, Optional, Type, Union, get_args

from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataSet,
    DataSetError,
    Version,
)

from kedro_azureml.datasets.pipeline_dataset import AzureMLPipelineDataSet

AzureMLDataAssetType = Literal["uri_file", "uri_folder"]
logger = logging.getLogger(__name__)


class AzureMLFolderDataSet(AzureMLPipelineDataSet):
    def __init__(
        self,
        azureml_dataset: str,
        dataset: Union[str, Type[AbstractDataSet], Dict[str, Any]],
        folder: str = "data",
        filepath_arg: str = "filepath",
        azureml_type: AzureMLDataAssetType = "uri_folder",
        version: Optional[Version] = None,
    ):
        super().__init__(dataset=dataset, folder=folder, filepath_arg=filepath_arg)

        self._azureml_dataset = azureml_dataset
        self._version = version
        self._azureml_type = azureml_type
        if self._azureml_type not in get_args(AzureMLDataAssetType):
            raise DataSetError(
                f"Invalid azureml_type '{self._azureml_type}' in dataset definition. "
                f"Valid values are: {get_args(AzureMLDataAssetType)}"
            )

        # TODO: remove and disable versioning in Azure ML runner?
        if VERSION_KEY in self._dataset_config:
            raise DataSetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

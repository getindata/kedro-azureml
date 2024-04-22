import logging
from functools import partial
from operator import attrgetter
from pathlib import Path
from typing import Any, Dict, Literal, Optional, Type, Union, get_args

from azure.core.exceptions import ResourceNotFoundError
from azureml.fsspec import AzureMachineLearningFileSystem
from cachetools import Cache, cachedmethod
from cachetools.keys import hashkey
from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataset,
    AbstractVersionedDataset,
    DatasetError,
    DatasetNotFoundError,
    Version,
    VersionNotFoundError,
)

from kedro_azureml.client import _get_azureml_client
from kedro_azureml.config import AzureMLConfig
from kedro_azureml.datasets.pipeline_dataset import AzureMLPipelineDataset

AzureMLDataAssetType = Literal["uri_file", "uri_folder"]
logger = logging.getLogger(__name__)


class AzureMLAssetDataset(AzureMLPipelineDataset, AbstractVersionedDataset):
    """
    AzureMLAssetDataset enables kedro-azureml to use azureml
    v2-sdk Folder/File datasets for remote and local runs.

    Args
    ----

     | - ``azureml_dataset``: Name of the AzureML dataset.
     | - ``dataset``: Definition of the underlying dataset saved in the Folder/Filedataset.
        ``e.g. Parquet, Csv etc.
     | - ``root_dir``: The local folder where the dataset should be saved during local runs.
        ``Relevant for local execution via `kedro run`.
     | - ``filepath_arg``: Filepath arg on the wrapped dataset, defaults to `filepath`
     | - ``azureml_type``: Either `uri_folder` or `uri_file`
     | - ``version``: Version of the AzureML dataset to be used in kedro format.

    Example
    -------

    Example of a catalog.yml entry:

    .. code-block:: yaml

        my_folder_dataset:
          type: kedro_azureml.datasets.AzureMLAssetDataset
          azureml_dataset: my_azureml_folder_dataset
          root_dir: data/01_raw/some_folder/
          versioned: True
          dataset:
            type: pandas.ParquetDataset
            filepath: "."

        my_file_dataset:
            type: kedro_azureml.datasets.AzureMLAssetDataset
            azureml_dataset: my_azureml_file_dataset
            root_dir: data/01_raw/some_other_folder/
            versioned: True
            dataset:
                type: pandas.ParquetDataset
                filepath: "companies.csv"

    """

    versioned = True

    def __init__(
        self,
        azureml_dataset: str,
        dataset: Union[str, Type[AbstractDataset], Dict[str, Any]],
        root_dir: str = "data",
        filepath_arg: str = "filepath",
        azureml_type: AzureMLDataAssetType = "uri_folder",
        version: Optional[Version] = None,
        metadata: Dict[str, Any] = None,
    ):
        """
        azureml_dataset: Name of the AzureML file azureml_dataset.
        dataset: Type of the underlying dataset that is saved on AzureML e.g. Parquet, Csv etc.
        root_dir: The local folder where the dataset should be saved during local runs.
                Relevant only for local execution via `kedro run`.
        filepath_arg: Filepath arg on the wrapped dataset, defaults to `filepath`
        azureml_type: Either `uri_folder` or `uri_file`
        version: Version of the AzureML dataset to be used in kedro format.
        metadata: Any arbitrary metadata.
            This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        super().__init__(
            dataset=dataset,
            root_dir=root_dir,
            filepath_arg=filepath_arg,
            metadata=metadata,
        )

        self._azureml_dataset = azureml_dataset
        self._version = version
        # 1 entry for load version, 1 for save version
        self._version_cache = Cache(maxsize=2)  # type: Cache
        self._download = True
        self._local_run = True
        self._azureml_config = None
        self._azureml_type = azureml_type
        if self._azureml_type not in get_args(AzureMLDataAssetType):
            raise DatasetError(
                f"Invalid azureml_type '{self._azureml_type}' in dataset definition. "
                f"Valid values are: {get_args(AzureMLDataAssetType)}"
            )

        # TODO: remove and disable versioning in Azure ML runner?
        if VERSION_KEY in self._dataset_config:
            raise DatasetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"underlying dataset. Please remove '{VERSIONED_FLAG_KEY}' flag from "
                f"the dataset definition."
            )

    @property
    def azure_config(self) -> AzureMLConfig:
        """AzureML config to be used by the dataset."""
        return self._azureml_config

    @azure_config.setter
    def azure_config(self, azure_config: AzureMLConfig) -> None:
        self._azureml_config = azure_config

    @property
    def path(self) -> str:
        # For local runs we want to replicate the folder structure of the remote dataset.
        # Otherwise kedros versioning would version at the file/folder level and not the
        # AzureML dataset level
        if self._local_run:
            return (
                Path(self.root_dir)
                / self._azureml_dataset
                / self.resolve_load_version()
                / Path(self._dataset_config[self._filepath_arg])
            )
        else:
            return Path(self.root_dir) / Path(self._dataset_config[self._filepath_arg])

    @property
    def download_path(self) -> str:
        # Because `is_dir` and `is_file` don't work if the path does not
        # exist, we use this heuristic to identify paths vs folders.
        if self.path.suffix != "":
            return str(self.path.parent)
        else:
            return str(self.path)

    def _construct_dataset(self) -> AbstractDataset:
        dataset_config = self._dataset_config.copy()
        dataset_config[self._filepath_arg] = str(self.path)
        return self._dataset_type(**dataset_config)

    def _get_latest_version(self) -> str:
        try:
            with _get_azureml_client(
                subscription_id=None, config=self._azureml_config
            ) as ml_client:
                return ml_client.data.get(self._azureml_dataset, label="latest").version
        except ResourceNotFoundError:
            raise DatasetNotFoundError(f"Did not find Azure ML Data Asset for {self}")

    @cachedmethod(cache=attrgetter("_version_cache"), key=partial(hashkey, "load"))
    def _fetch_latest_load_version(self) -> str:
        return self._get_latest_version()

    def _get_azureml_dataset(self):
        with _get_azureml_client(
            subscription_id=None, config=self._azureml_config
        ) as ml_client:
            return ml_client.data.get(
                self._azureml_dataset, version=self.resolve_load_version()
            )

    def _load(self) -> Any:
        if self._download:
            try:
                azureml_ds = self._get_azureml_dataset()
            except ResourceNotFoundError:
                raise VersionNotFoundError(
                    f"Did not find version {self.resolve_load_version()} for {self}"
                )
            fs = AzureMachineLearningFileSystem(azureml_ds.path)
            if azureml_ds.type == "uri_file":
                # relative (to storage account root) path of the file dataset on azure
                # Note that path is converted to str for compatibility reasons with
                # fsspec AbstractFileSystem expand_path function
                path_on_azure = str(fs._infer_storage_options(azureml_ds.path)[1])
            elif azureml_ds.type == "uri_folder":
                # relative (to storage account root) path of the folder dataset on azure
                dataset_root_on_azure = fs._infer_storage_options(azureml_ds.path)[1]
                # relative (to storage account root) path of the dataset in the folder on azure
                path_on_azure = str(
                    Path(dataset_root_on_azure)
                    / self._dataset_config[self._filepath_arg]
                )
            else:
                raise ValueError("Unsupported AzureMLDataset type")
            if fs.isfile(path_on_azure):
                # using APPEND will keep the local file if it already exists
                # as versions are unique this will prevent unnecessary file download
                fs.download(path_on_azure, self.download_path, overwrite="APPEND")
            else:
                # we take the relative within the Azure dataset to avoid downloading
                # all files in a folder dataset.
                for fpath in fs.ls(path_on_azure):
                    logger.info(f"Downloading {fpath} for local execution")
                    fs.download(fpath, self.download_path, overwrite="APPEND")
        return self._construct_dataset().load()

    def _save(self, data: Any) -> None:
        self._construct_dataset().save(data)

    def as_local_intermediate(self):
        self._download = False
        # for local runs we want the data to be saved as a "local version"
        self._version = Version("local", "local")

    def as_remote(self):
        self._version = None
        self._local_run = False
        self._download = False

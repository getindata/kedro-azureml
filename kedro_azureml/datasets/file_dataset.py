import typing as t
from dataclasses import dataclass

from azureml.core import Dataset, Datastore, Workspace
from azureml.data.dataset_factory import FileDatasetFactory
from kedro.io import PartitionedDataSet

from kedro_azureml.datasets.utils import get_workspace


@dataclass
class BlobPath:
    storage_container: str
    blob_path: str

    @classmethod
    def from_abfs_path(cls, path: str):
        """
        Parse blob storage path, assuming path is in the format:
        - abfs://<storage_account_name>.blob.core.windows.net/<storage_container>/<blob_path>
        - abfs://<storage_container>/<blob_path>
        """
        p = path.replace("abfs://", "")
        if ".blob.core.windows.net/" in p:
            p = p.split(".blob.core.windows.net/", 1)[1]
        p = p.split("/")
        return cls(storage_container=p[0], blob_path="/".join(p[1:]))


class AzureMLFileDataSet(PartitionedDataSet):
    """
    AzureML file dataset integration with Kedro, using `kedro.io.PartitionedDataSet` as base class.
    Can be used to save (register) data stored in azure blob storage as an AzureML file dataset.
    The data can then be loaded from the AzureML file dataset into a convenient format (e.g. pandas, pillow image etc).

    Args
    ----

     | - ``azureml_dataset``: Name of the AzureML file dataset.
     | - ``azureml_datastore``: Name of the AzureML datastore. If not provided, the default datastore will be used.
     | - ``azureml_dataset_save_args``: Additional arguments to pass to ``AbstractDataset.register`` method.
            make sure to pass ``create_new_version=True`` to create a new version of an existing dataset.
            note: if there's no difference in file paths, a new version will not be created and the existing version
            will be overwritten, even if ``create_new_version=True``.
            Read more: `AbstractDataset.register`_.
     | - ``azureml_dataset_load_args``: Additional arguments to pass to `azureml.core.Dataset.get_by_name` method.
            Read more: `azureml.core.Dataset.get_by_name`_.
     | - ``workspace``: AzureML Workspace. If not specified, will attempt to load the workspace automatically.
     | - ``workspace_args``: Additional arguments to pass to ``utils.get_workspace()``.
     | - ``kwargs``: Additional arguments to pass to ``PartitionedDataSet`` constructor.
            make sure to not pass `path` argument, as it will be built from ``azureml_datastore`` argument.

    .. _`AbstractDataset.register`: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.data.
        abstract_dataset.abstractdataset?view=azure-ml-py#azureml-data-abstract-dataset-abstractdataset-register
    .. _`azureml.core.Dataset.get_by_name`: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.core.
        dataset.dataset?view=azure-ml-py#azureml-core-dataset-dataset-get-by-name

    Example
    -------

    Example of a catalog.yml entry:

    .. code-block:: yaml

        processed_images:
          type: kedro_azureml.datasets.AzureMLFileDataSet
          dataset: pillow.ImageDataSet
          filename_suffix: '.png'
          azureml_dataset: processed_images
          azureml_dataset_save_args:
            create_new_version: true

          # if version is not provided, the latest dataset version will be used
          azureml_dataset_load_args:
            version: 1

          # optional, if not provided, the environment variable
          # `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCOUNT_KEY` will be used
          credentials:
            account_name: my_storage_account_name
            account_key: my_storage_account_key


    Example of Python API usage:

    .. code-block:: python

        import pandas as pd

        # create dummy data
        dict_df = {}
        dict_df['path/in/azure/blob/storage/file_1'] = pd.DataFrame({'a': [1,2], 'b': [3,4]})
        dict_df['path/in/azure/blob/storage/file_2'] = pd.DataFrame({'c': [3,4], 'd': [5,6]})

        # init AzureMLFileDataSet
        data_set = AzureMLFileDataSet(
            azureml_dataset='my_azureml_file_dataset_name',
            azureml_datastore='my_azureml_datastore_name',  # optional, if not provided, the default datastore will be used  # noqa
            dataset='pandas.CSVDataSet',
            filename_suffix='.csv',  # optional, will add this suffix to the file names (file_1.csv, file_2.csv)

            # optional - if not provided, will use the environment variables
            # AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY
            credentials={
                'account_name': 'my_storage_account_name',
                'account_key': 'my_storage_account_key',
            },

            # create version if the dataset already exists (otherwise, when trying to save, will get an error)
            azureml_dataset_save_args={
                'create_new_version': True,
            }
        )

        # this will create 2 blobs, one for each dataframe, in the following paths:
        # <my_storage_account_name/my_container/path/in/azure/blob/storage/file_1.csv>
        # <my_storage_account_name/my_container/path/in/azure/blob/storage/file_2.csv>
        # also, it will register a corresponding AzureML file-dataset under the name <my_azureml_file_dataset_name> # noqa
        data_set.save(dict_df)

        # this will create lazy load functions instead of loading data into memory immediately.
        loaded = data_set.load()

        # load all the partitions
        for file_path, load_func in loaded.items():
            df = load_func()

            # process pandas dataframe
            # ...
    """

    def __init__(
        self,
        azureml_dataset: str,
        azureml_datastore: t.Optional[str] = None,
        azureml_dataset_save_args: t.Optional[t.Dict[str, t.Any]] = None,
        azureml_dataset_load_args: t.Optional[t.Dict[str, t.Any]] = None,
        workspace: t.Optional[Workspace] = None,
        workspace_args: t.Optional[t.Dict[str, t.Any]] = None,
        **kwargs,
    ):
        """
        AzureML file dataset integration with Kedro, using `kedro.io.PartitionedDataSet` as base class.
        Can be used to save (register) data stored in azure blob storage as an AzureML file dataset.
        The data can then be loaded from the AzureML file dataset into a convenient format (e.g. pandas,
        pillow image etc).

        Args:
            azureml_dataset: Name of the AzureML file dataset.
            azureml_datastore: Name of the AzureML datastore. If not provided, the default datastore will be used.
            azureml_dataset_save_args: Additional arguments to pass to `AbstractDataset.register` method.
                make sure to pass `create_new_version=True` to create a new version of an existing dataset.
                note: if there's no difference in file paths, a new version will not be created and the existing version
                will be overwritten, even if `create_new_version=True`.
                please read more here: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.data.abstract_dataset.abstractdataset?view=azure-ml-py#azureml-data-abstract-dataset-abstractdataset-register # noqa
            azureml_dataset_load_args: Additional arguments to pass to `azureml.core.Dataset.get_by_name` method.
                please read more here: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.core.dataset.dataset?view=azure-ml-py#azureml-core-dataset-dataset-get-by-name # noqa
            workspace: AzureML Workspace. If not specified, will attempt to load the workspace automatically.
            workspace_args: Additional arguments to pass to `utils.get_workspace()`.
            kwargs: Additional arguments to pass to `PartitionedDataSet` constructor.
                make sure to not pass `path` argument, as it will be built from `azureml_datastore` argument.
        """
        # validate that `path` is not part of kwargs, as we are building the `path` from `azureml_datastore` argument.
        if "path" in kwargs:
            raise ValueError(
                f"`path` is not a valid argument for {self.__class__.__name__}"
            )

        self._workspace_args = workspace_args or dict()
        self._workspace = workspace or get_workspace(**self._workspace_args)
        self._azureml_dataset = azureml_dataset
        self._azureml_dataset_save_args = azureml_dataset_save_args or dict()
        self._azureml_dataset_load_args = azureml_dataset_load_args or dict()
        self._azureml_datastore = (
            azureml_datastore or self._workspace.get_default_datastore().name
        )
        ds = Datastore.get(self._workspace, self._azureml_datastore)

        # validate that azureml_datastore exists
        if self._azureml_datastore not in self._workspace.datastores:
            raise ValueError(
                f"Datastore {self._azureml_datastore} not found in workspace {self._workspace.name}"
            )

        # init `PartitionedDataSet` with `path` as the blob storage container (the container of the azureml_datastore)
        path = f"abfs://{ds.container_name}/"
        super().__init__(path, **kwargs)

    def _save(self, data: t.Dict[str, t.Any]) -> None:

        # save to azure blob storage
        super()._save(data)

        # save to azureml file-azureml_dataset
        datastore = Datastore.get(self._workspace, self._azureml_datastore)
        abs_paths = [self._partition_to_path(p) for p in data.keys()]
        paths = [(datastore, BlobPath.from_abfs_path(p).blob_path) for p in abs_paths]
        ds = FileDatasetFactory.from_files(path=paths)
        ds.register(
            workspace=self._workspace,
            name=self._azureml_dataset,
            **self._azureml_dataset_save_args,
        )

    def _list_partitions(self) -> t.List[str]:
        """
        `PartitionedDataSet._load` is using `_list_partitions`, which will list all files in the blob storage.
        however, we want to load the files listed in the azureml file-azureml_dataset, not the ones in the blob
        storage. therefore, override existing `_list_partitions` method to return `partitioned_paths`,
        so that we can utilize the existing `PartitionedDataSet._load` method to load the data.
        """

        dataset = Dataset.get_by_name(
            self._workspace,
            name=self._azureml_dataset,
            **self._azureml_dataset_load_args,
        )
        steps = dataset._dataflow._get_steps()  # noqa
        step_arguments = steps[0].arguments
        azureml_paths = [blob["path"] for blob in step_arguments["datastores"]]

        # parse paths in <container>/<blob_path> format (`PartitionedDataSet` compatible format)
        container = self._workspace.datastores[self._azureml_datastore].container_name
        partitioned_paths = [f"{container}/{p}" for p in azureml_paths]

        return partitioned_paths

import typing as t

import pandas as pd
from azureml.core import Dataset, Datastore, Workspace
from azureml.data.dataset_factory import TabularDatasetFactory
from kedro.io import AbstractDataSet

from kedro_azureml.datasets.utils import get_workspace


class AzureMLPandasDataSet(AbstractDataSet):
    """
    AzureML tabular dataset integration with Pandas DataFrame and kedro.
    Can be used to save Pandas DataFrame to AzureML tabular dataset, and load it back to Pandas DataFrame.

    Args
    ----

     | - ``azureml_dataset``: Name of the AzureML file azureml_dataset.
     | - ``azureml_datastore``: Name of the AzureML azureml_datastore. If not provided, the default azureml_datastore
        will be used.
     | - ``azureml_dataset_save_args``: Additional arguments to pass to
        ``TabularDatasetFactory.register_pandas_dataframe`` method. Read more: `register_pandas_dataframe`_
     | - ``azureml_dataset_load_args``: Additional arguments to pass to ``azureml.core.Dataset.get_by_name`` method.
        Read more: `Dataset.get_by_name`_
     | - ``workspace``: AzureML Workspace. If not specified, will attempt to load the workspace automatically.
     | - ``workspace_args``: Additional arguments to pass to ``utils.get_workspace()``.

    .. _`register_pandas_dataframe`: https://learn.microsoft.com/en-us/python/api/azureml-core/
        azureml.data.dataset_factory.tabulardatasetfactory?view=azure-ml-py#azureml-data-dataset-factory-
        tabulardatasetfactory-register-pandas-dataframe

    .. _`Dataset.get_by_name`: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.core.dataset.dataset?
        view=azure-ml-py#azureml-core-dataset-dataset-get-by-name

    Example
    -------

    Example of a catalog.yml entry:

    .. code-block:: yaml

        my_pandas_dataframe_dataset:
          type: kedro_azureml.datasets.AzureMLPandasDataSet
          azureml_dataset: my_new_azureml_dataset

          # if version is not provided, the latest dataset version will be used
          azureml_dataset_load_args:
            version: 1
    """

    def __init__(
        self,
        azureml_dataset: str,
        azureml_datastore: t.Optional[str] = None,
        azureml_dataset_save_args: t.Optional[t.Dict[str, t.Any]] = None,
        azureml_dataset_load_args: t.Optional[t.Dict[str, t.Any]] = None,
        workspace: t.Optional[Workspace] = None,
        workspace_args: t.Optional[t.Dict[str, t.Any]] = None,
    ):
        """
        AzureML tabular dataset integration with Pandas DataFrame and kedro.
        Can be used to save Pandas DataFrame to AzureML tabular dataset, and load it back to Pandas DataFrame.

        Args:
            azureml_dataset: Name of the AzureML file azureml_dataset.
            azureml_datastore: Name of the AzureML azureml_datastore. If not provided, the default azureml_datastore
                will be used.
            azureml_dataset_save_args: Additional arguments to pass to
                `TabularDatasetFactory.register_pandas_dataframe` method.
                please read more here: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.data.dataset_factory.tabulardatasetfactory?view=azure-ml-py#azureml-data-dataset-factory-tabulardatasetfactory-register-pandas-dataframe # noqa
            azureml_dataset_load_args: Additional arguments to pass to `azureml.core.Dataset.get_by_name` method.
                please read more here: https://learn.microsoft.com/en-us/python/api/azureml-core/azureml.core.dataset.dataset?view=azure-ml-py#azureml-core-dataset-dataset-get-by-name # noqa
            workspace: AzureML Workspace. If not specified, will attempt to load the workspace automatically.
            workspace_args: Additional arguments to pass to `utils.get_workspace()`.
        """
        self._workspace_args = workspace_args or dict()
        self._workspace = workspace or get_workspace(**self._workspace_args)
        self._azureml_dataset = azureml_dataset
        self._azureml_dataset_save_args = azureml_dataset_save_args or dict()
        self._azureml_dataset_load_args = azureml_dataset_load_args or dict()
        self._azureml_datastore = (
            azureml_datastore or self._workspace.get_default_datastore().name
        )

        # validate that azureml_datastore exists
        if self._azureml_datastore not in self._workspace.datastores:
            raise ValueError(
                f"Datastore {self._azureml_datastore} not found in workspace {self._workspace.name}"
            )

    def _load(self) -> pd.DataFrame:
        azureml_dataset = Dataset.get_by_name(
            self._workspace,
            name=self._azureml_dataset,
            **self._azureml_dataset_load_args,
        )
        return azureml_dataset.to_pandas_dataframe()

    def _save(self, data: pd.DataFrame) -> None:
        datastore = Datastore.get(self._workspace, self._azureml_datastore)
        TabularDatasetFactory.register_pandas_dataframe(
            data,
            target=datastore,
            name=self._azureml_dataset,
            **self._azureml_dataset_save_args,
        )

    def _describe(self):
        return dict(dataset=self._azureml_dataset)

    def _exists(self) -> bool:
        return self._azureml_dataset in self._workspace.datasets.keys()

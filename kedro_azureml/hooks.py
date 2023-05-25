from pathlib import Path

from kedro.framework.hooks import hook_impl

from kedro_azureml.datasets.folder_dataset import AzureMLFolderDataSet
from kedro_azureml.runner import AzurePipelinesRunner


class AzureMLLocalRunHook:
    """Hook class that allows local runs using AML datasets."""

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        """Hook implementation to change dataset path for local runs.
        Args:
            run_params: The parameters that are passed to the run command.
            pipeline: The ``Pipeline`` object representing the pipeline to be run.
            catalog: The ``DataCatalog`` from which to fetch data.
        """
        # we don't want the hook to work when we are running on AML
        if run_params["runner"] != "AzurePipelinesRunner":
            for dataset_name, dataset in catalog._data_sets.items():
                # This limits support for azureml:// paths to AzureMLFolderDataSet
                # because others have their filesystem already initalised and the
                # complete configuration of the dataset is potentially unknown here
                # we could support for using Pickle instead.
                if isinstance(dataset, AzureMLFolderDataSet):
                    if dataset_name not in pipeline.inputs():
                        project_path = Path(run_params["project_path"])
                        new_filepath = (
                            project_path
                            / "data"
                            / "local_run"
                            / Path(dataset._filepath).name
                        )
                        dataset.path = str(new_filepath)
                        catalog.add(dataset_name, dataset, replace=True)


azureml_local_run_hook = AzureMLLocalRunHook()

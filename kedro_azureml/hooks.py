from pathlib import Path

from kedro.extras.datasets.pickle import PickleDataSet
from kedro.framework.hooks import hook_impl


class AzureMLLocalRunHook:
    """Hook class that allows local runs using AML datasets.

    This class hooks in before pipeline run and does the following
    given the pipeline that is to be run:
    - change the dataset type to PickleDataSet
      for each dataset that uses the `azureml://` protocol but is not an input
      dataset to AzureMLLocalOutputDataset
    """

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        """Hook implementation to change dataset types to PickleDataSet
        for local runs.

        Args:
            run_params: The parameters that are passed to the run command.
            pipeline: The ``Pipeline`` object representing the pipeline to be run.
            catalog: The ``DataCatalog`` from which to fetch data.
        """
        for dataset_name, dataset in catalog._data_sets.items():
            if hasattr(dataset, "_protocol") and (dataset._protocol == "azureml"):
                if dataset_name not in pipeline.inputs():
                    project_path = Path(run_params["project_path"])
                    new_filepath = (
                        project_path / "data" / "local_run" / dataset._filepath.name
                    )
                    catalog._data_sets[dataset_name] = PickleDataSet(str(new_filepath))


azureml_local_run_hook = AzureMLLocalRunHook()

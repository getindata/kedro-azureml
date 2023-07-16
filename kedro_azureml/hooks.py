from kedro.framework.hooks import hook_impl
from kedro.io.core import Version

from kedro_azureml.config import AzureMLConfig
from kedro_azureml.datasets.asset_dataset import AzureMLAssetDataSet


class AzureMLLocalRunHook:
    """Hook class that allows local runs using AML datasets."""

    @hook_impl
    def after_context_created(self, context) -> None:
        self.azure_config = AzureMLConfig(
            **context.config_loader.get("azureml*")["azure"]
        )

    @hook_impl
    def before_pipeline_run(self, run_params, pipeline, catalog):
        """Hook implementation to change dataset path for local runs.
        Args:
            run_params: The parameters that are passed to the run command.
            pipeline: The ``Pipeline`` object representing the pipeline to be run.
            catalog: The ``DataCatalog`` from which to fetch data.
        """
        # we don't want the hook to work when we are running on AML
        if "AzurePipelinesRunner" not in run_params["runner"]:
            for dataset_name, dataset in catalog._data_sets.items():
                if isinstance(dataset, AzureMLAssetDataSet):
                    if dataset_name in pipeline.inputs():
                        dataset._local_run = True
                        dataset._download = True
                        dataset._azureml_config = self.azure_config
                        catalog.add(dataset_name, dataset, replace=True)

                    # we are adding this so that if an intermediate dataset in one
                    # run becomes the root dataset in another run we don't get problems
                    # with files being folder in the kedro verion way.
                    else:
                        dataset._local_run = True
                        dataset._version = Version("local", "local")
                        catalog.add(dataset_name, dataset, replace=True)


azureml_local_run_hook = AzureMLLocalRunHook()

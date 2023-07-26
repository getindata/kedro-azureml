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
        for dataset_name, dataset in catalog._data_sets.items():
            if isinstance(dataset, AzureMLAssetDataSet):
                if "AzurePipelinesRunner" not in run_params["runner"]:
                    # when running locally using an AzureMLAssetDataSet
                    # as an intermediate dataset we don't want download
                    # but still set to run local with a local version.
                    download = dataset_name in pipeline.inputs()
                    dataset.as_local(self.azure_config, download=download)
                # when running remotely we still want to provide information
                # from the azureml config for getting the dataset version during
                # remote runs
                else:
                    # dataset._azureml_config = self.azure_config
                    dataset._version = None

                catalog.add(dataset_name, dataset, replace=True)


azureml_local_run_hook = AzureMLLocalRunHook()

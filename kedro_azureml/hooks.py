from pathlib import Path, PurePosixPath

from azureml.fsspec import AzureMachineLearningFileSystem
from kedro.framework.hooks import hook_impl

from kedro_azureml.config import AzureMLConfig
from kedro_azureml.client import _get_azureml_client
from kedro_azureml.datasets.folder_dataset import AzureMLFolderDataSet

import logging

logger = logging.getLogger(__name__)

class TooManyFilesError(Exception):
    pass

def get_versioned_path(filepath: PurePosixPath, version: str) -> PurePosixPath:
        if filepath.is_dir():
            return filepath / version / filepath.parts[-1]
        else:
            return filepath / version / filepath.name


class AzureMLLocalRunHook:
    """Hook class that allows local runs using AML datasets."""

    @hook_impl
    def after_context_created(self, context) -> None:
        self.azure_config = AzureMLConfig(**context.config_loader.get("azureml*")['azure'])

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
            with _get_azureml_client(subscription_id=None, config=self.azure_config) as ml_client:
                for dataset_name, dataset in catalog._data_sets.items():
                    if isinstance(dataset, AzureMLFolderDataSet) and (
                        dataset_name in pipeline.inputs()
                    ):
                        version = "latest" if dataset._version is None else str(dataset._version)
                        if version == "latest":
                            azure_ds = ml_client.data.get(
                                dataset._azureml_dataset, label="latest"
                            )
                            # in case of latest there might be files already so
                            # we definitly want to overwrite on download
                            overwrite_mode = "MERGE_WITH_OVERWRITE"
                        else:
                            azure_ds = ml_client.data.get(
                                dataset._azureml_dataset, version=dataset._version
                            )
                            overwrite_mode = "APPEND"
                        if azure_ds.type == "uri_file":
                            # azure_path = azure_ds.path.rsplit("/", 1)[0]
                            # fs = AzureMachineLearningFileSystem(azure_path)
                            raise NotImplementedError(
                                "AzureMLFileDataSet not yet implemented"
                            )
                        else:
                            azure_path = azure_ds.path
                            fs = AzureMachineLearningFileSystem(azure_path)
                        # The datasets filepath is always absolute due to AbstractDataset
                        ds_local_absolute_fpath = Path(dataset._filepath)
                        new_filepath = get_versioned_path(ds_local_absolute_fpath, version)
                        # if the path is a file we'll take the parent directory to download into
                        download_path = new_filepath.parent if ("." in new_filepath.name) else new_filepath
                        # using APPEND will keep the local file if exists
                        # as versions are unique this will prevent unnecessary file download
                        for fpath in fs.ls():
                            logger.info(f"Downloading {fpath} for local execution")
                            fs.download(fpath, str(download_path), overwrite=overwrite_mode)
                        dataset.path = str(new_filepath)
                        catalog.add(dataset_name, dataset, replace=True)

                    # we are adding this so that if an intermediate dataset in one
                    # run becomes the root dataset in another run we don't get problems
                    # with files being folder in the kedro verion way.
                    elif isinstance(dataset, AzureMLFolderDataSet) and (
                        dataset_name not in pipeline.inputs()
                    ):
                        new_filepath = get_versioned_path(Path(dataset._filepath), "local")
                        dataset.path = str(new_filepath)
                        catalog.add(dataset_name, dataset, replace=True)


azureml_local_run_hook = AzureMLLocalRunHook()

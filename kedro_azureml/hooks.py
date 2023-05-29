from pathlib import Path

from azureml.fsspec import AzureMachineLearningFileSystem
from kedro.framework.hooks import hook_impl

from kedro_azureml.client import _get_azureml_client
from kedro_azureml.config import AzureMLBase
from kedro_azureml.datasets.folder_dataset import AzureMLFolderDataSet


class TooManyFilesError(Exception):
    pass


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
            config = AzureMLBase.parse_file(
                Path(run_params["project_path"]) / "conf/base/config.json"
            )
            with _get_azureml_client(subscription_id=None, config=config) as ml_client:
                for dataset_name, dataset in catalog._data_sets.items():
                    if isinstance(dataset, AzureMLFolderDataSet) and (
                        dataset_name in pipeline.inputs()
                    ):
                        azure_ds = ml_client.data.get(
                            dataset._azureml_dataset, label="latest"
                        )
                        if azure_ds.type == "uri_file":
                            # azure_path = azure_ds.path.rsplit("/", 1)[0]
                            # fs = AzureMachineLearningFileSystem(azure_path)
                            raise NotImplementedError(
                                "AzureMLFileDataSet not yet implemented"
                            )
                        else:
                            azure_path = azure_ds.path
                            fs = AzureMachineLearningFileSystem(azure_path)
                        # TODO: See if there is a better way to isolate the right file
                        fpaths = [
                            name for name in fs.ls() if Path(dataset.path).name in name
                        ]
                        if len(fpaths) < 1:
                            raise FileNotFoundError(
                                f"File {Path(dataset.path).name} not found uri_folder dataset"
                            )
                        elif len(fpaths) > 1:
                            raise TooManyFilesError(
                                f"Multiple files with name: {Path(dataset.path).name} found in folder dataset"
                            )
                        else:
                            fpath = fpaths[0]
                        # The datasets filepath is always absolute due to AbstractDataset
                        ds_local_absolute_fpath = Path(dataset._filepath)
                        # TODO: Figure out the best path structure depending on versioning implementation.
                        project_path = Path(run_params["project_path"])
                        new_base_path = project_path / "data" / "local_run"
                        # I thought about adding `data/` to it and make it relative but that assumes
                        # that everyone uses a `data/` directory which might not be the case.
                        ds_local_relative_fpath = ds_local_absolute_fpath.relative_to(
                            project_path
                        )
                        new_filepath = (
                            new_base_path
                            / str(azure_ds.version)
                            / ds_local_relative_fpath
                        )
                        # using APPEND will keep the local file if exists
                        # as versions are unique this will prevent unnecessary file download
                        fs.download(fpath, str(new_filepath.parent), overwrite="APPEND")
                        dataset.path = str(new_filepath)
                        catalog.add(dataset_name, dataset, replace=True)


azureml_local_run_hook = AzureMLLocalRunHook()

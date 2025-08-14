import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd
import pytest
from kedro.io import DataCatalog
from kedro.io.core import Version
from kedro.pipeline import Pipeline, node, pipeline
from kedro_datasets.pandas import CSVDataset, ParquetDataset

from kedro_azureml.config import (
    _CONFIG_TEMPLATE,
    AzureTempStorageConfig,
    KedroAzureMLConfig,
    KedroAzureRunnerConfig,
)
from kedro_azureml.constants import KEDRO_AZURE_RUNNER_CONFIG
from kedro_azureml.datasets import AzureMLAssetDataset, KedroAzureRunnerDataset
from kedro_azureml.runner import AzurePipelinesRunner
from kedro_azureml.utils import CliContext
from tests.utils import identity


@pytest.fixture()
def dummy_pipeline() -> Pipeline:
    return pipeline(
        [
            node(identity, inputs="input_data", outputs="i2", name="node1"),
            node(identity, inputs="i2", outputs="i3", name="node2"),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )


@pytest.fixture()
def dummy_pipeline_compute_tag() -> Pipeline:
    return pipeline(
        [
            node(
                identity,
                inputs="input_data",
                outputs="i2",
                name="node1",
                tags=["compute-2"],
            ),
            node(identity, inputs="i2", outputs="i3", name="node2"),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )


@pytest.fixture()
def dummy_pipeline_deterministic_tag() -> Pipeline:
    return pipeline(
        [
            node(
                identity,
                inputs="input_data",
                outputs="i2",
                name="node1",
                tags=["deterministic"],
            ),
            node(identity, inputs="i2", outputs="i3", name="node2"),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )


@pytest.fixture()
def dummy_plugin_config() -> KedroAzureMLConfig:
    return _CONFIG_TEMPLATE.model_copy(deep=True)


@pytest.fixture()
def patched_kedro_package():
    with patch("kedro.framework.project.PACKAGE_NAME", "tests") as patched_package:
        # original_dir = os.getcwd()
        # os.chdir("tests")
        yield patched_package
        # os.chdir(original_dir)


@pytest.fixture()
def cli_context() -> CliContext:
    metadata = MagicMock()
    metadata.package_name = "tests"
    return CliContext("base", metadata)


@pytest.fixture()
def patched_azure_dataset():
    with TemporaryDirectory() as tmp_dir:
        target_path = Path(tmp_dir) / (uuid4().hex + ".bin")
    with patch.object(
        KedroAzureRunnerDataset,
        "_get_target_path",
        return_value=str(target_path.absolute()),
    ):
        yield KedroAzureRunnerDataset("", "", "", "unit_tests", uuid4().hex)


@pytest.fixture()
def patched_azure_runner(patched_azure_dataset):
    backup = os.environ.copy()
    try:
        cfg = KedroAzureRunnerConfig(
            temporary_storage=AzureTempStorageConfig(
                account_name="unit_test", container="container"
            ),
            run_id=uuid4().hex,
            storage_account_key="",
        )
        os.environ[KEDRO_AZURE_RUNNER_CONFIG] = cfg.model_dump_json()
        yield AzurePipelinesRunner()
    except Exception:
        pass
    os.environ = backup


@pytest.fixture()
def patched_azure_pipeline_data_passing_runner():
    yield AzurePipelinesRunner(pipeline_data_passing=True)


class ExtendedMagicMock(MagicMock):
    def to_dict(self):
        return {
            "subscription_id": self.subscription_id,
            "resource_group": self.resource_group,
            "workspace_name": self.workspace_name,
            "experiment_name": self.experiment_name,
        }


@pytest.fixture
def mock_azureml_config():
    mock_config = ExtendedMagicMock()
    mock_config.subscription_id = "123"
    mock_config.resource_group = "456"
    mock_config.workspace_name = "best"
    mock_config.experiment_name = "test"
    return mock_config


@pytest.fixture
def simulated_azureml_dataset(tmp_path):
    df = pd.DataFrame({"data": [1, 2, 3], "partition_idx": [1, 2, 3]})

    test_data_file = tmp_path / "test_file"
    test_data_file.mkdir(parents=True)
    df.to_pickle(test_data_file / "test.pickle")

    test_data_nested = test_data_file / "random" / "subfolder"
    test_data_nested.mkdir(parents=True)

    df.to_pickle(test_data_nested / "test.pickle")

    test_data_folder_nested_file = (
        tmp_path / "test_folder_nested_file" / "random" / "subfolder"
    )
    test_data_folder_nested_file.mkdir(parents=True)
    df.to_pickle(test_data_folder_nested_file / "test.pickle")

    test_data_folder_root_file = tmp_path / "test_folder_file"
    test_data_folder_root_file.mkdir(parents=True)
    df.to_pickle(test_data_folder_root_file / "test.pickle")

    test_data_folder_root = tmp_path / "test_folder"

    test_data_folder_nested = tmp_path / "test_folder_nested" / "random" / "subfolder"
    test_data_folder_nested.mkdir(parents=True)
    test_data_folder_root.mkdir(parents=True)

    for _, sub_df in df.groupby("partition_idx"):
        filename = test_data_folder_nested / f"partition_{_}.parquet"
        filename2 = test_data_folder_root / f"partition_{_}.parquet"
        sub_df.to_parquet(filename)
        sub_df.to_parquet(filename2)

    return tmp_path


def mock_download_artifact_from_aml_uri_with_dataset(
    uri, destination, datastore_operation, simulated_dataset_path
):
    """Mock function to simulate downloading Azure ML artifacts locally"""
    import shutil

    # Create destination directory if it doesn't exist
    dest_path = Path(destination)
    dest_path.mkdir(parents=True, exist_ok=True)

    # Map Azure ML URIs to local test directories within the simulated dataset
    prefix = "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces/dummy_ws/datastores/some_datastore/paths"
    uri_to_source_map = {
        f"{prefix}/test_file/": "test_file",
        f"{prefix}/test_folder_file/": "test_folder_file",
        f"{prefix}/test_folder_nested_file/": "test_folder_nested_file",
        f"{prefix}/test_folder/": "test_folder",
        f"{prefix}/test_folder_nested/": "test_folder_nested",
    }

    # Find the source directory based on URI
    source_folder = None
    for test_uri, folder_name in uri_to_source_map.items():
        if (
            test_uri in uri
        ):  # Use 'in' instead of 'startswith' to handle both folder and file URIs
            source_folder = simulated_dataset_path / folder_name
            break

    # Copy all files from source folder to destination
    if source_folder and source_folder.exists():
        # Special handling for test_folder_nested_file - copy only from the nested subfolder
        if (
            "test_folder_nested_file" in str(source_folder)
            and (source_folder / "random" / "subfolder").exists()
        ):
            nested_source = source_folder / "random" / "subfolder"
            for item in nested_source.rglob("*"):
                if item.is_file():
                    # Copy directly to destination without preserving nested structure
                    dest_file = dest_path / item.name
                    shutil.copy2(item, dest_file)
        else:
            # Normal copy preserving relative structure
            for item in source_folder.rglob("*"):
                if item.is_file():
                    relative_path = item.relative_to(source_folder)
                    dest_file = dest_path / relative_path
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(item, dest_file)


@pytest.fixture
def mock_azureml_fs(simulated_azureml_dataset):
    def mock_with_dataset(uri, destination, datastore_operation):
        return mock_download_artifact_from_aml_uri_with_dataset(
            uri, destination, datastore_operation, simulated_azureml_dataset
        )

    with patch(
        "kedro_azureml.datasets.asset_dataset.artifact_utils.download_artifact_from_aml_uri",
        side_effect=mock_with_dataset,
    ):
        yield


@pytest.fixture
def mock_azureml_client(request):
    mock_data_asset = MagicMock()
    mock_data_asset.version = "1"
    mock_data_asset.path = request.param["path"]
    mock_data_asset.type = request.param["type"]

    with patch(
        "kedro_azureml.datasets.asset_dataset._get_azureml_client"
    ) as mock_get_client:
        mock_client = MagicMock()
        mock_client.data.get.return_value = mock_data_asset

        mock_context_manager = MagicMock()
        mock_context_manager.__enter__.return_value = mock_client
        mock_context_manager.__exit__.return_value = None

        mock_get_client.return_value = mock_context_manager

        yield mock_get_client


@pytest.fixture
def in_temp_dir(tmp_path):
    original_cwd = os.getcwd()

    os.chdir(tmp_path)

    yield

    os.chdir(original_cwd)


@pytest.fixture
def multi_catalog():
    csv = AzureMLAssetDataset(
        dataset={
            "type": CSVDataset,
            "filepath": "abc.csv",
        },
        azureml_dataset="test_dataset",
        version=Version(None, None),
    )
    parq = AzureMLAssetDataset(
        dataset={
            "type": ParquetDataset,
            "filepath": "xyz.parq",
        },
        azureml_dataset="test_dataset_2",
        version=Version(None, None),
    )
    return DataCatalog({"input_data": csv, "i2": parq})

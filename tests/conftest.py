import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch
from uuid import uuid4

import fsspec
import pandas as pd
import pytest
from azureml.fsspec import AzureMachineLearningFileSystem
from kedro.extras.datasets.pandas import CSVDataSet, ParquetDataSet
from kedro.io import DataCatalog
from kedro.io.core import Version
from kedro.pipeline import Pipeline, node, pipeline

from kedro_azureml.config import (
    _CONFIG_TEMPLATE,
    AzureTempStorageConfig,
    KedroAzureMLConfig,
    KedroAzureRunnerConfig,
)
from kedro_azureml.constants import KEDRO_AZURE_RUNNER_CONFIG
from kedro_azureml.datasets import AzureMLAssetDataSet, KedroAzureRunnerDataset
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
    return _CONFIG_TEMPLATE.copy(deep=True)


@pytest.fixture()
def patched_kedro_package():
    with patch("kedro.framework.project.PACKAGE_NAME", "tests") as patched_package:
        original_dir = os.getcwd()
        os.chdir("tests")
        yield patched_package
        os.chdir(original_dir)


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
        os.environ[KEDRO_AZURE_RUNNER_CONFIG] = cfg.json()
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


class AzureMLFileSystemMock(fsspec.implementations.local.LocalFileSystem):
    _prefix = Path(".")

    def __init__(self, uri):
        super().__init__()

    def _infer_storage_options(self, uri):
        path_on_azure = Path(
            AzureMachineLearningFileSystem._infer_storage_options(uri)[-1]
        )
        if path_on_azure.suffix != "":
            path_on_azure = str(path_on_azure.parent)
        else:
            path_on_azure = str(path_on_azure)
        return [self._prefix / path_on_azure]

    def download(self, *args, **kwargs):
        p = Path(args[1])
        p.mkdir(parents=True, exist_ok=True)
        super().download(args[0], args[1], *args[2:], **kwargs)


@pytest.fixture
def mock_azureml_fs(simulated_azureml_dataset):
    with patch(
        "kedro_azureml.datasets.asset_dataset.AzureMachineLearningFileSystem",
        new=AzureMLFileSystemMock,
    ):
        with patch.object(
            AzureMLFileSystemMock, "_prefix", new=simulated_azureml_dataset
        ):
            yield mock_azureml_fs


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
    csv = AzureMLAssetDataSet(
        dataset={
            "type": CSVDataSet,
            "filepath": "abc.csv",
        },
        azureml_dataset="test_dataset",
        version=Version(None, None),
    )
    parq = AzureMLAssetDataSet(
        dataset={
            "type": ParquetDataSet,
            "filepath": "xyz.parq",
        },
        azureml_dataset="test_dataset_2",
        version=Version(None, None),
    )
    return DataCatalog({"input_data": csv, "i2": parq})

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from kedro.pipeline import Pipeline, node, pipeline

from kedro_azureml.config import (
    _CONFIG_TEMPLATE,
    AzureTempStorageConfig,
    KedroAzureMLConfig,
    KedroAzureRunnerConfig,
)
from kedro_azureml.constants import KEDRO_AZURE_RUNNER_CONFIG
from kedro_azureml.datasets import KedroAzureRunnerDataset
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

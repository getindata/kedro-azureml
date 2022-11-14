import os
from pathlib import Path
from unittest import mock
from unittest.mock import patch
from uuid import uuid4

import pytest
import yaml
from click.testing import CliRunner
from kedro.framework.startup import ProjectMetadata

from kedro_azureml import cli
from kedro_azureml.config import KedroAzureMLConfig
from kedro_azureml.constants import KEDRO_AZURE_RUNNER_DATASET_TIMEOUT
from kedro_azureml.generator import AzureMLPipelineGenerator
from tests.utils import create_kedro_conf_dirs


def test_can_initialize_basic_plugin_config(
    patched_kedro_package,
    cli_context,
    tmp_path: Path,
):

    config_path = create_kedro_conf_dirs(tmp_path)
    unique_id = uuid4().hex
    with patch.object(Path, "cwd", return_value=tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli.init,
            [
                f"resource_group_{unique_id}",
                f"workspace_name_{unique_id}",
                f"experiment_name_{unique_id}",
                f"cluster_name_{unique_id}",
                f"storage_account_name_{unique_id}",
                f"storage_container_{unique_id}",
                f"environment_name_{unique_id}",
            ],
            obj=cli_context,
        )
        assert result.exit_code == 0

        azureml_config_path = config_path / "azureml.yml"
        assert (
            azureml_config_path.exists() and azureml_config_path.is_file()
        ), f"{azureml_config_path.absolute()} is not a valid file"

        config: KedroAzureMLConfig = KedroAzureMLConfig.parse_obj(
            yaml.safe_load(azureml_config_path.read_text())
        )
        assert config.azure.resource_group == f"resource_group_{unique_id}"
        assert config.azure.workspace_name == f"workspace_name_{unique_id}"
        assert config.azure.experiment_name == f"experiment_name_{unique_id}"
        assert (
            config.azure.compute["__default__"].cluster_name
            == f"cluster_name_{unique_id}"
        )
        assert (
            config.azure.temporary_storage.account_name
            == f"storage_account_name_{unique_id}"
        )
        assert (
            config.azure.temporary_storage.container == f"storage_container_{unique_id}"
        )
        assert config.azure.environment_name == f"environment_name_{unique_id}"


@pytest.mark.parametrize(
    "extra_params",
    ("", '{"unit_test_param": 666.0}'),
    ids=("without params", "with extra params"),
)
@pytest.mark.parametrize(
    "storage_account_key",
    ("", "dummy"),
    ids=("without storage key env", "with storage key env set"),
)
def test_can_compile_pipeline(
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    dummy_plugin_config,
    tmp_path: Path,
    extra_params,
    storage_account_key,
):
    with patch.object(
        AzureMLPipelineGenerator, "get_kedro_pipeline", return_value=dummy_pipeline
    ), patch(
        "kedro_azureml.utils.KedroContextManager.plugin_config",
        new_callable=mock.PropertyMock,
        return_value=dummy_plugin_config,
    ), patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": storage_account_key}
    ), patch(
        "click.prompt", return_value="dummy"
    ) as click_prompt:
        runner = CliRunner()
        output_path = tmp_path / "pipeline.yml"
        result = runner.invoke(
            cli.compile,
            ["--output", str(output_path.absolute()), "--params", extra_params],
            obj=cli_context,
        )
        assert result.exit_code == 0
        assert isinstance(p := yaml.safe_load(output_path.read_text()), dict) and all(
            k in p for k in ("display_name", "type", "jobs")
        )

        if not storage_account_key:
            click_prompt.assert_called()


@pytest.mark.parametrize(
    "distributed_env_variables,should_create_output",
    [
        ({"RANK": "0"}, True),
        ({"RANK": "2", KEDRO_AZURE_RUNNER_DATASET_TIMEOUT: "1"}, False),
    ],
    ids=("master node", "worker node"),
)
def test_can_invoke_execute_cli(
    distributed_env_variables,
    should_create_output,
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    dummy_plugin_config,
    patched_azure_runner,
    tmp_path: Path,
):
    create_kedro_conf_dirs(tmp_path)
    with patch(
        "kedro_azureml.runner.AzurePipelinesRunner", new=patched_azure_runner
    ), patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch.object(
        Path, "cwd", return_value=tmp_path
    ), patch.dict(
        os.environ, distributed_env_variables, clear=False
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli.execute,
            ["--node", "node1", "--az-output", str(tmp_path)],
            obj=cli_context,
        )
        assert result.exit_code == 0
        p = tmp_path / "output.txt"
        if should_create_output:
            assert (
                p.exists() and p.stat().st_size > 0
            ), "Output placeholders were not created"
        else:
            assert not p.exists(), "Output placeholders should not have been created"


@pytest.mark.parametrize(
    "wait_for_completion", (False, True), ids=("no wait", "wait for completion")
)
@pytest.mark.parametrize(
    "aml_env",
    ("", "unit_test_aml_env@latest"),
    ids=("aml_env default", "aml_env overridden"),
)
@pytest.mark.parametrize(
    "use_default_credentials",
    (False, True),
    ids=("interactive credentials", "default_credentials"),
)
def test_can_invoke_run(
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    tmp_path: Path,
    wait_for_completion: bool,
    aml_env: str,
    use_default_credentials: bool,
):
    create_kedro_conf_dirs(tmp_path)
    with patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch.object(Path, "cwd", return_value=tmp_path), patch(
        "kedro_azureml.client.MLClient"
    ) as ml_client_patched, patch(
        "kedro_azureml.client.DefaultAzureCredential"
    ) as default_credentials, patch(
        "kedro_azureml.client.InteractiveBrowserCredential"
    ) as interactive_credentials, patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "dummy_key"}
    ):
        if not use_default_credentials:
            default_credentials.side_effect = ValueError()

        runner = CliRunner()
        result = runner.invoke(
            cli.run,
            ["-s", "subscription_id"]
            + (["--wait-for-completion"] if wait_for_completion else [])
            + (["--aml_env", aml_env] if aml_env else []),
            obj=cli_context,
        )
        assert result.exit_code == 0
        ml_client_patched.from_config.assert_called_once()
        ml_client = ml_client_patched.from_config()
        ml_client.jobs.create_or_update.assert_called_once()
        ml_client.compute.get.assert_called_once()

        if wait_for_completion:
            ml_client.jobs.stream.assert_called_once()

        default_credentials.assert_called_once()

        if not use_default_credentials:
            interactive_credentials.assert_called_once()
        else:
            interactive_credentials.assert_not_called()


def test_can_invoke_run_with_failed_pipeline(
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    tmp_path: Path,
):
    create_kedro_conf_dirs(tmp_path)
    with patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch.object(Path, "cwd", return_value=tmp_path), patch(
        "kedro_azureml.client.MLClient"
    ) as ml_client_patched, patch(
        "kedro_azureml.client.DefaultAzureCredential"
    ), patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "dummy_key"}
    ):
        ml_client = ml_client_patched.from_config()
        ml_client.jobs.stream.side_effect = ValueError()

        runner = CliRunner()
        result = runner.invoke(
            cli.commands,
            [
                "azureml",
                "-e",
                "base",
                "run",
                "-s",
                "subscription_id",
                "--wait-for-completion",
            ],
            obj=ProjectMetadata(
                tmp_path, "tests", "project", tmp_path, "1.0", Path.cwd()
            ),
        )
        assert result.exit_code == 1
        ml_client.jobs.create_or_update.assert_called_once()
        ml_client.compute.get.assert_called_once()
        ml_client.jobs.stream.assert_called_once()

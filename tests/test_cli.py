import os
from pathlib import Path
from typing import List
from unittest import mock
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
import yaml
from click.testing import CliRunner
from kedro.framework.startup import ProjectMetadata

from kedro_azureml import cli
from kedro_azureml.config import KedroAzureMLConfig
from kedro_azureml.constants import KEDRO_AZURE_RUNNER_DATASET_TIMEOUT
from kedro_azureml.generator import AzureMLPipelineGenerator
from kedro_azureml.utils import CliContext
from tests.utils import create_kedro_conf_dirs


@pytest.mark.parametrize(
    "env_or_docker",
    [
        ["--docker-image", "my_docker/image:latest"],
        ["--aml-env", f"{uuid4().hex}@latest"],
        ["--azureml-environment", f"{uuid4().hex}:v1"],
        ["--docker-image", "a", "--aml-env", "b"],
        [],
    ],
    ids=("with docker", "with AML env", "with AML (long param name)", "both", "none"),
)
@pytest.mark.parametrize("use_pipeline_data_passing", (True, False))
def test_can_initialize_basic_plugin_config(
    patched_kedro_package,
    cli_context,
    tmp_path: Path,
    env_or_docker: List[str],
    use_pipeline_data_passing: bool,
):
    config_path = create_kedro_conf_dirs(tmp_path)
    unique_id = uuid4().hex
    with patch.object(Path, "cwd", return_value=tmp_path):
        runner = CliRunner()
        storage_args = (
            ["--use-pipeline-data-passing"]
            if use_pipeline_data_passing
            else [
                "-a",
                f"storage_account_name_{unique_id}",
                "-c",
                f"storage_container_{unique_id}",
            ]
        )

        result = runner.invoke(
            cli.init,
            [
                f"subscription_id_{unique_id}",
                f"resource_group_{unique_id}",
                f"workspace_name_{unique_id}",
                f"experiment_name_{unique_id}",
                f"cluster_name_{unique_id}",
            ]
            + storage_args
            + env_or_docker,
            obj=cli_context,
        )

        if "--aml-env" in env_or_docker and "--docker-image" in env_or_docker:
            assert result.exit_code == 2
            assert "You cannot specify both" in result.output
            return
        elif len(env_or_docker) == 0:
            assert result.exit_code == 2
            assert "You must specify either" in result.output
            return

        assert result.exit_code == 0, result.exception

        azureml_config_path = config_path / "azureml.yml"
        assert (
            azureml_config_path.exists() and azureml_config_path.is_file()
        ), f"{azureml_config_path.absolute()} is not a valid file"

        config: KedroAzureMLConfig = KedroAzureMLConfig.parse_obj(
            yaml.safe_load(azureml_config_path.read_text())
        )
        assert config.azure.subscription_id == f"subscription_id_{unique_id}"
        assert config.azure.resource_group == f"resource_group_{unique_id}"
        assert config.azure.workspace_name == f"workspace_name_{unique_id}"
        assert config.azure.experiment_name == f"experiment_name_{unique_id}"
        assert (
            config.azure.compute["__default__"].cluster_name
            == f"cluster_name_{unique_id}"
        )
        if use_pipeline_data_passing:
            assert (
                config.azure.pipeline_data_passing.enabled
            ), "Data passing should be enabled"
            assert not (
                config.azure.temporary_storage.account_name
                or config.azure.temporary_storage.container
            )
        else:
            assert (
                config.azure.temporary_storage.account_name
                == f"storage_account_name_{unique_id}"
            )
            assert (
                config.azure.temporary_storage.container
                == f"storage_container_{unique_id}"
            )

        if "--aml-env" in env_or_docker or "--azureml-environment" in env_or_docker:
            assert config.azure.environment_name == env_or_docker[1]
            assert config.docker.image is None
        else:
            assert config.azure.environment_name is None
            assert config.docker.image == env_or_docker[1]


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
        "kedro_azureml.manager.KedroContextManager.plugin_config",
        new_callable=mock.PropertyMock,
        return_value=dummy_plugin_config,
    ), patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": storage_account_key}
    ), patch.object(
        Path, "cwd", return_value=tmp_path
    ), patch(
        "click.prompt", return_value="dummy"
    ) as click_prompt:
        _ = create_kedro_conf_dirs(tmp_path)
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
@pytest.mark.parametrize(
    "pipeline_data_passing_enabled",
    (False, True),
    ids=("temporary storage", "pipeline data passing"),
)
def test_can_invoke_execute_cli(
    distributed_env_variables,
    should_create_output,
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    dummy_plugin_config,
    pipeline_data_passing_enabled,
    request,
    tmp_path: Path,
):
    if pipeline_data_passing_enabled:
        patched_azure_runner = request.getfixturevalue(
            "patched_azure_pipeline_data_passing_runner"
        )
        dummy_plugin_config.azure.pipeline_data_passing.enabled = True
        output_path = tmp_path / "i2.pickle"
    else:
        patched_azure_runner = request.getfixturevalue("patched_azure_runner")
        output_path = tmp_path / "output.txt"
    create_kedro_conf_dirs(tmp_path)
    with patch(
        "kedro_azureml.runner.AzurePipelinesRunner", new=patched_azure_runner
    ), patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch(
        "kedro_azureml.manager.KedroContextManager.plugin_config",
        new_callable=mock.PropertyMock,
        return_value=dummy_plugin_config,
    ), patch.object(
        Path, "cwd", return_value=tmp_path
    ), patch.dict(
        os.environ, distributed_env_variables, clear=False
    ):
        runner = CliRunner()
        result = runner.invoke(
            cli.execute,
            ["--node", "node1", "--az-output", "i2", str(tmp_path)],
            obj=cli_context,
        )
        assert result.exit_code == 0

        if should_create_output:
            assert (
                output_path.exists() and output_path.stat().st_size > 0
            ), "Output placeholders were not created"
        else:
            assert (
                not output_path.exists()
            ), "Output placeholders/datasets should not have been created"


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
@pytest.mark.parametrize("amlignore", ("empty", "missing", "filled"))
@pytest.mark.parametrize("gitignore", ("empty", "missing", "filled"))
@pytest.mark.parametrize(
    "extra_env",
    (
        ([], {}),
        (["A=B", "C="], {"A": "B", "C": ""}),
        (["A=CDE=F123"], {"A": "CDE=F123"}),
    ),
)
def test_can_invoke_run(
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    tmp_path: Path,
    wait_for_completion: bool,
    aml_env: str,
    use_default_credentials: bool,
    amlignore: str,
    gitignore: str,
    extra_env: list,
):
    create_kedro_conf_dirs(tmp_path)
    with patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch.object(Path, "cwd", return_value=tmp_path), patch(
        "kedro_azureml.client.MLClient"
    ) as ml_client_patched, patch(
        "kedro_azureml.auth.utils.DefaultAzureCredential"
    ) as default_credentials, patch(
        "kedro_azureml.auth.utils.InteractiveBrowserCredential"
    ) as interactive_credentials, patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "dummy_key"}
    ):
        if not use_default_credentials:
            default_credentials.side_effect = ValueError()

        if amlignore != "missing":
            Path.cwd().joinpath(".amlignore").write_text(
                "" if amlignore == "empty" else "unittest"
            )

        if gitignore != "missing":
            Path.cwd().joinpath(".gitignore").write_text(
                "" if gitignore == "empty" else "unittest"
            )

        runner = CliRunner()
        result = runner.invoke(
            cli.run,
            ["-s", "subscription_id"]
            + (["--wait-for-completion"] if wait_for_completion else [])
            + (["--aml-env", aml_env] if aml_env else [])
            + (sum([["--env-var", k] for k in extra_env[0]], [])),
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

        created_pipeline = ml_client.jobs.create_or_update.call_args[0][0]
        populated_env_vars = list(created_pipeline.jobs.values())[
            0
        ].environment_variables
        del populated_env_vars["KEDRO_AZURE_RUNNER_CONFIG"]
        assert populated_env_vars == extra_env[1]


@pytest.mark.parametrize(
    "kedro_environment_name",
    ("empty", "non_existing", "gitkeep", "nested"),
)
@pytest.mark.parametrize("confirm", (True, False))
def test_run_is_interrupted_if_used_on_empty_env(
    confirm,
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    tmp_path: Path,
    kedro_environment_name: str,
):
    metadata = MagicMock()
    metadata.package_name = "tests"
    cli_context = CliContext(env=kedro_environment_name, metadata=metadata)

    create_kedro_conf_dirs(tmp_path)  # for base env

    # setup Kedro env to handle test case
    cfg_path = tmp_path / "conf" / kedro_environment_name
    if kedro_environment_name == "empty":
        cfg_path.mkdir(parents=True)
    elif kedro_environment_name == "gitkeep":
        cfg_path.mkdir(parents=True)
        (cfg_path / ".gitkeep").touch()
    elif kedro_environment_name == "nested":
        (cfg_path / "nested2").mkdir(parents=True)
    else:
        pass  # nothing to do for non_existing environment, do not remove this empty block

    with patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch.object(Path, "cwd", return_value=tmp_path), patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "dummy_key"}
    ), patch(
        "kedro_azureml.auth.utils.DefaultAzureCredential"
    ), patch(
        "click.confirm", return_value=confirm
    ) as click_confirm:
        runner = CliRunner()
        result = runner.invoke(cli.run, ["-s", "subscription_id"], obj=cli_context)
        assert result.exit_code == (
            1 if confirm else 2
        ), "run should have exited with code: 1 if confirmed, 2 if stopped"
        click_confirm.assert_called_once()


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
        "kedro_azureml.auth.utils.DefaultAzureCredential"
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
                tmp_path, "tests", "project", tmp_path, "1.0", Path.cwd(), "0.18.5"
            ),
        )
        assert result.exit_code == 1
        ml_client.jobs.create_or_update.assert_called_once()
        ml_client.compute.get.assert_called_once()
        ml_client.jobs.stream.assert_called_once()


@pytest.mark.parametrize("env_var", ("INVALID", "2+2=4"))
def test_fail_if_invalid_env_provided_in_run(
    patched_kedro_package,
    cli_context,
    dummy_pipeline,
    tmp_path: Path,
    env_var: str,
):
    create_kedro_conf_dirs(tmp_path)
    with patch.dict(
        "kedro.framework.project.pipelines", {"__default__": dummy_pipeline}
    ), patch.object(Path, "cwd", return_value=tmp_path), patch(
        "kedro_azureml.client.MLClient"
    ) as ml_client_patched, patch(
        "kedro_azureml.auth.utils.DefaultAzureCredential"
    ), patch.dict(
        os.environ, {"AZURE_STORAGE_ACCOUNT_KEY": "dummy_key"}
    ):
        ml_client = ml_client_patched.from_config()
        ml_client.jobs.stream.side_effect = ValueError()

        runner = CliRunner()
        result = runner.invoke(cli.run, ["--env-var", env_var], obj=cli_context)
        assert result.exit_code == 1
        assert (
            str(result.exception)
            == f"Invalid env-var: {env_var}, expected format: KEY=VALUE"
        )

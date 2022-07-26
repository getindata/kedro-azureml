from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock
from uuid import uuid4

import pytest
import yaml
from click.testing import CliRunner
from kedro_azureml.cli import init
from kedro_azureml.config import KedroAzureMLConfig
from kedro_azureml.constants import FILL_IN_DOCKER_IMAGE
from kedro_azureml.utils import CliContext
from pathlib import Path


@pytest.mark.parametrize("with_acr", (True, False), ids=("with ACR", "without ACR"))
def test_can_initialize_empty_plugin_config(
    patched_kedro_package, tmp_path: Path, with_acr: bool
):
    metadata = MagicMock()
    metadata.package_name = "tests"
    config_path = tmp_path / "conf" / "base"
    config_path.mkdir(parents=True)
    unique_id = uuid4().hex
    with patch.object(Path, "cwd", return_value=tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                f"resource_group_{unique_id}",
                f"workspace_name_{unique_id}",
                f"experiment_name_{unique_id}",
                f"cluster_name_{unique_id}",
                f"storage_account_name_{unique_id}",
                f"storage_container_{unique_id}",
            ]
            + ([f"--acr", f"unit_test_acr_{unique_id}"] if with_acr else []),
            obj=CliContext("base", metadata),
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
        assert config.azure.cluster_name == f"cluster_name_{unique_id}"
        assert (
            config.azure.temporary_storage.account_name
            == f"storage_account_name_{unique_id}"
        )
        assert (
            config.azure.temporary_storage.container == f"storage_container_{unique_id}"
        )
        if with_acr:
            assert (
                config.docker.image.startswith(f"unit_test_acr_{unique_id}")
                and "azurecr.io" in config.docker.image
                and config.docker.image.endswith(":latest")
            )
        else:
            assert config.docker.image == FILL_IN_DOCKER_IMAGE

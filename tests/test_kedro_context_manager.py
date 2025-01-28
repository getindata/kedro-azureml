from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kedro.config import OmegaConfigLoader
from kedro.framework.context import KedroContext

from kedro_azureml.config import KedroAzureMLConfig
from kedro_azureml.manager import KedroContextManager


def test_can_create_context_manager(patched_kedro_package):
    with KedroContextManager(project_path="tests", env="base") as mgr:
        assert mgr is not None and isinstance(
            mgr, KedroContextManager
        ), "Invalid object returned"
        assert isinstance(mgr.context, KedroContext), "No KedroContext"
        assert isinstance(
            mgr.plugin_config, KedroAzureMLConfig
        ), "Invalid plugin config"


def test_can_create_context_manager_with_omegaconf(patched_kedro_package):
    with KedroContextManager(project_path="tests", env="local") as mgr:
        with patch.object(mgr, "context") as context:
            context.mock_add_spec(KedroContext)
            context.config_loader = OmegaConfigLoader(
                str(Path.cwd() / "tests" / "conf"),
                config_patterns={"azureml": ["azureml*"]},
                default_run_env="local",
            )
            assert isinstance(mgr.context, KedroContext), "No KedroContext"
            assert isinstance(
                mgr.plugin_config, KedroAzureMLConfig
            ), "Invalid plugin config"


@pytest.mark.parametrize("as_custom_config_loader", (True, False))
def test_context_manager_with_missing_config(
    patched_kedro_package, as_custom_config_loader
):
    with KedroContextManager(project_path="tests", env="local") as mgr:
        with patch.object(mgr, "context") as context:
            context.mock_add_spec(KedroContext)
            context.config_loader = (cl := MagicMock())
            if not as_custom_config_loader:
                cl.mock_add_spec(OmegaConfigLoader)
            cl.get = lambda *_: None
            cl.__getitem__ = lambda *_: None
            with pytest.raises(
                ValueError,
                match=(
                    "You're using a custom config loader.*"
                    if as_custom_config_loader
                    else "Missing azureml.yml files.*"
                ),
            ):
                _ = mgr.plugin_config

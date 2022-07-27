from unittest.mock import patch

from kedro.framework.context import KedroContext

from kedro_azureml.config import KedroAzureMLConfig
from kedro_azureml.utils import KedroContextManager


def test_can_create_context_manager(patched_kedro_package):
    with KedroContextManager("tests", "base") as mgr:
        assert mgr is not None and isinstance(
            mgr, KedroContextManager
        ), "Invalid object returned"
        assert isinstance(mgr.context, KedroContext), "No KedroContext"
        assert isinstance(
            mgr.plugin_config, KedroAzureMLConfig
        ), "Invalid plugin config"

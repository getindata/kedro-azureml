from unittest.mock import MagicMock, Mock

import pytest
from kedro.io.core import Version
from kedro.runner import SequentialRunner

from kedro_azureml.datasets.asset_dataset import AzureMLAssetDataSet
from kedro_azureml.hooks import azureml_local_run_hook
from kedro_azureml.runner import AzurePipelinesRunner


@pytest.mark.parametrize(
    "config_patterns", [({"azureml": ["azureml*", "azureml*/**", "**/azureml*"]}), ({})]
)
@pytest.mark.parametrize(
    "runner",
    [(AzurePipelinesRunner.__name__,), (SequentialRunner.__name__)],
)
def test_hook_after_context_created(
    mock_azureml_config, dummy_pipeline, multi_catalog, runner, config_patterns
):
    context_mock = Mock(
        config_loader=MagicMock(
            __getitem__=Mock(return_value={"azure": mock_azureml_config.to_dict()})
        )
    )
    context_mock.config_loader.config_patterns.keys.return_value = (
        config_patterns.keys()
    )

    azureml_local_run_hook.after_context_created(context_mock)
    assert azureml_local_run_hook.azure_config.subscription_id == "123"
    assert azureml_local_run_hook.azure_config.workspace_name == "best"

    azureml_local_run_hook.after_catalog_created(multi_catalog)
    for dataset in multi_catalog._data_sets.values():
        if isinstance(dataset, AzureMLAssetDataSet):
            assert dataset._download is True
            assert dataset._local_run is True
            assert dataset._azureml_config is not None

    run_params = {"runner": runner}

    azureml_local_run_hook.before_pipeline_run(
        run_params, dummy_pipeline, multi_catalog
    )
    if runner == SequentialRunner.__name__:
        assert multi_catalog.datasets.input_data._download is True
        assert multi_catalog.datasets.input_data._local_run is True
        assert (
            multi_catalog.datasets.input_data._azureml_config
            == azureml_local_run_hook.azure_config
        )
        assert multi_catalog.datasets.i2._download is True
        assert multi_catalog.datasets.i2._local_run is True
        assert multi_catalog.datasets.i2._version == Version("local", "local")
    else:
        assert multi_catalog.datasets.input_data._download is False
        assert multi_catalog.datasets.input_data._local_run is False
        assert multi_catalog.datasets.input_data._azureml_config is not None
        assert multi_catalog.datasets.i2._download is False
        assert multi_catalog.datasets.i2._local_run is False
        assert multi_catalog.datasets.i2._version is None

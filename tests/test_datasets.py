from pathlib import Path
from typing import Type
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from kedro.io.core import VERSIONED_FLAG_KEY, DataSetError, Version
from kedro_datasets.pandas import ParquetDataSet
from kedro_datasets.pickle import PickleDataSet

from kedro_azureml.constants import KEDRO_AZURE_BLOB_TEMP_DIR_NAME
from kedro_azureml.datasets import (
    AzureMLAssetDataSet,
    AzureMLPipelineDataSet,
    KedroAzureRunnerDataset,
    KedroAzureRunnerDistributedDataset,
)


@pytest.mark.parametrize(
    "dataset_class", (KedroAzureRunnerDataset, KedroAzureRunnerDistributedDataset)
)
def test_azure_dataset_config(dataset_class: Type):
    run_id = uuid4().hex
    ds = dataset_class(
        "storage_acc", "test_container", "key123", "unit_tests_dataset", run_id
    )
    target_path = ds._get_target_path()
    cfg = ds._get_storage_options()
    assert (
        target_path.startswith("abfs://")
        and target_path.endswith(".bin")
        and all(
            part in target_path
            for part in (
                "test_container",
                "unit_tests_dataset",
                KEDRO_AZURE_BLOB_TEMP_DIR_NAME,
                run_id,
            )
        )
    ), "Invalid target path"

    assert all(k in cfg for k in ("account_name", "account_key")), "Invalid ABFS config"


@pytest.mark.parametrize(
    "dataset_type,path_in_aml,path_locally,download_path,local_run,download,mock_azureml_client",
    [
        (
            PickleDataSet,
            "test.pickle",
            "data/test.pickle",
            "data",
            False,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_file/test.pickle"
                ),
                "type": "uri_file",
            },
        ),
        (
            PickleDataSet,
            "test.pickle",
            "data/test_dataset/1/test.pickle",
            "data/test_dataset/1",
            True,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_file/test.pickle"
                ),
                "type": "uri_file",
            },
        ),
        (
            PickleDataSet,
            "test.pickle",
            "data/test_dataset/1/test.pickle",
            "data/test_dataset/1",
            True,
            True,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_file/test.pickle"
                ),
                "type": "uri_file",
            },
        ),
        (
            PickleDataSet,
            "random/subfolder/test.pickle",
            "data/random/subfolder/test.pickle",
            "data/random/subfolder",
            False,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/random/subfolder/test.pickle"
                ),
                "type": "uri_file",
            },
        ),
        (
            PickleDataSet,
            "random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder",
            True,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/random/subfolder/test.pickle"
                ),
                "type": "uri_file",
            },
        ),
        (
            PickleDataSet,
            "random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder",
            True,
            True,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_file/random/subfolder/test.pickle"
                ),
                "type": "uri_file",
            },
        ),
        (
            ParquetDataSet,
            ".",
            "data/",
            "data",
            False,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder/"
                ),
                "type": "uri_file",
            },
        ),
        (
            ParquetDataSet,
            ".",
            "data/test_dataset/1/",
            "data/test_dataset/1",
            True,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            ParquetDataSet,
            ".",
            "data/test_dataset/1/",
            "data/test_dataset/1",
            True,
            True,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            ParquetDataSet,
            "random/subfolder/",
            "data/random/subfolder/",
            "data/random/subfolder",
            False,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_nested/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            ParquetDataSet,
            "random/subfolder/",
            "data/test_dataset/1/random/subfolder/",
            "data/test_dataset/1/random/subfolder",
            True,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_nested/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            ParquetDataSet,
            "random/subfolder/",
            "data/test_dataset/1/random/subfolder/",
            "data/test_dataset/1/random/subfolder",
            True,
            True,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_nested/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            PickleDataSet,
            "test.pickle",
            "data/test.pickle",
            "data",
            False,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_file/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            PickleDataSet,
            "test.pickle",
            "data/test_dataset/1/test.pickle",
            "data/test_dataset/1",
            True,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_file/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            PickleDataSet,
            "test.pickle",
            "data/test_dataset/1/test.pickle",
            "data/test_dataset/1",
            True,
            True,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_file/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            PickleDataSet,
            "random/subfolder/test.pickle",
            "data/random/subfolder/test.pickle",
            "data/random/subfolder",
            False,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_nested_file/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            PickleDataSet,
            "random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder",
            True,
            False,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_nested_file/"
                ),
                "type": "uri_folder",
            },
        ),
        (
            PickleDataSet,
            "random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder/test.pickle",
            "data/test_dataset/1/random/subfolder",
            True,
            True,
            {
                "path": (
                    "azureml://subscriptions/1234/resourcegroups/dummy_rg/workspaces"
                    "/dummy_ws/datastores/some_datastore/paths/test_folder_nested_file/"
                ),
                "type": "uri_folder",
            },
        ),
    ],
    indirect=["mock_azureml_client"],
)
def test_azureml_asset_dataset(
    in_temp_dir,
    mock_azureml_client,
    mock_azureml_config,
    mock_azureml_fs,
    dataset_type,
    path_in_aml,
    path_locally,
    download_path,
    local_run,
    download,
):
    # ensure that AzureMLAssetDataSet type matches path in the mock_azureml_client
    with mock_azureml_client() as mac:
        azureml_type = mac.data.get().type
    ds = AzureMLAssetDataSet(
        dataset={
            "type": dataset_type,
            "filepath": path_in_aml,
        },
        azureml_dataset="test_dataset",
        azureml_type=azureml_type,
        version=Version(None, None),
    )
    ds._local_run = local_run
    ds._download = download
    ds._azureml_config = Path(mock_azureml_config)
    assert ds.path == Path(path_locally)
    assert ds.download_path == download_path
    df = pd.DataFrame({"data": [1, 2, 3], "partition_idx": [1, 2, 3]})
    if download:
        assert (ds._load()["data"] == df["data"]).all()
        if dataset_type is not ParquetDataSet:
            ds.path.unlink()
            assert not ds.path.exists()
            ds._save(df)
            assert ds.path.exists()
    else:
        ds._save(df)
        assert (ds._load()["data"] == df["data"]).all()


@pytest.mark.parametrize(
    "azureml_root_dir",
    [
        "azureml/root",
        "/azureml/root",
        "azureml/root/",
        "/azureml/root/",
    ],
    ids=("noslash", "leading", "trailing", "both"),
)
def test_azureml_root_dir_has_no_leading_and_trailing_slashes(azureml_root_dir):
    aml_dataset = AzureMLAssetDataSet(
        dataset={
            "type": PickleDataSet,
            "filepath": "some/random/path/test.pickle",
        },
        azureml_dataset="test_dataset",
        version=Version(None, None),
        azureml_root_dir=azureml_root_dir,
    )
    assert aml_dataset._azureml_root_dir == azureml_root_dir.strip("/")


def test_azureml_assetdataset_raises_DataSetError_azureml_type():
    with pytest.raises(DataSetError, match="mltable"):
        AzureMLAssetDataSet(
            dataset={
                "type": PickleDataSet,
                "filepath": "some/random/path/test.pickle",
            },
            azureml_dataset="test_dataset",
            version=Version(None, None),
            azureml_type="mltable",
        )


def test_azureml_assetdataset_raises_DataSetError_wrapped_dataset_versioned():
    with pytest.raises(DataSetError, match=VERSIONED_FLAG_KEY):
        AzureMLAssetDataSet(
            dataset={
                "type": PickleDataSet,
                "filepath": "some/random/path/test.pickle",
                "versioned": True,
            },
            azureml_dataset="test_dataset",
            version=Version(None, None),
        )


@pytest.mark.parametrize(
    "datastore",
    [
        "my-datastore",
        "my/datastore",
        "MyDatastore",
    ],
    ids=("hyphen", "slash", "uppercase"),
)
def test_azureml_assetdataset_raises_ValueError_bad_datastore_name(datastore):
    with pytest.raises(ValueError, match=r".*datastore.*"):
        AzureMLAssetDataSet(
            dataset={
                "type": PickleDataSet,
                "filepath": "some/random/path/test.pickle",
            },
            azureml_dataset="test_dataset",
            version=Version(None, None),
            datastore=datastore,
        )


@pytest.mark.parametrize(
    "azureml_root_dir",
    ["my dir", "my//dir", "my\\dir", "my$dir"],
    ids=("space", "doubleslash", "backslash", "special_char"),
)
def test_azureml_assetdataset_raises_ValueError_bad_azureml_root_dir(azureml_root_dir):
    with pytest.raises(ValueError, match=r".*azureml_root_dir.*"):
        AzureMLAssetDataSet(
            dataset={
                "type": PickleDataSet,
                "filepath": "some/random/path/test.pickle",
            },
            azureml_dataset="test_dataset",
            version=Version(None, None),
            azureml_root_dir=azureml_root_dir,
        )


def test_azureml_pipeline_dataset(tmp_path: Path):
    ds = AzureMLPipelineDataSet(
        {
            "type": PickleDataSet,
            "backend": "cloudpickle",
            "filepath": (original_path := str(tmp_path / "test.pickle")),
        }
    )
    assert (
        str(ds.path) == original_path
    ), "Path should be set to the underlying filepath"

    ds.root_dir = (modified_path := str(tmp_path))
    modified_path = Path(modified_path) / "test.pickle"
    assert ds.path == modified_path, "Path should be modified to the supplied value"

    ds.save("test")
    assert Path(modified_path).stat().st_size > 0, "File does not seem to be saved"
    assert ds.load() == "test", "Objects are not the same after deserialization"


@pytest.mark.parametrize(
    "obj,comparer",
    [
        (
            pd.DataFrame(np.random.rand(1000, 3), columns=["a", "b", "c"]),
            lambda a, b: a.equals(b),
        ),
        (np.random.rand(100, 100), lambda a, b: np.equal(a, b).all()),
        (["just", "a", "list"], lambda a, b: all(a[i] == b[i] for i in range(len(a)))),
        ({"some": "dictionary"}, lambda a, b: all(a[k] == b[k] for k in a.keys())),
        (set(["python", "set"]), lambda a, b: len(a - b) == 0),
        ("this is a string", lambda a, b: a == b),
        (1235, lambda a, b: a == b),
        ((1234, 5678), lambda a, b: all(a[i] == b[i] for i in range(len(a)))),
    ],
)
def test_can_save_python_objects_using_fspec(obj, comparer, patched_azure_dataset):
    ds = patched_azure_dataset
    ds.save(obj)
    assert (
        Path(ds._get_target_path()).stat().st_size > 0
    ), "File does not seem to be saved"
    assert comparer(obj, ds.load()), "Objects are not the same after deserialization"

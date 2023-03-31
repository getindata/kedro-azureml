from pathlib import Path
from typing import Type
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from kedro.extras.datasets.pickle import PickleDataSet

from kedro_azureml.constants import KEDRO_AZURE_BLOB_TEMP_DIR_NAME
from kedro_azureml.datasets import (
    AzureMLPipelineDataSet,
    AzureMLPipelineDistributedDataSet,
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
    "dataset_class", (AzureMLPipelineDataSet, AzureMLPipelineDistributedDataSet)
)
def test_azureml_pipeline_dataset(dataset_class: Type, tmp_path: Path):

    ds = dataset_class(
        {
            "type": PickleDataSet,
            "backend": "cloudpickle",
            "filepath": (original_path := str(tmp_path / "test.pickle")),
        }
    )
    assert ds.path == original_path, "Path should be set to the underlying filepath"

    ds.path = (modified_path := str(tmp_path / "test2.pickle"))
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

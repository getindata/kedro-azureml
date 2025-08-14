Azure Data Assets
=================

``kedro-azureml`` adds support for two new datasets that can be used in the Kedro catalog. Right now we support both Azure ML v1 SDK (direct Python) and Azure ML v2 SDK (fsspec-based) APIs.

**For v2 API (fspec-based)** - use ``AzureMLAssetDataset`` that enables to use Azure ML v2 SDK Folder/File datasets for remote and local runs.
Currently only the `uri_file` and `uri_folder` types are supported. Because of limitations of the Azure ML SDK, the `uri_file` type can only be used for pipeline inputs,
not for outputs. The `uri_folder` type can be used for both inputs and outputs.

The ``AzureMLAssetDataset`` supports specifying exact dataset versions using the ``azureml_version`` parameter. If not specified, the latest version will be used automatically.

**For v1 API** (deprecated ⚠️) use the ``AzureMLFileDataset`` and the ``AzureMLPandasDataset`` which translate to `File/Folder dataset`_ and `Tabular dataset`_ respectively in
Azure Machine Learning. Both fully support the Azure versioning mechanism and can be used in the same way as any
other dataset in Kedro.


Apart from these, ``kedro-azureml`` also adds the ``AzureMLPipelineDataset`` which is used to pass data between
pipeline nodes when the pipeline is run on Azure ML and the *pipeline data passing* feature is enabled.
By default, data is then saved and loaded using the ``PickleDataset`` as underlying dataset.
Any other underlying dataset can be used instead by adding a ``AzureMLPipelineDataset`` to the catalog.

All of these can be found under the `kedro_azureml.datasets`_ module.

For details on usage, see the :ref:`API Reference` below

Dataset Versioning
^^^^^^^^^^^^^^^^^^^

The ``AzureMLAssetDataset`` supports specifying exact Azure ML dataset versions using the ``azureml_version`` parameter in your ``catalog.yml``:

.. code-block:: yaml

    # Use a specific version
    my_dataset:
      type: kedro_azureml.datasets.AzureMLAssetDataset
      azureml_dataset: my-dataset-from-azureml
      azureml_version: "100"
      root_dir: data/01_raw/some_data
      dataset:
        type: pandas.ParquetDataset
        filepath: .

    # Use latest version (default behavior)
    my_latest_dataset:
      type: kedro_azureml.datasets.AzureMLAssetDataset
      azureml_dataset: my-dataset-from-azureml
      root_dir: data/01_raw/some_data
      dataset:
        type: pandas.ParquetDataset
        filepath: .

**Note**: The ``azureml_version`` parameter accepts both string and integer values (e.g., ``"100"`` or ``100``). If omitted, the latest available version will be used.

.. _`kedro_azureml.datasets`: https://github.com/getindata/kedro-azureml/blob/master/kedro_azureml/datasets
.. _`File/Folder dataset`: https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?tabs=cli#create-a-file-asset
.. _`Tabular dataset`: https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?tabs=cli#create-a-table-asset

.. _`API Reference`:

API Reference
-------------

Pipeline data passing
^^^^^^^^^^^^^

⚠️ Cannot be used when run locally.

.. autoclass:: kedro_azureml.datasets.AzureMLPipelineDataset
    :members:

-----------------


V2 SDK
^^^^^^^^^^^^^
Use the dataset below when you're using Azure ML SDK v2 (fsspec-based).

✅ Can be used for both remote and local runs.

.. autoclass:: kedro_azureml.datasets.asset_dataset.AzureMLAssetDataset
    :members:

V1 SDK
^^^^^^^^^^^^^
Use the datasets below when you're using Azure ML SDK v1 (direct Python).

⚠️ Deprecated - will be removed in future version of `kedro-azureml`.

.. autoclass:: kedro_azureml.datasets.AzureMLPandasDataset
    :members:

-----------------

.. autoclass:: kedro_azureml.datasets.AzureMLFileDataset
    :members:


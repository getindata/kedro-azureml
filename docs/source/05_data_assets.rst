Azure Data Assets
=================

``kedro-azureml`` adds support for two new datasets that can be used in the Kedro catalog. Right now we support both Azure ML v1 SDK (direct Python) and Azure ML v2 SDK (fsspec-based) APIs.

**For v2 API (fspec-based)** - use ``AzureMLAssetDataSet`` that enables to use Azure ML v2 SDK Folder/File datasets for remote and local runs.
Currently only the `uri_file` and `uri_folder` types are supported. Because of limitations of the Azure ML SDK, the `uri_file` type can only be used for pipeline inputs,
not for outputs. The `uri_folder` type can be used for both inputs and outputs.

**For v1 API** (deprecated ⚠️) use the ``AzureMLFileDataSet`` and the ``AzureMLPandasDataSet`` which translate to `File/Folder dataset`_ and `Tabular dataset`_ respectively in
Azure Machine Learning. Both fully support the Azure versioning mechanism and can be used in the same way as any
other dataset in Kedro.


Apart from these, ``kedro-azureml`` also adds the ``AzureMLPipelineDataSet`` which is used to pass data between
pipeline nodes when the pipeline is run on Azure ML and the *pipeline data passing* feature is enabled.
By default, data is then saved and loaded using the ``PickleDataSet`` as underlying dataset.
Any other underlying dataset can be used instead by adding a ``AzureMLPipelineDataSet`` to the catalog.

All of these can be found under the `kedro_azureml.datasets`_ module.

For details on usage, see the :ref:`API Reference` below

.. _`kedro_azureml.datasets`: https://github.com/getindata/kedro-azureml/blob/master/kedro_azureml/datasets
.. _`File/Folder dataset`: https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?tabs=cli#create-a-file-asset
.. _`Tabular dataset`: https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?tabs=cli#create-a-table-asset

.. _`API Reference`:

API Reference
-------------

Pipeline data passing
^^^^^^^^^^^^^

⚠️ Cannot be used when run locally.

.. autoclass:: kedro_azureml.datasets.AzureMLPipelineDataSet
    :members:

-----------------


V2 SDK
^^^^^^^^^^^^^
Use the dataset below when you're using Azure ML SDK v2 (fsspec-based).

✅ Can be used for both remote and local runs.

.. autoclass:: kedro_azureml.datasets.asset_dataset.AzureMLAssetDataSet
    :members:

V1 SDK
^^^^^^^^^^^^^
Use the datasets below when you're using Azure ML SDK v1 (direct Python).

⚠️ Deprecated - will be removed in future version of `kedro-azureml`.

.. autoclass:: kedro_azureml.datasets.AzureMLPandasDataSet
    :members:

-----------------

.. autoclass:: kedro_azureml.datasets.AzureMLFileDataSet
    :members:


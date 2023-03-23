Azure Data Assets
-----------------

``kedro-azureml`` adds support for two new datasets that can be used in the Kedro catalog, the ``AzureMLFileDataSet``
and the ``AzureMLPandasDataSet`` which translate to `File/Folder dataset`_ and `Tabular dataset`_ respectively in
Azure Machine Learning. Both fully support the Azure versioning mechanism and can be used in the same way as any
other dataset in Kedro.

Both of these can be found under the `kedro_azureml.datasets`_ module.

For details on usage, see the :ref:`API Reference` below

.. _`kedro_azureml.datasets`: https://github.com/getindata/kedro-azureml/blob/master/kedro_azureml/datasets
.. _`File/Folder dataset`: https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?tabs=cli#create-a-file-asset
.. _`Tabular dataset`: https://learn.microsoft.com/en-us/azure/machine-learning/how-to-create-data-assets?tabs=cli#create-a-table-asset

.. _`API Reference`:

API Reference
=============

.. autoclass:: kedro_azureml.datasets.AzureMLPandasDataSet
    :members:

-----------------

.. autoclass:: kedro_azureml.datasets.AzureMLFileDataSet
    :members:

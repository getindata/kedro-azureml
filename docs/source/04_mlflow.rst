==================
MLflow integration
==================

The plugin is compatible with ``mlflow``. You can use native mlflow logging capabilities
provided by Azure ML. See the guide here:
`https://docs.microsoft.com/en-us/azure/machine-learning/how-to-use-mlflow-cli-runs?tabs=azuremlsdk <https://docs.microsoft.com/en-us/azure/machine-learning/how-to-use-mlflow-cli-runs?tabs=azuremlsdk>`__.

There is no additional configuration for MLflow required in order to use
it with Azure ML pipelines. All the settings are provided automatically
by the Azure ML service via environment variables.

|Kedro AzureML MLflow integration|

.. |Kedro AzureML Pipeline execution| image:: ../images/azureml_running_pipeline.gif
.. |Kedro AzureML MLflow integration| image:: ../images/kedro-azureml-mlflow.png

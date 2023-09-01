from kedro.io import AbstractDataSet

from kedro_azureml.datasets.v1_datasets import REMOVED_DATASETS_WARNING


class AzureMLPandasDataSet(AbstractDataSet):
    def _load(self):
        raise REMOVED_DATASETS_WARNING

    def _save(self, data):
        raise REMOVED_DATASETS_WARNING

    def _describe(self):
        raise REMOVED_DATASETS_WARNING

    def __init__(self, *args, **kwargs):
        raise REMOVED_DATASETS_WARNING

import warnings

REMOVED_DATASETS_WARNING = DeprecationWarning(
    (
        REMOVED_DATASETS_TEXT := "This dataset was removed in kedro-azureml 0.6.0. "
        "Use kedro_azureml.datasets.asset_dataset.AzureMLAssetDataset instead."
    )
)

warnings.warn(REMOVED_DATASETS_TEXT, DeprecationWarning)

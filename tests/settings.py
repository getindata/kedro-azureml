# Empty file to load `tests` module as Kedro project
from kedro.config import OmegaConfigLoader  # new import

CONFIG_LOADER_CLASS = OmegaConfigLoader
CONFIG_LOADER_ARGS = {
    # other args
    "config_patterns": {"azureml": ["azureml*"]}
}
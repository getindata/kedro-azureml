import os
from functools import cached_property
from typing import Optional

from kedro.config import (
    AbstractConfigLoader,
    ConfigLoader,
    MissingConfigException,
)
from kedro.framework.session import KedroSession
from omegaconf import DictConfig, OmegaConf

from kedro_azureml.config import KedroAzureMLConfig


class KedroContextManager:
    def __init__(
        self, package_name: str, env: str, extra_params: Optional[dict] = None
    ):
        self.extra_params = extra_params
        self.env = env
        self.package_name = package_name
        self.session: Optional[KedroSession] = None

    @cached_property
    def context(self):
        assert self.session is not None, "Session not  initialized yet"
        return self.session.load_context()

    def _ensure_obj_is_dict(self, obj):
        if isinstance(obj, DictConfig):
            obj = OmegaConf.to_container(obj)
        elif isinstance(obj, dict) and any(
            isinstance(v, DictConfig) for v in obj.values()
        ):
            obj = {
                k: (OmegaConf.to_container(v) if isinstance(v, DictConfig) else v)
                for k, v in obj.items()
            }
        return obj

    @cached_property
    def plugin_config(self) -> KedroAzureMLConfig:
        cl: AbstractConfigLoader = self.context.config_loader
        try:
            obj = self.context.config_loader.get("azureml*")
        except:  # noqa
            obj = None

        if obj is None:
            try:
                obj = self._ensure_obj_is_dict(self.context.config_loader["azureml"])
            except (KeyError, MissingConfigException):
                obj = None

        if obj is None:
            if not isinstance(cl, ConfigLoader):
                raise ValueError(
                    f"You're using a custom config loader: {cl.__class__.__qualname__}{os.linesep}"
                    f"you need to add the azureml config to it.{os.linesep}"
                    "Make sure you add azureml* to config_pattern in CONFIG_LOADER_ARGS "
                    f"in the settings.py file.{os.linesep}"
                    """Example:
CONFIG_LOADER_ARGS = {
    # other args
    "config_patterns": {"azureml": ["azureml*"]}
}
                    """.strip()
                )
            else:
                raise ValueError(
                    "Missing azureml.yml files in configuration. Make sure that you configure your project first"
                )
        return KedroAzureMLConfig.parse_obj(obj)

    def __enter__(self):
        self.session = KedroSession.create(
            self.package_name, env=self.env, extra_params=self.extra_params
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)

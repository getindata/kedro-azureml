from dataclasses import dataclass
from functools import cached_property
from typing import Any, Optional

from kedro.framework.session import KedroSession

from kedro_azureml.config import KedroAzureMLConfig


@dataclass
class CliContext:
    env: str
    metadata: Any


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

    @cached_property
    def plugin_config(self) -> KedroAzureMLConfig:
        return KedroAzureMLConfig.parse_obj(self.context.config_loader.get("azureml*"))

    def __enter__(self):
        self.session = KedroSession.create(
            self.package_name, env=self.env, extra_params=self.extra_params
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)

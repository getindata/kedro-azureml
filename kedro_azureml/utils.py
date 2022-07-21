from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional

from kedro.framework.context import KedroContext
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
        self.context: Optional[KedroContext] = None
        self.session: Optional[KedroSession] = None

    @property
    @lru_cache()
    def plugin_config(self) -> KedroAzureMLConfig:
        return KedroAzureMLConfig.parse_obj(self.context.config_loader.get("azureml*"))

    def __enter__(self):
        self.session = KedroSession.create(
            self.package_name, env=self.env, extra_params=self.extra_params
        )
        self.context = self.session.load_context()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.__exit__(exc_type, exc_val, exc_tb)

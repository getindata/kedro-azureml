from dataclasses import dataclass
from typing import Any

from kedro.framework.session import KedroSession


@dataclass
class CliContext:
    env: str
    metadata: Any

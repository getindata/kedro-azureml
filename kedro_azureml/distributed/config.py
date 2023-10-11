import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Optional, Union


# can be replaced by StrEnum starting from python 3.11
class Framework(str, Enum):
    PyTorch = "PyTorch"
    TensorFlow = "TensorFlow"
    MPI = "MPI"


@dataclass
class DistributedNodeConfig:
    framework: Framework
    num_nodes: Union[str, int]
    processes_per_node: Optional[Union[str, int]] = None

    def __repr__(self):
        return json.dumps(asdict(self))

    def __str__(self):
        return self.__repr__()

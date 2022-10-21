from functools import wraps
from typing import Union

from kedro_azureml.constants import DISTRIBUTED_CONFIG_FIELD
from kedro_azureml.distributed.config import DistributedNodeConfig, Framework


def distributed_job(framework: Framework, num_nodes: Union[str, int], **kwargs):
    def _decorator(func):
        config = DistributedNodeConfig(framework, num_nodes, **kwargs)
        setattr(
            func,
            DISTRIBUTED_CONFIG_FIELD,
            config,
        )

        @wraps(func)
        def wrapper(*args, **kws):
            # for later use, maybe we will actually need to plug-in custom actions
            return func(*args, **kws)

        return wrapper

    return _decorator

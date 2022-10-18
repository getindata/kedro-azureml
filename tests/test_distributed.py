import json

import pytest
from kedro.pipeline import node, pipeline

from kedro_azureml.constants import DISTRIBUTED_CONFIG_FIELD
from kedro_azureml.distributed import distributed_job
from kedro_azureml.distributed.config import Framework
from tests.utils import identity


@pytest.mark.parametrize(
    "framework", [Framework.PyTorch, Framework.MPI, Framework.TensorFlow]
)
@pytest.mark.parametrize("num_nodes", [1, 2, 4])
def test_can_annotate_kedro_node_with_distributed_decorator(framework, num_nodes):
    @distributed_job(framework, num_nodes=num_nodes)
    def my_distributed_node(x):
        return x

    p = pipeline(
        [
            node(identity, inputs="input_data", outputs="i2", name="node1"),
            node(my_distributed_node, inputs="i2", outputs="i3", name="node2"),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )

    assert p is not None and hasattr(
        p.nodes[1].func, DISTRIBUTED_CONFIG_FIELD
    ), "Distributed note was not marked properly"

    dummy_input = object()
    assert (
        p.nodes[1].func(dummy_input) == dummy_input
    ), "Function behaviour should not change after distributed annotation"

    assert isinstance(
        json.loads(str(getattr(p.nodes[1].func, DISTRIBUTED_CONFIG_FIELD))), dict
    ), "Could not validate string representation of distributed config"

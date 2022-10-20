import json
import os
from unittest.mock import patch

import pytest
from azure.ai.ml.entities import Job
from kedro.pipeline import node, pipeline

from kedro_azureml.cli_functions import is_distributed_master_node
from kedro_azureml.constants import DISTRIBUTED_CONFIG_FIELD
from kedro_azureml.distributed import distributed_job
from kedro_azureml.distributed.config import Framework
from kedro_azureml.generator import AzureMLPipelineGenerator
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


@pytest.mark.parametrize(
    "framework", [Framework.PyTorch, Framework.MPI, Framework.TensorFlow]
)
@pytest.mark.parametrize(
    "num_nodes,kedro_params",
    [
        (1, {}),
        (2, {}),
        ("params:number_of_nodes", {"number_of_nodes": 8}),
        ("params:data_science.nodes", {"data_science": {"nodes": 12}}),
    ],
)
def test_can_generate_azure_pipeline_with_distributed_node(
    dummy_plugin_config, framework, num_nodes, kedro_params
):
    @distributed_job(framework, num_nodes=num_nodes)
    def my_distributed_node(x):
        return x

    p = pipeline(
        [
            node(identity, inputs="input_data", outputs="i2", name="node1"),
            node(
                my_distributed_node, inputs="i2", outputs="i3", name="distributed_node"
            ),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )

    with patch.object(AzureMLPipelineGenerator, "get_kedro_pipeline", return_value=p):
        env_name = "unit_test_env"
        docker_image = "unit_test/docker_image:latest"
        generator = AzureMLPipelineGenerator(
            "dummy_pipeline",
            env_name,
            dummy_plugin_config,
            kedro_params,
            docker_image=docker_image,
        )

        az_pipeline = generator.generate()
        assert isinstance(az_pipeline, Job)
        expected_num_nodes = num_nodes
        if isinstance(num_nodes, str):
            expected_num_nodes = (
                kedro_params["number_of_nodes"]
                if "number_of_nodes" in kedro_params
                else kedro_params["data_science"]["nodes"]
            )
        assert (
            az_pipeline.jobs["distributed_node"].resources["instance_count"]
            == expected_num_nodes
        ), "Invalid number of nodes set"


@pytest.mark.parametrize("invalid_num_nodes", [False, 123.0, {}, "asdf"])
def test_generator_raises_on_invalid_distributed_config(
    dummy_plugin_config, invalid_num_nodes
):
    @distributed_job(Framework.PyTorch, num_nodes=invalid_num_nodes)
    def my_distributed_node(x):
        return x

    p = pipeline(
        [
            node(identity, inputs="input_data", outputs="i2", name="node1"),
            node(
                my_distributed_node, inputs="i2", outputs="i3", name="distributed_node"
            ),
            node(identity, inputs="i3", outputs="output_data", name="node3"),
        ]
    )

    with patch.object(AzureMLPipelineGenerator, "get_kedro_pipeline", return_value=p):
        env_name = "unit_test_env"
        docker_image = "unit_test/docker_image:latest"
        generator = AzureMLPipelineGenerator(
            "dummy_pipeline",
            env_name,
            dummy_plugin_config,
            {},
            docker_image=docker_image,
        )

        with pytest.raises(ValueError):
            generator.generate()


@pytest.mark.parametrize(
    "environment,expected_master",
    [
        ({"TF_CONFIG": "ASD"}, False),
        ({"TF_CONFIG": json.dumps({"my_config": "not valid"})}, False),
        ({"RANK": "0"}, True),
        ({"RANK": "1"}, False),
        ({"RANK": "666"}, False),
        ({"OMPI_COMM_WORLD_RANK": "0"}, True),
        ({"OMPI_COMM_WORLD_RANK": "1"}, False),
        ({"TF_CONFIG": json.dumps({"task": {"type": "master"}})}, True),
        ({"TF_CONFIG": json.dumps({"task": {"type": "chief"}})}, True),
        ({"TF_CONFIG": json.dumps({"task": {"type": "worker"}})}, False),
    ],
)
def test_can_detect_distributed_master_node(environment, expected_master):
    with patch.dict(os.environ, environment, clear=False):
        assert (
            status := is_distributed_master_node()
        ) == expected_master, f"Invalid master node status detected, should be {expected_master} but was {status}"

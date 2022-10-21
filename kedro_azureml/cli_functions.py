import json
import logging
import os
from contextlib import contextmanager

import click

from kedro_azureml.generator import AzureMLPipelineGenerator
from kedro_azureml.utils import CliContext, KedroContextManager

logger = logging.getLogger()


@contextmanager
def get_context_and_pipeline(ctx: CliContext, image: str, pipeline: str, params: str):
    with KedroContextManager(
        ctx.metadata.package_name, ctx.env, parse_extra_params(params, True)
    ) as mgr:
        storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        if not storage_account_key:
            click.echo(
                click.style(
                    "Environment variable AZURE_STORAGE_ACCOUNT_KEY not set, falling back to CLI prompt",
                    fg="yellow",
                )
            )
            storage_account_key = click.prompt(
                f"Please provide Azure Storage Account Key for "
                f"storage account {mgr.plugin_config.azure.temporary_storage.account_name}",
                hide_input=True,
            )

        generator = AzureMLPipelineGenerator(
            pipeline,
            ctx.env,
            mgr.plugin_config,
            mgr.context.params,
            image,
            params,
            storage_account_key,
        )
        az_pipeline = generator.generate()
        yield mgr, az_pipeline


def parse_extra_params(params, silent=False):
    if params and (parameters := json.loads(params.strip("'"))):
        if not silent:
            click.echo(
                f"Running with extra parameters:\n{json.dumps(parameters, indent=4)}"
            )
    else:
        parameters = None
    return parameters


def is_distributed_master_node() -> bool:
    is_rank_0 = True
    try:
        if "TF_CONFIG" in os.environ:
            # TensorFlow
            tf_config = json.loads(os.environ["TF_CONFIG"])
            worker_type = tf_config["task"]["type"].lower()
            is_rank_0 = (worker_type == "chief" or worker_type == "master") or (
                worker_type == "worker" and tf_config["task"]["index"] == 0
            )
        else:
            # MPI + PyTorch
            for e in ("OMPI_COMM_WORLD_RANK", "RANK"):
                if e in os.environ:
                    is_rank_0 = int(os.environ[e]) == 0
                    break
    except:  # noqa
        logger.error(
            "Could not parse environment variables related to distributed computing. "
            "Set appropriate values for one of: RANK, OMPI_COMM_WORLD_RANK or TF_CONFIG",
            exc_info=True,
        )
        logger.warning("Assuming this node is not a master node, due to error.")
        return False
    return is_rank_0

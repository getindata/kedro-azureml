import os
from typing import Optional

import click
from click import Context
from kedro.framework.session import KedroSession

from kedro_azureml.config import KedroAzureMLConfig
from kedro_azureml.generator import AzureMLPipelineGenerator
from kedro_azureml.utils import CliContext


@click.group("AzureML")
def commands():
    """Kedro plugin adding support for Azure ML Pipelines"""
    pass


@commands.group(
    name="azureml", context_settings=dict(help_option_names=["-h", "--help"])
)
@click.option(
    "-e",
    "--env",
    "env",
    type=str,
    default=lambda: os.environ.get("KEDRO_ENV", "local"),
    help="Environment to use.",
)
@click.pass_obj
@click.pass_context
def azureml_group(ctx, metadata, env):
    ctx.ensure_object(dict)
    ctx.obj = CliContext(env, metadata)


@azureml_group.command()
@click.option(
    "-i",
    "--image",
    type=str,
    help="Docker image to use for pipeline execution.",
)
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    help="Name of pipeline to run",
    default="__default__",
)
@click.option(
    "--param",
    "params",
    type=str,
    multiple=True,
    help="Parameters override in form of `key=value`",
)
@click.option("--wait-for-completion", type=bool, is_flag=True, default=False)
@click.pass_obj
def run(
    ctx: CliContext,
    image: Optional[str],
    pipeline: str,
    params: list,
    wait_for_completion: bool,
    timeout_seconds: int,
):
    with KedroSession(ctx.metadata) as session:
        kedro_context = session.load_context()
        # TODO? ContextHelper?
        plugin_config = KedroAzureMLConfig.parse_raw(
            kedro_context.config_loader.get("azureml*")
        )
        generator = AzureMLPipelineGenerator(
            pipeline, ctx.env, plugin_config, image, params
        )

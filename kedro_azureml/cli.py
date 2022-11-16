import json
import logging
import os
from pathlib import Path
from typing import List, Optional

import click
from kedro.framework.startup import ProjectMetadata

from kedro_azureml.cli_functions import (
    get_context_and_pipeline,
    parse_extra_params,
)
from kedro_azureml.client import AzureMLPipelinesClient
from kedro_azureml.config import CONFIG_TEMPLATE_YAML
from kedro_azureml.constants import (
    AZURE_SUBSCRIPTION_ID,
    KEDRO_AZURE_BLOB_TEMP_DIR_NAME,
)
from kedro_azureml.distributed.utils import is_distributed_master_node
from kedro_azureml.runner import AzurePipelinesRunner
from kedro_azureml.utils import CliContext, KedroContextManager

logger = logging.getLogger(__name__)


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
def azureml_group(ctx, metadata: ProjectMetadata, env):
    click.echo(metadata)
    ctx.obj = CliContext(env, metadata)


@azureml_group.command()
@click.argument("resource_group")
@click.argument("workspace_name")
@click.argument("experiment_name")
@click.argument("cluster_name")
@click.argument("storage_account_name")
@click.argument("storage_container")
@click.argument("environment_name")
@click.pass_obj
def init(
    ctx: CliContext,
    resource_group,
    workspace_name,
    experiment_name,
    cluster_name,
    storage_account_name,
    storage_container,
    environment_name,
):
    """
    Creates basic configuration for Kedro AzureML plugin
    """
    target_path = Path.cwd().joinpath("conf/base/azureml.yml")
    cfg = CONFIG_TEMPLATE_YAML.format(
        **{
            "resource_group": resource_group,
            "workspace_name": workspace_name,
            "experiment_name": experiment_name,
            "cluster_name": cluster_name,
            "environment_name": environment_name,
            "storage_container": storage_container,
            "storage_account_name": storage_account_name,
        }
    )
    target_path.write_text(cfg)

    click.echo(f"Configuration generated in {target_path}")

    click.echo(
        click.style(
            f"It's recommended to set Lifecycle management rule for storage container {storage_container} "
            f"to avoid costs of long-term storage of the temporary data."
            f"\nTemporary data will be stored under abfs://{storage_container}/{KEDRO_AZURE_BLOB_TEMP_DIR_NAME} path"  # noqa
            f"\nSee https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-policy-configure?tabs=azure-portal",  # noqa
            fg="green",
        )
    )

    aml_ignore = Path.cwd().joinpath(".amlignore")
    if aml_ignore.exists():
        click.echo(
            click.style(
                ".amlignore file already exist, make sure that all of the relevant files"
                "\nwill get uploaded to Azure ML if you're using Code Upload option with this plugin",
                fg="yellow",
            )
        )
    else:
        aml_ignore.write_text("")


@azureml_group.command()
@click.option(
    "-s",
    "--subscription_id",
    help=f"Azure Subscription ID. Defaults to env `{AZURE_SUBSCRIPTION_ID}`",
    default=lambda: os.getenv(AZURE_SUBSCRIPTION_ID, ""),
    type=str,
)
@click.option(
    "--azureml_environment",
    "--aml_env",
    "aml_env",
    type=str,
    help="Azure ML Environment to use for pipeline execution.",
)
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
    "--params",
    "params",
    type=str,
    help="Parameters override in form of JSON string",
)
@click.option("--wait-for-completion", type=bool, is_flag=True, default=False)
@click.pass_obj
@click.pass_context
def run(
    click_context: click.Context,
    ctx: CliContext,
    subscription_id: str,
    aml_env: Optional[str],
    image: Optional[str],
    pipeline: str,
    params: str,
    wait_for_completion: bool,
):
    """Runs the specified pipeline in Azure ML Pipelines; Additional parameters can be passed from command line.
    Can be used with --wait-for-completion param to block the caller until the pipeline finishes in Azure ML.
    """
    params = json.dumps(p) if (p := parse_extra_params(params)) else ""
    assert (
        subscription_id
    ), f"Please provide Azure Subscription ID or set `{AZURE_SUBSCRIPTION_ID}` env"

    if aml_env:
        click.echo(f"Overriding Azure ML Environment for run by: {aml_env}")

    aml_ignore = Path.cwd().joinpath(".amlignore")
    git_ignore = Path.cwd().joinpath(".gitignore")
    if aml_ignore.exists():
        ignore_contents = aml_ignore.read_text().strip()
        if not ignore_contents:
            click.echo(
                click.style(
                    f".amlignore file is empty, which means all of the files from {Path.cwd()}"
                    "\nwill be uploaded to Azure ML. Make sure that you excluded sensitive files first!",
                    fg="yellow",
                )
            )
    elif git_ignore.exists():
        ignore_contents = git_ignore.read_text().strip()
        if ignore_contents:
            click.echo(
                click.style(
                    ".gitignore file detected, ignored files will not be uploaded to Azure ML"
                    "\nWe recommend to use .amlignore instead of .gitignore when working with Azure ML"
                    "\nSee https://github.com/MicrosoftDocs/azure-docs/blob/047cb7f625920183438f3e66472014ac2ebab098/includes/machine-learning-amlignore-gitignore.md",  # noqa
                    fg="yellow",
                )
            )

    mgr: KedroContextManager
    with get_context_and_pipeline(ctx, image, pipeline, params, aml_env) as (
        mgr,
        az_pipeline,
    ):
        az_client = AzureMLPipelinesClient(az_pipeline, subscription_id)

        is_ok = az_client.run(
            mgr.plugin_config.azure,
            wait_for_completion,
            lambda job: click.echo(job.studio_url),
        )

        if is_ok:
            exit_code = 0
            click.echo(
                click.style(
                    "Pipeline {} successfully".format(
                        "finished" if wait_for_completion else "started"
                    ),
                    fg="green",
                )
            )
        else:
            exit_code = 1
            click.echo(
                click.style("There was an error while running the pipeline", fg="red")
            )

        click_context.exit(exit_code)


@azureml_group.command()
@click.option(
    "--azureml_environment",
    "--aml_env",
    "aml_env",
    type=str,
    help="Azure ML Environment to use for pipeline execution.",
)
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
    "--params",
    "params",
    type=str,
    help="Parameters override in form of JSON string",
)
@click.option(
    "-o",
    "--output",
    type=click.types.Path(exists=False, dir_okay=False),
    default="pipeline.yaml",
    help="Pipeline YAML definition file.",
)
@click.pass_obj
def compile(
    ctx: CliContext,
    aml_env: Optional[str],
    image: Optional[str],
    pipeline: str,
    params: list,
    output: str,
):
    """Compiles the pipeline into YAML format"""
    params = json.dumps(p) if (p := parse_extra_params(params)) else ""
    with get_context_and_pipeline(ctx, image, pipeline, params, aml_env) as (
        _,
        az_pipeline,
    ):
        Path(output).write_text(str(az_pipeline))
        click.echo(f"Compiled pipeline to {output}")


@azureml_group.command(hidden=True)
@click.option(
    "-p",
    "--pipeline",
    "pipeline",
    type=str,
    help="Name of pipeline to run",
    default="__default__",
)
@click.option(
    "-n", "--node", "node", type=str, help="Name of the node to run", required=True
)
@click.option(
    "--params",
    "params",
    type=str,
    help="Parameters override in form of `key=value`",
)
@click.option(
    "--az-output",
    "azure_outputs",
    type=str,
    multiple=True,
    help="Paths of Azure ML Pipeline outputs to save dummy data into",
)
@click.pass_obj
def execute(
    ctx: CliContext, pipeline: str, node: str, params: str, azure_outputs: List[str]
):
    # 1. Run kedro
    parameters = parse_extra_params(params)
    with KedroContextManager(
        ctx.metadata.package_name, env=ctx.env, extra_params=parameters
    ) as mgr:
        runner = AzurePipelinesRunner()
        mgr.session.run(pipeline, node_names=[node], runner=runner)

    # 2. Save dummy outputs
    # In distributed computing, it will only happen on nodes with rank 0
    if is_distributed_master_node():
        for dummy_output in azure_outputs:
            (Path(dummy_output) / "output.txt").write_text("#getindata")
    else:
        logger.info("Skipping saving Azure outputs on non-master distributed nodes")

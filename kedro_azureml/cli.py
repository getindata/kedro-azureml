import json
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple

import click
from kedro.framework.startup import ProjectMetadata

from kedro_azureml.cli_functions import (
    get_context_and_pipeline,
    parse_extra_env_params,
    parse_extra_params,
    verify_configuration_directory_for_azure,
    warn_about_ignore_files,
)
from kedro_azureml.client import AzureMLPipelinesClient
from kedro_azureml.config import CONFIG_TEMPLATE_YAML
from kedro_azureml.constants import (
    AZURE_SUBSCRIPTION_ID,
    KEDRO_AZURE_BLOB_TEMP_DIR_NAME,
)
from kedro_azureml.distributed.utils import is_distributed_master_node
from kedro_azureml.manager import KedroContextManager
from kedro_azureml.runner import AzurePipelinesRunner
from kedro_azureml.utils import CliContext

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
    ctx.obj = CliContext(env, metadata)


@azureml_group.command()
@click.argument("subscription_id")
@click.argument("resource_group")
@click.argument("workspace_name")
@click.argument("experiment_name")
@click.argument("cluster_name")
@click.option(
    "--azureml-environment",
    "--aml-env",
    default=None,
    type=str,
    help="Azure ML environment to use with code flow",
)
@click.option(
    "-d", "--docker-image", default=None, type=str, help="Docker image to use"
)
@click.option(
    "-a",
    "--storage-account-name",
    help="Name of the storage account (if you want to use Azure Blob Storage for temporary data)",
)
@click.option(
    "-c",
    "--storage-container",
    help="Name of the storage container (if you want to use Azure Blob Storage for temporary data)",
)
@click.option(
    "--use-pipeline-data-passing",
    is_flag=True,
    default=False,
    help="(flag) Set, to use EXPERIMENTAL pipeline data passing",
)
@click.pass_obj
def init(
    ctx: CliContext,
    subscription_id,
    resource_group,
    workspace_name,
    experiment_name,
    cluster_name,
    azureml_environment: Optional[str],
    docker_image: Optional[str],
    storage_account_name,
    storage_container,
    use_pipeline_data_passing: bool,
):
    """
    Creates basic configuration for Kedro AzureML plugin
    """

    # Check whether docker_image and azure_ml_environment are specified, they cannot be, they are mutually exclusive
    if docker_image and azureml_environment:
        raise click.UsageError(
            "You cannot specify both --docker_image/-d and --azure_ml_environment/--aml_env"
        )
    elif not (docker_image or azureml_environment):
        raise click.UsageError(
            "You must specify either --docker_image/-d or --azure_ml_environment/--aml_env"
        )

    if (
        not (storage_account_name and storage_container)
        and not use_pipeline_data_passing
    ):
        raise click.UsageError(
            "You need to specify storage account (-a) and container name (-c) "
            "or enable pipeline data passing (--use_pipeline_data_passing)"
        )

    target_path = Path.cwd().joinpath("conf/base/azureml.yml")
    cfg = CONFIG_TEMPLATE_YAML.format(
        **{
            "subscription_id": subscription_id,
            "resource_group": resource_group,
            "workspace_name": workspace_name,
            "experiment_name": experiment_name,
            "cluster_name": cluster_name,
            "storage_account_name": storage_account_name or "~",
            "storage_container": storage_container or "~",
            "environment_name": azureml_environment or "~",
            "pipeline_data_passing": use_pipeline_data_passing,
            "docker_image": docker_image or "~",
            "code_directory": "." if azureml_environment else "~",
        }
    )
    target_path.write_text(cfg)

    click.echo(f"Configuration generated in {target_path}")

    if storage_account_name and storage_container:
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
    "--subscription-id",
    help=f"Azure Subscription ID. Defaults to env `{AZURE_SUBSCRIPTION_ID}`",
    default=lambda: os.getenv(AZURE_SUBSCRIPTION_ID, ""),
    type=str,
)
@click.option(
    "--azureml-environment",
    "--aml-env",
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
@click.option(
    "--env-var",
    type=str,
    multiple=True,
    help="Environment variables to be injected in the steps, format: KEY=VALUE",
)
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
    env_var: Tuple[str],
):
    """Runs the specified pipeline in Azure ML Pipelines; Additional parameters can be passed from command line.
    Can be used with --wait-for-completion param to block the caller until the pipeline finishes in Azure ML.
    """
    params = json.dumps(p) if (p := parse_extra_params(params)) else ""

    if subscription_id:
        click.echo(f"Overriding Azure Subscription ID for run to: {subscription_id}")

    if aml_env:
        click.echo(f"Overriding Azure ML Environment for run by: {aml_env}")

    warn_about_ignore_files()

    verify_configuration_directory_for_azure(click_context, ctx)

    mgr: KedroContextManager
    extra_env = parse_extra_env_params(env_var)
    with get_context_and_pipeline(ctx, image, pipeline, params, aml_env, extra_env) as (
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
    "--azureml-environment",
    "--aml-env",
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
    "--az-input",
    "azure_inputs",
    type=(str, click.Path(exists=True, file_okay=False, dir_okay=True)),
    multiple=True,
    help="Name and path of Azure ML Pipeline input",
)
@click.option(
    "--az-output",
    "azure_outputs",
    type=(str, click.Path(exists=True, file_okay=False, dir_okay=True)),
    multiple=True,
    help="Name and path of Azure ML Pipeline output",
)
@click.pass_obj
def execute(
    ctx: CliContext,
    pipeline: str,
    node: str,
    params: str,
    azure_inputs: List[Tuple[str, str]],
    azure_outputs: List[Tuple[str, str]],
):
    # 1. Run kedro
    parameters = parse_extra_params(params)
    azure_inputs = {ds_name: data_path for ds_name, data_path in azure_inputs}
    azure_outputs = {ds_name: data_path for ds_name, data_path in azure_outputs}
    data_paths = {**azure_inputs, **azure_outputs}

    with KedroContextManager(
        ctx.metadata.package_name, env=ctx.env, extra_params=parameters
    ) as mgr:
        pipeline_data_passing = (
            mgr.plugin_config.azure.pipeline_data_passing is not None
            and mgr.plugin_config.azure.pipeline_data_passing.enabled
        )
        runner = AzurePipelinesRunner(
            data_paths=data_paths, pipeline_data_passing=pipeline_data_passing
        )
        mgr.session.run(pipeline, node_names=[node], runner=runner)

    # 2. Save dummy outputs
    # In distributed computing, it will only happen on nodes with rank 0
    if not pipeline_data_passing and is_distributed_master_node():
        for data_path in azure_outputs.values():
            (Path(data_path) / "output.txt").write_text("#getindata")
    else:
        logger.info("Skipping saving Azure outputs on non-master distributed nodes")

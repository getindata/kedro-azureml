import json
import logging
import os
from contextlib import contextmanager
from typing import Optional

import click

from kedro_azureml.generator import AzureMLPipelineGenerator
from kedro_azureml.utils import CliContext, KedroContextManager

logger = logging.getLogger()


@contextmanager
def get_context_and_pipeline(
    ctx: CliContext,
    docker_image: Optional[str],
    pipeline: str,
    params: str,
    aml_env: Optional[str] = None,
):
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
            aml_env,
            docker_image,
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

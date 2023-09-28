import importlib
import json
import logging
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Dict, Optional

import click

from kedro_azureml.generator import AzureMLPipelineGenerator
from kedro_azureml.manager import KedroContextManager
from kedro_azureml.utils import CliContext

logger = logging.getLogger()


@contextmanager
def get_context_and_pipeline(
    ctx: CliContext,
    docker_image: Optional[str],
    pipeline: str,
    params: str,
    aml_env: Optional[str] = None,
    extra_env: Dict[str, str] = {},
    load_versions: Dict[str, str] = {},
):
    with KedroContextManager(
        ctx.metadata.package_name, ctx.env, parse_extra_params(params, True)
    ) as mgr:
        storage_account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "")
        pipeline_data_passing = (
            mgr.plugin_config.azure.pipeline_data_passing is not None
            and mgr.plugin_config.azure.pipeline_data_passing.enabled
        )
        if not pipeline_data_passing and not storage_account_key:
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
            mgr.context.catalog,
            aml_env,
            docker_image,
            params,
            storage_account_key,
            extra_env,
            load_versions,
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


def warn_about_ignore_files():
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
                    # noqa
                    fg="yellow",
                )
            )


def verify_configuration_directory_for_azure(click_context, ctx: CliContext):
    """
    Checks whether the Kedro conf directory of used environment contains only empty files or is empty.
    If it is, prompts the user to continue or exit, as continuing might break execution in Azure ML.
    :param click_context:
    :param ctx:
    :return:
    """
    conf_dir = Path.cwd().joinpath(f"conf/{ctx.env}")

    exists = conf_dir.exists() and conf_dir.is_dir()
    is_empty = True
    has_only_empty_files = True

    if exists:
        for p in conf_dir.iterdir():
            is_empty = False
            if p.is_file():
                has_only_empty_files = p.lstat().st_size == 0

            if not has_only_empty_files:
                break

    msg = f"Configuration folder for your Kedro environment {conf_dir} "
    if not exists:
        msg += "does not exist or is not a directory,"
    if is_empty:
        msg += "is empty,"
    elif has_only_empty_files:
        msg += "contains only empty files,"

    if is_empty or has_only_empty_files:
        msg += (
            "\nwhich might cause issues when running in Azure ML."
            + "\nEither use different environment or provide non-empty configuration for your env."
            + "\nContinue?"
        )
        if not click.confirm(click.style(msg, fg="yellow")):
            click_context.exit(2)


def parse_extra_env_params(extra_env):
    for entry in extra_env:
        if not re.match("[A-Za-z0-9_]+=.*", entry):
            raise Exception(f"Invalid env-var: {entry}, expected format: KEY=VALUE")

    return {(e := entry.split("=", maxsplit=1))[0]: e[1] for entry in extra_env}


def dynamic_import_job_schedule_func_from_str(
    ctx: click.Context,
    param: click.Parameter,
    import_str: str,
) -> Optional[Callable]:
    """
    Dynamically import and retrieve a function from a specified module.

    The function must have exactly one parameter of type azure.ai.ml.entities.Job.
    Note that there is no  check on the parameter type
    This function is designed to be used in Click-based command-line applications.

    :param ctx: The Click context.
    :type ctx: click.Context
    :param param: The Click parameter associated with this function.
    :type param: click.Parameter
    :param import_str: A string in the format 'path.to.file:function'
        specifying the module and function to import.
    :type import_str: str

    :returns: The imported function.
    :rtype: Any

    :raises click.BadParameter: If the `import_str` is not in the correct format,
        if the specified module cannot be imported,
        if the specified attribute cannot be retrieved from the module,
        if the retrieved attribute is not a callable function,

    Example usage:
    >>> instance = dynamic_import_job_schedule_func_from_str(
        ctx, param, "my_module:my_function"
    )

    Inspired by the `uvicorn/importer.py` module's `import_from_string` function.
    """
    # base case: no callback
    if import_str is None:
        return

    # check format
    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        raise click.BadParameter(
            "import_str must be in format <module>:<function>", param=param
        )

    try:
        module = importlib.import_module(module_str)
        instance = getattr(module, attrs_str)

        # fails if we try to import an attribute that is not a function
        if not callable(instance):
            raise click.BadParameter(
                f"The attribute '{attrs_str}' is not a callable function.", param=param
            )

        return instance
    except (ImportError, AttributeError, ValueError) as e:
        # catches errors if module or attribute does not exist
        raise click.BadParameter(f"Error: {e}", param=param)


def default_job_callback(job):
    click.echo(job.studio_url)

import os
from functools import cached_property

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azureml.core import Datastore, Run, Workspace
from azureml.exceptions import UserErrorException


def get_azureml_credentials():
    try:
        # On a AzureML compute instance, the managed identity will take precedence,
        # while it does not have enough permissions.
        # So, if we are on an AzureML compute instance, we disable the managed identity.
        is_azureml_managed_identity = "MSI_ENDPOINT" in os.environ
        credential = DefaultAzureCredential(
            exclude_managed_identity_credential=is_azureml_managed_identity
        )
        # Check if given credential can get token successfully.
        credential.get_token("https://management.azure.com/.default")
    except Exception:
        # Fall back to InteractiveBrowserCredential in case DefaultAzureCredential not work
        credential = InteractiveBrowserCredential()
    return credential


def get_workspace(*args, **kwargs) -> Workspace:
    """
    Get an AzureML workspace.

    Args:
        *args: Positional arguments to pass to the Workspace constructor.
        **kwargs: Keyword arguments to pass to the Workspace constructor.
    """
    if args or kwargs:
        workspace = Workspace(*args, **kwargs)
    else:
        try:
            # if running on azureml compute instance
            workspace = Workspace.from_config()
        except UserErrorException:
            try:
                # if running on azureml compute cluster.
                workspace = Run.get_context().experiment.workspace
            except AttributeError as e:
                raise UserErrorException(
                    "Could not connect to AzureML workspace."
                ) from e
    return workspace


class AzureMLDataStoreMixin:
    def __init__(self, workspace_args, azureml_datastore=None, workspace=None):
        self._workspace_instance = workspace
        self._azureml_datastore_name = azureml_datastore
        self._workspace_args = workspace_args or dict()

    @cached_property
    def _workspace(self) -> Workspace:
        return self._workspace_instance or get_workspace(**self._workspace_args)

    @cached_property
    def _azureml_datastore(self) -> str:
        return (
            self._azureml_datastore_name or self._workspace.get_default_datastore().name
        )

    @cached_property
    def _datastore_container_name(self) -> str:
        ds = Datastore.get(self._workspace, self._azureml_datastore)
        return ds.container_name

    @cached_property
    def _azureml_path(self):
        return f"abfs://{self._datastore_container_name}/"

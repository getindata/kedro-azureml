import os

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azureml.core import Run, Workspace
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
        if kwargs is not None and "auth" not in kwargs:
            kwargs["auth"] = get_azureml_credentials()

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

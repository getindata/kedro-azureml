from azureml.core import Run, Workspace
from azureml.exceptions import UserErrorException


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

import os

from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential

# Make the Azure exception import optional to avoid hard failures when azure.core is absent
try:
    from azure.core.exceptions import ClientAuthenticationError  # type: ignore
except ImportError:  # azure.core not installed or version mismatch
    class ClientAuthenticationError(Exception):
        """Fallback exception to allow local/dev environments without azure.core."""
        pass


def get_azureml_credentials():
    try:
        # On an AzureML compute instance, the managed identity will take precedence,
        # while it may not have enough permissions.
        # So, if we are on an AzureML compute instance, we disable the managed identity.
        is_azureml_managed_identity = "MSI_ENDPOINT" in os.environ
        credential = DefaultAzureCredential(
            exclude_managed_identity_credential=is_azureml_managed_identity
        )
        # Check if given credential can get token successfully.
        credential.get_token("https://management.azure.com/.default")
    except (ValueError, ClientAuthenticationError):
        # Fall back to InteractiveBrowserCredential in case DefaultAzureCredential does not work
        credential = InteractiveBrowserCredential()
    return credential

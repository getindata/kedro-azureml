import yaml
from pydantic import BaseModel


class DockerConfig(BaseModel):
    image: str


class AzureTempStorageConfig(BaseModel):
    account_name: str
    container: str


class AzureMLConfig(BaseModel):
    experiment_name: str
    workspace_name: str
    resource_group: str
    cluster_name: str
    temporary_storage: AzureTempStorageConfig


class KedroAzureMLConfig(BaseModel):
    azure: AzureMLConfig
    docker: DockerConfig


class KedroAzureRunnerConfig(BaseModel):
    # Class for use only in the runner
    temporary_storage: AzureTempStorageConfig
    run_id: str
    storage_account_key: str


CONFIG_TEMPLATE_YAML = """
azure:
  # Name of the Azure ML Compute Cluster
  cluster_name: "{cluster_name}"
  # Azure ML Experiment Name
  experiment_name: "{experiment_name}"
  # Azure resource group to use
  resource_group: "{resource_group}"
  # Azure ML Workspace name
  workspace_name: "{workspace_name}"

  # Temporary storage settings - this is used to pass some data between steps
  # if the data is not specified in the catalog directly
  temporary_storage:
    # Azure Storage account name, where the temp data should be stored
    # It's recommended to set Lifecycle management rule for storage container, to avoid costs of long-term storage
    # of the temporary data. Temporary data will be stored under abfs://<containter>/kedro-azureml-temp path
    # See https://docs.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-policy-configure?tabs=azure-portal
    account_name: "{storage_account_name}"
    # Name of the storage container
    container: "{storage_container}"
docker:
  # Docker image to use during pipeline execution
  image: "{docker_image}"
""".strip()

# This auto-validates the template above during import
_CONFIG_TEMPLATE = KedroAzureMLConfig.parse_obj(yaml.safe_load(CONFIG_TEMPLATE_YAML))

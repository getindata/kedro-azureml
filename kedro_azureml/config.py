from typing import Optional

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


CONFIG_TEMPLATE = KedroAzureMLConfig(
    azure=AzureMLConfig(
        experiment_name="{experiment_name}",
        workspace_name="{workspace_name}",
        resource_group="{resource_group}",
        cluster_name="{cluster_name}",
        temporary_storage=AzureTempStorageConfig(
            account_name="{storage_account_name}", container="{storage_container}"
        ),
    ),
    docker=DockerConfig(image="{docker_image}"),
)

from pydantic import BaseModel


class DockerConfig(BaseModel):
    image: str


class AzureMLConfig(BaseModel):
    experiment_name: str
    workspace_name: str
    resource_group: str
    subscription_id: str
    cluster_name: str


class KedroAzureMLConfig(BaseModel):
    azure: AzureMLConfig
    docker: DockerConfig

# Installation guide

## Kedro setup

First, you need to install base Kedro package

```console
$ pip install "kedro>=0.18.1,<0.19.0"
```

## Plugin installation

### Install from PyPI

You can install ``kedro-azureml`` plugin from ``PyPi`` with `pip`:

```console
pip install --upgrade kedro-azureml
```

### Install from sources

You may want to install the develop branch which has unreleased features:

```console
pip install git+https://github.com/getindata/kedro-azureml.git@develop
```

## Available commands

You can check available commands by going into project directory and runnning:

```console
$ kedro azureml
Usage: kedro azureml [OPTIONS] COMMAND [ARGS]...

  Interact with Azure ML Pipelines

Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  compile         Translates Kedro pipeline into JSON file with VertexAI...
  init            Initializes configuration for the plugin
  list-pipelines  List deployed pipeline definitions
  run-once        Deploy pipeline as a single run within given experiment...
  schedule        Schedules recurring execution of latest version of the...
  ui              Open VertexAI Pipelines UI in new browser tab
```

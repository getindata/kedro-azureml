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
kedro azureml                                                                                                                                                                                                                                                                                                                                     
Usage: kedro azureml [OPTIONS] COMMAND [ARGS]...

Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  compile
  init
  run
```

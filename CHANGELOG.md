# Changelog

## [Unreleased]

## [0.3.4] - 2022-12-30

-   Add lazy initialization and cache to Kedro's context in the `KedroContextManager` class to prevent re-loading

## [0.3.3] - 2022-12-08

-   Upgrade `azure-ai-ml` to `>=1.2.0` to adress code upload file ignore issues (see <https://github.com/Azure/azure-sdk-for-python/pull/27338#issuecomment-1337454472> and <https://github.com/getindata/kedro-azureml/issues/33>).

## [0.3.2] - 2022-12-02

-   Add a control gate for Kedro environments before starting the pipeline in Azure ML (<https://github.com/getindata/kedro-azureml/issues/33>)

## [0.3.1] - 2022-11-18

-   Fix default configuration, to make code upload as default
-   Improved documentation and quickstart related to the code upload feature

## [0.3.0] - 2022-11-16

-   Added support for execution via code upload for faster development cycles <https://github.com/getindata/kedro-azureml/pull/15>
-   Quickstart documentation improvements

## [0.2.2] - 2022-10-26

-   Added sychronization of automatic datasets for distributed training use case

## [0.2.1] - 2022-10-24

### Added

-   Ability to overwrite the compute target at a Node level using a [Node tag](https://kedro.readthedocs.io/en/stable/kedro.pipeline.node.html) that references a compute alias defined in the compute section of `azureml.yaml`.
-   Improvements in build process, synchronised with getindata `python-opensource-template`
-   Add support for distributed training in PyTorch, TensorFlow and MPI via native Azure ML integration

## [0.1.0] - 2022-07-28

-   Initial plugin release

[Unreleased]: https://github.com/getindata/kedro-azureml/compare/0.3.4...HEAD

[0.3.4]: https://github.com/getindata/kedro-azureml/compare/0.3.3...0.3.4

[0.3.3]: https://github.com/getindata/kedro-azureml/compare/0.3.2...0.3.3

[0.3.2]: https://github.com/getindata/kedro-azureml/compare/0.3.1...0.3.2

[0.3.1]: https://github.com/getindata/kedro-azureml/compare/0.3.0...0.3.1

[0.3.0]: https://github.com/getindata/kedro-azureml/compare/0.2.2...0.3.0

[0.2.2]: https://github.com/getindata/kedro-azureml/compare/0.2.1...0.2.2

[0.2.0]: https://github.com/getindata/kedro-azureml/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/kedro-azureml/compare/d492a61d26a1927ca216fa10fa48077a1dee2062...0.1.0

# Changelog

## [Unreleased]

-   Added support for pydantic v2 and bumped minimal required pydantic version to `2.0.0` by [@froessler](https://github.com/fdroessler)
-   Added adbility to mark a node as deterministic (enables caching on AzureML) by [@tomasvanpottelbergh](https://github.com/tomasvanpottelbergh)

## [0.5.0] - 2023-08-11

-   [🚀 New dataset] Added support for `AzureMLAssetDataSet` based on Azure ML SDK v2 (fsspec) by [@tomasvanpottelbergh](https://github.com/tomasvanpottelbergh) & [@froessler](https://github.com/fdroessler)
-   [📝 Docs] Updated datasets docs with sections
-   Bumped minimal required Kedro version to \`0.18.11
-   [⚠️ Deprecation warning] - starting from `0.4.0` the plugin is not compatible with ARM macOS versions due to internal azure dependencies (v1 SDKs). V1 SDK-based datasets will be removed in the future

## [0.4.1] - 2023-05-04

-   [📝 Docs] Revamp the quickstart guide in documentation
-   Refactor `kedro azureml init` command to be more user-friendly
-   Add dependency on `kedro-datasets` to prepare for Kedro `0.19.0`; Remove `kedro.datasets.*` imports

## [0.4.0] - 2023-04-28

-   [🧑‍🔬 Experimental ] Added support for pipeline-native data passing (allows to preview intermediate data in AzureML Studio UI) by [@tomasvanpottelbergh](https://github.com/tomasvanpottelbergh)
-   New `AzureMLFileDataSet` and `AzureMLPandasDataSet`by [@asafalinadsg](https://github.com/asafalinadsg) & [@eliorc](https://github.com/eliorc)
-   E2E tests for `AzureMLPandasDataSet` dataset
-   Bumped minimal required Kedro version to `0.18.5`
-   Added support for `OmegaConfigLoader`

## [0.3.6] - 2023-03-08

## [0.3.5] - 2023-02-20

-   Ability to pass extra environment variables to the Kedro nodes using `--env-var` option
-   Default configuration for docker-flow adjusted for the latest kedro-docker plugin
-   Fix authorization issues on AzureML Compute Instance (<https://github.com/getindata/kedro-azureml/pull/47>) by [@j0rd1smit](https://github.com/j0rd1smit)

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

[Unreleased]: https://github.com/getindata/kedro-azureml/compare/0.5.0...HEAD

[0.5.0]: https://github.com/getindata/kedro-azureml/compare/0.4.1...0.5.0

[0.4.1]: https://github.com/getindata/kedro-azureml/compare/0.4.0...0.4.1

[0.4.0]: https://github.com/getindata/kedro-azureml/compare/0.3.6...0.4.0

[0.3.6]: https://github.com/getindata/kedro-azureml/compare/0.3.5...0.3.6

[0.3.5]: https://github.com/getindata/kedro-azureml/compare/0.3.4...0.3.5

[0.3.4]: https://github.com/getindata/kedro-azureml/compare/0.3.3...0.3.4

[0.3.3]: https://github.com/getindata/kedro-azureml/compare/0.3.2...0.3.3

[0.3.2]: https://github.com/getindata/kedro-azureml/compare/0.3.1...0.3.2

[0.3.1]: https://github.com/getindata/kedro-azureml/compare/0.3.0...0.3.1

[0.3.0]: https://github.com/getindata/kedro-azureml/compare/0.2.2...0.3.0

[0.2.2]: https://github.com/getindata/kedro-azureml/compare/0.2.1...0.2.2

[0.2.0]: https://github.com/getindata/kedro-azureml/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/kedro-azureml/compare/d492a61d26a1927ca216fa10fa48077a1dee2062...0.1.0

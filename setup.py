from setuptools import setup, find_packages

setup(
    name='kedro-azureml',
    version='0.8.0.1',
    packages=find_packages(),
    entry_points={
        'kedro.hooks': [
            'azure_local_run_hook = kedro_azureml.hooks:azureml_local_run_hook',
        ],
        'kedro.project_commands': [
            'azureml = kedro_azureml.cli:commands',
        ],
    },
    # Include other setup arguments like install_requires, etc.
)

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kedro plugin enabling pipeline execution on Azure ML Pipelines. Supports both Data Scientist workflows (fast code upload) and MLOps workflows (stable Docker-based).

## Essential Commands

```bash
# Setup
poetry install
pre-commit install

# Testing (ALWAYS run before committing)
poetry run pytest --cov kedro_azureml --cov-report term-missing
tox  # Cross-version testing (Python 3.9-3.12)

# Code quality (MUST pass before committing)
pre-commit run --all-files

# E2E testing (optional, requires Docker + Azure)
./dev-utils/run_e2e_tests.sh
```

## Architecture

### Core Components
- `cli.py` - Commands: `compile`, `init`, `run`
- `client.py` - Azure ML Pipelines deployment client
- `generator.py` - Kedro to Azure ML pipeline translation
- `config.py` - Pydantic models, configuration templates
- `runner.py` - Pipeline execution logic
- `manager.py` - Pipeline management utilities

### Key Concepts
- **Pipeline Translation**: Kedro nodes to Azure ML pipeline steps
- **Authentication**: Azure auth in `auth/` module
- **Datasets**: Custom Azure datasets in `datasets/` module
- **Distributed**: Distributed computing support in `distributed/` module
- **Configuration**: Expects Azure ML configuration in Kedro config

## CLI Usage

```bash
kedro azureml init [-e ENV]        # Initialize Azure ML config
kedro azureml compile [-e ENV]     # Compile to YAML pipeline
kedro azureml run [-e ENV]         # Deploy and run on Azure ML
```

## Code Quality Standards

Always run these before any commit:
1. `poetry run pytest --cov kedro_azureml --cov-report term-missing`
2. `pre-commit run --all-files`

**Style Guidelines:**
- Line length: 120 chars (flake8), 79 for isort
- Formatting: black + isort with black profile
- Type hints: mypy (configured in `tox.ini`)
- Coverage: Must maintain current coverage levels

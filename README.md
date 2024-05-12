# spacex-data-platform

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit\&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-blue)](http://mypy-lang.org/)
[![Docstring checked with Interrogate](./badges/interrogate.svg)](https://interrogate.readthedocs.io/en/latest/)

Automate the build of a simple yet scalable Data Platform

## Requirements

- Python 3.10
- [Poetry](https://python-poetry.org)

## Setup

For setting up the project locally, follow the steps below:

```bash
poetry install
...
poetry shell
```

Also uses [pre-commit](https://pre-commit.com) for code formatting and linting. To install the pre-commit hooks, run:

```bash
pre-commit install
```

We are using [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit messages. To enforce this, we are using a `pre-commit hook` that will ensure your commits are following the conventional commits format.

## Project Structure

The project is composed by:

- ingestion: it contains the code to ingest data from the SpaceX API
  - raw: it contains the raw data from the API
  - bronze: it transforms the raw data in `json` to `parquet` setting the index to `id`

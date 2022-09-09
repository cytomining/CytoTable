# Development

Development for pcytominer-transform takes place using Python environments managed by Python [Poetry](https://python-poetry.org/).

## Documentation

Documentation is provided through [MyST (or Markedly Structured Text)](https://myst-parser.readthedocs.io/en/latest/index.html) markdown documents are transformed into docsite content using Sphinx.
Documentation content assumes a "one sentence per line" style.
Diagrams may be added using the [Sphinx extension for Mermaid](https://github.com/mgaitan/sphinxcontrib-mermaid#markdown-support).
Documentation is automatically published to a docsite via [Github Actions](https://docs.github.com/en/actions).

## Getting started

Local development may be enabled using roughly the following steps.

1. [Install Python](https://www.python.org/downloads/)
1. [Install Poetry](https://python-poetry.org/docs/#installation)
1. [Install Poetry Environment](https://python-poetry.org/docs/basic-usage/#installing-dependencies): `poetry install`
1. Use the IDE of your choice to add or edit related content.

## Linting (and fixing) with pre-commit

Work added to this repo is automatically checked using [pre-commit](https://pre-commit.com/) (managed by this repo's poetry environment) via [Github Actions](https://docs.github.com/en/actions).
Pre-commit may be configured to work alongside your local [git with hooks](https://pre-commit.com/index.html#3-install-the-git-hook-scripts) or you can use the following command to check your work:

```{shell}
poetry run pre-commit run --all-files
```

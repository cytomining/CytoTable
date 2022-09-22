# Development

```{mermaid}
flowchart LR
    subgraph Dagger
        containers["Container(s)"]
    end
    subgraph Poetry
        python[Python]
    end
    python --> |running within| containers
```

Pcytominer-transform is primarily written in Python with related environments managed by Python [Poetry](https://python-poetry.org/).
[Dagger](https://docs.dagger.io/) is used to help create consistent testing locally and with automated tests via containers.

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
1. [Install Dagger](https://docs.dagger.io/install/)
1. Install [Buildkit](https://docs.dagger.io/1223/custom-buildkit/) runtime environment for Dagger (for ex. [Docker Desktop](https://www.docker.com/products/docker-desktop/))
1. Initialize Dagger Project: `dagger project update`
1. Use the IDE of your choice to add or edit related content.

## Linting

Work added to this repo is automatically checked using [pre-commit](https://pre-commit.com/) (managed by this repo's poetry environment) via [Github Actions](https://docs.github.com/en/actions).
Pre-commit may be configured to work alongside your local [git with hooks](https://pre-commit.com/index.html#3-install-the-git-hook-scripts) or you can use the following command to check your work via the Poetry environment:

```sh
% poetry run pre-commit run --all-files
```

## Testing

Automated or manual testing for this repo may be performed using Dagger actions.
The Dagger test action performs [pytest](https://pytest.org/en/latest/contents.html) testing and also pre-commit checks mentioned above.
Automated tests are run using Dagger actions through Github actions.
Similar Dagger-based manual testing may be performed using the following:

```sh
# update the dagger project
% dagger project update

# gather data for testing
% dagger do gather_data

# perform the testing
% dagger do test
```

It's also possible to test locally using Poetry.
Testing in this way without Dagger does not guarantee a successful automated test as environmental differences could exist.
See below for an example of testing without Dagger:

```sh
% poetry run pytest
```

# Contributing

First of all, thank you for contributing to CytoTable! 🎉 💯

This document contains guidelines on how to most effectively contribute to the codebase.

If you are stuck, please feel free to ask any questions or ask for help.

## Code of conduct

This project is governed by our [code of conduct](code_of_conduct.md).
By participating, you are expected to uphold this code.
Please report unacceptable behavior to [cytodata.info@gmail.com](mailto:cytodata.info@gmail.com).

## Quick links

- Documentation: <https://cytomining.github.io/CytoTable/>
- Issue tracker: <https://github.com/cytomining/CytoTable/issues>
- Package Dependencies (via [Poetry configuration](https://python-poetry.org/docs/pyproject/)): <https://github.com/cytomining/CytoTable/blob/main/pyproject.toml>

## Process

### Bug reporting

We love hearing about use-cases when our software does not work.
This provides us an opportunity to improve.
However, in order for us to fix a bug, you need to tell us exactly what went wrong.

When you report a bug, please tell us as much pertinent information as possible.
This information includes:

- The CytoTable version you’re using
- The format of input data
- Copy and paste two pieces of information: 1) your command and 2) the specific error message
- What you’ve tried to overcome the bug
- What environment you're running CytoTable in (for example, OS, hardware, etc.)

Please provide this information as an issue in the repository: <https://github.com/cytomining/CytoTable/issues>

Please also search the issues (and documentation) for an existing solution.
It’s possible we solved the bug already!
If you find an issue already describing the bug, please add a comment to the issue instead of opening a new one.

### Suggesting enhancements

We’re deeply committed to a simple, intuitive user experience, and to support core profiling pipeline data processing.
This commitment requires a good relationship, and open communication, with our users.

We encourage you to propose enhancements to improve the CytoTable package as an issue in the repository.

First, figure out if your proposal is already implemented by reading the documentation!
Next, check the issues (<https://github.com/cytomining/CytoTable/issues>) to see if someone else has already proposed the enhancement you have in mind.
If you do find the suggestion, please comment on the existing issue noting that you are also interested in this functionality.
If you do not find the suggestion, please open a new issue and clearly document the specific enhancement and why it would be helpful for your particular use case.

### Your first code contribution

Contributing code for the first time can be a daunting task.
However, in our community, we strive to be as welcoming as possible to newcomers, while ensuring rigorous software development practices.

The first thing to figure out is exactly what you’re going to contribute!
We describe all future work as individual [github issues](https://github.com/cytomining/CytoTable/issues).
For first time contributors we have specifically tagged [beginner issues](https://github.com/cytomining/CytoTable/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22).

If you want to contribute code that we haven’t already outlined, please start a discussion in a new issue before writing any code.
A discussion will clarify the new code and reduce merge time.
Plus, it’s possible your contribution belongs in a different code base, and we do not want to waste your time (or ours)!

### Pull requests

After you’ve decided to contribute code and have written it up, please file a pull request.
We specifically follow a [forked pull request model](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork).
Please create a fork of the CytoTable repository, clone the fork, and then create a new, feature-specific branch.
Once you make the necessary changes on this branch, you should file a pull request to incorporate your changes into the main CytoTable repository.

The content and description of your pull request are directly related to the speed at which we are able to review, approve, and merge your contribution into CytoTable.
To ensure an efficient review process please perform the following steps:

1. Follow all instructions in the [pull request template](https://github.com/cytomining/CytoTable/blob/main/.github/PULL_REQUEST_TEMPLATE.md)
1. Triple check that your pull request is adding _one_ specific feature. Small, bite-sized pull requests move so much faster than large pull requests.
1. After submitting your pull request, ensure that your contribution passes all status checks (e.g. passes all tests)

Pull request review and approval is required by at least one project maintainer to merge.
We will do our best to review the code addition in a timely fashion.
Ensuring that you follow all steps above will increase our speed and ability to review.
We will check for accuracy, style, code coverage, and scope.

### Git commit messages

For all commit messages, please use a short phrase that describes the specific change.
For example, “Add feature to check normalization method string” is much preferred to “change code”.
When appropriate, reference issues (via `#` plus number) .

## Development

### Overview

CytoTable is primarily written in Python with related environments managed by Python [Poetry](https://python-poetry.org/).
We use [pytest](https://docs.pytest.org/) for local testing and [GitHub actions](https://docs.github.com/en/actions) for automated tests via containers.

### Getting started

To enable local development, perform the following steps.

1. [Install Python](https://www.python.org/downloads/)
1. [Install Poetry](https://python-poetry.org/docs/#installation)
1. [Install Poetry Environment](https://python-poetry.org/docs/basic-usage/#installing-dependencies): `poetry install`

### Code style

For general Python code style, we use [black](https://github.com/psf/black).
For Python import order, we use [isort](https://github.com/PyCQA/isort).
Please use black and isort before committing any code.
We will not accept code contributions that do not use black and isort.
We use the [Google Style Python Docstrings](https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html).

### Linting

Work added to this repo is automatically checked using [pre-commit](https://pre-commit.com/) via [GitHub Actions](https://docs.github.com/en/actions).
Pre-commit can work alongside your local [git with hooks](https://pre-commit.com/index.html#3-install-the-git-hook-scripts)
After [installing pre-commit](https://pre-commit.com/#installation) within your development environment, the following command also can perform the same checks:

```sh
% pre-commit run --all-files
```

### Testing

Manual testing for this project may be performed using the following tools.
Automated testing is performed using [GitHub Actions](https://docs.github.com/en/actions) and follows the same checks.

1. [pytest](https://pytest.org/en/latest/contents.html) provides unit, functional, or integration testing.
   Example test command: `% poetry run pytest`
1. [sphinx-build](https://www.sphinx-doc.org/en/master/man/sphinx-build.html) provides documentation website build checks via [`-W`](https://www.sphinx-doc.org/en/master/man/sphinx-build.html#cmdoption-sphinx-build-W) (which turns warnings into errors).
   Example command: `% poetry run sphinx-build docs/source docs/build -W`
1. [cffconvert](https://github.com/citation-file-format/cffconvert) provides [CITATION.cff file](https://citation-file-format.github.io/) formatting checks. Example command: `% poetry run cffconvert --validate`
1. [pre-commit](https://pre-commit.com/) provides various checks which are treated as failures in automated testing.
   Example command `% pre-commit run -all-files`

#### Test Coverage

Test coverage is provided via [coverage](https://github.com/nedbat/coveragepy) and [pytest-cov](https://github.com/pytest-dev/pytest-cov).
Use the following command to generate HTML coverage reports (reports made available at `./htmlcov/index.html`):

```sh
% poetry run pytest --cov=cytotable tests/
```

## Documentation

Documentation is generally provided through [MyST (or Markedly Structured Text)](https://myst-parser.readthedocs.io/en/latest/index.html) markdown documents.
Markdown content is transformed into docsite content using [Sphinx](https://www.sphinx-doc.org/).
Documentation content assumes a "one sentence per line" style.
Diagrams may be added using the [Sphinx extension for Mermaid](https://github.com/mgaitan/sphinxcontrib-mermaid#markdown-support).

### Documentation Linting

Documentation content is tested using the `sphinx-build ... -W` command to avoid missing autodoc members, etc.
To check your documentation updates before pushing, use the following to trigger a related `sphinx-build` (content made available at `./docs/build/index.html`):

```sh
% poetry run sphinx-build docs/source doctest -W
```

### Documentation Builds

Documentation builds presume HTML as the primary export, e.g. `sphinx-build -b html ...`.
Documentation is automatically published to a docsite via [GitHub Actions](https://docs.github.com/en/actions).

## Attribution

Portions of this contribution guide were sourced from [pyctyominer](https://github.com/cytomining/pycytominer/blob/master/CONTRIBUTING.md).
Many thanks go to the developers and contributors of that repository.

## Publishing Releases

### Release Locations

We utilize [semantic versioning](https://en.wikipedia.org/wiki/Software_versioning#Semantic_versioning)("semver") in order to distinguish between major, minor, and patch releases.
We publish source code by using [GitHub Releases](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases) available [here](https://github.com/cytomining/CytoTable/releases).
We publish Python packages through the [Python Packaging Index (PyPI)](https://pypi.org/) available [here](https://pypi.org/project/cytotable/).

### Publishing Process

There are several manual and automated steps involved with publishing CytoTable releases.
See below for an overview of how this works.

1. Prepare a release in a code contribution which utilizes the command [`poetry version ...`](https://python-poetry.org/docs/cli/#version) to update the version (this updates `pyproject.toml` with automatically incremented versions under `version = "..."`).
1. Open a pull request and use a repository label for `release-<semver release type>` to label the pull request for visibility with [`release-drafter`](https://github.com/release-drafter/release-drafter).
1. On merging the pull request for the release, a [GitHub Actions workflow](https://docs.github.com/en/actions/using-workflows) defined in `draft-release.yml` leveraging [`release-drafter`](https://github.com/release-drafter/release-drafter) will draft a release for maintainers to modify.
1. Make modifications as necessary to the draft GitHub release, then publish the release.
1. On publishing the release, another GitHub Actions workflow defined in `publish-pypi.yml` will run to build and deploy the Python package to PyPI (utilizing the earlier modified `pyproject.toml` semantic version reference for labeling the release).

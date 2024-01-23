# Reporting Security Issues

CytoTable maintainers and community take security bugs seriously. We appreciate your efforts to responsibly disclose your findings, and will make every effort to acknowledge your contributions.

To report a security issue, please use the GitHub Security Advisory ["Report a Vulnerability"](https://github.com/cytomining/cytotable/security/advisories/new) tab.

## Using "Development" vs "Non-development" Dependencies

A number of development-only dependencies are included with this project for maintenance and testing purposes.
Please see the `pyproject.toml` table `[tool.poetry.dependencies]` for a list of non-development dependencies and `[tool.poetry.group.dev.dependencies]` for a list of development dependencies.
Development dependencies are by default not shipped with distributed versions of the code for this project (for example, distributed code on [PyPI](https://pypi.org/)).
Just the same, we strongly recommend validating included dependencies and potential vulnerabilities for your environment as well as relevant policy requirements.

<img height="200" src="https://raw.githubusercontent.com/cytomining/cytotable/main/logo/with-text-for-light-bg.png?raw=true">

# CytoTable

![dataflow](https://raw.githubusercontent.com/cytomining/cytotable/main/docs/source/_static/dataflow.svg?raw=true)
_Diagram showing data flow relative to this project._

## Summary

CytoTable enables single-cell morphology data analysis by cleaning and transforming CellProfiler (`.csv` or `.sqlite`), cytominer-database (`.sqlite`), and DeepProfiler (`.npz`), and other sources such as IN Carta data output data at scale.
CytoTable creates parquet files for both independent analysis and for input into [Pycytominer](https://github.com/cytomining/pycytominer).
The Parquet files will have a unified and documented data model, including referenceable schema where appropriate (for validation within Pycytominer or other projects).

The name for the project is inspired from:

- __Cyto__: "1. (biology) cell." ([Wiktionary: Cyto-](https://en.wiktionary.org/wiki/cyto-))
- __Table__:
  - "1. Furniture with a top surface to accommodate a variety of uses."
  - "3.1. A matrix or grid of data arranged in rows and columns." <br> ([Wiktionary: Table](https://en.wiktionary.org/wiki/table))

## Installation

Install CytoTable from [PyPI](https://pypi.org/) or from source:

```shell
# install from pypi
pip install cytotable

# install directly from source
pip install git+https://github.com/cytomining/CytoTable.git
```

## Contributing, Development, and Testing

We test CytoTable using `ubuntu-latest` and `macos-latest` [GitHub Actions runner images](https://github.com/actions/runner-images#available-images).

Please see [contributing.md](docs/source/contributing.md) for more details on contributions, development, and testing.

## References

- [pycytominer](https://github.com/cytomining/pycytominer)
- [cytominer-database](https://github.com/cytomining/cytominer-database)
- [DeepProfiler](https://github.com/cytomining/DeepProfiler)
- [CellProfiler](https://github.com/CellProfiler/CellProfiler)
- [cytominer-eval](https://github.com/cytomining/cytominer-eval)

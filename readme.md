<img height="200" src="https://raw.githubusercontent.com/cytomining/cytotable/main/logo/with-text-for-light-bg.png?raw=true">

# CytoTable

![PyPI - Version](https://img.shields.io/pypi/v/cytotable)
[![Build Status](https://github.com/cytomining/cytotable/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/cytomining/cytotable/actions/workflows/test.yml?query=branch%3Amain)
[![Poetry](https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json)](https://python-poetry.org/)
[![Preprint DOI badge](https://img.shields.io/badge/Preprint_DOI-10.1101/2025.06.19.660613-blue)](https://doi.org/10.1101/2025.06.19.660613)
[![Software DOI badge](https://img.shields.io/badge/Software_DOI-10.5281/zenodo.14888111-blue)](https://doi.org/10.5281/zenodo.14888111)

![dataflow](https://raw.githubusercontent.com/cytomining/cytotable/main/docs/source/_static/dataflow.svg?raw=true)
_Diagram showing data flow relative to this project._

## Summary

Image-based profiling often entails preparing data for analysis by transforming the outputs of image analysis tools into a consistent, analysis-ready format.
CytoTable streamlines analyses by harmonizing CellProfiler (`.csv` or `.sqlite`), cytominer-database (`.sqlite`), DeepProfiler (`.npz`), or other sources such as IN Carta output data at scale.
This helps biologists by lowering the barrier between image acquisition and data interpretation, enabling them to focus on biological insights rather than file formats or data wrangling.

CytoTable creates Parquet or AnnData files for both independent analysis and for input into [Pycytominer](https://github.com/cytomining/pycytominer).
The output files (such as [Parquet](https://parquet.apache.org/) and [AnnData](https://github.com/scverse/anndata) file formats) have a documented data model, including referenceable schema where appropriate (for validation within Pycytominer or other image-based profiling projects).

The name for the project is inspired by:

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

## Getting started

Check out the following resources to get started!
We created tutorials which follow a narrative-driven approach.
There are also Jupyter notebooks for pragmatic, hand-on explanations of working with CytoTable.

We suggest image analysts begin with the tutorials and explore the example notebooks afterwards.

| Resource         | What it covers                                  | Link                                                                                                           |
| ---------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Tutorial         | CellProfiler SQLite or CSV to Parquet           | [Open tutorial](https://cytomining.github.io/CytoTable/tutorials/cellprofiler_to_parquet.html)                 |
| Tutorial         | NPZ embeddings to Parquet                       | [Open tutorial](https://cytomining.github.io/CytoTable/tutorials/npz_embeddings_to_parquet.html)               |
| Tutorial         | Multi-plate merge with tablenumber              | [Open tutorial](https://cytomining.github.io/CytoTable/tutorials/multi_plate_merge_tablenumber.html)           |
| Example notebook | CytoTable mise en place (general overview)      | [Open notebook](https://cytomining.github.io/CytoTable/examples/cytotable_mise_en_place_general_overview.html) |
| Example notebook | CytoTable from the cloud (cloud-based overview) | [Open notebook](https://cytomining.github.io/CytoTable/examples/cytotable_from_the_cloud.html)                 |

## Contributing, Development, and Testing

We test CytoTable using `ubuntu-latest` and `macos-latest` [GitHub Actions runner images](https://github.com/actions/runner-images#available-images).

Please see [contributing.md](docs/source/contributing.md) for more details on contributions, development, and testing.

## Relationship to other projects

CytoTable focuses on image-based profiling data harmonization and serialization.
At scale, CytoTable transforms data into file formats which can be directly integrated with:

**Please let us know how you use CytoTable (we'd love to add your project to this list)**!

- [Pycytominer](https://github.com/cytomining/pycytominer) for the bioinformatics pipeline for image-based profiling.
- [coSMicQC](https://github.com/cytomining/coSMicQC) for quality control.
- [CytoDataFrame](https://github.com/cytomining/CytoDataFrame) for interactive visualization of profiles with single cell images.

## References

- [Pycytominer](https://github.com/cytomining/pycytominer)
- [cytominer-database](https://github.com/cytomining/cytominer-database)
- [DeepProfiler](https://github.com/cytomining/DeepProfiler)
- [CellProfiler](https://github.com/CellProfiler/CellProfiler)
- [cytominer-eval](https://github.com/cytomining/cytominer-eval)

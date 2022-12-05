# pycytominer-transform

![dataflow](docs/source/_static/dataflow.svg)
_Diagram showing data flow relative to this project._

## Summary

pycytominer-transform performs data transformations to assist cell biology image analysis for [Pycytominer](https://github.com/cytomining/pycytominer).
It takes input data from CSV's, SQLite, or NPZ, for conversion to Pycytominer relevant output in Parquet format.
The Parquet files will have a unified and documented data model, including referenceable schema where appropriate (for validation within Pycytominer or other projects).

## Installation

Install pycytominer-transform with the following command:

```shell
pip install git+https://github.com/cytomining/pycytominer-transform.git
```

## Contributing, Development, and Testing

Please see [contributing.md](docs/source/contributing.md) for more details on contributions, development, and testing.

## References

- [pycytominer](https://github.com/cytomining/pycytominer)
- [cytominer-database](https://github.com/cytomining/cytominer-database)
- [DeepProfiler](https://github.com/cytomining/DeepProfiler)
- [CellProfiler](https://github.com/CellProfiler/CellProfiler)
- [cytominer-eval](https://github.com/cytomining/cytominer-eval)

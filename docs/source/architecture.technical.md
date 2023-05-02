# Technical Architecture

Documentation covering technical architecture for CytoTable.

## Workflows

CytoTable uses [Parsl](https://parsl.readthedocs.io/) to execute collections of tasks as [`python_app`'s](https://parsl.readthedocs.io/en/stable/quickstart.html#application-types).
In Parsl, work may be isolated using Python functions decorated with the `@python_app` decorator.
[`join_app`'s](https://parsl.readthedocs.io/en/stable/1-parsl-introduction.html#Dynamic-workflows-with-apps-that-generate-other-apps) are collections of one or more other apps and are decorated using the `@join_app` decorator.
See the following documentation for more information on how apps may be used within Parsl: [Parsl: Apps](https://parsl.readthedocs.io/en/stable/userguide/apps.html)

### Workflow Execution

Procedures within CytoTable are executed using [Parsl Executors](https://parsl.readthedocs.io/en/stable/userguide/execution.html).
Parsl Executors may be configured through [Parsl Configuration's](https://parsl.readthedocs.io/en/stable/userguide/execution.html#configuration).

```{eval-rst}
Parsl configurations may be passed to :code:`convert(..., parsl_config=parsl.Config)` (:mod:`convert() <cytotable.convert.convert>`)
```

By default, CytoTable assumes local task execution with [LocalProvider](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LocalProvider.html#parsl.providers.LocalProvider).
For greater scalability, CytoTable may be used with a [HighThroughputExecutor](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html#parsl.executors.HighThroughputExecutor) (See [Parsl's scalability documentation](https://parsl.readthedocs.io/en/stable/userguide/performance.html) for more information).

## Data Technologies

### Data Paths

Data source paths handled by CytoTable may be local or cloud-based paths.
Local data paths are handled using [Python's Pathlib](https://docs.python.org/3/library/pathlib.html) module.
Cloud-based data paths are managed by [cloudpathlib](https://cloudpathlib.drivendata.org/~latest/).
Reference the following page for how cloudpathlib client arguments may be used: [Overview: Data Source Locations](overview.md#data-source-locations)

#### Data Paths - Cloud-based SQLite

SQLite data stored in cloud-based paths are downloaded locally using cloudpathlib's [caching capabilities](https://cloudpathlib.drivendata.org/stable/caching/) to perform SQL queries.
Using data in this way may require the use of an additional parameter for the cloud storage provider to set the cache directory explicitly to avoid storage limitations (some temporary directories are constrained to system memory, etc).

For example:

```python
import cytotable

# Convert CellProfiler SQLite to parquet
cytotable.convert(
    source_path="s3://bucket-name/single-cells.sqlite",
    dest_path="test.parquet",
    dest_datatype="parquet",
    # set the local cache dir to `./tmpdata`
    # this will get passed to cloudpathlib's client
    local_cache_dir="./tmpdata",
)
```

### In-process Data Format

In addition to using Python native data types, we also accomplish internal data management for CytoTable using [PyArrow (Apache Arrow) Tables](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html).
Using Arrow-compatible formats is intended to assist cross-platform utility, encourage high-performance, and enable advanced data integration with non-Python tools.

### SQL-based Data Management

We use the [DuckDB Python API client](https://duckdb.org/docs/api/python/overview) in some areas to interface with [SQL](https://en.wikipedia.org/wiki/SQL) (for example, SQLite databases) and other data formats.
We use DuckDB SQL statements to organize joined datasets or tables as Arrow format results.

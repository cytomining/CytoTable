# Tutorial

This page covers brief tutorials and notes on how to use pycytominer-transform.

## Data Sources

### Data Source Locations

```{eval-rst}
Data sources may be provided to pycytominer-transform using local filepaths or remote object-storage filepaths (for example, AWS S3, GCP Cloud Storage, Azure Storage).
We use `cloudpathlib <https://cloudpathlib.drivendata.org/~latest/>`_  under the hood to reference files in a unified way, whether they're local or remote.

Remote object storage paths which require authentication or other specialized configuration may use cloudpathlib client arguments (`S3Client <https://cloudpathlib.drivendata.org/~latest/api-reference/s3client/>`_, `AzureBlobClient <https://cloudpathlib.drivendata.org/~latest/api-reference/azblobclient/>`_, `GSClient <https://cloudpathlib.drivendata.org/~latest/api-reference/gsclient/>`_) and :code:`convert(..., **kwargs)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
For example, remote AWS S3 paths which are public-facing and do not require authentication (like, or similar to, :code:`aws s3 ... --no-sign-request`) may be used via :code:`convert(..., no_sign_request=True)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

### Data Source Types

- __Comma-separated values (CSV)__: "A comma-separated values (CSV) file is a delimited text file that uses a comma to separate values." ([reference](https://en.wikipedia.org/wiki/Comma-separated_values))
  CSV data sources generally follow the format provided as output by [CellProfiler ExportToSpreadsheet](https://cellprofiler-manual.s3.amazonaws.com/CPmanual/ExportToSpreadsheet.html).

```{eval-rst}
  CSV data source type may be specified by using :code:`convert(..., source_datatype="csv", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

## Data Destinations

### Data Destination Locations

```{eval-rst}
Converted data destinations are may be provided to pycytominer-transform using only local filepaths (in contrast to data sources, which may also be remote).
Specify the converted data destination using the  :code:`convert(..., dest_path="<a local filepath>")` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

### Data Destination Types

- __Apache Parquet__: "Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.
  It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk." ([reference](https://parquet.apache.org/))

```{eval-rst}
  Parquet data destination type may be specified by using :code:`convert(..., dest_datatype="parquet", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

## CellProfiler CSV Output to Parquet

[CellProfiler](https://cellprofiler.org/) pipelines or projects may produce various CSV-based compartment output (for example, "Cells.csv", "Cytoplasm.csv", etc.).
Pycytominer-transform converts this data to Parquet from local or object-storage based locations.

Files with similar names nested within sub-folders will be concatenated by default (appended to the end of each data file) together and used to create a single Parquet file per data target.
For example: if we have `folder/subfolder_a/cells.csv` and `folder/subfolder_b/cells.csv`, using `convert(source_path="folder", ...)` will result in `folder.cells.parquet` (unless `concat=False`).

For example, see below:

```python
from pycytominer_transform import convert

# using a local path
convert(
    source_path="./local/file/path",
    source_datatype="csv",
    dest_path=".",
    dest_datatype="parquet",
)

# using an s3-compatible path with no signature for client
convert(
    source_path="s3://s3path",
    source_datatype="csv",
    dest_path=".",
    dest_datatype="parquet",
    concat=True,
    no_sign_request=True,
)
```

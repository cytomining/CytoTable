# Tutorial

This page covers brief tutorials on how to use pycytominer-transform.

## Data Sources

```{eval-rst}
Data sources may be provided to pycytominer-transform using local filepaths or remote object-storage filepaths (for example, AWS S3, GCP Cloud Storage, Azure Storage).
We use `cloudpathlib <https://cloudpathlib.drivendata.org/~latest/>`_  under the hood to reference files in a unified way, whether they're local or remote.

Remote object storage paths which require authentication or other specialized configuration may use cloudpathlib client arguments (`S3Client <https://cloudpathlib.drivendata.org/~latest/api-reference/s3client/>`_, `AzureBlobClient <https://cloudpathlib.drivendata.org/~latest/api-reference/azblobclient/>`_, `GSClient <https://cloudpathlib.drivendata.org/~latest/api-reference/gsclient/>`_) and :code:`convert(..., **kwargs)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
For example, remote AWS S3 paths which are public-facing and do not require authentication (like, or similar to, :code:`aws s3 ... --no-sign-request`) may be used via :code:`convert(..., no_sign_request=True)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

## Data Conversion Types

Source data may be converted to the following type(s):

- __Apache Parquet__: "Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk." ([reference](https://parquet.apache.org/))

## CellProfiler CSV Output to Parquet

CellProfiler pipelines or projects may produce various CSV-based component output (for example, "Cells.csv", "Cytoplasm.csv", etc.).
This data may be converted to Parquet using local or object-storage based sources.

By default data with common names nested within sub-folders will be concatenated (appended to the end of each data file) together and used to create a single Parquet file per data target.
For example: if we have `folder/subfolder_a/cells.csv` and `folder/subfolder_b/cells.csv`, when using `convert(source_path="folder", ...)` files will by default be combined into `folder.cells.parquet` within the destination path (unless `concat=False`).

For example, see below:

```python
from pycytominer_transform import convert

# using an local path
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

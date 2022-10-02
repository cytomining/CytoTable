# Tutorial

This page covers a brief tutorial on how to use pycytominer-transform.

## Data Source Locations

Data sources may be provided to pycytominer-transform using local filepaths or object-storage (for example, "AWS S3", "GCP Cloud Storage", "Azure Storage") filepaths (via [cloudpathlib](https://cloudpathlib.drivendata.org/~latest/)).
Object storage paths which are public-facing and do not require authentication may be enabled using `convert(..., no_sign_request=True)`.
Object storage paths which require configuration may be enabled using [cloudpathlib authentication options](https://cloudpathlib.drivendata.org/~latest/authentication/) and `convert(..., **kwargs)`.

## CellProfiler CSV Output to Parquet

CellProfiler pipelines or projects may produce various CSV-based component output (for example, "Cells.csv", "Cytoplasm.csv", etc.).
This data may be converted to parquet using local or object-storage based sources.

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
    default_targets=True,
    dest_datatype="parquet",
)

# using an s3-compatible path with no signature for client
convert(
    source_path="s3://s3path",
    source_datatype="csv",
    dest_path=".",
    dest_datatype="parquet",
    concat=True,
    default_targets=True,
    no_sign_request=True,
)
```

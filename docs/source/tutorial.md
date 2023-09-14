# Tutorial

This page covers brief tutorials and notes on how to use CytoTable.

## CellProfiler CSV Output to Parquet

[CellProfiler](https://cellprofiler.org/) pipelines or projects may produce various CSV-based compartment output (for example, "Cells.csv", "Cytoplasm.csv", etc.).
CytoTable converts this data to Parquet from local or object-storage based locations.

Files with similar names nested within sub-folders will be concatenated by default (appended to the end of each data file) together and used to create a single Parquet file per compartment.
For example: if we have `folder/subfolder_a/cells.csv` and `folder/subfolder_b/cells.csv`, using `convert(source_path="folder", ...)` will result in `folder.cells.parquet` (unless `concat=False`).

Note: The `dest_path` parameter (`convert(dest_path="")`) will be used for intermediary data work and must be a new file or directory path.
This path will result directory output on `join=False` and a single file output on `join=True`.

For example, see below:

```python
from cytotable import convert

# using a local path with cellprofiler csv presets
convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="ExampleHuman.parquet",
    dest_datatype="parquet",
    preset="cellprofiler_csv",
)

# using an s3-compatible path with no signature for client
# and cellprofiler csv presets
convert(
    source_path="s3://s3path",
    source_datatype="csv",
    dest_path="s3_local_result",
    dest_datatype="parquet",
    concat=True,
    preset="cellprofiler_csv",
    no_sign_request=True,
)
```

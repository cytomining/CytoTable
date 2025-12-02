# Tutorials and How-to Guides

Start here if you are new to CytoTable. We’ve split material by audience:

- **Image analysts (no engineering background required):** follow the narrative tutorials below. They include downloadable data, exact commands, and what to expect.
- **Engineers / power users:** see the Software Engineering Guide for tuning and integration details, or use the quick recipe below.

```{toctree}
---
maxdepth: 2
caption: Narrative tutorials (start here)
---
tutorials/cellprofiler_sqlite_to_parquet
software_engineering
```

## Quick how-to: CellProfiler CSV to Parquet (recipe)

This short recipe is for people comfortable with Python/CLI and parallels our older tutorial. If you prefer a guided, narrative walkthrough with downloadable inputs and expected outputs, use the tutorial above.

[CellProfiler](https://cellprofiler.org/) exports compartment CSVs (for example, "Cells.csv", "Cytoplasm.csv"). CytoTable converts this data to Parquet from local or object-storage locations.

Files with similar names nested within sub-folders are concatenated by default (for example, `folder/sub_a/cells.csv` and `folder/sub_b/cells.csv` become a single `folder.cells.parquet` unless `concat=False`).

The `dest_path` parameter is used for intermediary work and must be a new file or directory path. It will be a directory when `join=False` and a single file when `join=True`.

```python
from cytotable import convert

# Local CSVs with CellProfiler preset
convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="ExampleHuman.parquet",
    dest_datatype="parquet",
    preset="cellprofiler_csv",
)

# S3 CSVs (unsigned) with CellProfiler preset
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

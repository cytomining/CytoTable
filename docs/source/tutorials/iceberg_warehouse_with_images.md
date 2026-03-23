# Tutorial: Linking CellProfiler morphology features with single-cell image crops

The Apache ecosystem tool, Iceberg, builds a warehouse data structure to manage connections between single-cell features and OME-arrow image crops.

In this tutorial, you will see a start-to-finish walkthrough of using CytoTable to link CellProfiler measurements and cropped microscopy images.

## What you will accomplish

- Convert CellProfiler outputs (e.g., SQLite file) to an Iceberg warehouse instead of a single Parquet file (which is default CytoTable behavior).
- Using Iceberg, create a materialized `profiles.joined_profiles` table that connects single-cell profiles to cropped images.
- Optionally build a separate `images.image_crops` Iceberg table containing OME-Arrow image crops.
- Optionally build a separate `images.source_images` Iceberg table containing full OME-Arrow source images.
- Save a `profiles.profile_with_images` warehouse "view" that displays joined profiles with image crops.
- Overlay mask or outline images into this "view".

````{admonition} When to use this tutorial
- Use this tutorial when you want to bundle single-cell morphology features with single-cell image crops instead of a single Parquet file.
- Skip this tutorial if you only need the standard joined measurement table; use the Parquet tutorial instead.

## Setup

*Note:* This tutorial installs `cytotable[iceberg-images]` rather than
`cytotable[iceberg]` because the image tables and views require both
`pyiceberg` and `ome-arrow`. If you only need the Iceberg warehouse for profile
tables and do not plan to export `images.image_crops` or `images.source_images`,
then `cytotable[iceberg]` is sufficient. Image crop export requires Python 3.11
or newer because the optional `ome-arrow` dependency is only available on
Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "cytotable[iceberg-images]"
````

## Inputs and outputs

- **Measurement input:** a CellProfiler SQLite file or CSV folder
- **Image input:** a local directory or cloud object-storage prefix containing source TIFF files
- **Optional segmentation input:** a local directory or cloud object-storage prefix containing mask and/or outline TIFF files
- **Output:** a new local Iceberg warehouse directory containing
  `profiles.joined_profiles` and, when requested, image tables and views
  such as `images.image_crops`, `images.source_images`, and
  `profiles.profile_with_images` (see below for more details on Iceberg
  warehouse outputs)

## Basic warehouse export

This creates a materialized `profiles.joined_profiles` table in Iceberg.

```python
from cytotable import convert

warehouse_path = convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="./example_warehouse",
    dest_backend="iceberg",
    dest_datatype="parquet",
    preset="cellprofiler_csv",
)

print(warehouse_path)
```

*Note:* `dest_backend="iceberg"` means the final output is an Iceberg
warehouse. CytoTable still requires `dest_datatype="parquet"` because CytoTable stages
joined data as parquet internally before writing the warehouse tables.

## Add image crops with OME-Arrow

When a user specifies `image_dir`, CytoTable uses temporary parquet staging and
chunked single-cell joins to append cropped image payloads into a separate
`images.image_crops` table inside the warehouse.

`image_dir`, `mask_dir`, and `outline_dir` may reference local paths or cloud
object-storage paths, following the same `s3://...`, `gs://...`, or `az://...`
style supported for measurement inputs. If your cloud provider needs extra
configuration, pass the relevant `cloudpathlib` client arguments through
`convert(..., **kwargs)`.

If `image_dir` is not provided, CytoTable writes only the profile-side Iceberg
output, which is usually the right choice when you only need measurement data
or want a lighter-weight warehouse. Provide `image_dir` when you want the
warehouse to connect single-cell profiles to cropped images.

```python
from cytotable import convert

warehouse_path = convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="./example_warehouse_with_images",
    dest_backend="iceberg",
    dest_datatype="parquet",
    preset="cellprofiler_csv",
    image_dir="./images",
    mask_dir="./masks",
    outline_dir="./outlines",
)

print(warehouse_path)
```

Important behavior:

- image export requires `dest_backend="iceberg"`
- image export requires `join=True` (the default)
- CytoTable writes cropped images to a separate `images.image_crops` table
- full source images may also be written to `images.source_images` with `include_source_images=True`
- CytoTable deterministically generates `Metadata_ObjectID` values in
  `images.image_crops` for object-level references, rather than assigning them
  randomly
- CytoTable also deterministically generates `Metadata_ImageCropID` values
  unique to each crop row
- when both `outline_dir` and `mask_dir` produce a matching overlay for the
  same source image, CytoTable stores both and uses the outline for the
  generated `ome_arrow_label` overlay field

If you also want the original images stored in the warehouse:

```python
convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="./example_warehouse_with_source_images",
    dest_backend="iceberg",
    preset="cellprofiler_csv",
    image_dir="./images",
    include_source_images=True,
)
```

The same pattern also works with cloud image paths, for example
`image_dir="s3://example-bucket/images"` plus any needed authentication or
client configuration arguments passed through `convert(..., **kwargs)`.

## Bounding boxes

CytoTable uses bounding box columns from the joined measurement rows to dynamically crop each image.
In the materialized `joined_profiles` table, the resolved bbox columns are
recoded as `Metadata_SourceBBoxXMin`, `Metadata_SourceBBoxXMax`,
`Metadata_SourceBBoxYMin`, and `Metadata_SourceBBoxYMax`.

CytoTable searches for bounding box columns in the following order and only moves to
the next option if the earlier one does not provide all four required columns:

1. user-defined explicit setting of `bbox_column_map` in `CytoTable.convert()`
1. CellProfiler-style `AreaShape_BoundingBox...` column names
1. substring fallback using `Minimum_X`, `Maximum_X`, `Minimum_Y`, `Maximum_Y`

*Note:* The substring fallback is a broad last-resort match. It is useful when
your data do not follow the standard CellProfiler bbox naming conventions, but
if multiple unrelated columns contain those substrings, you should prefer an
explicit `bbox_column_map`.

If you need to override the automatic choice:

```python
convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="./example_warehouse_bbox_override",
    dest_backend="iceberg",
    preset="cellprofiler_csv",
    image_dir="./images",
    bbox_column_map={
        "x_min": "Cells_AreaShape_BoundingBoxMinimum_X",
        "x_max": "Cells_AreaShape_BoundingBoxMaximum_X",
        "y_min": "Cells_AreaShape_BoundingBoxMinimum_Y",
        "y_max": "Cells_AreaShape_BoundingBoxMaximum_Y",
    },
)
```

## Segmentation matching

By default, CytoTable matches the CellProfiler columns that store mask and outline information by basename or stem.

If your segmentation files follow a different naming convention, use `segmentation_file_regex` to map segmentation filename patterns to source image filename patterns.

```python
convert(
    source_path="./tests/data/cellprofiler/ExampleHuman",
    source_datatype="csv",
    dest_path="./example_warehouse_regex_match",
    dest_backend="iceberg",
    preset="cellprofiler_csv",
    image_dir="./images",
    outline_dir="./outlines",
    segmentation_file_regex={
        r".*_outline\\.tiff$": r"(plateA_well_B03_site_1)\\.tiff$",
    },
)
```

The mapping uses:

- key: regex for segmentation filenames
- value: regex for the source image filename

## Reading the warehouse

CytoTable exposes helper functions for local Iceberg warehouses so you can list
available tables and views, inspect the warehouse contents, and read tables
back into Python:

```python
from cytotable import describe_iceberg_warehouse, list_tables, read_table

print(list_tables("./example_warehouse_with_images"))
print(describe_iceberg_warehouse("./example_warehouse_with_images"))

profiles = read_table("./example_warehouse_with_images", "joined_profiles")
image_crops = read_table("./example_warehouse_with_images", "image_crops")
profile_with_images = read_table(
    "./example_warehouse_with_images", "profile_with_images"
)
```

If you want to see the rendered outputs from this workflow, including example
table listings and readback results, see the notebook example
`examples/cytotable_with_profiles_and_images`.

Unqualified reads still work for unique table or view names. The current layout is:

- `profiles.joined_profiles`
- `images.image_crops`
- `images.source_images`
- `profiles.profile_with_images`

The same helpers also work for a single Parquet file:

```python
from cytotable import list_tables, read_table

print(list_tables("./ExampleHuman.parquet"))
profiles = read_table("./ExampleHuman.parquet")
```

## What success looks like

*Note:* In this tutorial, a "materialized table" means a table whose rows are
stored directly in the warehouse. A "view" stores the logic for producing a
result and re-runs that logic each time you read it, rather than storing its
own rows directly.

- the warehouse directory exists and contains Iceberg metadata/data files
- `profiles.joined_profiles` appears as a materialized table, meaning a stored
  table with data rather than just a saved view definition
- `images.image_crops` appears as a table only when the user specifies the
  `image_dir` argument and crop rows are actually written; this is where
  CytoTable stores single-cell image crops as separate rows in the warehouse
- `images.source_images` appears as a table only when `include_source_images=True`
- `profiles.profile_with_images` appears as a saved view only when `images.image_crops` exists and contains rows
- `images.image_crops` rows include a deterministic `Metadata_ObjectID`
  derived from measurement keys and crop bounds, rather than a random ID
- `images.image_crops` rows include a deterministic
  `Metadata_ImageCropID` derived from measurement keys, crop bounds, and the
  source image reference
- `images.image_crops` rows include `ome_arrow_image` data and optional
  `ome_arrow_label` data stored as OME-Arrow objects
- `images.source_images` rows include a deterministic `Metadata_ImageID`
  derived from image-level keys and the source image reference

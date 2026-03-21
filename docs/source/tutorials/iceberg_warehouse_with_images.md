# Tutorial: Linking CellProfiler morphology features with single-cell image crops

The Apache ecosystem tool, Iceberg, builds a warehouse data structure to manage connections between single-cell features and OME-arrow image crops.

In this tutorial, you will see a start-to-finish walkthrough of using CytoTable to link CellProfiler measurements and cropped microscopy images.

## What you will accomplish

- Convert CellProfiler outputs (e.g., SQLite file) to an Iceberg warehouse instead of a single Parquet file (which is default CytoTable behavior).
- Using Iceberg, create a materialized `profiles.joined_profiles` table that
  connects single-cell profiles to cropped images.
- Optionally build a separate `images.image_crops` Iceberg table containing OME-Arrow image crops.
- Optionally build a separate `images.source_images` Iceberg table containing full OME-Arrow source images.
- Add a saved `profiles.profile_with_images` warehouse view that manifests joined profiles with image crop references.
- Overlay mask or outline images into this "view".

````{admonition} When to use this tutorial
- Use this tutorial when you want to bundle single-cell morphology features with single-cell image crops instead of a single Parquet file.
- Skip this tutorial if you only need the standard joined measurement table; use the Parquet tutorial instead.

## Setup

*Note:* Image crop export requires Python 3.11 or newer because the optional `ome-arrow`
dependency is only available on Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "cytotable[iceberg-images]"
````

## Inputs and outputs

- **Measurement input:** a CellProfiler SQLite file or CSV folder
- **Image input:** a directory containing source TIFF files
- **Optional segmentation input:** a directory or directories containing mask and/or outline TIFF files
- **Output:** a new local Iceberg warehouse directory

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

## Add image crops with OME-Arrow

When `image_dir` is provided, CytoTable will process joined rows in chunks and append cropped image payloads into a separate `images.image_crops` table inside the warehouse.

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
- cropped images are written to a separate `images.image_crops` table
- full source images may also be written to `images.source_images` with `include_source_images=True`
- each `images.image_crops` row includes a stable `Metadata_ObjectID` for object-level references
- each `images.image_crops` row also includes a stable `Metadata_ImageCropID` unique to that crop row
- outline files are preferred over mask files when both resolve

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

## Bounding boxes

CytoTable uses bounding box columns from the joined measurement rows to dynamically crop each image.
In the materialized `joined_profiles` table, the resolved bbox columns are
normalized as `Metadata_SourceBBoxXMin`, `Metadata_SourceBBoxXMax`,
`Metadata_SourceBBoxYMin`, and `Metadata_SourceBBoxYMax`.

Resolution order:

1. explicit `bbox_column_map`
1. CellProfiler-style `AreaShape_BoundingBox...` column names
1. substring fallback using `Minimum_X`, `Maximum_X`, `Minimum_Y`, `Maximum_Y`

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

CytoTable exposes small helpers for local Iceberg warehouses:

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

- the warehouse directory exists and contains Iceberg metadata/data files
- `profiles.joined_profiles` appears as a materialized table
- `images.image_crops` appears as a table only when `image_dir` was provided and crop rows were actually written
- `images.source_images` appears as a table only when `include_source_images=True`
- `profiles.profile_with_images` appears as a saved view only when `images.image_crops` exists and contains rows
- `images.image_crops` rows include a stable `Metadata_ObjectID` derived from measurement keys and crop bounds
- `images.image_crops` rows include a stable `Metadata_ImageCropID` derived from measurement keys, crop bounds, and the source image reference
- `images.image_crops` rows include `ome_arrow_image` and optional `ome_arrow_label` payloads
- `images.source_images` rows include a stable `Metadata_ImageID` derived from image-level keys and the source image reference

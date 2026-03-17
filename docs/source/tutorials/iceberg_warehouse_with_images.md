# Tutorial: CellProfiler to Iceberg Warehouse with OME-Arrow Image Crops

A start-to-finish walkthrough for users who want to keep CytoTable measurements and cropped images together in a local Iceberg warehouse.

## What you will accomplish

- Convert CellProfiler outputs to an Iceberg warehouse instead of a single Parquet file.
- Store the normalized measurement tables and joined view in Iceberg.
- Optionally build a separate `image_crops` Iceberg table containing OME-Arrow image crops.
- Add a saved `profile_with_images` warehouse view that manifests joined profiles with image crop references.
- Use mask or outline images when available, with optional regex-based segmentation matching.

```{admonition} When to use this tutorial
- Use this workflow when you want a multi-table result bundle instead of a single joined Parquet file.
- Use it when you need cropped image payloads stored next to measurements for downstream analysis.
- Skip it if you only need the standard joined measurement table; the Parquet tutorial is simpler.
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "cytotable[iceberg-images]"
```

## Inputs and outputs

- **Measurement input:** a CellProfiler SQLite file or CSV folder
- **Image input:** a directory containing source TIFF files
- **Optional segmentation input:** directories containing mask or outline TIFF files
- **Output:** a new local Iceberg warehouse directory

## Basic warehouse export

This creates the normal CytoTable measurement tables in Iceberg.

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

When `image_dir` is provided, CytoTable will process joined rows in chunks and append cropped image payloads into a separate `image_crops` table inside the warehouse.

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
- cropped images are written to a separate `image_crops` table
- each `image_crops` row includes a stable `object_id` for cross-table references
- outline files are preferred over mask files when both resolve

## Bounding boxes

CytoTable uses bounding box columns from the joined measurement rows to crop each image.

Resolution order:

1. explicit `bbox_column_map`
2. CellProfiler-style `AreaShape_BoundingBox...` column names
3. substring fallback using `Minimum_X`, `Maximum_X`, `Minimum_Y`, `Maximum_Y`

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

By default, CytoTable matches masks and outlines by basename or stem.

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
from cytotable import describe_iceberg_warehouse, list_iceberg_tables, read_iceberg_table

print(list_iceberg_tables("./example_warehouse_with_images"))
print(describe_iceberg_warehouse("./example_warehouse_with_images"))

joined = read_iceberg_table("./example_warehouse_with_images", "cytotable_joined")
image_crops = read_iceberg_table("./example_warehouse_with_images", "image_crops")
profile_with_images = read_iceberg_table(
    "./example_warehouse_with_images", "profile_with_images"
)
```

## What success looks like

- the warehouse directory exists and contains Iceberg metadata/data files
- `cytotable_joined` appears as a saved view
- `profile_with_images` appears as a saved view when image export is enabled
- `image_crops` appears as a table when `image_dir` was provided
- `image_crops` rows include a stable `object_id` value derived from measurement keys, source image column, and crop bounds
- `image_crops` rows include `ome_image` and optional `ome_label` payloads

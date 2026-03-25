# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: CytoTable
#     language: python
#     name: python3
# ---

# # CytoTable with Iceberg profiles and images
#
# This notebook shows how CytoTable can:
#
# - write a materialized `profiles.joined_profiles` table
# - write cropped images to `images.image_crops`
# - optionally write whole source images to `images.source_images`
# - read both Iceberg and single-Parquet outputs through the same
#   `cytotable.read_table` and `cytotable.list_tables` API
#
# The example uses the local `ExampleHuman` CellProfiler dataset bundled with the
# repository test data.

# +
import pathlib
import shutil

from ome_arrow import OMEArrow

import cytotable

# paths used throughout the notebook
source_path = "../../../tests/data/cellprofiler/ExampleHuman"
warehouse_path = pathlib.Path("./examplehuman_iceberg_warehouse")
parquet_path = pathlib.Path("./examplehuman.parquet")
# -

# Remove prior outputs so the example can be rerun cleanly.

if warehouse_path.exists():
    shutil.rmtree(warehouse_path)
if parquet_path.exists():
    parquet_path.unlink()

# Show the available CellProfiler source files.

sorted(path.name for path in pathlib.Path(source_path).glob("*"))

# ## Build an Iceberg warehouse with joined profiles, image crops, and source images

# +
# %%time

iceberg_result = cytotable.convert(
    source_path=source_path,
    source_datatype="csv",
    dest_path=str(warehouse_path),
    dest_backend="iceberg",
    dest_datatype="parquet",
    preset="cellprofiler_csv",
    image_dir=source_path,
    include_source_images=True,
)
print(iceberg_result)
# -

# List the tables and views in the warehouse using the top-level CytoTable API.

cytotable.list_tables(warehouse_path)

# Read the joined measurement table.

joined_profiles = cytotable.read_table(warehouse_path, "joined_profiles")
joined_profiles[["Metadata_ObjectID"]].head()

# Read the object-level image crop table.

image_crops = cytotable.read_table(warehouse_path, "image_crops")
image_crops[
    [
        "Metadata_ObjectID",
        "Metadata_ImageCropID",
        "source_image_file",
        "ome_arrow_image",
    ]
].head()

# Read the full source image table.

source_images = cytotable.read_table(warehouse_path, "source_images")
source_images[
    [
        "Metadata_ImageID",
        "source_image_file",
        "ome_arrow_image",
    ]
].head()

# Read the profile-centric manifest view that joins profile rows to image crop
# references.

profile_with_images = cytotable.read_table(warehouse_path, "profile_with_images")
profile_with_images[["Metadata_ObjectID", "source_image_file"]].head()

# ## Show a cropped image next to profile values
#
# The image tables store `OME-Arrow` objects rather than plain NumPy arrays, so
# this helper searches the nested object for the first array-like image payload
# and reshapes it into something Matplotlib can display.

# use ome_arrow to display a whole image (from the source_images table)
OMEArrow(source_images["ome_arrow_image"].iloc[0])

# use ome_arrow to display a cropped image (from the image_crops table)
OMEArrow(image_crops["ome_arrow_image"].iloc[3])

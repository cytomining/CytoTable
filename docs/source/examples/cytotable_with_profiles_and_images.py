# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.19.1
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

# +
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython.display import display


def extract_display_array(ome_arrow_value):
    """
    Find the first array-like image payload inside an OME-Arrow object.
    """

    if ome_arrow_value is None:
        return None

    if hasattr(ome_arrow_value, "as_py"):
        ome_arrow_value = ome_arrow_value.as_py()

    def walk(value):
        if value is None:
            return None
        if isinstance(value, dict):
            for nested in value.values():
                result = walk(nested)
                if result is not None:
                    return result
            return None
        if isinstance(value, (list, tuple)):
            if value and all(
                isinstance(item, (int, float, np.integer, np.floating))
                for item in value
            ):
                array = np.asarray(value)
                if array.ndim >= 2:
                    return array
            for nested in value:
                result = walk(nested)
                if result is not None:
                    return result
            return None
        if isinstance(value, np.ndarray):
            return value if value.ndim >= 2 else None
        return None

    array = walk(ome_arrow_value)
    if array is None:
        return None
    array = np.asarray(array)
    if array.ndim == 2:
        return array
    if array.ndim == 3:
        return array[0] if array.shape[0] <= array.shape[-1] else array[..., 0]
    return np.squeeze(array)


example_row = profile_with_images.dropna(subset=["ome_arrow_image"]).iloc[0]
example_profile = joined_profiles.loc[
    joined_profiles["Metadata_ObjectID"] == example_row["Metadata_ObjectID"]
].iloc[0]
example_image = extract_display_array(example_row["ome_arrow_image"])

profile_preview = pd.Series(
    {
        "Metadata_ObjectID": example_profile["Metadata_ObjectID"],
        "Metadata_ImageNumber": example_profile.get("Metadata_ImageNumber"),
        "Metadata_SourceBBoxXMin": example_profile.get("Metadata_SourceBBoxXMin"),
        "Metadata_SourceBBoxXMax": example_profile.get("Metadata_SourceBBoxXMax"),
        "Metadata_SourceBBoxYMin": example_profile.get("Metadata_SourceBBoxYMin"),
        "Metadata_SourceBBoxYMax": example_profile.get("Metadata_SourceBBoxYMax"),
        "source_image_file": example_row["source_image_file"],
    }
)

display(profile_preview.to_frame(name="value"))

fig, ax = plt.subplots(figsize=(4, 4))
if example_image is None:
    ax.text(
        0.5,
        0.5,
        "No displayable image array\nfound in ome_arrow_image",
        ha="center",
        va="center",
    )
    ax.set_axis_off()
else:
    ax.imshow(example_image, cmap="gray")
    ax.set_title(str(example_row["source_image_file"]))
    ax.set_axis_off()

plt.show()

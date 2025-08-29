# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # CytoTable mise en place
#
# This notebook includes a quick demonstration of CytoTable to help you understand the basics of using the package and the biological basis of each step.
#
# The name of the notebook comes from the french _mise en place_:
# > "Mise en place (French pronunciation: [mi zɑ̃ ˈplas]) is a French culinary phrase which means "putting in place"
# > or "gather". It refers to the setup required before cooking, and is often used in professional kitchens to
# > refer to organizing and arranging the ingredients ..."
# > - [Wikipedia](https://en.wikipedia.org/wiki/Mise_en_place)

# +
import pathlib
from collections import Counter

import pandas as pd
import pyarrow.parquet as pq
from IPython.display import Image, display
from PIL import Image

import cytotable

# setup variables for use throughout the notebook
source_path = "../../../tests/data/cellprofiler/examplehuman"
dest_path = "./example.parquet"
# -

# remove the dest_path if it's present
if pathlib.Path(dest_path).is_file():
    pathlib.Path(dest_path).unlink()

# show the files we will use as source data with CytoTable
list(pathlib.Path(source_path).glob("*"))

# +
# display the images we will gather features from
image_name_map = {"d0.tif": "DNA", "d1.tif": "PH3", "d2.tif": "Cells"}

for image in pathlib.Path(source_path).glob("*.tif"):
    stain = ""
    for key, val in image_name_map.items():
        if key in str(image):
            stain = val
    print(f"\nImage with stain: {stain}")
    display(Image.open(image))
# -

# show the segmentations through an overlay with outlines
for image in pathlib.Path(source_path).glob("*Overlay.png"):
    print(f"Image outlines from segmentation (composite)")
    print("Color key: {dark blue: nuclei, light blue: cells, yellow: PH3}")
    display(Image.open(image))

for profiles in pathlib.Path(source_path).glob("*.csv"):
    print(f"\nProfiles from CellProfiler: {profiles}")
    display(pd.read_csv(profiles).head())

# +
# %%time

# run cytotable convert
result = cytotable.convert(
    source_path=source_path,
    dest_path=dest_path,
    # specify a destination data format type
    dest_datatype="parquet",
    # specify a preset which enables quick use of common input file formats
    preset="cellprofiler_csv",
)
result.name
# -

# show the table head using pandas
pq.read_table(source=result).to_pandas().head()

# show metadata for the result file
pq.read_metadata(result)

# show schema metadata which includes CytoTable information
# note: this information will travel with the file.
pq.read_schema(result).metadata

# show schema column name summaries
print("Column name prefix counts:")
dict(Counter(w.split("_", 1)[0] for w in pq.read_schema(result).names))

# show full schema details
pq.read_schema(result)

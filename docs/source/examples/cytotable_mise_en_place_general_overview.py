# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.17.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # CytoTable mise en place (general overview)
#
# This notebook will help you understand the basics of using CytoTable and the biological basis of each step.
# We provide a high-level overview of the related concepts to give greater context about where and how the data are changed in order to gain new insights.
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

# ## Phase 1: Cells are imaged by microscopes, with optional fluorescence staining
#
# ![Image showing cells being stained and captured as images using a microscope.](../_static/cell_to_image.png)
#
# __Figure 1.__ _A microscope images cells to highlight cell processes. Often, fluorescence dyes stain the cells to mark specific proteins, compartments, and/or organelles._
#
# CytoTable processes microscopy-based data that are created from multiple upstream steps (image analysis).
# CytoTable does not require any specific sample preparation, and can work with any microscopy experimental design.
# However, most often, CytoTable processes fluorescence microscopy images from the Cell Painting assay.
# In the Cell Painting assay, scientists stain cells with a panel of six fluorescent dyes that mark key cellular compartments and organelles, including the nucleus, nucleoli/RNA, endoplasmic reticulum, mitochondria, actin cytoskeleton, Golgi apparatus, and plasma membrane ([Bray et al., 2016](https://doi.org/10.1038/nprot.2016.105); [Gustafsdottir et al., 2013](https://doi.org/10.1371/journal.pone.0080999)). Scientists then use microscopes to image these cells across fluorescence channels, and use image analysis software to produce high-content morphology profiles of individual cells for downstream analysis .
#
# We use the ExampleHuman dataset provided from CellProfiler Examples ([Moffat et al., 2006](https://doi.org/10.1016/j.cell.2006.01.040), [CellProfiler Examples Link](https://github.com/CellProfiler/examples/tree/master/ExampleHuman)) to describe this process below.

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

# ## Phase 2: CellProfiler segments cells and measures numeric features
#
# ![Image showing CellProfiler being used to create image segmentations, measurements, and exporting numeric feature data to a file.](../_static/image_to_features.png)
#
#
# __Figure 2.__ _CellProfiler takes in microscopy images and performs single-cell segmentation to distinguish cells from background. CellProfiler then measures "hand-engineered" computer vision features from every single cell. These data are captured for later use in multiple CSV tables or SQLite database._
#
# After acquisition, scientists process the images using image-analysis software such as CellProfiler. CellProfiler segments single cells and their biological compartments into distinct regions of interest. From these segmented cells, CellProfiler extracts hundreds to thousands of quantitative features per cell, capturing properties such as size, shape, intensity, texture, and spatial organization.
# These high-dimensional feature datasets provide a numerical representation of cell morphology that serves as the foundation for downstream profiling and analysis ([Carpenter et al., 2006](https://doi.org/10.1186/gb-2006-7-10-r100)).
#
# We use CellProfiler (with a prespecified configuration `.cppipe` file) to produce the following images and data tables from the ExampleHuman dataset.

# show the segmentations through an overlay with outlines
for image in pathlib.Path(source_path).glob("*Overlay.png"):
    print(f"Image outlines from segmentation (composite)")
    print("Color key: (dark blue: nuclei, light blue: cells, yellow: PH3)")
    display(Image.open(image))

# show the tables generated from the resulting CSV files
for profiles in pathlib.Path(source_path).glob("*.csv"):
    print(f"\nProfiles from CellProfiler: {profiles}")
    display(pd.read_csv(profiles).head())

# ## Phase 3: CytoTable harmonizes the feature datasets that CellProfiler generates
#
# ![Image showing feature data being read by CytoTable and exported to a CytoTable file.](../_static/features_to_cytotable.png)
#
# CellProfiler produces high-dimensional feature tables that vary in format depending on the imaging pipeline, experiment, or storage system. Sometimes these feature tables are thousands of columns and hundreds of thousands of rows. CytoTable harmonizes these outputs into consistent, analysis-ready formats such as Parquet or AnnData. This unification ensures that data from diverse experiments can be readily integrated and processed by downstream profiling tools like Pycytominer or coSMicQC, enabling scalable and reproducible bioinformatics workflows.
#
# We use CytoTable below to process the numeric feature data we generated above.

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
print(pathlib.Path(result).name)
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

# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # CytoTable from the cloud (using cloud-based data sources)
#
# ![Image showing feature data from the cloud being read by CytoTable and exported to a CytoTable file afterwards.](../_static/features_to_cytotable_cloud.png)
#
# __Figure 1.__ _CytoTable is capable of reading data from cloud-based locations such as AWS S3._
#
# This notebook includes a quick demonstration of CytoTable with cloud-based data sources.
# For a more general overview of using CytoTable and the concepts behind the work please see: [CytoTable mise en place (general overview)](https://cytomining.github.io/CytoTable/examples/cytotable_mise_en_place_general_overview.html)

# %%
import pathlib
from collections import Counter
from cloudpathlib import S3Client, CloudPath

import pandas as pd
import pyarrow.parquet as pq
from IPython.display import Image, display
from PIL import Image

import cytotable

# setup variables for use throughout the notebook
source_path = "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1"
dest_path = "./cloud_example.parquet"

# %%
# setup a source cloudpath using unsigned (anonymous) requests to AWS S3
# to access publicly-available data using CytoTable
source_cloud_path = S3Client(no_sign_request=True).CloudPath(source_path)
source_cloud_path

# %%
# remove the dest_path if it's present
if pathlib.Path(dest_path).is_file():
    pathlib.Path(dest_path).unlink()

# %%
# show the files we will use as source data with CytoTable
list(source_cloud_path.glob("*"))

# %%
# %%time

# run cytotable convert
result = cytotable.convert(
    source_path=source_path,
    dest_path=dest_path,
    # specify a destination data format type
    dest_datatype="parquet",
    # specify a preset which enables quick use of common input file formats
    preset="cellprofiler_csv",
    # use unsigned (anonymous) requests to AWS S3
    no_sign_request=True,
)
print(pathlib.Path(result).name)

# %%
# show the table head using pandas
pq.read_table(source=result).to_pandas().head()

# %%
# show metadata for the result file
pq.read_metadata(result)

# %%
# show schema metadata which includes CytoTable information
# note: this information will travel with the file.
pq.read_schema(result).metadata

# %%
# show schema column name summaries
print("Column name prefix counts:")
dict(Counter(w.split("_", 1)[0] for w in pq.read_schema(result).names))

# %%
# show full schema details
pq.read_schema(result)

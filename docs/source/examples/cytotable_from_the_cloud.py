# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # CytoTable from the cloud (using cloud-based data sources)
#
# ![Image showing feature data from the cloud being read by CytoTable and exported to a CytoTable file afterwards.](../_static/features_to_cytotable_cloud.png)
#
# __Figure 1.__ _CytoTable is capable of reading data from cloud-based locations such as AWS S3._
#
# This notebook includes a quick demonstration of CytoTable with cloud-based data sources.
# For a more general overview of using CytoTable and the concepts behind the work please see: [CytoTable mise en place (general overview)](https://cytomining.github.io/CytoTable/examples/cytotable_mise_en_place_general_overview.html)

# +
import pathlib
from collections import Counter

import cytotable
import pandas as pd
import pyarrow.parquet as pq
from cloudpathlib import CloudPath, S3Client
# -

# ## Using CytoTable with cloud-based CSV's

# setup variables for use in this section of the notebook
source_path = (
    "s3://cellpainting-gallery/cpg0037-oasis/broad/workspace/"
    "analysis/2025_01_12_U2OS_Batch2/BR00146052/analysis/BR00146052-A01-1/"
)
dest_path = "./cloud_example.parquet"

# remove the dest_path if it's present
if pathlib.Path(dest_path).is_file():
    pathlib.Path(dest_path).unlink()

# setup a source cloudpath using unsigned (anonymous) requests to AWS S3
# to access publicly-available data using CytoTable
source_cloud_path = S3Client(no_sign_request=True).CloudPath(source_path)
print(source_cloud_path)
# show the files within the path
list(source_cloud_path.glob("*"))

# +
# %%time

# run cytotable convert
result = cytotable.convert(
    source_path=source_path,
    source_datatype="csv",
    # Set the chunk size for paginated processing.
    # Smaller values use fewer resources but take longer;
    # larger values use more resources and process faster.
    chunk_size=30000,
    # specify the destination path
    dest_path=dest_path,
    # specify a destination data format type
    dest_datatype="parquet",
    # specify a preset which enables quick use of common input file formats
    preset="cellprofiler_csv",
    # use unsigned (anonymous) requests to AWS S3
    no_sign_request=True,
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

# ## Using CytoTable with cloud-based SQLite databases

# setup variables for use in this section of the notebook
source_path = (
    "s3://cellpainting-gallery/cpg0016-jump/source_4/"
    "workspace/backend/2021_08_23_Batch12/BR00126114"
)
dest_path = "./cloud_example.parquet"

# remove the dest_path if it's present
if pathlib.Path(dest_path).is_file():
    pathlib.Path(dest_path).unlink()

# setup a source cloudpath using unsigned (anonymous) requests to AWS S3
# to access publicly-available data using CytoTable
source_cloud_path = S3Client(no_sign_request=True).CloudPath(source_path)
print(source_cloud_path)
# show the files within the path
list(source_cloud_path.glob("*"))

# +
# %%time

# run cytotable convert
result = cytotable.convert(
    source_path=source_path,
    source_datatype="sqlite",
    # Set the chunk size for paginated processing.
    # Smaller values use fewer resources but take longer;
    # larger values use more resources and process faster.
    chunk_size=30000,
    # specify the destination path
    dest_path=dest_path,
    # specify a destination data format type
    dest_datatype="parquet",
    # specify a preset which enables quick use of common input file formats
    preset="cellprofiler_sqlite_cpg0016_jump",
    # use unsigned (anonymous) requests to AWS S3
    no_sign_request=True,
    # set a local cache to avoid challenges with
    # OS-specific temp disk space
    local_cache_dir=f"./sqlite_s3_cache",
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

# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # CytoTable mise en place
#
# This notebook includes a quick demonstration of CytoTable to help you understand the basics of using this project.

# +
import pathlib
from collections import Counter

import pyarrow.parquet as pq

import cytotable

# setup variables for use throughout the notebook
source_path = "../../tests/data/cellprofiler/examplehuman"
dest_path = "./example.parquet"
# -

# remove the dest_path if it's present
if pathlib.Path(dest_path).is_file():
    pathlib.Path(dest_path).unlink()

# show the files we will use as source data with CytoTable
list(pathlib.Path(source_path).glob("*.csv"))

# run cytotable convert
result = cytotable.convert(
    source_path=source_path,
    dest_path=dest_path,
    # specify a destination data format type
    dest_datatype="parquet",
    # specify a preset which enables the use of
    preset="cellprofiler_csv",
)
result.name

# show metadata for the result file
pq.read_metadata(result)

# show schema metadata which includes CytoTable information
# note: this information will travel with the file.
pq.read_schema(result).metadata

# show schema column name summaries
print("Columnn name prefix counts:")
dict(Counter(w.split("_", 1)[0] for w in pq.read_schema(result).names))

# show full schema details
pq.read_schema(result)

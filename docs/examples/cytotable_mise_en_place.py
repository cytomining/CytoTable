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
import cytotable

# setup a data directory to reference
data_dir = "../../tests/data/cellprofiler/examplehuman"
# -

# show the files we will use as source data with CytoTable
list(pathlib.Path(data_dir).glob("*.csv"))



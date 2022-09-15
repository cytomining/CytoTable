"""
Tests for cyctominer_transform/convert.py
"""
import os

from pycytominer_transform import convert


def test_convert():
    """
    Tests convert functionality.
    """
    cellprofiler_dir = f"{os.path.dirname(__file__)}/data/cellprofiler"
    convert(
        path=f"{cellprofiler_dir}/csv_single",
        source_datatype="csv",
        dest_datatype="parquet",
    )
    convert(
        path=f"{cellprofiler_dir}/csv_multi",
        source_datatype="csv",
        dest_datatype="parquet",
    )

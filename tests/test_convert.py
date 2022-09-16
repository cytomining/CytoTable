"""
Tests for cyctominer_transform/convert.py
"""
import os

from pycytominer_transform import convert, get_source_filepaths


def test_get_source_filepaths():
    """
    Tests get_source_filepaths
    """
    cellprofiler_dir = f"{os.path.dirname(__file__)}/data/cellprofiler"
    get_source_filepaths.fn(
        path=f"{cellprofiler_dir}/csv_single",
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )


def test_convert(get_tempdir: str):
    """
    Tests convert
    """

    cellprofiler_dir = f"{os.path.dirname(__file__)}/data/cellprofiler"
    convert(
        source_path=f"{cellprofiler_dir}/csv_single",
        dest_path=get_tempdir,
        dest_datatype="parquet",
    )
    convert(
        source_path=f"{cellprofiler_dir}/csv_multi",
        dest_path=get_tempdir,
        dest_datatype="parquet",
    )

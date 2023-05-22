"""
Tests for cytotable.apps.path
"""

# pylint: disable=unused-argument

import pathlib

import pytest

from cytotable.apps.path import _get_filepaths, _infer_path_datatype


def test_get_filepaths(load_parsl: None, get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests _get_filepaths
    """

    # test that no sources raises an exception
    empty_dir = pathlib.Path(f"{get_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = _get_filepaths(  # pylint: disable=no-member
            path=empty_dir,
            targets=["image", "cells", "nuclei", "cytoplasm"],
        ).result()

    # check that single sqlite file is returned as desired
    single_file_result = _get_filepaths(  # pylint: disable=no-member
        path=pathlib.Path(
            f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"
        ),
        targets=["cells"],
    ).result()
    assert len(set(single_file_result.keys())) == 1

    # check that single csv file is returned as desired
    single_file_result = _get_filepaths(  # pylint: disable=no-member
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman/Cells.csv"),
        targets=["cells"],
    ).result()
    assert len(set(single_file_result.keys())) == 1

    single_dir_result = _get_filepaths(  # pylint: disable=no-member
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["cells"],
    ).result()
    # test that the single dir structure includes 1 unique key (for cells)
    assert len(set(single_dir_result.keys())) == 1

    single_dir_result = _get_filepaths(  # pylint: disable=no-member
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["image", "cells", "nuclei", "cytoplasm"],
    ).result()
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4


def test_infer_path_datatype(
    load_parsl: None,
):
    """
    Tests _infer_path_datatype
    """

    data = {
        "sample_1.csv": [{"source_path": "stub"}],
        "sample_2.CSV": [{"source_path": "stub"}],
    }
    assert _infer_path_datatype(sources=data).result() == "csv"
    with pytest.raises(Exception):
        _infer_path_datatype(sources=data, source_datatype="parquet").result()

    data["sample_3.parquet"] = [{"source_path": "stub"}]
    assert (
        _infer_path_datatype(sources=data, source_datatype="parquet").result()
        == "parquet"
    )
    with pytest.raises(Exception):
        _infer_path_datatype(sources=data).result()

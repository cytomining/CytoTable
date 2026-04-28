"""
Testing CytoTable utility functions found within util.py
"""

# pylint: disable=C0301

import pathlib
from typing import List

import pandas as pd
import pyarrow as pa
import pytest
from botocore.exceptions import EndpointConnectionError

from cytotable.utils import (
    _generate_pagesets,
    _natural_sort,
    cloud_glob,
    find_anndata_metadata_field_names,
    map_pyarrow_type,
)


def test_generate_pageset():  # pylint: disable=too-many-statements
    """
    Test the generate_pageset function with various scenarios.
    """

    # Test case with a single element
    keys = [1]
    chunk_size = 3
    expected = [(1, 1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case when chunk size is larger than the list
    keys = [1, 2, 3]
    chunk_size = 10
    expected = [(1, 3)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with all elements being the same
    keys = [1, 1, 1, 1, 1]
    chunk_size = 2
    expected = [(1, 1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with one duplicate of chunk size and others
    keys = [1, 1, 1, 2, 3, 4]
    chunk_size = 3
    expected = [(1, 1), (2, 4)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with a chunk size of one
    keys = [1, 2, 3, 4, 5]
    chunk_size = 1
    expected = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with no duplicates
    keys = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunk_size = 3
    expected = [(1, 3), (4, 6), (7, 9), (10, 10)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with non-continuous keys
    keys = [1, 3, 5, 7, 9, 12, 14]
    chunk_size = 2
    expected = [(1, 3), (5, 7), (9, 12), (14, 14)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with inconsistent duplicates
    keys = [1, 1, 3, 4, 5, 5, 8, 8, 8]
    chunk_size = 3
    expected = [(1, 3), (4, 5), (8, 8)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Bigger test case with inconsistent duplicates
    keys = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 12, 12, 12]
    chunk_size = 3
    expected = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (12, 12)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with no duplicates
    keys = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1]
    chunk_size = 3
    expected = [(1.1, 3.3), (4.4, 6.6), (7.7, 9.9), (10.1, 10.1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with non-continuous float keys
    keys = [1.1, 3.3, 5.5, 7.7, 9.9, 12.12, 14.14]
    chunk_size = 2
    expected = [(1.1, 3.3), (5.5, 7.7), (9.9, 12.12), (14.14, 14.14)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with inconsistent duplicates
    keys = [1.1, 1.1, 3.3, 4.4, 5.5, 5.5, 8.8, 8.8, 8.8]
    chunk_size = 3
    expected = [(1.1, 3.3), (4.4, 5.5), (8.8, 8.8)]
    assert _generate_pagesets(keys, chunk_size) == expected


@pytest.mark.parametrize(
    "input_list, expected",
    [
        ([], []),
        (["a1"], ["a1"]),
        (["a1", "a10", "a2", "a3"], ["a1", "a2", "a3", "a10"]),
        (["1", "10", "2", "11", "21", "20"], ["1", "2", "10", "11", "20", "21"]),
        (["b1", "a1", "b2", "a2"], ["a1", "a2", "b1", "b2"]),
        (["apple1", "Apple10", "apple2"], ["Apple10", "apple1", "apple2"]),
        (["a1", "A1", "a10", "A10"], ["A1", "A10", "a1", "a10"]),
        (
            ["a-1", "a-10", "b-2", "B-1", "b-3", "a-2", "A-3"],
            ["A-3", "B-1", "a-1", "a-2", "a-10", "b-2", "b-3"],
        ),
    ],
)
def test_natural_sort(input_list, expected):
    """
    Tests for _natural_sort
    """
    assert _natural_sort(input_list) == expected


def test_map_pyarrow_type():
    """
    Testing map_pyarrow_type
    """
    # Test simple types
    assert map_pyarrow_type(pa.float32(), None) == pa.float64()
    assert map_pyarrow_type(pa.int32(), None) == pa.int64()
    assert map_pyarrow_type(pa.string(), None) == pa.string()
    assert map_pyarrow_type(pa.null(), None) == pa.null()

    # Test nested list types
    assert map_pyarrow_type(pa.list_(pa.float32()), None) == pa.list_(pa.float64())
    assert map_pyarrow_type(pa.list_(pa.int32()), None) == pa.list_(pa.int64())

    # Test custom type casting with data_type_cast_map
    data_type_cast_map = {"float": "float32", "integer": "int32"}
    assert map_pyarrow_type(pa.float64(), data_type_cast_map) == pa.float32()
    assert map_pyarrow_type(pa.int64(), data_type_cast_map) == pa.int32()
    assert map_pyarrow_type(pa.list_(pa.float64()), data_type_cast_map) == pa.list_(
        pa.float32()
    )
    assert map_pyarrow_type(pa.list_(pa.int64()), data_type_cast_map) == pa.list_(
        pa.int32()
    )

    # Test unsupported types (should return the original type)
    unsupported_type = pa.binary()
    assert map_pyarrow_type(unsupported_type, None) == unsupported_type

    # Test nested struct types
    struct_type = pa.struct([("field1", pa.float32()), ("field2", pa.int32())])
    expected_struct_type = pa.struct([("field1", pa.float64()), ("field2", pa.int64())])
    assert map_pyarrow_type(struct_type, None) == expected_struct_type

    # Test custom type casting with nested struct types
    custom_struct_type = pa.struct([("field1", pa.float64()), ("field2", pa.int64())])
    expected_custom_struct_type = pa.struct(
        [("field1", pa.float32()), ("field2", pa.int32())]
    )
    assert (
        map_pyarrow_type(custom_struct_type, data_type_cast_map)
        == expected_custom_struct_type
    )


def test_find_anndata_metadata_field_names(tmp_path: pathlib.Path):
    """
    Testing find_anndata_metadata_field_names
    """
    # Test with a simple schema

    # export a dataframe to parquet with numeric
    # and non-numeric columns
    pd.DataFrame(
        {
            "feature1": [1.0, 2.0, 3.0],
            "feature2": [4.0, 5.0, 6.0],
            "feature3": ["a", "b", "c"],
            "obs1": ["x", "y", "z"],
            "obs2": [True, False, True],
        }
    ).to_parquet((tmp_file := tmp_path / "test.parquet"))

    numeric_colnames, nonnumeric_colnames = find_anndata_metadata_field_names(tmp_file)

    assert numeric_colnames == ["feature1", "feature2"]
    assert nonnumeric_colnames == ["feature3", "obs1", "obs2"]


@pytest.mark.parametrize(
    "root_path, max_matches, expected_result",
    [
        # local data
        (
            "tests/data/cellprofiler/ExampleHuman",
            None,
            [
                "tests/data/cellprofiler/ExampleHuman/Cells.csv",
                "tests/data/cellprofiler/ExampleHuman/Cytoplasm.csv",
                "tests/data/cellprofiler/ExampleHuman/Experiment.csv",
                "tests/data/cellprofiler/ExampleHuman/Image.csv",
                "tests/data/cellprofiler/ExampleHuman/Nuclei.csv",
                "tests/data/cellprofiler/ExampleHuman/PH3.csv",
            ],
        ),
        # large directory of nested results on AWS S3
        (
            "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/",
            10,
            [
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/BR00116991_A01_1/load_data_Cells_BoundingBox_Overlap.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/BR00116991_A01_1/load_data_Nuclei_BoundingBox_Overlap.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_BoundingBox_First.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_BoundingBox_Overlap.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_Cellpose3.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_Donut_Large.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_Donut_Small.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_HighThresh.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_LowThresh.csv",
            ],
        ),
        # small directory of flat results (only the root contains csv's) on AWS S3
        (
            "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/",
            None,
            [
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Cells.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Cytoplasm.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Experiment.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Image.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Nuclei.csv",
            ],
        ),
    ],
)
def test_cloud_glob(root_path: str, max_matches: int, expected_result: List[str]):
    """
    Testing cloud_glob utility function.
    """

    # check that our results match expected
    try:
        result = sorted(
            [
                str(p)
                for p in cloud_glob(
                    start=root_path, pattern="**/*.csv", max_matches=max_matches
                )
            ]
        )
    except EndpointConnectionError as exc:
        pytest.skip(
            f"Skipping live cloud listing test because the endpoint is unavailable: {exc}"
        )

    assert result == sorted(expected_result)


def test_cloud_glob_follows_symlinked_directories(tmp_path: pathlib.Path):
    """
    Regression test for https://github.com/cytomining/CytoTable/issues/440.

    Mimics the Nextflow `stageInMode='symlink'` layout: per-site CSVs live in a
    real directory and are exposed under a staging directory through a symlinked
    subdirectory. cloud_glob must discover the CSVs via the symlinked path.
    """
    real = tmp_path / "real" / "analysis"
    real.mkdir(parents=True)
    expected_names = ["Cells.csv", "Cytoplasm.csv", "Image.csv", "Nuclei.csv"]
    for name in expected_names:
        (real / name).write_text("ImageNumber,ObjectNumber\n1,1\n")

    staged = tmp_path / "staged" / "1"
    staged.mkdir(parents=True)
    (staged / "analysis").symlink_to(real, target_is_directory=True)

    staged_root = tmp_path / "staged"

    # Path input
    path_results = sorted(
        p.name for p in cloud_glob(start=staged_root, pattern="**/*.csv")
    )
    assert path_results == sorted(expected_names)

    # str input (exercises the string-path branch)
    str_results = sorted(
        pathlib.Path(p).name
        for p in cloud_glob(start=str(staged_root), pattern="**/*.csv")
    )
    assert str_results == sorted(expected_names)

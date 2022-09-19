"""
Tests for cyctominer_transform/convert.py
"""
import itertools

from pyarrow import csv, parquet

from pycytominer_transform import convert, get_source_filepaths


def test_get_source_filepaths(data_dir_cellprofiler: str):
    """
    Tests get_source_filepaths
    """

    single_dir_result = get_source_filepaths.fn(
        path=f"{data_dir_cellprofiler}/csv_single",
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4

    multi_dir_result = get_source_filepaths.fn(
        path=f"{data_dir_cellprofiler}/csv_multi",
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )
    # test that a multi-file dataset has more than one value under group
    assert len(list(multi_dir_result.values())[0]) == 2
    # test that we only have a source path for each item within groupings
    assert list(
        # gather only unique values within grouped dicts
        set(
            # use itertools.chain to collapse list of lists
            itertools.chain(
                *[
                    list(item.keys())
                    for group in multi_dir_result
                    for item in multi_dir_result[group]
                ]
            )
        )
    ) == ["source_path"]


def test_convert_cellprofiler_csv(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests convert
    """

    single_dir_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_single",
        dest_path=get_tempdir,
        dest_datatype="parquet",
    )
    # loop through the results to ensure data matches what we expect
    for result in itertools.chain(*list(single_dir_result.values())):
        parquet_result = parquet.read_table(source=result["destination_path"])
        csv_source = csv.read_csv(input_file=result["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape

    convert(
        source_path=f"{data_dir_cellprofiler}/csv_multi",
        dest_path=get_tempdir,
        dest_datatype="parquet",
        concat=False,
    )

    convert(
        source_path=f"{data_dir_cellprofiler}/csv_multi",
        dest_path=get_tempdir,
        dest_datatype="parquet",
    )

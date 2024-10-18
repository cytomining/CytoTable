"""
ThreadPoolExecutor-based tests for CytoTable.convert and related.
"""

# pylint: disable=no-member,too-many-lines,unused-argument,line-too-long


import pathlib

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyarrow import parquet

from cytotable.convert import convert
from cytotable.sources import _get_source_filepaths


def test_convert_tpe_cellprofiler_csv(
    load_parsl_threaded: None,
    fx_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_examplehuman: pa.Table,
):
    """
    Tests non-S3 convert with Parsl ThreadPoolExecutor
    """

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{fx_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
        )
    )

    # sort all values by the same columns
    # we do this due to the potential for inconsistently ordered results
    control_result = control_result.sort_by(
        [(colname, "ascending") for colname in control_result.column_names]
    )
    test_result = test_result.sort_by(
        [(colname, "ascending") for colname in test_result.column_names]
    )

    assert test_result.shape == control_result.shape
    assert test_result.equals(control_result)


def test_convert_s3_path_csv(
    load_parsl_threaded: None, fx_tempdir: str, example_s3_path_csv_jump: str
):
    """
    Tests convert with mocked csv s3 object storage endpoint
    """

    s3_result = convert(
        source_path=example_s3_path_csv_jump,
        dest_path=f"{fx_tempdir}/s3_test",
        dest_datatype="parquet",
        source_datatype="csv",
        preset="cellprofiler_csv",
        no_sign_request=True,
    )

    # read only the metadata from parquet file
    parquet_file_meta = parquet.ParquetFile(s3_result).metadata

    # check the shape of the data
    assert (parquet_file_meta.num_rows, parquet_file_meta.num_columns) == (109, 5794)


@pytest.mark.large_data_tests
def test_convert_s3_path_sqlite_join(
    load_parsl_threaded: None,
    fx_tempdir: str,
    example_s3_path_sqlite_jump: str,
):
    """
    Tests convert with mocked sqlite s3 object storage endpoint

    Note: we use a dedicated tmpdir for work in this test to avoid
    race conditions with nested pytest fixture post-yield deletions.
    """

    s3_result = convert(
        source_path=example_s3_path_sqlite_jump,
        dest_path=f"{fx_tempdir}/s3_test",
        dest_datatype="parquet",
        source_datatype="sqlite",
        # set chunk size to amount which operates within
        # github actions runner images and related resource constraints.
        chunk_size=30000,
        preset="cellprofiler_sqlite_cpg0016_jump",
        no_sign_request=True,
        # use explicit cache to avoid temp cache removal / overlaps with
        # sequential s3 SQLite files. See below for more information
        # https://cloudpathlib.drivendata.org/stable/caching/#automatically
        local_cache_dir=f"{fx_tempdir}/sqlite_s3_cache/2",
    )

    # read only the metadata from parquet file
    parquet_file_meta = parquet.ParquetFile(s3_result).metadata

    # check the shape of the data
    assert (parquet_file_meta.num_rows, parquet_file_meta.num_columns) == (74226, 5928)

    # check that dropping duplicates results in the same shape
    assert pd.read_parquet(s3_result).drop_duplicates().shape == (74226, 5928)


def test_get_source_filepaths(
    load_parsl_threaded: None, fx_tempdir: str, data_dir_cellprofiler: str
):
    """
    Tests _get_source_filepaths
    """

    # test that no sources raises an exception
    empty_dir = pathlib.Path(f"{fx_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = _get_source_filepaths(
            path=empty_dir,
            targets=["image", "cells", "nuclei", "cytoplasm"],
        )

    # check that single sqlite file is returned as desired
    single_file_result = _get_source_filepaths(
        path=pathlib.Path(
            f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"
        ),
        targets=["cells"],
    )
    assert len(set(single_file_result.keys())) == 1

    # check that single csv file is returned as desired
    single_file_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman/Cells.csv"),
        targets=["cells"],
    )
    assert len(set(single_file_result.keys())) == 1

    single_dir_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["cells"],
    )
    # test that the single dir structure includes 1 unique key (for cells)
    assert len(set(single_dir_result.keys())) == 1

    single_dir_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4


def test_gather_tablenumber(
    load_parsl_threaded: None,
    fx_tempdir: str,
    data_dirs_cytominerdatabase: List[str],
    cytominerdatabase_to_manual_join_parquet: List[str],
):
    """
    Tests _gather_tablenumber
    """

    for unprocessed_cytominerdatabase, processed_cytominerdatabase in zip(
        data_dirs_cytominerdatabase, cytominerdatabase_to_manual_join_parquet
    ):
        test_table = parquet.read_table(
            source=convert(
                source_path=unprocessed_cytominerdatabase,
                dest_path=(
                    f"{fx_tempdir}/{pathlib.Path(unprocessed_cytominerdatabase).name}.test_table.parquet"
                ),
                dest_datatype="parquet",
                source_datatype="csv",
                join=True,
                joins="""
                    WITH Image_Filtered AS (
                        SELECT
                            Metadata_TableNumber,
                            Metadata_ImageNumber
                        FROM
                            read_parquet('image.parquet')
                        )
                    SELECT
                        *
                    FROM
                        read_parquet('cytoplasm.parquet') AS cytoplasm
                    LEFT JOIN read_parquet('cells.parquet') AS cells ON
                        cells.Metadata_TableNumber = cells.Metadata_TableNumber
                        AND cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                        AND cells.Cells_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
                    LEFT JOIN read_parquet('nuclei.parquet') AS nuclei ON
                        nuclei.Metadata_TableNumber = nuclei.Metadata_TableNumber
                        AND nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                        AND nuclei.Nuclei_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
                    LEFT JOIN Image_Filtered AS image ON
                        image.Metadata_TableNumber = cytoplasm.Metadata_TableNumber
                        AND image.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                """,
                preset="cell-health-cellprofiler-to-cytominer-database",
            )
        )
        control_table = parquet.read_table(source=processed_cytominerdatabase)

        # test_unique_tablenumbers = pc.unique(test_table["Metadata_TableNumber"])
        control_unique_tablenumbers = pc.unique(control_table["Metadata_TableNumber"])

        assert (
            test_table.filter(
                # we use only those tablenumbers which appear in cytominer-database related results
                # to help compare. CytoTable only removes datasets which have no image table whereas
                # cytominer-database removes any dataset which has no image table or problematic
                # compartment tables (any compartment table with errors triggers the entire dataset
                # being removed).
                pc.field("Metadata_TableNumber").isin(control_unique_tablenumbers)
            )
            .sort_by([(name, "ascending") for name in test_table.column_names])
            .equals(
                control_table.sort_by(
                    [(name, "ascending") for name in control_table.column_names]
                )
            )
        )

    assert False
def test_avoid_na_row_output(
    load_parsl_threaded: None, fx_tempdir: str, data_dir_cellprofiler: str
):
    """
    Test to help detect and avoid scenarios where CytoTable returns rows of
    NA-based data. This occurs when CytoTable processes CellProfiler data
    sources with images that do not contain segmented objects. In other words,
    this test ensures CytoTable produces correct output data when the input
    CellProfiler image table contains imagenumbers that do not exist in any
    compartment object.

    Therefore, CytoTable does not return single-cell rows which include image
    table metadata and NA feature data. Using compartment tables as the basis
    of data joins avoids this issue.
    """

    # run convert using a dataset known to contain the scenario outlined above.
    parquet_file = convert(
        source_path=(
            f"{data_dir_cellprofiler}"
            "/nf1_cellpainting_data/test-Plate_3_nf1_analysis.sqlite"
        ),
        dest_path=f"{fx_tempdir}/nf1_cellpainting_data/test-Plate_3_nf1_analysis.parquet",
        dest_datatype="parquet",
        preset="cellprofiler_sqlite_pycytominer",
    )

    # check that we have no nulls within Metadata_ImageNumber column
    assert not pc.sum(
        pc.is_null(
            parquet.read_table(
                source=parquet_file,
            ).column("Metadata_ImageNumber")
        )
    ).as_py()

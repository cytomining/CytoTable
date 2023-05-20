"""
Tests for CytoTable.convert and related.
"""

# pylint: disable=no-member,unused-argument


import pathlib
from typing import Any, Dict, List, cast

import duckdb
import parsl
import pyarrow as pa
import pytest
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from pyarrow import parquet
from pycytominer.cyto_utils.cells import SingleCells

from cytotable.convert import convert
from cytotable.utils import (
    _column_sort,
    _default_parsl_config,
    _duckdb_reader,
    _sqlite_mixed_type_query_to_parquet,
)


def test_convert_s3_path_csv(
    load_parsl: None,
    get_tempdir: str,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    example_s3_endpoint: str,
):
    """
    Tests convert with mocked csv s3 object storage endpoint
    """

    multi_dir_nonconcat_s3_result = convert(
        source_path="s3://example/",
        dest_path=f"{get_tempdir}/s3_test",
        dest_datatype="parquet",
        concat=False,
        join=False,
        joins=None,
        source_datatype="csv",
        compartments=["cytoplasm", "cells"],
        metadata=["image"],
        identifying_columns=["imagenumber"],
        # endpoint_url here will be used with cloudpathlib client(**kwargs)
        endpoint_url=example_s3_endpoint,
    )

    # compare each of the results using files from the source
    for control_path, test_path in zip(
        [
            source["table"]
            for group in cast(Dict, multi_dir_nonconcat_s3_result).values()
            for source in group
        ],
        [
            source["table"]
            for group in example_local_sources.values()
            for source in group
        ],
    ):
        parquet_control = parquet.ParquetDataset(path_or_paths=control_path).read()
        parquet_result = parquet.ParquetDataset(
            path_or_paths=test_path, schema=parquet_control.schema
        ).read()

        assert parquet_result.schema.equals(parquet_control.schema)
        assert parquet_result.shape == parquet_control.shape


def test_convert_s3_path_sqlite(
    load_parsl: None,
    get_tempdir: str,
    data_dir_cellprofiler_sqlite_nf1: str,
    example_s3_endpoint: str,
):
    """
    Tests convert with mocked sqlite s3 object storage endpoint
    """

    # local sqlite read
    local_cytotable_table = parquet.read_table(
        source=convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=(
                f"{get_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.parquet"
            ),
            dest_datatype="parquet",
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
        )
    )

    # s3 sqlite read with single and directly referenced file
    s3_cytotable_table = parquet.read_table(
        source=convert(
            source_path=f"s3://example/nf1/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}",
            dest_path=(
                f"{get_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.parquet"
            ),
            dest_datatype="parquet",
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
            endpoint_url=example_s3_endpoint,
        )
    )

    # s3 sqlite read with nested sqlite file
    s3_cytotable_table_nested = parquet.read_table(
        source=convert(
            source_path="s3://example/nf1/",
            dest_path=(
                f"{get_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.parquet"
            ),
            dest_datatype="parquet",
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
            endpoint_url=example_s3_endpoint,
        )
    )

    assert local_cytotable_table.equals(s3_cytotable_table)
    assert local_cytotable_table.equals(s3_cytotable_table_nested)


def test_convert_cytominerdatabase_csv(
    load_parsl: None,
    get_tempdir: str,
    data_dirs_cytominerdatabase: List[str],
    cytominerdatabase_to_pycytominer_merge_single_cells_parquet: List[str],
):
    """
    Tests convert with cytominerdatabase csvs and processed
    csvs from cytominer-database to pycytominer merge_single_cells
    """

    for cytominerdatabase_dir, pycytominer_merge_dir in zip(
        data_dirs_cytominerdatabase,
        cytominerdatabase_to_pycytominer_merge_single_cells_parquet,
    ):
        # load control table, dropping unlabeled objectnumber (no compartment specified)
        control_table = parquet.read_table(source=pycytominer_merge_dir).drop(
            [
                # objectnumber references are provided via cytoplasm parent object joins
                "Metadata_ObjectNumber",
                "Metadata_ObjectNumber_cells",
            ]
        )
        # rename column to account for minor difference in processing
        control_table = control_table.rename_columns(
            [
                # rename based on compartment prefix name within CytoTable format
                col if col != "Cells_Parent_Nuclei" else "Metadata_Cells_Parent_Nuclei"
                for col in control_table.schema.names
            ]
        )

        # load test table by reading parquet-based output from convert
        test_table = parquet.read_table(
            source=convert(
                source_path=cytominerdatabase_dir,
                dest_path=(
                    f"{get_tempdir}/{pathlib.Path(cytominerdatabase_dir).name}.test_table.parquet"
                ),
                dest_datatype="parquet",
                source_datatype="csv",
                join=True,
                drop_null=False,
            ),
            schema=control_table.schema,
        )

        assert control_table.schema.equals(test_table.schema)
        assert control_table.num_columns == test_table.num_columns
        assert control_table.num_rows <= test_table.num_rows


def test_convert_cellprofiler_sqlite(
    load_parsl: None,
    get_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_nf1data: pa.Table,
):
    """
    Tests convert with cellprofiler sqlite exports
    """

    control_result = cellprofiler_merged_nf1data

    test_result = parquet.read_table(
        convert(
            source_path=(
                f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"
            ),
            dest_path=f"{get_tempdir}/NF1_data.parquet",
            dest_datatype="parquet",
            source_datatype="sqlite",
            preset="cellprofiler_sqlite",
            add_tablenumber=False,
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


def test_convert_cellprofiler_csv(
    load_parsl: None,
    get_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_examplehuman: pa.Table,
):
    """
    Tests convert

    Note: uses default prefect task_runner from convert
    Dedicated tests for prefect-dask runner elsewhere.
    """

    # expects exception due to no compartments
    with pytest.raises(Exception):
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            compartments=[],
        )

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
            add_tablenumber=False,
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


def test_convert_cellprofiler_sqlite_pycytominer_merge(
    load_parsl: None,
    get_tempdir: str,
    data_dir_cellprofiler_sqlite_nf1: str,
):
    """
    Tests for alignment with Pycytominer SingleCells.merge_single_cells
    compatibility along with conversion functionality.
    """

    # use pycytominer SingleCells.merge_single_cells to produce parquet file
    pycytominer_table = parquet.read_table(
        source=SingleCells(
            sql_file=f"sqlite:///{data_dir_cellprofiler_sqlite_nf1}",
            compartments=["Per_Cells", "Per_Cytoplasm", "Per_Nuclei"],
            compartment_linking_cols={
                "Per_Cytoplasm": {
                    "Per_Cells": "Cytoplasm_Parent_Cells",
                    "Per_Nuclei": "Cytoplasm_Parent_Nuclei",
                },
                "Per_Cells": {"Per_Cytoplasm": "Cells_Number_Object_Number"},
                "Per_Nuclei": {"Per_Cytoplasm": "Nuclei_Number_Object_Number"},
            },
            image_table_name="Per_Image",
            strata=["Image_Metadata_Well", "Image_Metadata_Plate"],
            merge_cols=["ImageNumber"],
            image_cols="ImageNumber",
            load_image_data=True,
            # perform merge_single_cells without annotation
            # and receive parquet filepath
        ).merge_single_cells(
            sc_output_file=(
                f"{get_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".pycytominer.parquet"
            ),
            output_type="parquet",
        )
    )
    # sort the result column order by cytotable sorting preference
    pycytominer_table = pycytominer_table.select(
        sorted(sorted(pycytominer_table.column_names), key=_column_sort)
    )

    # use cytotable convert to produce parquet file
    cytotable_table = parquet.read_table(
        source=convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=(
                f"{get_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.parquet"
            ),
            dest_datatype="parquet",
            join=True,
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
            add_tablenumber=False,
        )
    )

    # find the difference in column names and display it as part of an assertion
    column_diff = list(
        set(pycytominer_table.schema.names) - set(cytotable_table.schema.names)
    )
    # if there are no differences in column names, we should pass the assertion
    # (empty collections evaluate to false)
    assert not column_diff

    # check the schemas, shape, and equality between tables
    # note: we cast into pycytominer_table's schema types in order to
    # properly perform comparisons as pycytominer and cytotable differ in their
    # datatyping implementations
    assert pycytominer_table.schema.equals(
        cytotable_table.cast(target_schema=pycytominer_table.schema).schema
    )
    assert pycytominer_table.shape == cytotable_table.shape


def test_sqlite_mixed_type_query_to_parquet(
    load_parsl: None, get_tempdir: str, example_sqlite_mixed_types_database: str
):
    """
    Testing _sqlite_mixed_type_query_to_parquet
    """

    result_filepath = f"{get_tempdir}/example_mixed_types_tbl_a.parquet"
    table_name = "tbl_a"

    try:
        # attempt to read the data using DuckDB
        result = _duckdb_reader().execute(
            f"""COPY (
                select * from sqlite_scan('{example_sqlite_mixed_types_database}','{table_name}')
                LIMIT 2 OFFSET 0
                ) TO '{result_filepath}'
                (FORMAT PARQUET)
            """
        )
    except duckdb.Error as duckdb_exc:
        # if we see a mismatched type error
        # run a more nuanced query through sqlite
        # to handle the mixed types
        if "Mismatch Type Error" in str(duckdb_exc):
            result = _sqlite_mixed_type_query_to_parquet(
                source_path=example_sqlite_mixed_types_database,
                table_name=table_name,
                chunk_size=2,
                offset=0,
                result_filepath=result_filepath,
            )

    # check schema names
    assert parquet.read_schema(where=result).names == [
        "col_integer",
        "col_text",
        "col_blob",
        "col_real",
    ]
    # check schema types
    assert parquet.read_schema(where=result).types == [
        pa.int64(),
        pa.string(),
        pa.binary(),
        pa.float64(),
    ]
    # check the values per column
    assert parquet.read_table(source=result).to_pydict() == {
        "col_integer": [1, None],
        "col_text": ["sample", "sample"],
        "col_blob": [b"sample_blob", b"another_blob"],
        "col_real": [0.5, None],
    }


def test_convert_hte_cellprofiler_csv(
    get_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_examplehuman: pa.Table,
):
    """
    Tests convert with Parsl HighThroughputExecutor

    See the following for more details.
    https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html#parsl.executors.HighThroughputExecutor
    """

    local_htex = Config(
        executors=[
            HighThroughputExecutor(
                label="htex_Local",
                worker_debug=True,
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        ],
        strategy=None,
    )

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
            parsl_config=local_htex,
            add_tablenumber=False,
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

    # clean up the parsl config for other tests
    parsl.clear()
    parsl.load(_default_parsl_config())

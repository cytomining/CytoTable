"""
Tests for CytoTable.convert and related.

Note: these use the _default_parsl_config.
"""

# pylint: disable=no-member,too-many-lines,unused-argument

import itertools
import os
import pathlib
from shutil import copy
from typing import Any, Dict, List, Tuple, cast
from parsl.app.app import python_app
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from cloudpathlib import CloudPath
from pyarrow import csv, parquet
from pycytominer.cyto_utils.cells import SingleCells

from cytotable.convert import (
    _concat_join_sources,
    _concat_source_group,
    _infer_source_group_common_schema,
    _join_source_chunk,
    _prepare_join_sql,
    _prepend_column_name,
    _to_parquet,
    convert,
)
from cytotable.exceptions import CytoTableException
from cytotable.presets import config
from cytotable.sources import _infer_source_datatype
from cytotable.utils import (
    _column_sort,
    _duckdb_reader,
    _expand_path,
    _get_cytotable_version,
    _sqlite_mixed_type_query_to_parquet,
    _write_parquet_table_with_metadata,
    evaluate_futures,
)


def test_config():
    """
    Tests config to ensure proper values
    """

    # check that we have relevant keys for each preset
    for config_preset in config.values():
        assert sorted(
            [
                "CONFIG_NAMES_COMPARTMENTS",
                "CONFIG_NAMES_METADATA",
                "CONFIG_IDENTIFYING_COLUMNS",
                "CONFIG_CHUNK_SIZE",
                "CONFIG_JOINS",
                "CONFIG_SOURCE_VERSION",
            ]
        ) == sorted(config_preset.keys())


def test_get_cytotable_version():
    """
    Tests get_cytotable_version
    """

    assert isinstance(_get_cytotable_version(), str)

def test_write_parquet_table_with_metadata(fx_tempdir: str):
    """
    Tests _write_parquet_table_with_metadata
    """

    # create a test dir and str with path for test file
    pathlib.Path(f"{fx_tempdir}/pq_metadata_test").mkdir()
    destination_file = f"{fx_tempdir}/pq_metadata_test/test.parquet"

    # test writing to parquet file with metadata
    _write_parquet_table_with_metadata(
        table=pa.Table.from_pydict(
            {
                "col_1": [
                    "a",
                ],
                "col_2": [
                    1,
                ],
            }
        ),
        where=destination_file,
    )

    # assert that we have the metadata in place as expected
    assert (
        parquet.read_table(source=destination_file).schema.metadata[b"data-producer"]
        == b"https://github.com/cytomining/CytoTable"
    )
    assert (
        parquet.read_table(source=destination_file).schema.metadata[
            b"data-producer-version"
        ]
        == _get_cytotable_version().encode()
    )


def test_existing_dest_path(fx_tempdir: str, data_dir_cellprofiler_sqlite_nf1: str):
    """
    Tests running cytotable.convert with existing dest_path
    where we expect a raised exception to avoid data loss.
    """

    # locations for test dir and file
    test_dir = f"{fx_tempdir}/existing_dir"
    test_file = f"{fx_tempdir}/existing_file"

    # Create an empty directory
    pathlib.Path(test_dir).mkdir()

    # Create an empty file
    pathlib.Path(test_file).touch()

    # test raise with existing dir as dest_path
    with pytest.raises(CytoTableException):
        convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=test_dir,
            dest_datatype="parquet",
            join=False,
            preset="cellprofiler_sqlite_pycytominer",
        )

    # test raise with existing file as dest_path
    with pytest.raises(CytoTableException):
        convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=test_file,
            dest_datatype="parquet",
            join=True,
            preset="cellprofiler_sqlite_pycytominer",
        )

    # test raise with $HOME as dest_path
    with pytest.raises(CytoTableException):
        convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path="$HOME",
            dest_datatype="parquet",
            preset="cellprofiler_sqlite_pycytominer",
        )

    # test raise with "." as dest_path
    with pytest.raises(CytoTableException):
        convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=".",
            dest_datatype="parquet",
            preset="cellprofiler_sqlite_pycytominer",
        )


def test_extend_path(fx_tempdir: str):
    """
    Tests _extend_path
    """

    # check that we have a pathlib path returned for local paths
    assert isinstance(_expand_path(path=fx_tempdir), pathlib.Path)

    # check that we have a cloudpath path returned for simulated cloud path
    assert isinstance(_expand_path(path=f"s3://{fx_tempdir}"), CloudPath)

    # test that `~` and `$HOME` resolve properly to home
    home_dir = str(os.environ.get("HOME"))
    assert _expand_path(path="~") == pathlib.Path(home_dir)
    assert _expand_path(path="$HOME") == pathlib.Path(home_dir)

    # create a subdir and test path resolution to a root
    subdir = f"{fx_tempdir}/test_subdir"
    pathlib.Path(subdir).mkdir()
    assert _expand_path(path=f"{subdir}/..") == pathlib.Path(fx_tempdir).resolve()


def test_evaluate_futures(load_parsl_default: None):
    """
    Tests evaluate_futures
    """

    @python_app
    def example_parsl_task(input: int):
        # an example of a parsl task
        return input

    example_sources = {
        "a": [
            {
                "one": example_parsl_task(input=1),
                "two": [example_parsl_task(input=1), example_parsl_task(input=1)],
            }
        ],
        "b": [
            {
                "one": example_parsl_task(input=2),
                "two": [example_parsl_task(input=2), example_parsl_task(input=2)],
            }
        ]
    }

    example_result = example_parsl_task(input=3)

    assert evaluate_futures(sources=example_sources) == {
        "a": [
            {
                "one": 1,
                "two": [1, 1],
            }
        ],
        "b": [
            {
                "one": 2,
                "two": [2, 2],
            }
        ]
    }

    assert evaluate_futures(sources=example_result) == 3


def test_prepend_column_name(load_parsl_default: None, fx_tempdir: str):
    """
    Tests _prepend_column_name
    """

    # example cytoplasm csv table run
    prepend_testpath_1 = f"{fx_tempdir}/prepend_testpath_1.parquet"
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "ImageNumber": [1, 2, 3, 1, 2, 3],
                "ObjectNumber": [1, 1, 1, 2, 2, 2],
                "Parent_Cells": [1, 1, 1, 2, 2, 2],
                "Parent_Nuclei": [1, 1, 1, 2, 2, 2],
                "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
                "field2": [True, False, True, True, False, True],
            }
        ),
        where=prepend_testpath_1,
    )
    result = _prepend_column_name(
        table_path=prepend_testpath_1,
        source_group_name="Cytoplasm.csv",
        identifying_columns=[
            "ImageNumber",
            "ObjectNumber",
            "Parent_Cells",
            "Parent_Nuclei",
        ],
        metadata=["image"],
        compartments=["image", "cells", "nuclei", "cytoplasm"],
    ).result()

    # compare the results with what's expected for column names
    assert parquet.read_table(source=result).column_names == [
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
        "Metadata_Cytoplasm_Parent_Cells",
        "Metadata_Cytoplasm_Parent_Nuclei",
        "Cytoplasm_field1",
        "Cytoplasm_field2",
    ]

    # example cells sqlite table run
    repend_testpath_2 = f"{fx_tempdir}/prepend_testpath_2.parquet"
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "ImageNumber": [1, 2, 3, 1, 2, 3],
                "Cells_Number_Object_Number": [1, 1, 1, 2, 2, 2],
                "Parent_OrigNuclei": [1, 1, 1, 2, 2, 2],
                "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
                "field2": [True, False, True, True, False, True],
            }
        ),
        where=repend_testpath_2,
    )
    result = _prepend_column_name(
        table_path=repend_testpath_2,
        source_group_name="Per_Cells.sqlite",
        identifying_columns=[
            "ImageNumber",
            "Parent_Cells",
            "Parent_OrigNuclei",
        ],
        metadata=["image"],
        compartments=["image", "cells", "nuclei", "cytoplasm"],
    ).result()

    # compare the results with what's expected for column names
    assert parquet.read_table(source=result).column_names == [
        "Metadata_ImageNumber",
        "Cells_Number_Object_Number",
        "Metadata_Cells_Parent_OrigNuclei",
        "Cells_field1",
        "Cells_field2",
    ]


def test_concat_source_group(
    load_parsl_default: None,
    fx_tempdir: str,
    example_tables: Tuple[pa.Table, ...],
    example_local_sources: Dict[str, List[Dict[str, Any]]],
):
    """
    Tests _concat_source_group
    """

    _, _, _, table_nuclei_1, table_nuclei_2 = example_tables

    # simulate concat
    concat_table = pa.concat_tables([table_nuclei_1, table_nuclei_2])

    result = _concat_source_group(
        source_group_name="nuclei",
        source_group=example_local_sources["nuclei.csv"],
        dest_path=fx_tempdir,
        common_schema=table_nuclei_1.schema,
    ).result()
    assert len(result) == 1
    assert parquet.read_schema(result[0]["table"][0]) == concat_table.schema
    assert (
        parquet.read_metadata(result[0]["table"][0]).num_rows,
        parquet.read_metadata(result[0]["table"][0]).num_columns,
    ) == concat_table.shape

    # add a mismatching source to group
    mismatching_table = pa.Table.from_pydict(
        {
            "color": pa.array(["blue", "red", "green", "orange"]),
        }
    )
    pathlib.Path(f"{fx_tempdir}/example/5").mkdir(parents=True, exist_ok=True)
    csv.write_csv(mismatching_table, f"{fx_tempdir}/example/5/nuclei.csv")
    parquet.write_table(mismatching_table, f"{fx_tempdir}/example/5.nuclei.parquet")
    example_local_sources["nuclei.csv"].append(
        {
            "source_path": pathlib.Path(f"{fx_tempdir}/example/5/nuclei.csv"),
            "destination_path": pathlib.Path(f"{fx_tempdir}/example/5.nuclei.parquet"),
        }
    )

    with pytest.raises(Exception):
        _concat_source_group(
            source_group_name="nuclei",
            source_group=example_local_sources["nuclei.csv"],
            dest_path=fx_tempdir,
        ).result()


def test_prepare_join_sql(
    load_parsl_default: None,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
):
    """
    Tests _prepare_join_sql

    After running _prepare_join_sql we'd expect something like:
        SELECT
            *
        FROM
            read_parquet(['<temp_dir_location>/image.parquet']) AS image
        LEFT JOIN read_parquet(['<temp_dir_location>/cytoplasm.parquet']) AS cytoplasm ON
            cytoplasm.ImageNumber = image.ImageNumber
        LEFT JOIN read_parquet(['<temp_dir_location>/cells.parquet']) AS cells ON
            cells.ImageNumber = cytoplasm.ImageNumber
        LEFT JOIN read_parquet(['<temp_dir_location>/nuclei.parquet']) AS nuclei ON
            nuclei.ImageNumber = cytoplasm.ImageNumber
    """

    # attempt to run query against prepared_join_sql with test data
    with _duckdb_reader() as ddb_reader:
        result = (
            ddb_reader.execute(
                _prepare_join_sql(
                    sources=example_local_sources,
                    # simplified join for example dataset
                    joins="""
                    SELECT
                        *
                    FROM
                        read_parquet('image.parquet') AS image
                    LEFT JOIN read_parquet('cytoplasm.parquet') AS cytoplasm ON
                        cytoplasm.ImageNumber = image.ImageNumber
                    LEFT JOIN read_parquet('cells.parquet') AS cells ON
                        cells.ImageNumber = cytoplasm.ImageNumber
                    LEFT JOIN read_parquet('nuclei.parquet') AS nuclei ON
                        nuclei.ImageNumber = cytoplasm.ImageNumber
                    """,
                ).result()
            )
            .arrow()
            .to_pydict()
        )

    # check that we received data back
    assert len(result) == 9


def test_join_source_chunk(load_parsl_default: None, fx_tempdir: str):
    """
    Tests _join_source_chunk
    """

    # form test path a
    test_path_a = f"{fx_tempdir}/example_a_merged.parquet"
    # form test path b
    test_path_b = f"{fx_tempdir}/example_b_merged.parquet"

    # write test data to file
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "id1": [1, 2, 3, 1, 2, 3],
                "id2": ["a", "a", "a", "b", "b", "b"],
                "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
            }
        ),
        where=test_path_a,
    )
    # write test data to file
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "id1": [1, 2, 3, 1, 2, 3],
                "id2": ["a", "a", "a", "b", "b", "b"],
                "field2": [True, False, True, True, False, True],
            }
        ),
        where=test_path_b,
    )

    result = _join_source_chunk(
        dest_path=f"{fx_tempdir}/destination.parquet",
        joins=f"""
            SELECT *
            FROM read_parquet('{fx_tempdir}/example_a_merged.parquet') as example_a
            JOIN read_parquet('{fx_tempdir}/example_b_merged.parquet') as example_b ON
                example_b.id1 = example_a.id1
                AND example_b.id2 = example_a.id2
        """,
        chunk_size=2,
        offset=0,
        drop_null=True,
    ).result()

    assert isinstance(result, str)

    result_table = parquet.read_table(source=result)
    assert result_table.equals(
        other=pa.Table.from_pydict(
            {
                "field1": ["foo", "foo"],
                "field2": [True, True],
                "id1": [1, 1],
                "id2": ["a", "b"],
            },
            # use schema from result as a reference for col order
            schema=result_table.schema,
        )
    )


def test_concat_join_sources(load_parsl_default: None, fx_tempdir: str):
    """
    Tests _concat_join_sources
    """

    # create a test dir
    pathlib.Path(f"{fx_tempdir}/concat_join/").mkdir(exist_ok=True)

    # form test paths
    test_path_a = f"{fx_tempdir}/concat_join/join_chunks_test_a.parquet"
    test_path_b = f"{fx_tempdir}/concat_join/join_chunks_test_b.parquet"
    test_path_a_join_chunk = f"{fx_tempdir}/join_chunks_test_a.parquet"
    test_path_b_join_chunk = f"{fx_tempdir}/join_chunks_test_b.parquet"

    # form test data
    test_table_a = pa.Table.from_pydict(
        {
            "id1": [
                1,
                2,
                3,
            ],
            "id2": [
                "a",
                "a",
                "a",
            ],
            "field1": [
                "foo",
                "bar",
                "baz",
            ],
            "field2": [
                True,
                False,
                True,
            ],
        }
    )
    test_table_b = pa.Table.from_pydict(
        {
            "id1": [1, 2, 3],
            "id2": ["b", "b", "b"],
            "field1": ["foo", "bar", "baz"],
            "field2": [True, False, True],
        }
    )

    # write test data to file
    parquet.write_table(
        table=test_table_a,
        where=test_path_a,
    )
    parquet.write_table(
        table=test_table_b,
        where=test_path_b,
    )

    # copy the files for testing purposes
    copy(test_path_a, test_path_a_join_chunk)
    copy(test_path_b, test_path_b_join_chunk)

    pathlib.Path(f"{fx_tempdir}/test_concat_join_sources").mkdir(
        parents=True, exist_ok=True
    )

    result = _concat_join_sources(
        dest_path=f"{fx_tempdir}/test_concat_join_sources/example_concat_join.parquet",
        join_sources=[test_path_a_join_chunk, test_path_b_join_chunk],
        sources={
            "join_chunks_test_a.parquet": [{"table": [test_path_a]}],
            "join_chunks_test_b.parquet": [{"table": [test_path_b]}],
        },
    ).result()

    # ensure the concatted result is what we expect
    assert parquet.read_table(source=result).equals(
        pa.concat_tables(tables=[test_table_a, test_table_b])
    )

    # ensure the test paths provided via sources were removed (unlinked)
    assert (pathlib.Path(test_path_a).exists() is False) and (
        pathlib.Path(test_path_a).exists() is False
    )


def test_infer_source_datatype(
    load_parsl_default: None,
):
    """
    Tests _infer_source_datatype
    """

    data = {
        "sample_1.csv": [{"source_path": "stub"}],
        "sample_2.CSV": [{"source_path": "stub"}],
    }
    assert _infer_source_datatype(sources=data).result() == "csv"
    with pytest.raises(Exception):
        _infer_source_datatype(sources=data, source_datatype="parquet").result()

    data["sample_3.parquet"] = [{"source_path": "stub"}]
    assert (
        _infer_source_datatype(sources=data, source_datatype="parquet").result()
        == "parquet"
    )
    with pytest.raises(Exception):
        _infer_source_datatype(sources=data).result()


def test_to_parquet(
    load_parsl_default: None,
    fx_tempdir: str,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
):
    """
    Tests _to_parquet
    """

    flattened_example_sources = list(
        itertools.chain(*list(example_local_sources.values()))
    )

    # note: we cast here for mypy linting (dict and str treatment differ)
    result: Dict[str, List[Dict[str, Any]]] = cast(
        dict,
        _to_parquet(
            source_path=str(
                example_local_sources["image.csv"][0]["source_path"].parent
            ),
            dest_path=fx_tempdir,
            source_datatype=None,
            compartments=["cytoplasm", "cells", "nuclei"],
            metadata=["image"],
            identifying_columns=["imagenumber"],
            concat=False,
            join=False,
            joins=None,
            chunk_columns=None,
            chunk_size=4,
            infer_common_schema=False,
            drop_null=True,
        ),
    )

    flattened_results = list(itertools.chain(*list(result.values())))
    for i, flattened_result in enumerate(flattened_results):
        csv_source = (
            _duckdb_reader()
            .execute(
                f"""
                select * from
                read_csv_auto('{str(flattened_example_sources[i]["source_path"])}',
                ignore_errors=TRUE)
                """
            )
            .arrow()
        )
        parquet_result = parquet.ParquetDataset(
            path_or_paths=flattened_result["table"],
            # set the order of the columns uniformly for schema comparison
            schema=csv_source.schema,
        ).read()
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape


def test_infer_source_group_common_schema(
    load_parsl_default: None,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    example_tables: Tuple[pa.Table, ...],
):
    """
    Tests _infer_source_group_common_schema
    """
    _, _, _, table_nuclei_1, _ = example_tables

    result = _infer_source_group_common_schema(
        source_group=example_local_sources["nuclei.csv"],
    ).result()

    assert table_nuclei_1.schema.equals(pa.schema(result))


def test_convert_cytominerdatabase_csv(
    load_parsl_default: None,
    fx_tempdir: str,
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
        # load control table, dropping tablenumber
        # and unlabeled objectnumber (no compartment specified)
        control_table = parquet.read_table(source=pycytominer_merge_dir).drop(
            [
                # tablenumber is not implemented within CytoTable
                "Metadata_TableNumber",
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
                    f"{fx_tempdir}/{pathlib.Path(cytominerdatabase_dir).name}.test_table.parquet"
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
    load_parsl_default: None,
    fx_tempdir: str,
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
            dest_path=f"{fx_tempdir}/NF1_data.parquet",
            dest_datatype="parquet",
            source_datatype="sqlite",
            preset="cellprofiler_sqlite",
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
    load_parsl_default: None,
    fx_tempdir: str,
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
            dest_path=f"{fx_tempdir}/ExampleHuman_result_1",
            dest_datatype="parquet",
            source_datatype="csv",
            compartments=[],
        )

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{fx_tempdir}/ExampleHuman_result_2",
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


def test_cast_data_types(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cellprofiler_sqlite_nf1: str,
):
    """
    Tests _cast_data_types to ensure data types are casted as expected
    """

    test_dir = f"{fx_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
    # default data types
    convert(
        source_path=data_dir_cellprofiler_sqlite_nf1,
        dest_path=f"{test_dir}.cytotable_type_check_default.parquet",
        dest_datatype="parquet",
        join=True,
        chunk_size=100,
        preset="cellprofiler_sqlite_pycytominer",
    )

    # update the data types
    convert(
        source_path=data_dir_cellprofiler_sqlite_nf1,
        dest_path=f"{test_dir}.cytotable_type_check_updated.parquet",
        dest_datatype="parquet",
        join=True,
        chunk_size=100,
        preset="cellprofiler_sqlite_pycytominer",
        data_type_cast_map={
            "float": "float32",
            "integer": "int32",
        },
    )

    # gather float columns from default
    float_cols_to_check = [
        field.name
        for field in parquet.read_schema(
            f"{test_dir}.cytotable_type_check_default.parquet"
        )
        if pa.types.is_floating(field.type)
    ]

    # check that we only have "float32" types based on the columns above
    assert pa.types.is_float32(
        # a set comprehension to gather unique datatypes from the test table
        {
            field.type
            for field in parquet.read_schema(
                f"{test_dir}.cytotable_type_check_updated.parquet"
            )
            if field.name in float_cols_to_check
        }.pop()
    )

    # gather integer columns from default
    int_cols_to_check = [
        field.name
        for field in parquet.read_schema(
            f"{test_dir}.cytotable_type_check_default.parquet"
        )
        if pa.types.is_integer(field.type)
    ]

    # check that we only have "int32" types based on the columns above
    assert pa.types.is_int32(
        # a set comprehension to gather unique datatypes from the test table
        {
            field.type
            for field in parquet.read_schema(
                f"{test_dir}.cytotable_type_check_updated.parquet"
            )
            if field.name in int_cols_to_check
        }.pop()
    )

    # gather string columns from default
    string_cols_to_check = [
        field.name
        for field in parquet.read_schema(
            f"{test_dir}.cytotable_type_check_default.parquet"
        )
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
    ]

    # check that we only have "string" types based on the columns above
    assert pa.types.is_string(
        # a set comprehension to gather unique datatypes from the test table
        {
            field.type
            for field in parquet.read_schema(
                f"{test_dir}.cytotable_type_check_updated.parquet"
            )
            if field.name in string_cols_to_check
        }.pop()
    )


def test_convert_cellprofiler_sqlite_pycytominer_merge(
    load_parsl_default: None,
    fx_tempdir: str,
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
                f"{fx_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
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
                f"{fx_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.parquet"
            ),
            dest_datatype="parquet",
            join=True,
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
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
    load_parsl_default: None, fx_tempdir: str, example_sqlite_mixed_types_database: str
):
    """
    Testing _sqlite_mixed_type_query_to_parquet
    """

    result_filepath = f"{fx_tempdir}/example_mixed_types_tbl_a.parquet"
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
            parquet.write_table(
                table=_sqlite_mixed_type_query_to_parquet(
                    source_path=example_sqlite_mixed_types_database,
                    table_name=table_name,
                    chunk_size=2,
                    offset=0,
                ),
                where=result_filepath,
            )

    # check schema names
    assert parquet.read_schema(where=result_filepath).names == [
        "col_integer",
        "col_text",
        "col_blob",
        "col_real",
    ]
    # check schema types
    assert parquet.read_schema(where=result_filepath).types == [
        pa.int64(),
        pa.string(),
        pa.binary(),
        pa.float64(),
    ]
    # check the values per column
    assert parquet.read_table(source=result_filepath).to_pydict() == {
        "col_integer": [None, 1],
        "col_text": ["sample", "sample"],
        "col_blob": [b"another_blob", b"sample_blob"],
        "col_real": [None, 0.5],
    }

    # run full convert on mixed type database
    result = convert(
        source_path=example_sqlite_mixed_types_database,
        dest_path=f"{fx_tempdir}/example_mixed_types_tbl_a.cytotable.parquet",
        dest_datatype="parquet",
        source_datatype="sqlite",
        compartments=[table_name],
        join=False,
    )

    # assert that the single table result looks like the following dictionary
    assert parquet.read_table(
        source=result["Tbl_a.sqlite"][0]["table"][0]
    ).to_pydict() == {
        "Tbl_a_col_integer": [None, 1],
        "Tbl_a_col_text": ["sample", "sample"],
        "Tbl_a_col_blob": [b"another_blob", b"sample_blob"],
        "Tbl_a_col_real": [None, 0.5],
    }


def test_cell_health_cellprofiler_to_cytominer_database_legacy(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cytominerdatabase: str,
    fixture_cytominerdatabase_merged_cellhealth: pa.Table,
):
    """
    Tests cytotable functionality leveraging a preset for
    Cell-Health datasets which were generated using a combination
    of CellProfiler and cytominer-database feature data.
    """

    # run convert on the test dataset and read the file into an arrow table
    test_result = parquet.read_table(
        source=convert(
            source_path=f"{data_dir_cytominerdatabase}/Cell-Health/test-SQ00014613.sqlite",
            dest_path=f"{fx_tempdir}/Cell-Health",
            dest_datatype="parquet",
            source_datatype="sqlite",
            preset="cell-health-cellprofiler-to-cytominer-database",
        )
    )

    # check that we have the expected shape
    assert test_result.shape == (12, 1790)
    # check that the tablenumber data arrived properly
    assert set(test_result["Metadata_TableNumber"].to_pylist()) == {
        "88ac13033d9baf49fda78c3458bef89e",
        "1e5d8facac7508cfd4086f3e3e950182",
    }
    # check that mixed-type data was successfully transitioned into
    # a compatible and representative data type.
    assert (
        # filter the table using the parameters below to gather
        # what was originally a 'nan' string value in a double column
        # which will translate from CytoTable into a
        # parquet NULL, arrow null, and Python None
        test_result.filter(
            (pc.field("Metadata_TableNumber") == "88ac13033d9baf49fda78c3458bef89e")
            & (pc.field("Nuclei_ObjectNumber") == 5)
        )["Nuclei_Correlation_Costes_AGP_DNA"].to_pylist()[0]
        is None
    ) and (
        # similar to the above filter but gathering all other
        # results from the same column to verify they are of
        # float type.
        all(
            isinstance(value, float)
            for value in test_result.filter(
                (pc.field("Metadata_TableNumber") == "88ac13033d9baf49fda78c3458bef89e")
                & (pc.field("Nuclei_ObjectNumber") != 5)
            )["Nuclei_Correlation_Costes_AGP_DNA"].to_pylist()
        )
    )

    # assert that a manually configured table is equal to the cytotable result
    # note: we sort values by all column names ascendingly for equality comparisons
    assert test_result.sort_by(
        [(name, "ascending") for name in test_result.column_names]
    ).equals(
        fixture_cytominerdatabase_merged_cellhealth.sort_by(
            [
                (name, "ascending")
                for name in fixture_cytominerdatabase_merged_cellhealth.column_names
            ]
        )
    )


def test_in_carta_to_parquet(
    load_parsl_default: None, fx_tempdir: str, data_dirs_in_carta: List[str]
):
    """
    Testing IN Carta preset with CytoTable convert to parquet output.
    """

    for data_dir in data_dirs_in_carta:
        # read the directory of data with wildcard
        with duckdb.connect() as ddb:
            ddb_result = ddb.execute(
                f"""
                SELECT *
                FROM read_csv_auto('{data_dir}/*.csv')
                """
            ).arrow()

        # process the data with cytotable using in-carta preset
        cytotable_result = convert(
            source_path=data_dir,
            dest_path=f"{fx_tempdir}/{pathlib.Path(data_dir).name}",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="in-carta",
            join=False,
        )

        # Check that the returned value isn't a string with an extension but
        # no common path.
        assert list(cast(dict, cytotable_result).keys())[0] != ".csv"

        # read the result from CytoTable as a table
        cytotable_result_table = parquet.read_table(
            # note: we use cast here to explicitly tell mypy about the types involved
            cast(list, cytotable_result[list(cast(dict, cytotable_result).keys())[0]])[
                0
            ]["table"][0]
        )

        # check the data against one another
        assert cytotable_result_table.schema.equals(ddb_result.schema)
        assert cytotable_result_table.shape == ddb_result.shape

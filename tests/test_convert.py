"""
Tests for CytoTable.convert and related.
"""

# pylint: disable=no-member

import itertools
import pathlib
from shutil import copy
from typing import Any, Dict, List, Tuple, cast

import parsl
import pyarrow as pa
import pytest
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from pyarrow import csv, parquet
from pycytominer.cyto_utils.cells import SingleCells

from cytotable.convert import (
    _concat_join_sources,
    _concat_source_group,
    _get_join_chunks,
    _infer_source_group_common_schema,
    _join_source_chunk,
    _prepend_column_name,
    _to_parquet,
    convert,
)
from cytotable.presets import config
from cytotable.sources import _get_source_filepaths, _infer_source_datatype
from cytotable.utils import _column_sort, _duckdb_reader


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
                "CONFIG_CHUNK_COLUMNS",
                "CONFIG_JOINS",
                "CONFIG_SOURCE_VERSION",
            ]
        ) == sorted(config_preset.keys())


def test_get_source_filepaths(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests _get_source_filepaths
    """

    # test that no sources raises an exception
    empty_dir = pathlib.Path(f"{get_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = _get_source_filepaths(
            path=empty_dir,
            targets=["image", "cells", "nuclei", "cytoplasm"],
        ).result()

    # check that single sqlite file is returned as desired
    single_file_result = _get_source_filepaths(
        path=pathlib.Path(
            f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"
        ),
        targets=["cells"],
    ).result()
    assert len(set(single_file_result.keys())) == 1

    # check that single csv file is returned as desired
    single_file_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman/Cells.csv"),
        targets=["cells"],
    ).result()
    assert len(set(single_file_result.keys())) == 1

    single_dir_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["cells"],
    ).result()
    # test that the single dir structure includes 1 unique key (for cells)
    assert len(set(single_dir_result.keys())) == 1

    single_dir_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["image", "cells", "nuclei", "cytoplasm"],
    ).result()
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4


def test_prepend_column_name(get_tempdir: str):
    """
    Tests _prepend_column_name
    """

    # example cytoplasm csv table run
    prepend_testpath_1 = f"{get_tempdir}/prepend_testpath_1.parquet"
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
    repend_testpath_2 = f"{get_tempdir}/prepend_testpath_2.parquet"
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
    get_tempdir: str,
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
        dest_path=get_tempdir,
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
    pathlib.Path(f"{get_tempdir}/example/5").mkdir(parents=True, exist_ok=True)
    csv.write_csv(mismatching_table, f"{get_tempdir}/example/5/nuclei.csv")
    parquet.write_table(mismatching_table, f"{get_tempdir}/example/5.nuclei.parquet")
    example_local_sources["nuclei.csv"].append(
        {
            "source_path": pathlib.Path(f"{get_tempdir}/example/5/nuclei.csv"),
            "destination_path": pathlib.Path(f"{get_tempdir}/example/5.nuclei.parquet"),
        }
    )

    with pytest.raises(Exception):
        _concat_source_group(
            source_group_name="nuclei",
            source_group=example_local_sources["nuclei.csv"],
            dest_path=get_tempdir,
        ).result()


def test_get_join_chunks(get_tempdir: str):
    """
    Tests _get_join_chunks
    """

    # form test path
    test_path = f"{get_tempdir}/merge_chunks_test.parquet"

    # write test data to file
    parquet.write_table(
        table=pa.Table.from_pydict(
            {
                "id1": [1, 2, 3, 1, 2, 3],
                "id2": ["a", "a", "a", "b", "b", "b"],
                "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
                "field2": [True, False, True, True, False, True],
            }
        ),
        where=test_path,
    )

    result = _get_join_chunks(
        sources={"merge_chunks_test.parquet": [{"table": [test_path]}]},
        metadata=["merge_chunks_test"],
        chunk_columns=["id1", "id2"],
        chunk_size=2,
    ).result()

    # test that we have 3 chunks of merge columns
    assert len(result) == 3
    # test that we have only the columns we specified
    assert set(
        itertools.chain(
            *[list(chunk_item.keys()) for chunk in result for chunk_item in chunk]
        )
    ) == {"id1", "id2"}


def test_join_source_chunk(get_tempdir: str):
    """
    Tests _get_join_chunks
    """

    # form test path a
    test_path_a = f"{get_tempdir}/example_a_merged.parquet"
    # form test path b
    test_path_b = f"{get_tempdir}/example_b_merged.parquet"

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
        sources={
            "example_a": [{"table": [test_path_a]}],
            "example_b": [{"table": [test_path_b]}],
        },
        dest_path=f"{get_tempdir}/destination.parquet",
        joins=f"""
            SELECT *
            FROM read_parquet('{get_tempdir}/example_a_merged.parquet') as example_a
            JOIN read_parquet('{get_tempdir}/example_b_merged.parquet') as example_b ON
                example_b.id1 = example_a.id1
                AND example_b.id2 = example_a.id2
        """,
        join_group=[{"id1": 1, "id2": "a"}, {"id1": 2, "id2": "a"}],
        drop_null=True,
    ).result()

    assert isinstance(result, str)
    result_table = parquet.read_table(source=result)
    assert result_table.equals(
        other=pa.Table.from_pydict(
            {
                "id1": [1, 2],
                "id2": ["a", "a"],
                "field1": ["foo", "bar"],
                "field2": [True, False],
            },
            # use schema from result as a reference for col order
            schema=result_table.schema,
        )
    )


def test_concat_join_sources(get_tempdir: str):
    """
    Tests _concat_join_sources
    """

    # create a test dir
    pathlib.Path(f"{get_tempdir}/concat_join/").mkdir(exist_ok=True)

    # form test paths
    test_path_a = f"{get_tempdir}/concat_join/join_chunks_test_a.parquet"
    test_path_b = f"{get_tempdir}/concat_join/join_chunks_test_b.parquet"
    test_path_a_join_chunk = f"{get_tempdir}/join_chunks_test_a.parquet"
    test_path_b_join_chunk = f"{get_tempdir}/join_chunks_test_b.parquet"

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

    pathlib.Path(f"{get_tempdir}/test_concat_join_sources").mkdir(
        parents=True, exist_ok=True
    )

    result = _concat_join_sources(
        dest_path=f"{get_tempdir}/test_concat_join_sources/example_concat_join.parquet",
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


def test_infer_source_datatype():
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
    get_tempdir: str, example_local_sources: Dict[str, List[Dict[str, Any]]]
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
            dest_path=get_tempdir,
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
        ).result(),
    )

    flattened_results = list(itertools.chain(*list(result.values())))
    for i, flattened_result in enumerate(flattened_results):
        parquet_result = parquet.ParquetDataset(
            path_or_paths=flattened_result["table"]
        ).read()
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
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape


def test_convert_s3_path_csv(
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

    assert local_cytotable_table.sort_by(
        [(name, "ascending") for name in local_cytotable_table.schema.names]
    ).equals(
        s3_cytotable_table.sort_by(
            [(name, "ascending") for name in s3_cytotable_table.schema.names]
        )
    )
    assert local_cytotable_table.sort_by(
        [(name, "ascending") for name in local_cytotable_table.schema.names]
    ).equals(
        s3_cytotable_table_nested.sort_by(
            [(name, "ascending") for name in s3_cytotable_table_nested.schema.names]
        )
    )


def test_infer_source_group_common_schema(
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
    get_tempdir: str, data_dir_cellprofiler: str, cellprofiler_merged_nf1data: pa.Table
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
    get_tempdir: str,
    data_dir_cellprofiler_sqlite_nf1: str,
):
    """
    Tests _cast_data_types to ensure data types are casted as expected
    """

    test_dir = f"{get_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
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
            "string": "string",
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

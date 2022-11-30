"""
Tests for cyctominer_transform/convert.py
"""
import io
import itertools
import pathlib
from shutil import copy
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pytest
from cloudpathlib import AnyPath
from prefect_dask.task_runners import DaskTaskRunner
from pyarrow import csv, parquet

from pycytominer_transform import (  # pylint: disable=R0801
    concat_join_records,
    concat_record_group,
    config,
    convert,
    get_join_chunks,
    get_source_filepaths,
    infer_record_group_common_schema,
    infer_source_datatype,
    join_record_chunk,
    prepend_column_name,
    read_data,
    to_parquet,
    write_parquet,
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
                "CONFIG_CHUNK_COLUMNS",
                "CONFIG_JOINS",
            ]
        ) == sorted(config_preset.keys())


def test_get_source_filepaths(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests get_source_filepaths
    """

    # test that no records raises an exception
    empty_dir = pathlib.Path(f"{get_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = get_source_filepaths.fn(
            path=empty_dir, targets=["image", "cells", "nuclei", "cytoplasm"],
        )

    single_dir_result = get_source_filepaths.fn(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"), targets=["cells"],
    )
    # test that the single dir structure includes 1 unique key (for cells)
    assert len(set(single_dir_result.keys())) == 1

    single_dir_result = get_source_filepaths.fn(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["image", "cells", "nuclei", "cytoplasm"],
    )
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4


def test_read_data(get_tempdir: str):
    """
    Tests read_csv
    """

    # valid csv data (last row invalid rowcount)
    data = io.BytesIO("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False".encode())
    table = csv.read_csv(input_file=data)
    destination = f"{get_tempdir}/sample.csv"

    csv.write_csv(data=table, output_file=destination)

    result = read_data.fn(record={"source_path": destination})

    assert isinstance(result, dict)
    assert sorted(list(result.keys())) == sorted(["source_path", "table"])
    assert result["table"].schema.equals(table.schema)
    assert result["table"].shape == table.shape

    # cover invalid number of rows read ignores
    destination_err = f"{get_tempdir}/sample.csv"
    with open(destination_err, "w", encoding="utf-8") as wfile:
        wfile.write("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False,1")

    assert isinstance(read_data.fn(record={"source_path": destination_err}), Dict)


def test_prepend_column_name():
    """
    Tests prepend_column_name
    """

    # write test data to file
    test_table = pa.Table.from_pydict(
        {
            "id1": [1, 2, 3, 1, 2, 3],
            "id2": ["a", "a", "a", "b", "b", "b"],
            "field1": ["foo", "bar", "baz", "foo", "bar", "baz"],
            "field2": [True, False, True, True, False, True],
        }
    )

    result = prepend_column_name.fn(
        record={"table": test_table},
        record_group_name="Cells.csv",
        identifying_columns=["id1", "id2"],
        metadata=[],
    )

    assert result["table"].column_names == [
        "Metadata_Cells_id1",
        "Metadata_Cells_id2",
        "Cells_field1",
        "Cells_field2",
    ]


def test_concat_record_group(
    get_tempdir: str,
    example_tables: Tuple[pa.Table, pa.Table, pa.Table],
    example_local_records: Dict[str, List[Dict[str, Any]]],
):
    """
    Tests concat_record_group
    """

    table_a, table_b, _ = example_tables

    # simulate concat
    concat_table = pa.concat_tables(
        [table_a.select(["n_legs", "animals"]).cast(table_b.schema), table_b]
    )

    result = concat_record_group.fn(
        record_group=example_local_records["animal_legs.csv"],
        dest_path=get_tempdir,
        common_schema=table_b.schema,
    )
    assert len(result) == 1
    assert parquet.read_schema(result[0]["destination_path"]) == concat_table.schema
    assert (
        parquet.read_metadata(result[0]["destination_path"]).num_rows,
        parquet.read_metadata(result[0]["destination_path"]).num_columns,
    ) == concat_table.shape

    # add a mismatching record to animal_legs.csv group
    table_e = pa.Table.from_pydict(
        {"color": pa.array(["blue", "red", "green", "orange"]),}
    )
    pathlib.Path(f"{get_tempdir}/animals/e").mkdir(parents=True, exist_ok=True)
    csv.write_csv(table_e, f"{get_tempdir}/animals/e/animal_legs.csv")
    parquet.write_table(table_e, f"{get_tempdir}/animals/e.animal_legs.parquet")
    example_local_records["animal_legs.csv"].append(
        {
            "source_path": pathlib.Path(f"{get_tempdir}/animals/e/animal_legs.csv"),
            "destination_path": pathlib.Path(
                f"{get_tempdir}/animals/e.animal_legs.parquet"
            ),
        }
    )

    with pytest.raises(Exception):
        concat_record_group.fn(
            record_group=example_local_records["animal_legs.csv"],
            dest_path=get_tempdir,
        )


def test_get_join_chunks(get_tempdir: str):
    """
    Tests get_join_chunks
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

    result = get_join_chunks.fn(
        records={"merge_chunks_test.parquet": [{"destination_path": test_path}]},
        metadata=["merge_chunks_test"],
        chunk_columns=["id1", "id2"],
        chunk_size=2,
    )

    # test that we have 3 chunks of merge columns
    assert len(result) == 3
    # test that we have only the columns we specified
    assert set(
        itertools.chain(
            *[list(chunk_item.keys()) for chunk in result for chunk_item in chunk]
        )
    ) == {"id1", "id2"}


def test_join_record_chunk(get_tempdir: str):
    """
    Tests get_join_chunks
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

    result = join_record_chunk.fn(
        records={
            "example_a": [{"destination_path": test_path_a}],
            "example_b": [{"destination_path": test_path_b}],
        },
        dest_path=f"{get_tempdir}/destination.parquet",
        joins="""
            SELECT *
            FROM read_parquet('example_a_merged.parquet') as example_a
            JOIN read_parquet('example_b_merged.parquet') as example_b ON
                example_b.id1 = example_a.id1
                AND example_b.id2 = example_a.id2
        """,
        join_group=[{"id1": 1, "id2": "a"}, {"id1": 2, "id2": "a"}],
        drop_null=True,
    )

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


def test_concat_join_records(get_tempdir: str):
    """
    Tests concat_join_records
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
            "id1": [1, 2, 3,],
            "id2": ["a", "a", "a",],
            "field1": ["foo", "bar", "baz",],
            "field2": [True, False, True,],
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
        table=test_table_a, where=test_path_a,
    )
    parquet.write_table(
        table=test_table_b, where=test_path_b,
    )

    # copy the files for testing purposes
    copy(test_path_a, test_path_a_join_chunk)
    copy(test_path_b, test_path_b_join_chunk)

    result = concat_join_records.fn(
        dest_path=f"{get_tempdir}/example_concat_join.parquet",
        join_records=[test_path_a_join_chunk, test_path_b_join_chunk],
        records={
            "join_chunks_test_a.parquet": [{"destination_path": test_path_a}],
            "join_chunks_test_b.parquet": [{"destination_path": test_path_b}],
        },
    )

    # ensure the concatted result is what we expect
    assert parquet.read_table(source=result).equals(
        pa.concat_tables(tables=[test_table_a, test_table_b])
    )

    # ensure the test paths provided via records were removed (unlinked)
    assert (pathlib.Path(test_path_a).exists() is False) and (
        pathlib.Path(test_path_a).exists() is False
    )


def test_write_parquet(get_tempdir: str):
    """
    Tests write_parquet
    """

    table = pa.Table.from_pydict(
        {"color": pa.array(["blue", "red", "green", "orange"])}
    )

    result = write_parquet.fn(
        record={
            "source_path": AnyPath(f"{get_tempdir}/example/colors.csv"),
            "table": table,
        },
        dest_path=f"{get_tempdir}/new_path",
        unique_name=False,
    )
    result_table = parquet.read_table(result["destination_path"])

    assert sorted(list(result.keys())) == sorted(["destination_path", "source_path"])
    assert result_table.schema == table.schema
    assert result_table.shape == table.shape
    result = write_parquet.fn(
        record={
            "source_path": pathlib.Path(f"{get_tempdir}/example/colors.csv"),
            "table": table,
        },
        dest_path=f"{get_tempdir}/new_path",
        unique_name=True,
    )
    assert (
        result["destination_path"].stem
        != pathlib.Path(f"{get_tempdir}/example/colors.csv").stem
    )


def test_infer_source_datatype():
    """
    Tests infer_source_datatype
    """

    data = {
        "sample_1.csv": [{"source_path": "stub"}],
        "sample_2.CSV": [{"source_path": "stub"}],
    }
    assert infer_source_datatype.fn(records=data) == "csv"
    with pytest.raises(Exception):
        infer_source_datatype.fn(records=data, source_datatype="parquet")

    data["sample_3.parquet"] = [{"source_path": "stub"}]
    assert (
        infer_source_datatype.fn(records=data, source_datatype="parquet") == "parquet"
    )
    with pytest.raises(Exception):
        infer_source_datatype.fn(records=data)


def test_to_parquet(
    get_tempdir: str, example_local_records: Dict[str, List[Dict[str, Any]]]
):
    """
    Tests to_parquet
    """

    flattened_example_records = list(
        itertools.chain(*list(example_local_records.values()))
    )

    result = to_parquet(
        source_path=str(
            example_local_records["animal_legs.csv"][0]["source_path"].parent
        ),
        dest_path=get_tempdir,
        source_datatype=None,
        compartments=["animal_legs", "colors"],
        metadata=[],
        identifying_columns=["animals"],
        concat=False,
        join=False,
        joins=None,
        chunk_columns=None,
        chunk_size=None,
        infer_common_schema=False,
        drop_null=True,
    )

    flattened_results = list(itertools.chain(*list(result.values())))
    for i, flattened_result in enumerate(flattened_results):
        parquet_result = parquet.read_table(source=flattened_result["destination_path"])
        csv_source = csv.read_csv(flattened_example_records[i]["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape


def test_convert_s3_path(
    get_tempdir: str,
    example_local_records: Dict[str, List[Dict[str, Any]]],
    example_s3_endpoint: str,
):
    """
    Tests convert with mocked s3 object storage endpoint
    """

    multi_dir_nonconcat_s3_result = convert(
        source_path="s3://example/",
        dest_path=f"{get_tempdir}/s3_test",
        dest_datatype="parquet",
        concat=False,
        join=False,
        source_datatype="csv",
        # override default compartments for those which were uploaded to mock instance
        compartments=["animal_legs", "colors"],
        # endpoint_url here will be used with cloudpathlib client(**kwargs)
        endpoint_url=example_s3_endpoint,
    )

    # gather destination paths used from source files for testing
    destination_paths = {
        pathlib.Path(record["destination_path"]).name: record["destination_path"]
        for group in example_local_records.values()
        for record in group
    }

    # compare each of the results using files from the source
    for destination_path in [
        record["destination_path"]
        for group in multi_dir_nonconcat_s3_result.values()
        for record in group
    ]:
        parquet_control = parquet.read_table(
            destination_paths[pathlib.Path(destination_path).name]
        )
        parquet_result = parquet.read_table(
            destination_path, schema=parquet_control.schema
        )
        assert parquet_result.schema.equals(parquet_control.schema)
        assert parquet_result.shape == parquet_control.shape


def test_infer_record_group_common_schema(
    example_local_records: Dict[str, List[Dict[str, Any]]],
    example_tables: Tuple[pa.Table, pa.Table, pa.Table],
):
    """
    Tests infer_record_group_common_schema
    """
    _, table_b, _ = example_tables

    result = infer_record_group_common_schema.fn(
        record_group=example_local_records["animal_legs.csv"],
    )

    assert table_b.schema.equals(pa.schema(result))


def test_convert_cytominerdatabase_csv(
    get_tempdir: str,
    data_dirs_cytominerdatabase: List[str],
    pycytominer_merge_single_cells_parquet: List[str],
):
    """
    Tests convert with cytominerdatabase csvs and processed
    csvs from cytominer-database to pycytominer merge_single_cells
    """

    for test_set in zip(
        data_dirs_cytominerdatabase, pycytominer_merge_single_cells_parquet
    ):
        # load control table, dropping tablenumber and unlabeled objectnumber (no compartment specified)
        control_table = parquet.read_table(source=test_set[1]).drop(
            [
                # tablenumber is not implemented within pycytominer-transform
                "Metadata_TableNumber",
                # objectnumber references are provided via cytoplasm parent object joins
                "Metadata_ObjectNumber",
                "Metadata_ObjectNumber_cells",
                "__index_level_0__",
            ]
        )
        # rename column to account for minor difference in processing
        control_table = control_table.rename_columns(
            [
                # rename based on compartment prefix name within pycytominer-transform format
                col if col != "Cells_Parent_Nuclei" else "Metadata_Cells_Parent_Nuclei"
                for col in control_table.schema.names
            ]
        )

        # load test table by reading parquet-based output from convert
        test_table = parquet.read_table(
            source=convert(
                source_path=test_set[0],
                dest_path=f"{get_tempdir}/{pathlib.Path(test_set[0]).name}.test_table.parquet",
                dest_datatype="parquet",
                source_datatype="csv",
                merge=True,
            )[pathlib.Path(f"{test_set[0]}.test_table.parquet").name][0][
                "destination_path"
            ],
            schema=control_table.schema,
        )
        assert control_table.schema.equals(test_table.schema)
        assert control_table.num_columns == test_table.num_columns
        assert control_table.num_rows <= test_table.num_rows
        # assert control_table.equals(test_table.drop_null())


def test_convert_cellprofiler_sqlite(
    get_tempdir: str, data_dirs_cellprofiler_sqlite: str,
):
    """
    Tests convert with cellprofiler sqlite exports
    """
    pass


def test_convert_cellprofiler_csv(
    get_tempdir: str, data_dir_cellprofiler: str,
):
    """
    Tests convert

    Note: uses default prefect task_runner from convert
    Dedicated tests for prefect-dask runner elsewhere.
    """

    with pytest.raises(Exception):
        single_dir_result = convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{get_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            compartments=[],
            source_datatype="csv",
        )

    single_dir_result = convert(
        source_path=f"{data_dir_cellprofiler}/ExampleHuman",
        dest_path=f"{get_tempdir}/ExampleHuman",
        dest_datatype="parquet",
        source_datatype="csv",
        concat=False,
        merge=False,
        merge_columns=None,
    )

    # loop through the results to ensure data matches what we expect
    # note: these are flattened and unique to each of the sets above.
    for result in itertools.chain(*(list(single_dir_result.values()))):
        parquet_result = parquet.read_table(source=result["destination_path"])
        csv_source = csv.read_csv(input_file=result["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape


def test_convert_dask_cellprofiler_csv(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests convert

    Note: dedicated test for prefect-dask runner.
    """

    multi_dir_nonconcat_result = convert(
        source_path=f"{data_dir_cellprofiler}/ExampleHuman",
        dest_path=f"{get_tempdir}/ExampleHuman",
        dest_datatype="parquet",
        concat=False,
        merge=False,
        task_runner=DaskTaskRunner,
    )

    # loop through the results to ensure data matches what we expect
    # note: these are flattened and unique to each of the sets above.
    for result in itertools.chain(*list(multi_dir_nonconcat_result.values())):
        parquet_result = parquet.read_table(source=result["destination_path"])
        csv_source = csv.read_csv(input_file=result["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape

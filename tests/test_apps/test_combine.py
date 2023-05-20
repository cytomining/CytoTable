"""
Tests for cytotable.apps.combine
"""

# pylint: disable=unused-argument

import itertools
import pathlib
from shutil import copy
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pytest
from pyarrow import csv, parquet

from cytotable.apps.combine import (
    _concat_join_sources,
    _concat_source_group,
    _get_join_chunks,
    _infer_source_group_common_schema,
    _join_source_chunk,
)


def test_concat_source_group(
    load_parsl: None,
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

    result = _concat_source_group(  # pylint: disable=no-member
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
        _concat_source_group(  # pylint: disable=no-member
            source_group_name="nuclei",
            source_group=example_local_sources["nuclei.csv"],
            dest_path=get_tempdir,
        ).result()


def test_get_join_chunks(load_parsl: None, get_tempdir: str):
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


def test_join_source_chunk(load_parsl: None, get_tempdir: str):
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


def test_concat_join_sources(load_parsl: None, get_tempdir: str):
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


def test_infer_source_group_common_schema(
    load_parsl: None,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    example_tables: Tuple[pa.Table, ...],
):
    """
    Tests _infer_source_group_common_schema
    """
    _, _, _, table_nuclei_1, _ = example_tables

    result = _infer_source_group_common_schema(  # pylint: disable=no-member
        source_group=example_local_sources["nuclei.csv"],
    ).result()

    assert table_nuclei_1.schema.equals(pa.schema(result))

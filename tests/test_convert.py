"""
Tests for cyctominer_transform/convert.py
"""
import io
import itertools
import pathlib
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pytest
from cloudpathlib import AnyPath
from prefect_dask.task_runners import DaskTaskRunner
from pyarrow import csv, parquet

from pycytominer_transform import (  # pylint: disable=R0801
    concat_record_group,
    convert,
    get_source_filepaths,
    infer_source_datatype,
    read_file,
    to_parquet,
    write_parquet,
)


def test_get_source_filepaths(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests get_source_filepaths
    """

    # test that no records raises an exception
    empty_dir = pathlib.Path(f"{get_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = get_source_filepaths.fn(
            path=empty_dir,
            compartments=["image", "cells", "nuclei", "cytoplasm"],
        )

    single_dir_result = get_source_filepaths.fn(
        path=pathlib.Path(f"{data_dir_cellprofiler}/csv_single"),
        compartments=["image", "cells", "nuclei", "cytoplasm"],
    )
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4

    multi_dir_result = get_source_filepaths.fn(
        path=pathlib.Path(f"{data_dir_cellprofiler}/csv_multi"),
        compartments=["image", "cells", "nuclei", "cytoplasm"],
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


def test_read_file(get_tempdir: str):
    """
    Tests read_csv
    """

    # valid csv data (last row invalid rowcount)
    data = io.BytesIO("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False".encode())
    table = csv.read_csv(input_file=data)
    destination = f"{get_tempdir}/sample.csv"

    csv.write_csv(data=table, output_file=destination)

    result = read_file.fn(record={"source_path": destination})

    assert isinstance(result, dict)
    assert sorted(list(result.keys())) == sorted(["source_path", "table"])
    assert result["table"].schema.equals(table.schema)
    assert result["table"].shape == table.shape

    # cover invalid number of rows read ignores
    destination_err = f"{get_tempdir}/sample.csv"
    with open(destination_err, "w", encoding="utf-8") as wfile:
        wfile.write("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False,1")

    assert isinstance(read_file.fn(record={"source_path": destination_err}), Dict)


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
    )
    assert len(result) == 1
    assert parquet.read_schema(result[0]["destination_path"]) == concat_table.schema
    assert (
        parquet.read_metadata(result[0]["destination_path"]).num_rows,
        parquet.read_metadata(result[0]["destination_path"]).num_columns,
    ) == concat_table.shape

    # add a mismatching record to animal_legs.csv group
    table_e = pa.Table.from_pydict(
        {
            "color": pa.array(["blue", "red", "green", "orange"]),
        }
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
        concat=False,
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


def test_convert_cellprofiler_csv(
    get_tempdir: str,
    data_dir_cellprofiler: str,
):
    """
    Tests convert

    Note: uses default prefect task_runner from convert
    Dedicated tests for prefect-dask runner elsewhere.
    """

    with pytest.raises(Exception):
        single_dir_result = convert(
            source_path=f"{data_dir_cellprofiler}/csv_single",
            dest_path=f"{get_tempdir}/csv_single",
            dest_datatype="parquet",
            compartments=[],
            source_datatype="csv",
        )

    single_dir_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_single",
        dest_path=f"{get_tempdir}/csv_single",
        dest_datatype="parquet",
        source_datatype="csv",
    )

    multi_dir_nonconcat_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_multi",
        dest_path=f"{get_tempdir}/csv_multi_nonconcat",
        dest_datatype="parquet",
        concat=False,
        source_datatype="csv",
    )

    # loop through the results to ensure data matches what we expect
    # note: these are flattened and unique to each of the sets above.
    for result in itertools.chain(
        *(list(single_dir_result.values()) + list(multi_dir_nonconcat_result.values()))
    ):
        parquet_result = parquet.read_table(source=result["destination_path"])
        csv_source = csv.read_csv(input_file=result["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape

    multi_dir_concat_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_multi",
        dest_path=f"{get_tempdir}/csv_multi_concat",
        dest_datatype="parquet",
        concat=True,
        source_datatype="csv",
    )

    # loop through the results to ensure data matches what we expect
    # note: these are flattened and unique to each of the sets above.
    for result in itertools.chain(*list(multi_dir_concat_result.values())):
        csv_source = pa.concat_tables(
            [
                csv.read_csv(file)
                for file in pathlib.Path(result["source_path"].parent).glob(
                    f"**/{result['source_path'].stem}.csv"
                )
            ]
        )

        parquet_result = parquet.read_table(source=result["destination_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape


def test_convert_dask_cellprofiler_csv(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests convert

    Note: dedicated test for prefect-dask runner.
    """

    multi_dir_nonconcat_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_multi",
        dest_path=f"{get_tempdir}/csv_multi_nonconcat",
        dest_datatype="parquet",
        concat=False,
        task_runner=DaskTaskRunner,
    )

    # loop through the results to ensure data matches what we expect
    # note: these are flattened and unique to each of the sets above.
    for result in itertools.chain(*list(multi_dir_nonconcat_result.values())):
        parquet_result = parquet.read_table(source=result["destination_path"])
        csv_source = csv.read_csv(input_file=result["source_path"])
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape

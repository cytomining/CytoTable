"""
Tests for cyctominer_transform/convert.py
"""
import io
import itertools
import pathlib

import pyarrow as pa
from pyarrow import csv, parquet

from pycytominer_transform import (
    concat_tables,
    convert,
    get_source_filepaths,
    read_csv,
    write_parquet,
)


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


def test_read_csv(get_tempdir: str):
    """
    Tests read_csv
    """

    data = io.BytesIO("col_1,col_2,col_3,col_4\n1,0.1,a,True\n2,0.2,b,False".encode())
    table = csv.read_csv(input_file=data)
    destination = f"{get_tempdir}/sample.csv"

    csv.write_csv(data=table, output_file=destination)

    result = read_csv.fn(record={"source_path": destination})

    assert isinstance(result, dict)
    assert sorted(list(result.keys())) == sorted(["source_path", "table"])
    assert result["table"].schema.equals(table.schema)
    assert result["table"].shape == table.shape


def test_concat_tables(get_tempdir: str):
    """
    Tests concat_tables
    """

    table_a = pa.Table.from_pydict(
        {
            "n_legs": pa.array(
                [
                    2,
                    4,
                ]
            ),
            "animals": pa.array(
                [
                    "Flamingo",
                    "Horse",
                ]
            ),
        }
    )
    table_b = pa.Table.from_pydict(
        {
            "n_legs": pa.array([5, 100]),
            "animals": pa.array(["Brittle stars", "Centipede"]),
        }
    )
    table_c = pa.Table.from_pydict(
        {
            "color": pa.array(["blue", "red", "green", "orange"]),
        }
    )

    result = concat_tables.fn(
        records={
            "animal_legs.csv": [
                {
                    "source_path": pathlib.Path(
                        f"{get_tempdir}/animals/a/animal_legs.csv"
                    ),
                    "table": table_a,
                },
                {
                    "source_path": pathlib.Path(
                        f"{get_tempdir}/animals/b/animal_legs.csv"
                    ),
                    "table": table_b,
                },
            ],
            "colors.csv": [
                {
                    "source_path": pathlib.Path(f"{get_tempdir}/animals/c/colors.csv"),
                    "table": table_c,
                }
            ],
        }
    )

    concat_table = pa.concat_tables([table_a, table_b])

    assert len(result["animal_legs.csv"]) == 1
    assert (
        pathlib.Path(f"{get_tempdir}/animals/animal_legs.csv")
        == result["animal_legs.csv"][0]["source_path"]
    )
    assert result["animal_legs.csv"][0]["table"].schema == concat_table.schema
    assert result["animal_legs.csv"][0]["table"].shape == concat_table.shape
    assert len(result["colors.csv"]) == 1
    assert (
        pathlib.Path(f"{get_tempdir}/animals/c/colors.csv")
        == result["colors.csv"][0]["source_path"]
    )
    assert result["colors.csv"][0]["table"].schema == table_c.schema
    assert result["colors.csv"][0]["table"].shape == table_c.shape


def test_write_parquet(get_tempdir: str):
    """
    Tests write_parquet
    """

    table = pa.Table.from_pydict(
        {"color": pa.array(["blue", "red", "green", "orange"])}
    )
    data = {
        "source_path": pathlib.Path(f"{get_tempdir}/example/colors.csv"),
        "table": table,
    }

    result = write_parquet.fn(
        record=data, dest_path=f"{get_tempdir}/new_path", unique_name=False
    )
    result_table = parquet.read_table(result["destination_path"])

    assert sorted(list(result.keys())) == sorted(
        ["destination_path", "source_path", "table"]
    )
    assert result_table.schema == table.schema
    assert result_table.shape == table.shape
    result = write_parquet.fn(
        record=data, dest_path=f"{get_tempdir}/new_path", unique_name=True
    )
    assert result["destination_path"].stem != data["source_path"].stem


def test_convert_cellprofiler_csv(get_tempdir: str, data_dir_cellprofiler: str):
    """
    Tests convert
    """

    single_dir_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_single",
        dest_path=f"{get_tempdir}/csv_single",
        dest_datatype="parquet",
    )

    multi_dir_nonconcat_result = convert(
        source_path=f"{data_dir_cellprofiler}/csv_multi",
        dest_path=f"{get_tempdir}/csv_multi_nonconcat",
        dest_datatype="parquet",
        concat=False,
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

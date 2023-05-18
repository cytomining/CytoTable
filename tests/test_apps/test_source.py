"""
Tests for cytotable.apps.source
"""

# pylint: disable=unused-argument

import itertools
from typing import Any, Dict, List

from cytotable.apps.source import _gather_tablenumber


def test_gather_tablenumber(
    load_parsl: None, example_local_sources: Dict[str, List[Dict[str, Any]]]
):
    """
    Tests _gather_tablenumber
    """

    tablenumber_prepared = {
        source_group_name: [
            dict(
                source,
                **{
                    "tablenumber": _gather_tablenumber(  # pylint: disable=no-member
                        source=source,
                        source_group_name=source_group_name,
                    ).result()
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in example_local_sources.items()
    }

    # compare to see that we have a tablenumber key for each element and also
    # that we received the checksum values for the related tables
    assert [
        elem["tablenumber"]
        for elem in list(itertools.chain(*list(tablenumber_prepared.values())))
    ] == [782642759, 2915137387, 2213917770, 744364272, 3277408204]


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

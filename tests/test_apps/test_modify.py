"""
Tests for cytotable.apps.modify
"""

import pyarrow as pa
from pyarrow import parquet

from cytotable.apps.modify import _prepend_column_name


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

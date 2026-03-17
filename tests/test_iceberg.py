"""
Tests for CytoTable Iceberg helpers.
"""

from importlib.util import find_spec

import pytest

from cytotable.iceberg import _rewrite_join_sql_for_warehouse, write_iceberg_warehouse
from cytotable.presets import config


def test_rewrite_join_sql_for_warehouse():
    """
    Tests replacing parquet reads with registered relation names.
    """

    joins = config["cellprofiler_csv"]["CONFIG_JOINS"]

    rewritten = _rewrite_join_sql_for_warehouse(
        joins,
        {
            "cytoplasm": "cytoplasm",
            "cells": "cells",
            "nuclei": "nuclei",
            "image": "image",
        },
    )

    assert "read_parquet('cytoplasm.parquet')" not in rewritten
    assert "read_parquet('cells.parquet')" not in rewritten
    assert "FROM\n                cytoplasm AS cytoplasm" in rewritten
    assert "LEFT JOIN image AS image" in rewritten


def test_write_iceberg_warehouse_requires_pyiceberg(
    fx_tempdir: str, data_dir_cellprofiler: str
):
    """
    Tests that write_iceberg_warehouse fails clearly without pyiceberg.
    """

    if find_spec("pyiceberg") is not None:
        pytest.skip("pyiceberg is installed in this environment")

    with pytest.raises(ImportError, match="pyiceberg"):
        write_iceberg_warehouse(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            source_datatype="csv",
            warehouse_path=f"{fx_tempdir}/example_warehouse",
            preset="cellprofiler_csv",
        )

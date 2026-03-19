"""
Tests for generic CytoTable read/list helpers.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import parquet

from cytotable.access import list_tables, read_table
from cytotable.exceptions import CytoTableException


def test_list_tables_for_single_parquet_file(fx_tempdir: str):
    """
    Tests list_tables for a single Parquet file.
    """

    parquet_path = Path(fx_tempdir) / "example.parquet"
    parquet.write_table(pa.table({"a": [1]}), parquet_path)

    assert list_tables(parquet_path) == ["example"]


def test_read_table_for_single_parquet_file(fx_tempdir: str):
    """
    Tests read_table for a single Parquet file.
    """

    parquet_path = Path(fx_tempdir) / "example.parquet"
    parquet.write_table(pa.table({"a": [1], "b": ["x"]}), parquet_path)

    table = read_table(parquet_path)

    assert table.to_dict(orient="list") == {"a": [1], "b": ["x"]}


def test_read_table_rejects_wrong_name_for_single_parquet_file(fx_tempdir: str):
    """
    Tests read_table validation for Parquet table_name mismatches.
    """

    parquet_path = Path(fx_tempdir) / "example.parquet"
    parquet.write_table(pa.table({"a": [1]}), parquet_path)

    with pytest.raises(CytoTableException, match="single table named 'example'"):
        read_table(parquet_path, table_name="wrong")


def test_read_table_requires_table_name_for_multi_table_iceberg(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Tests read_table validation for Iceberg warehouses with multiple tables.
    """

    monkeypatch.setattr("cytotable.access._is_iceberg_warehouse", lambda path: True)
    monkeypatch.setattr(
        "cytotable.access.list_iceberg_tables",
        lambda path, include_views=True: ["profiles.joined_profiles", "images.image_crops"],
    )

    with pytest.raises(CytoTableException, match="table_name is required"):
        read_table("example_warehouse")


def test_read_table_uses_iceberg_reader_when_name_is_provided(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Tests read_table dispatch to the Iceberg reader.
    """

    expected = pd.DataFrame({"a": [1]})
    reader = MagicMock(return_value=expected)

    monkeypatch.setattr("cytotable.access._is_iceberg_warehouse", lambda path: True)
    monkeypatch.setattr("cytotable.access.read_iceberg_table", reader)

    result = read_table("example_warehouse", table_name="joined_profiles")

    assert result.equals(expected)
    reader.assert_called_once_with(Path("example_warehouse"), "joined_profiles")

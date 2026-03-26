"""
Generic table access helpers for Parquet files and Iceberg warehouses.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

import pandas as pd
from pyarrow import parquet

from cytotable.exceptions import CytoTableException

from .iceberg import (
    DEFAULT_REGISTRY_FILE,
    DEFAULT_WAREHOUSE_DIR,
    list_iceberg_tables,
    read_iceberg_table,
)


def _is_iceberg_warehouse(path: Union[str, Path]) -> bool:
    """
    Determine whether a path points to a local Iceberg warehouse root.
    """

    root = Path(path)
    return (root / DEFAULT_REGISTRY_FILE).exists() or (
        root / DEFAULT_WAREHOUSE_DIR / DEFAULT_REGISTRY_FILE
    ).exists()


def _single_parquet_table_name(path: Union[str, Path]) -> str:
    """
    Return the implied single-table name for a Parquet file or dataset path.
    """

    resolved = Path(path)
    return resolved.stem if resolved.suffix == ".parquet" else resolved.name


def list_tables(path: Union[str, Path], *, include_views: bool = True) -> list[str]:
    """
    List available table names from a Parquet path or Iceberg warehouse.
    """

    resolved = Path(path)
    if _is_iceberg_warehouse(resolved):
        return list_iceberg_tables(resolved, include_views=include_views)
    if not resolved.exists():
        raise CytoTableException(f"Path does not exist: '{resolved}'.")
    return [_single_parquet_table_name(resolved)]


def read_table(
    path: Union[str, Path],
    table_name: Optional[str] = None,
) -> pd.DataFrame:
    """
    Read a table from a Parquet path or Iceberg warehouse.
    """

    resolved = Path(path)
    if _is_iceberg_warehouse(resolved):
        if table_name is None:
            tables = list_iceberg_tables(resolved)
            if len(tables) != 1:
                raise CytoTableException(
                    "table_name is required when reading from an Iceberg warehouse "
                    "with multiple tables or views."
                )
            table_name = tables[0]
        return read_iceberg_table(resolved, table_name)

    if not resolved.exists():
        raise CytoTableException(f"Path does not exist: '{resolved}'.")

    single_name = _single_parquet_table_name(resolved)
    if table_name is not None and table_name not in {single_name, resolved.name}:
        raise CytoTableException(
            f"Parquet path '{resolved}' exposes a single table named "
            f"'{single_name}', not '{table_name}'."
        )
    return parquet.read_table(resolved).to_pandas()


__all__ = ["list_tables", "read_table"]

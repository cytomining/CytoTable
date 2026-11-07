"""Regression tests for inferred pagination keys and warehouse configuration."""

# pylint: disable=unused-argument

import pathlib
from importlib.util import find_spec

import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import parquet

from cytotable.convert import convert
from cytotable.exceptions import CytoTableException
from cytotable.joins import (
    _column_has_nulls,
    _default_identifying_columns,
    _infer_source_page_keys,
    infer_join_sql,
)
from cytotable.presets import config
from cytotable.sources import _gather_sources
from cytotable.utils import _resolve_source_page_keys
from cytotable.warehouse.iceberg import (
    _apply_preset_defaults_to_convert_config,
    read_iceberg_table,
    write_iceberg_warehouse,
)
from cytotable.warehouse.images import add_object_id_to_profiles_frame


def test_join_page_key_is_null_free_across_files(tmp_path: pathlib.Path):
    """Reject a numeric object key with nulls in a later parquet file."""
    paths = []
    for number, object_number in ((1, 1), (2, None)):
        path = tmp_path / f"part{number}.parquet"
        parquet.write_table(
            pa.table(
                {
                    "ImageNumber": [number],
                    "ObjectNumber": pa.array([object_number], type=pa.int64()),
                }
            ),
            path,
        )
        paths.append(path)
    inferred = infer_join_sql(sources={"cells.parquet": [{"table": paths}]})
    assert inferred.page_key == "ImageNumber"


def test_join_page_key_requires_null_free_key(tmp_path: pathlib.Path):
    """Fail safely instead of choosing nullable image/object pagination keys."""
    path = tmp_path / "cells.parquet"
    parquet.write_table(
        pa.table(
            {
                "ImageNumber": pa.array([None], type=pa.int64()),
                "ObjectNumber": pa.array([None], type=pa.int64()),
            }
        ),
        path,
    )
    with pytest.raises(CytoTableException, match="null-free"):
        infer_join_sql(sources={"cells.parquet": [{"table": [path]}]})


def test_convert_preserves_explicit_join_page_key(
    load_parsl_default: None, tmp_path: pathlib.Path
):
    """An explicit exported key works even without usable automatic keys."""
    source = tmp_path / "Cells.csv"
    source.write_text(
        "ImageNumber,ObjectNumber,PageIndex\n,,10\n,,11\n", encoding="utf-8"
    )
    result = parquet.read_table(
        convert(
            source_path=str(source),
            dest_path=str(tmp_path / "joined.parquet"),
            dest_datatype="parquet",
            source_datatype="csv",
            preset=None,
            joins="infer",
            metadata=["image"],
            compartments=["cells"],
            page_keys={"cells": "PageIndex", "join": "Cells_PageIndex"},
            chunk_size=1,
            add_tablenumber=False,
        )
    )
    assert sorted(result["Cells_PageIndex"].to_pylist()) == [10, 11]
    assert result.num_rows == 2


def test_null_check_logs_query_failure(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
):
    """Unsafe candidates include the original query failure in the warning."""
    source = tmp_path / "Cells.csv"
    source.write_text("ObjectNumber\n1\n", encoding="utf-8")
    assert _column_has_nulls(sources=[{"source_path": source}], column="missing")
    warnings = [record for record in caplog.records if record.levelname == "WARNING"]
    assert any(
        "missing" in record.getMessage()
        and str(source) in record.getMessage()
        and record.exc_info is not None
        for record in warnings
    )


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize(
    "groups,keys,expected",
    [
        (
            ["Cells.csv", "Cells_summary.csv"],
            {"cells": "ObjectNumber", "cells_summary": "SummaryID"},
            {"Cells.csv": "ObjectNumber", "Cells_summary.csv": "SummaryID"},
        ),
        (
            ["Per_cells.sqlite", "Per_cytoplasm.sqlite"],
            {"cells": "CellID", "cytoplasm": "CytoplasmID"},
            {"Per_cells.sqlite": "CellID", "Per_cytoplasm.sqlite": "CytoplasmID"},
        ),
        (["test_welltbl.csv"], {"test": "OBJECTID"}, {"test_welltbl.csv": "OBJECTID"}),
        (["Per_cells.sqlite", "Cells_summary.csv"], {"cells": "ObjectNumber"}, {}),
        (["joined_profiles.csv"], {"join": "ObjectNumber"}, {}),
        (
            ["Per_cells.sqlite"],
            {"cells": "WrongID", "per_cells": "RightID"},
            {"Per_cells.sqlite": "RightID"},
        ),
    ],
)
def test_resolve_source_page_keys(
    groups: list, keys: dict, expected: dict, reverse: bool
):
    """Preserve preset shorthand without ambiguous or order-dependent matches."""
    if reverse:
        keys = dict(reversed(list(keys.items())))
    assert (
        _resolve_source_page_keys(source_group_names=groups, page_keys=keys) == expected
    )


@pytest.mark.parametrize("reverse", [False, True])
def test_resolve_source_page_keys_rejects_conflicting_aliases(reverse: bool):
    """Two shorthand keys with different columns cannot silently win by order."""
    keys = {"per": "FirstID", "cells": "SecondID"}
    if reverse:
        keys = dict(reversed(list(keys.items())))
    with pytest.raises(CytoTableException, match="Ambiguous pagination keys"):
        _resolve_source_page_keys(
            source_group_names=["Per_cells.sqlite"], page_keys=keys
        )


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_infer_page_keys_for_overlapping_groups(
    load_parsl_default: None, tmp_path: pathlib.Path, explicit: bool, reverse: bool
):
    """A cells key never masks inference for the separate cells_summary group."""
    (tmp_path / "Cells.csv").write_text("ObjectNumber\n1\n2\n", encoding="utf-8")
    (tmp_path / "Cells_summary.csv").write_text("SummaryID\n10\n11\n", encoding="utf-8")
    sources = _gather_sources(
        source_path=str(tmp_path),
        source_datatype="csv",
        targets=["cells", "cells_summary"],
    )
    if reverse:
        sources = dict(reversed(list(sources.items())))
    keys = _infer_source_page_keys(
        sources=sources,
        page_keys={"cells": "ObjectNumber"} if explicit else {},
        compartments=["cells", "cells_summary"],
    )
    assert keys == {"cells": "ObjectNumber", "cells_summary": "SummaryID"}


@pytest.mark.parametrize("infer", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_export_page_keys_for_overlapping_groups(
    load_parsl_default: None, tmp_path: pathlib.Path, infer: bool, reverse: bool
):
    """Export each overlapping group using its own column in either key order."""
    source = tmp_path / "input"
    source.mkdir()
    (source / "Cells.csv").write_text("ObjectNumber\n1\n2\n", encoding="utf-8")
    (source / "Cells_summary.csv").write_text("SummaryID\n10\n11\n", encoding="utf-8")
    entries = [("cells", "ObjectNumber")]
    if not infer:
        entries.append(("cells_summary", "SummaryID"))
    keys = dict(reversed(entries) if reverse else entries)
    result = convert(
        source_path=str(source),
        dest_path=str(tmp_path / "export"),
        dest_datatype="parquet",
        source_datatype="csv",
        preset=None,
        joins="infer" if infer else None,
        metadata=[],
        compartments=["cells", "cells_summary"],
        identifying_columns=[],
        concat=False,
        join=False,
        page_keys=keys,
        chunk_size=1,
        add_tablenumber=False,
    )
    assert isinstance(result, dict)
    for group, column in (
        ("Cells.csv", "ObjectNumber"),
        ("Cells_summary.csv", "SummaryID"),
    ):
        sources = result[group]
        assert all(entry["page_key"] == column for entry in sources)
        assert (
            sum(
                parquet.read_metadata(path).num_rows
                for entry in sources
                for path in entry["table"]
            )
            == 2
        )


@pytest.mark.parametrize("preset", [None, "cellprofiler_csv", "cellprofiler_sqlite"])
@pytest.mark.parametrize("explicit", [False, True])
def test_inferred_warehouse_configuration(preset: str, explicit: bool):
    """Apply inference defaults only when no explicit or preset value exists."""
    resolved = _apply_preset_defaults_to_convert_config(
        preset=preset,
        metadata=["image"],
        compartments=["cells", "nuclei", "cytoplasm"],
        identifying_columns=["CustomID"] if explicit else None,
        joins="infer",
        chunk_size=7 if explicit else None,
        page_keys=None,
    )
    expected_columns = (
        config[preset]["CONFIG_IDENTIFYING_COLUMNS"]
        if preset
        else _default_identifying_columns(compartments=["cells", "nuclei", "cytoplasm"])
    )
    expected_chunk_size = config[preset or "cellprofiler_csv"]["CONFIG_CHUNK_SIZE"]
    assert resolved["identifying_columns"] == (
        ("CustomID",) if explicit else expected_columns
    )
    assert resolved["chunk_size"] == (7 if explicit else expected_chunk_size)


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_inferred_warehouse_preserves_preset(
    load_parsl_default: None, tmp_path: pathlib.Path, data_dir_cellprofiler: str
):
    """Iceberg and Parquet preserve the same preset column names and values."""
    source = f"{data_dir_cellprofiler}/ExampleHuman"
    expected = parquet.read_table(
        convert(
            source_path=source,
            dest_path=str(tmp_path / "expected.parquet"),
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
            joins="infer",
        )
    ).to_pandas()
    # Iceberg intentionally adds object IDs and standardizes bbox metadata.
    expected = add_object_id_to_profiles_frame(expected)
    warehouse = write_iceberg_warehouse(
        source_path=source,
        source_datatype="csv",
        warehouse_path=str(tmp_path / "warehouse"),
        preset="cellprofiler_csv",
        joins="infer",
    )
    actual = read_iceberg_table(warehouse, "joined_profiles")
    assert set(actual.columns) == set(expected.columns)
    columns = list(expected.columns)
    pd.testing.assert_frame_equal(
        actual[columns].sort_values(columns).reset_index(drop=True),
        expected.sort_values(columns).reset_index(drop=True),
        check_dtype=False,
    )

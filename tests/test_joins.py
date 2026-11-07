"""
Tests for CytoTable.joins (join SQL inference) and related.

Note: these use the ThreadPoolExecutor-based parsl configuration
through the load_parsl_default fixture.
"""

# pylint: disable=too-many-lines,unused-argument

import pathlib
from importlib.util import find_spec

import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import parquet

from cytotable.convert import convert
from cytotable.exceptions import CytoTableException
from cytotable.joins import (
    INFER_JOINS_SENTINEL,
    _collect_tables,
    _column_has_nulls,
    _default_identifying_columns,
    _find_reference_edges,
    _infer_source_page_keys,
    _validate_edges,
    infer_join_sql,
    is_infer_joins_sentinel,
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


def test_is_infer_joins_sentinel():
    """
    Tests join SQL inference sentinel detection.
    """

    assert is_infer_joins_sentinel(INFER_JOINS_SENTINEL)
    assert is_infer_joins_sentinel("Infer")
    assert is_infer_joins_sentinel(" infer ")
    assert not is_infer_joins_sentinel(None)
    assert not is_infer_joins_sentinel("SELECT * FROM table")


def test_infer_source_page_keys(
    load_parsl_default: None,
    data_dir_cellprofiler: str,
):
    """
    Tests source pagination key inference with cellprofiler csv sources.
    """

    sources = _gather_sources(
        source_path=f"{data_dir_cellprofiler}/ExampleHuman",
        source_datatype="csv",
        targets=["cells", "nuclei", "cytoplasm", "image"],
    )

    page_keys = _infer_source_page_keys(
        sources=sources,
        page_keys={},
        compartments=["cells", "nuclei", "cytoplasm"],
    )

    assert page_keys == {
        "image": "ImageNumber",
        "cells": "ObjectNumber",
        "nuclei": "ObjectNumber",
        "cytoplasm": "ObjectNumber",
    }


def test_infer_source_page_keys_preserves_existing(
    load_parsl_default: None,
):
    """
    Tests that source pagination key inference preserves
    user-provided pagination keys.
    """

    sources = _gather_sources(
        source_path="tests/data/cellprofiler/ExampleHuman",
        source_datatype="csv",
        targets=["cells", "nuclei", "cytoplasm", "image"],
    )

    page_keys = _infer_source_page_keys(
        sources=sources,
        page_keys={"cells": "ImageNumber"},
        compartments=["cells", "nuclei", "cytoplasm"],
    )

    # user-provided key is preserved untouched
    assert page_keys["cells"] == "ImageNumber"
    # other groups are inferred
    assert page_keys["image"] == "ImageNumber"
    assert page_keys["nuclei"] == "ObjectNumber"
    assert page_keys["cytoplasm"] == "ObjectNumber"


def test_default_identifying_columns():
    """
    Tests default identifying columns for join SQL inference.
    """

    assert _default_identifying_columns() == (
        "ImageNumber",
        "ObjectNumber",
        "TableNumber",
    )
    assert _default_identifying_columns(compartments=["cells", "nuclei"]) == (
        "ImageNumber",
        "ObjectNumber",
        "TableNumber",
        "Parent_Cells",
        "Parent_Nuclei",
    )


def test_infer_join_sql_no_tables():
    """
    Tests that join SQL inference raises without source tables.
    """

    with pytest.raises(CytoTableException, match="No source tables"):
        infer_join_sql(sources={})


@pytest.mark.parametrize(
    "child_rows,matched_rows", [(10, 4), (10, 10), (10, 0), (0, 0)]
)
def test_validate_edges_logs_match_rates(
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
    child_rows: int,
    matched_rows: int,
):
    """Preserve edge decisions and log measured rates without copying edges."""
    caplog.set_level("INFO", logger="cytotable.joins")
    parquet.write_table(
        pa.table(
            {
                "ImageNumber": [1] * child_rows,
                "ObjectNumber": list(range(1, child_rows + 1)),
                "Parent_Cells": [1] * matched_rows + [99] * (child_rows - matched_rows),
            }
        ),
        tmp_path / "cytoplasm.parquet",
    )
    parquet.write_table(
        pa.table({"ImageNumber": [1], "ObjectNumber": [1]}),
        tmp_path / "cells.parquet",
    )
    tables = _collect_tables(
        sources={
            f"{name}.parquet": [{"table": [tmp_path / f"{name}.parquet"]}]
            for name in ("cytoplasm", "cells")
        },
        metadata=[],
        compartments=["cytoplasm", "cells"],
    )
    edges = _find_reference_edges(tables=tables)
    validated = _validate_edges(
        edges=edges, tables={table.alias: table for table in tables}
    )
    if child_rows and not matched_rows:
        assert not validated
        assert "no values match" in caplog.text
    else:
        assert len(validated) == 1 and validated[0] is edges[0]
        match_rate = matched_rows / child_rows if child_rows else 1.0
        assert f"{matched_rows} of {child_rows} child rows match" in caplog.text
        assert f"rate={match_rate:.3f}" in caplog.text
    warnings = [record for record in caplog.records if record.levelname == "WARNING"]
    assert bool(warnings) == (0 < matched_rows < child_rows / 2)


def test_convert_join_sql_inference_matches_preset_cellprofiler_csv(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cellprofiler: str,
):
    """
    Tests that convert with inferred join SQL matches the
    cellprofiler_csv preset output for shared columns.
    """

    preset_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{fx_tempdir}/preset.parquet",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
        )
    )

    infer_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{fx_tempdir}/infer.parquet",
            dest_datatype="parquet",
            source_datatype="csv",
            preset=None,
            joins=INFER_JOINS_SENTINEL,
            metadata=["image"],
            compartments=["cells", "nuclei", "cytoplasm"],
        )
    )

    # inference should not lose rows relative to the preset join
    assert infer_result.num_rows == preset_result.num_rows
    # every preset column is present in the inferred result
    assert set(preset_result.column_names).issubset(infer_result.column_names)
    # and every shared column carries identical values
    shared_columns = [
        column
        for column in preset_result.column_names
        if column in infer_result.column_names
    ]
    assert shared_columns == preset_result.column_names
    preset_sorted = preset_result.sort_by(
        [(column, "ascending") for column in shared_columns]
    )
    infer_sorted = infer_result.select(shared_columns).sort_by(
        [(column, "ascending") for column in shared_columns]
    )
    assert preset_sorted.equals(infer_sorted)


def test_convert_join_sql_inference_matches_preset_cellprofiler_sqlite(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cellprofiler_sqlite_nf1: str,
):
    """
    Tests that convert with inferred join SQL matches the
    cellprofiler_sqlite preset output for shared columns.
    """

    preset_result = parquet.read_table(
        convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=f"{fx_tempdir}/preset.parquet",
            dest_datatype="parquet",
            source_datatype="sqlite",
            preset="cellprofiler_sqlite",
        )
    )

    infer_result = parquet.read_table(
        convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=f"{fx_tempdir}/infer.parquet",
            dest_datatype="parquet",
            source_datatype="sqlite",
            preset=None,
            joins=INFER_JOINS_SENTINEL,
            metadata=["image"],
            compartments=["cells", "nuclei", "cytoplasm"],
        )
    )

    assert infer_result.num_rows == preset_result.num_rows
    assert set(preset_result.column_names).issubset(infer_result.column_names)
    shared_columns = [
        column
        for column in preset_result.column_names
        if column in infer_result.column_names
    ]
    assert shared_columns == preset_result.column_names
    preset_sorted = preset_result.sort_by(
        [(column, "ascending") for column in shared_columns]
    )
    infer_sorted = infer_result.select(shared_columns).sort_by(
        [(column, "ascending") for column in shared_columns]
    )
    assert preset_sorted.equals(infer_sorted)


def test_convert_join_sql_inference_cytominerdatabase_csv(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cytominerdatabase: str,
):
    """
    Tests convert with inferred join SQL against cytominer-database
    csv data which includes multiple plates (compound keys).
    """

    infer_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cytominerdatabase}/data_a",
            dest_path=f"{fx_tempdir}/infer.parquet",
            dest_datatype="parquet",
            source_datatype="csv",
            preset=None,
            joins=INFER_JOINS_SENTINEL,
            metadata=["image"],
            compartments=["cells", "nuclei", "cytoplasm"],
        )
    )

    # data_a includes two plates with 20 cytoplasm rows each;
    # the inferred join should retain every cytoplasm row
    # (no row multiplication from the joins)
    assert infer_result.num_rows == 40
    # compartment data columns are joined (non-null where matches exist)
    cells_area = infer_result.column("Cells_AreaShape_Area")
    assert cells_area.null_count == 0


def test_convert_join_sql_inference_no_fanout(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cytominerdatabase: str,
):
    """
    Tests that inferred joins do not multiply rows for data
    where image numbers repeat across plates (compound keys).
    """

    infer_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cytominerdatabase}/data_a",
            dest_path=f"{fx_tempdir}/infer.parquet",
            dest_datatype="parquet",
            source_datatype="csv",
            preset=None,
            joins=INFER_JOINS_SENTINEL,
            metadata=["image"],
            compartments=["cells", "nuclei", "cytoplasm"],
            # disable tablenumber to retain all plate sources, as
            # data_a's two plates reuse overlapping image numbers
            add_tablenumber=False,
        )
    )

    # every cytoplasm row across plates is retained exactly once
    assert infer_result.num_rows == 40

    # verify actual joined values, not just row count, for one row
    # from each plate (plate 1 uses ImageNumbers 1-4, plate 2 uses
    # 19-22; checking real values catches a join that silently
    # matched the wrong row)
    image_number = infer_result.column("Metadata_ImageNumber")
    object_number = infer_result.column("Metadata_ObjectNumber")
    cells_area = infer_result.column("Cells_AreaShape_Area")
    for expected_image, expected_object, expected_area in (
        (1, 1, 739),
        (19, 200, 379),
    ):
        matches = [
            index
            for index in range(infer_result.num_rows)
            if image_number[index].as_py() == expected_image
            and object_number[index].as_py() == expected_object
        ]
        assert len(matches) == 1
        assert cells_area[matches[0]].as_py() == expected_area


def test_infer_join_sql_unjoinable_table(
    load_parsl_default: None,
    fx_tempdir: str,
):
    """
    Tests that join SQL inference raises a helpful exception
    when a table cannot be joined to the others.
    """

    # build a table group with no shared keys or parent references
    dest_path = pathlib.Path(fx_tempdir) / "unjoinable"
    dest_path.mkdir()
    parquet.write_table(
        pa.table({"unrelated_column": [1, 2, 3]}),
        str(dest_path / "unrelated.parquet"),
    )
    parquet.write_table(
        pa.table({"other_column": ["a", "b", "c"]}),
        str(dest_path / "other.parquet"),
    )

    sources = {
        "unrelated.parquet": [{"table": [str(dest_path / "unrelated.parquet")]}],
        "other.parquet": [{"table": [str(dest_path / "other.parquet")]}],
    }

    with pytest.raises(CytoTableException, match="Unable to infer a join"):
        infer_join_sql(sources=sources)


def test_infer_join_sql_rejects_ambiguous_parent_match(
    load_parsl_default: None,
    fx_tempdir: str,
):
    """
    Tests that join SQL inference raises when a parent reference
    would match more than one parent row, which would multiply or
    ambiguously associate child rows rather than join them cleanly.
    """

    dest_path = pathlib.Path(fx_tempdir) / "ambiguous"
    dest_path.mkdir()
    # two "cells" rows share the same (image, object) key, so any
    # cytoplasm row referencing object 1 of image 1 matches both
    parquet.write_table(
        pa.table(
            {
                "Metadata_ImageNumber": [1, 1],
                "Metadata_ObjectNumber": [1, 1],
            }
        ),
        str(dest_path / "cells.parquet"),
    )
    parquet.write_table(
        pa.table(
            {
                "Metadata_ImageNumber": [1],
                "Metadata_ObjectNumber": [10],
                "Parent_Cells": [1],
            }
        ),
        str(dest_path / "cytoplasm.parquet"),
    )

    sources = {
        "cells.parquet": [{"table": [str(dest_path / "cells.parquet")]}],
        "cytoplasm.parquet": [{"table": [str(dest_path / "cytoplasm.parquet")]}],
    }

    with pytest.raises(CytoTableException, match="matches more than one"):
        infer_join_sql(sources=sources, compartments=["cells", "cytoplasm"])


def test_infer_join_sql_rejects_unsupported_compartment_join(
    load_parsl_default: None,
    fx_tempdir: str,
):
    """
    Tests that join SQL inference raises for two compartment tables
    which share only an image-level key and have no validated
    object-level parent reference between them, rather than silently
    joining on the shared image key (which would pair every object in
    one compartment with every object in the other for a given image).
    """

    dest_path = pathlib.Path(fx_tempdir) / "unsupported"
    dest_path.mkdir()
    parquet.write_table(
        pa.table(
            {
                "Metadata_ImageNumber": [1, 1],
                "Metadata_ObjectNumber": [1, 2],
            }
        ),
        str(dest_path / "cells.parquet"),
    )
    parquet.write_table(
        pa.table(
            {
                "Metadata_ImageNumber": [1, 1],
                "Metadata_ObjectNumber": [1, 2],
            }
        ),
        str(dest_path / "nuclei.parquet"),
    )

    sources = {
        "cells.parquet": [{"table": [str(dest_path / "cells.parquet")]}],
        "nuclei.parquet": [{"table": [str(dest_path / "nuclei.parquet")]}],
    }

    with pytest.raises(CytoTableException, match="Unable to infer a join"):
        infer_join_sql(sources=sources, compartments=["cells", "nuclei"])


def test_convert_join_sql_inference_raises_with_manual_override(
    load_parsl_default: None,
    fx_tempdir: str,
    data_dir_cellprofiler: str,
):
    """
    Tests that manually provided join SQL takes precedence over
    join SQL inference.
    """

    manual_sql = """
        SELECT
            image.Metadata_ImageNumber,
            cytoplasm.* EXCLUDE (Metadata_ImageNumber)
        FROM
            read_parquet('cytoplasm.parquet') AS cytoplasm
            LEFT JOIN read_parquet('image.parquet') AS image USING (Metadata_ImageNumber)
    """

    manual_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{fx_tempdir}/manual.parquet",
            dest_datatype="parquet",
            source_datatype="csv",
            preset=None,
            joins=manual_sql,
            metadata=["image"],
            compartments=["cells", "nuclei", "cytoplasm"],
            identifying_columns=[
                "ImageNumber",
                "ObjectNumber",
                "Parent_Cells",
                "Parent_Nuclei",
            ],
            chunk_size=1000,
            page_keys={
                "image": "ImageNumber",
                "cells": "ObjectNumber",
                "nuclei": "ObjectNumber",
                "cytoplasm": "ObjectNumber",
                "join": "Metadata_ObjectNumber",
            },
        )
    )

    # manual SQL limits the output to the cytoplasm + image columns
    # (cells and nuclei are deliberately not selected)
    assert manual_result.num_rows == 289
    assert "Cells_AreaShape_Area" not in manual_result.column_names


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
            metadata=[],
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

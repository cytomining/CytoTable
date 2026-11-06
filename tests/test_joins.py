"""
Tests for CytoTable.joins (join SQL inference) and related.

Note: these use the ThreadPoolExecutor-based parsl configuration
through the load_parsl_default fixture.
"""

# pylint: disable=too-many-lines,unused-argument

import pathlib

import pyarrow as pa
import pytest
from pyarrow import parquet

from cytotable.convert import convert
from cytotable.exceptions import CytoTableException
from cytotable.joins import (
    INFER_JOINS_SENTINEL,
    _collect_tables,
    _default_identifying_columns,
    _find_reference_edges,
    _infer_source_page_keys,
    _validate_edges,
    infer_join_sql,
    is_infer_joins_sentinel,
)
from cytotable.sources import _gather_sources


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

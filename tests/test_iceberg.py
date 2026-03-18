"""
Tests for CytoTable Iceberg helpers.
"""

import re
from importlib.util import find_spec
from json import dumps
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from duckdb import IOException as DuckDBIOException
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from pyarrow import parquet

from cytotable.exceptions import CytoTableException
from cytotable.iceberg import (
    _rewrite_join_sql_for_warehouse,
    _validate_image_export_prerequisites,
    list_iceberg_tables,
    read_iceberg_table,
    write_iceberg_warehouse,
)
from cytotable.images import (
    IMAGE_TABLE_NAME,
    _build_file_index,
    _find_matching_segmentation_path,
    image_crop_table_from_joined_chunk,
    object_id,
    profile_with_images_frame,
    resolve_bbox_columns,
)
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

    normalized = re.sub(r"\s+", " ", rewritten).strip()

    assert "read_parquet('cytoplasm.parquet')" not in rewritten
    assert "read_parquet('cells.parquet')" not in rewritten
    assert "FROM cytoplasm AS cytoplasm" in normalized
    assert "LEFT JOIN image AS image" in normalized


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


def test_validate_image_export_prerequisites_requires_image_dir():
    """
    Tests that ancillary image export options require an image directory.
    """

    with pytest.raises(CytoTableException, match="image_dir"):
        _validate_image_export_prerequisites(
            image_dir=None,
            mask_dir="masks",
            outline_dir=None,
            bbox_column_map=None,
            segmentation_file_regex=None,
            joins="SELECT 1",
            page_keys={"join": "Metadata_ImageNumber"},
        )


def test_validate_image_export_prerequisites_requires_join_page_key():
    """
    Tests that image export requires page_keys['join'].
    """

    with pytest.raises(CytoTableException, match=r"page_keys.*join"):
        _validate_image_export_prerequisites(
            image_dir="images",
            mask_dir=None,
            outline_dir=None,
            bbox_column_map=None,
            segmentation_file_regex=None,
            joins="SELECT 1",
            page_keys={},
        )


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_fails_fast_for_image_export_without_joins(
    fx_tempdir: str,
):
    """
    Tests that image export fails before staging when joins are missing.
    """

    with pytest.raises(CytoTableException, match="requires join SQL"):
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=f"{fx_tempdir}/example_warehouse",
            preset=None,
            image_dir=f"{fx_tempdir}/images",
            page_keys={"join": "Metadata_ImageNumber"},
        )


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_cleans_up_failed_build_root(fx_tempdir: str):
    """
    Tests that a failed warehouse write does not leave partial output behind.
    """

    warehouse_path = Path(fx_tempdir) / "failed_warehouse"

    with (
        patch("cytotable.iceberg.parsl.load"),
        patch("cytotable.iceberg.parsl.dfk") as dfk,
        patch("cytotable.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.iceberg._run_export_workflow",
            side_effect=RuntimeError("boom"),
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
        )

    assert not warehouse_path.exists()
    assert not list(warehouse_path.parent.glob(f"{warehouse_path.name}.tmp-*"))
    dfk.return_value.cleanup.assert_called_once()


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_cleans_up_failed_parsl_load(fx_tempdir: str):
    """
    Tests that a failing Parsl load does not leak the temporary build root.
    """

    warehouse_path = Path(fx_tempdir) / "failed_warehouse"

    with (
        patch(
            "cytotable.iceberg.parsl.load",
            side_effect=RuntimeError("parsl load failed"),
        ),
        patch("cytotable.iceberg._parsl_loaded", return_value=False),
        pytest.raises(RuntimeError, match="parsl load failed"),
    ):
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
        )

    assert not warehouse_path.exists()
    assert not list(warehouse_path.parent.glob(f"{warehouse_path.name}.tmp-*"))


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_does_not_cleanup_existing_parsl(fx_tempdir: str):
    """
    Tests that caller-owned Parsl state is not cleaned up by warehouse writes.
    """

    warehouse_path = Path(fx_tempdir) / "failed_warehouse"
    cleanup = MagicMock()

    with (
        patch("cytotable.iceberg.parsl.load"),
        patch("cytotable.iceberg.parsl.dfk") as dfk,
        patch("cytotable.iceberg._parsl_loaded", return_value=True),
        patch(
            "cytotable.iceberg._run_export_workflow",
            side_effect=RuntimeError("boom"),
        ),
        pytest.raises(RuntimeError, match="boom"),
    ):
        dfk.return_value.cleanup = cleanup
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
        )

    dfk.return_value.cleanup.assert_not_called()


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_skips_joined_view_without_source_tables(
    fx_tempdir: str,
):
    """
    Tests that no joined view is registered when export produces no source tables.
    """

    warehouse_path = Path(fx_tempdir) / "empty_warehouse"

    with (
        patch("cytotable.iceberg.parsl.load"),
        patch("cytotable.iceberg.parsl.dfk") as dfk,
        patch("cytotable.iceberg._parsl_loaded", return_value=False),
        patch("cytotable.iceberg._run_export_workflow", return_value={}),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
        )

    assert list_iceberg_tables(warehouse_path) == []


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_skips_profile_view_without_image_table(
    fx_tempdir: str,
):
    """
    Tests that no profile/image view is registered when no image crops are written.
    """

    warehouse_path = Path(fx_tempdir) / "no_image_view_warehouse"
    stage_parquet = Path(fx_tempdir) / "cells.parquet"
    joined_chunk = Path(fx_tempdir) / "joined_chunk.parquet"
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), stage_parquet)
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), joined_chunk)

    with (
        patch("cytotable.iceberg.parsl.load"),
        patch("cytotable.iceberg.parsl.dfk") as dfk,
        patch("cytotable.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.iceberg._run_export_workflow",
            side_effect=[
                {"cells.parquet": [{"table": [str(stage_parquet)]}]},
                [str(joined_chunk)],
            ],
        ),
        patch(
            "cytotable.iceberg.image_crop_table_from_joined_chunk",
            return_value=pa.table({"object_id": pa.array([], type=pa.string())}),
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
            image_dir=f"{fx_tempdir}/images",
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    assert "analytics.cells" in list_iceberg_tables(warehouse_path)
    assert "analytics.cytotable_joined" in list_iceberg_tables(warehouse_path)
    assert "analytics.profile_with_images" not in list_iceberg_tables(warehouse_path)
    assert "analytics.image_crops" not in list_iceberg_tables(warehouse_path)


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_skips_joined_view_with_unresolved_sql(
    fx_tempdir: str,
):
    """
    Tests that unresolved parquet references do not get registered as views.
    """

    warehouse_path = Path(fx_tempdir) / "unresolved_join_warehouse"
    stage_parquet = Path(fx_tempdir) / "cells.parquet"
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), stage_parquet)

    with (
        patch("cytotable.iceberg.parsl.load"),
        patch("cytotable.iceberg.parsl.dfk") as dfk,
        patch("cytotable.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.iceberg._run_export_workflow",
            return_value={"cells.parquet": [{"table": [str(stage_parquet)]}]},
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset=None,
            joins=(
                "SELECT * FROM read_parquet('cells.parquet') AS cells "
                "LEFT JOIN read_parquet('image.parquet') AS image "
                "ON cells.Metadata_ImageNumber = image.Metadata_ImageNumber"
            ),
            page_keys={"join": "Metadata_ImageNumber"},
        )

    assert "analytics.cells" in list_iceberg_tables(warehouse_path)
    assert "analytics.cytotable_joined" not in list_iceberg_tables(warehouse_path)


def test_resolve_bbox_columns_prefers_cellprofiler_names():
    """
    Tests bbox resolution for CellProfiler-style names.
    """

    bbox = resolve_bbox_columns(
        [
            "Cytoplasm_AreaShape_BoundingBoxMinimum_X",
            "Cytoplasm_AreaShape_BoundingBoxMaximum_X",
            "Cytoplasm_AreaShape_BoundingBoxMinimum_Y",
            "Cytoplasm_AreaShape_BoundingBoxMaximum_Y",
        ]
    )

    assert bbox is not None
    assert bbox.x_min == "Cytoplasm_AreaShape_BoundingBoxMinimum_X"
    assert bbox.y_max == "Cytoplasm_AreaShape_BoundingBoxMaximum_Y"


def test_build_file_index_includes_zarr_directories(fx_tempdir: str):
    """
    Tests that directory-backed zarr image stores are indexed.
    """

    image_root = Path(fx_tempdir) / "images"
    zarr_store = image_root / "example.ome.zarr"
    zarr_store.mkdir(parents=True)

    file_index = _build_file_index(str(image_root))

    assert file_index["example.ome.zarr"] == zarr_store


def test_object_id_is_stable():
    """
    Tests deterministic object id generation.
    """

    first = object_id("example-object")
    second = object_id("example-object")
    third = object_id("different-object")

    assert first == second
    assert first != third
    assert first.startswith("obj-")


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_table_from_joined_chunk(fx_tempdir: str):
    """
    Tests OME-Arrow crop export from a joined parquet chunk.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    outline_dir = Path(fx_tempdir) / "outlines"
    image_dir.mkdir()
    outline_dir.mkdir()

    image = np.arange(100, dtype=np.uint16).reshape(10, 10)
    outline = np.zeros((10, 10), dtype=np.uint16)
    outline[2:8, 3:7] = 1
    tifffile.imwrite(image_dir / "cell.tiff", image)
    tifffile.imwrite(outline_dir / "cell.tiff", outline)

    joined_chunk = pa.table(
        {
            "Metadata_ImageNumber": [1],
            "Image_FileName_DNA": ["cell.tiff"],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_X": [3],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_X": [7],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_Y": [2],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_Y": [8],
        }
    )
    chunk_path = Path(fx_tempdir) / "joined.parquet"
    parquet.write_table(joined_chunk, chunk_path)

    crop_table = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path),
        image_dir=str(image_dir),
        outline_dir=str(outline_dir),
    )

    assert crop_table.num_rows == 1
    assert IMAGE_TABLE_NAME == "image_crops"
    assert "object_id" in crop_table.column_names
    assert "ome_image" in crop_table.column_names
    assert "ome_label" in crop_table.column_names


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_table_skips_invalid_bbox_rows(fx_tempdir: str):
    """
    Tests that malformed or inverted bounding boxes are skipped.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()

    image = np.arange(100, dtype=np.uint16).reshape(10, 10)
    tifffile.imwrite(image_dir / "cell.tiff", image)

    joined_chunk = pa.Table.from_pandas(
        pd.DataFrame(
            {
                "Metadata_ImageNumber": [1, 2, 3],
                "Image_FileName_DNA": ["cell.tiff", "cell.tiff", "cell.tiff"],
                "Cytoplasm_AreaShape_BoundingBoxMinimum_X": ["3", "bad", "7"],
                "Cytoplasm_AreaShape_BoundingBoxMaximum_X": ["7", "8", "3"],
                "Cytoplasm_AreaShape_BoundingBoxMinimum_Y": ["2", "2", "8"],
                "Cytoplasm_AreaShape_BoundingBoxMaximum_Y": ["8", "8", "2"],
            }
        )
    )
    chunk_path = Path(fx_tempdir) / "joined_invalid_bbox.parquet"
    parquet.write_table(joined_chunk, chunk_path)

    crop_table = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path),
        image_dir=str(image_dir),
    )

    assert crop_table.num_rows == 1
    assert crop_table["bbox_x_min"].to_pylist() == [3]
    assert crop_table["bbox_x_max"].to_pylist() == [7]
    assert crop_table["bbox_y_min"].to_pylist() == [2]
    assert crop_table["bbox_y_max"].to_pylist() == [8]


def test_find_matching_segmentation_path_uses_regex_mapping(fx_tempdir: str):
    """
    Tests regex-based segmentation file resolution.
    """

    root = Path(fx_tempdir) / "outlines"
    root.mkdir()
    segmentation = root / "plateA_well_B03_site_1_outline.tiff"
    segmentation.touch()
    candidate = Path(fx_tempdir) / "images" / "plateA_well_B03_site_1.tiff"
    candidate.parent.mkdir()
    candidate.touch()

    result = _find_matching_segmentation_path(
        data_value="plateA_well_B03_site_1.tiff",
        pattern_map={
            r".*_outline\.tiff$": r"(plateA_well_B03_site_1)\.tiff$",
        },
        file_dir=str(root),
        candidate_path=candidate,
    )

    assert result == segmentation


def test_find_matching_segmentation_path_handles_dotted_identifiers(
    fx_tempdir: str,
):
    """
    Tests regex-based segmentation resolution with dotted image identifiers.
    """

    root = Path(fx_tempdir) / "outlines"
    root.mkdir()
    segmentation = root / "foo.ome_outline.tiff"
    segmentation.touch()
    candidate = Path(fx_tempdir) / "images" / "foo.ome.tiff"
    candidate.parent.mkdir()
    candidate.touch()

    result = _find_matching_segmentation_path(
        data_value="foo.ome.tiff",
        pattern_map={
            r".*_outline\.tiff$": r"(foo\.ome)\.tiff$",
        },
        file_dir=str(root),
        candidate_path=candidate,
    )

    assert result == segmentation


def test_find_matching_segmentation_path_uses_index_and_cache(fx_tempdir: str):
    """
    Tests segmentation resolution without repeated filesystem rescans.
    """

    root = Path(fx_tempdir) / "outlines"
    root.mkdir()
    segmentation = root / "cell_outline.tiff"
    segmentation.touch()
    candidate = Path(fx_tempdir) / "images" / "cell.tiff"
    candidate.parent.mkdir()
    candidate.touch()
    file_index = _build_file_index(str(root))
    lookup_cache: dict[str, Optional[Path]] = {}

    with patch("pathlib.Path.rglob", side_effect=AssertionError("unexpected rglob")):
        first = _find_matching_segmentation_path(
            data_value="cell.tiff",
            pattern_map={
                r".*_outline\.tiff$": r"(cell)\.tiff$",
            },
            file_dir=str(root),
            candidate_path=candidate,
            file_index=file_index,
            lookup_cache=lookup_cache,
        )
        second = _find_matching_segmentation_path(
            data_value="cell.tiff",
            pattern_map={
                r".*_outline\.tiff$": r"(cell)\.tiff$",
            },
            file_dir=str(root),
            candidate_path=candidate,
            file_index=file_index,
            lookup_cache=lookup_cache,
        )

    assert first == segmentation
    assert second == segmentation
    assert len(lookup_cache) == 1


def test_profile_with_images_frame_merges_by_object_id():
    """
    Tests manifest view expansion from joined rows to image crop rows.
    """

    joined = pa.table(
        {
            "Metadata_ImageNumber": [1],
            "Image_FileName_DNA": ["cell.tiff"],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_X": [3],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_X": [7],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_Y": [2],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_Y": [8],
        }
    ).to_pandas()

    image_rows = pd.DataFrame(
        {
            "object_id": [
                object_id(
                    dumps(
                        {
                            "bbox": {
                                "x_min": 3,
                                "x_max": 7,
                                "y_min": 2,
                                "y_max": 8,
                            },
                            "keys": {"Metadata_ImageNumber": 1},
                            "source_image_column": "Image_FileName_DNA",
                            "source_image_file": "cell.tiff",
                        },
                        sort_keys=True,
                    )
                )
            ],
            "source_image_column": ["Image_FileName_DNA"],
            "source_image_file": ["cell.tiff"],
            "label_source_kind": ["outline"],
        }
    )

    manifested = profile_with_images_frame(joined_frame=joined, image_frame=image_rows)

    assert len(manifested) == 1
    assert manifested.loc[0, "object_id"].startswith("obj-")
    assert manifested.loc[0, "label_source_kind"] == "outline"


@pytest.mark.skipif(
    find_spec("pyiceberg") is None or find_spec("ome_arrow") is None,
    reason="pyiceberg and ome-arrow are required",
)
def test_examplehuman_iceberg_warehouse_with_images(
    fx_tempdir: str,
    data_dir_cellprofiler: str,
):
    """
    Tests end-to-end Iceberg warehouse export with ExampleHuman images.
    """

    try:
        warehouse_path = write_iceberg_warehouse(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            warehouse_path=f"{fx_tempdir}/examplehuman_warehouse",
            source_datatype="csv",
            preset="cellprofiler_csv",
            image_dir=f"{data_dir_cellprofiler}/ExampleHuman",
            parsl_config=Config(
                executors=[ThreadPoolExecutor(label="tpe_for_iceberg_image_test")]
            ),
        )
    except DuckDBIOException as exc:
        if "extension" in str(exc).lower() or "http" in str(exc).lower():
            pytest.skip(
                f"DuckDB extension bootstrap unavailable in this environment: {exc}"
            )
        raise

    tables = list_iceberg_tables(warehouse_path)

    assert "analytics.cells" in tables
    assert "analytics.cytoplasm" in tables
    assert "analytics.image" in tables
    assert "analytics.nuclei" in tables
    assert "analytics.image_crops" in tables
    assert "analytics.cytotable_joined" in tables
    assert "analytics.profile_with_images" in tables

    image_crops = read_iceberg_table(warehouse_path, "image_crops")
    profile_with_images = read_iceberg_table(warehouse_path, "profile_with_images")

    assert len(image_crops) > 0
    assert len(profile_with_images) > 0
    assert "object_id" in image_crops.columns
    assert "ome_image" in image_crops.columns
    assert image_crops["object_id"].notna().all()
    assert profile_with_images["object_id"].notna().all()
    assert profile_with_images["ome_image"].notna().any()

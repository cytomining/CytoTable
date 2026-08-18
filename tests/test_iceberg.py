"""
Tests for CytoTable Iceberg helpers.
"""

# pylint: disable=too-many-lines

import pathlib
import re
from importlib.util import find_spec
from json import dumps, loads
from pathlib import Path
from typing import Any, Optional, cast
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from duckdb import IOException as DuckDBIOException
from parsl.config import Config
from parsl.executors import HighThroughputExecutor, ThreadPoolExecutor
from pyarrow import parquet

from cytotable.exceptions import CytoTableException
from cytotable.presets import config
from cytotable.utils import (
    CYTOTABLE_THREAD_EXECUTOR_LABEL,
    _default_parsl_config,
    _ensure_thread_executor,
)
from cytotable.warehouse import images as images_module
from cytotable.warehouse.iceberg import (
    _rewrite_join_sql_for_warehouse,
    _validate_iceberg_join_prerequisites,
    _validate_image_export_prerequisites,
    catalog,
    describe_iceberg_warehouse,
    list_iceberg_tables,
    read_iceberg_table,
    write_iceberg_warehouse,
)
from cytotable.warehouse.images import (
    IMAGE_TABLE_NAME,
    SOURCE_IMAGE_TABLE_NAME,
    FileIndex,
    _build_file_index,
    _crop_ome_arrow,
    _dedup_table_by_first_occurrence,
    _extract_crop_key_field_names,
    _extract_image_key_field_names,
    _find_matching_segmentation_path,
    _require_ome_arrow,
    _strip_null_fields_from_type,
    add_object_id_to_profiles_frame,
    image_crop_table_from_joined_chunk,
    object_id,
    profile_with_images_frame,
    resolve_bbox_columns,
    source_image_table_from_joined_chunk,
)


def test_ensure_thread_executor_adds_when_absent():
    """
    _ensure_thread_executor adds cytotable_threads when the config lacks it.
    """
    cfg = Config(executors=[])
    result = _ensure_thread_executor(cfg)
    labels = {e.label for e in result.executors}
    assert CYTOTABLE_THREAD_EXECUTOR_LABEL in labels


def test_ensure_thread_executor_does_not_duplicate():
    """
    _ensure_thread_executor leaves the config unchanged when the executor already exists.
    """
    cfg = Config(executors=[ThreadPoolExecutor(label=CYTOTABLE_THREAD_EXECUTOR_LABEL)])
    result = _ensure_thread_executor(cfg)
    matching = [
        e for e in result.executors if e.label == CYTOTABLE_THREAD_EXECUTOR_LABEL
    ]
    assert len(matching) == 1


def test_default_parsl_config_has_single_executor():
    """
    _default_parsl_config only includes a single HighThroughputExecutor by
    default; the cytotable thread executor is added on-demand via
    _ensure_thread_executor for workflows that need it (e.g. Iceberg
    image-crop export).
    """
    cfg = _default_parsl_config()
    assert len(cfg.executors) == 1
    default_executor = cfg.executors[0]
    assert isinstance(default_executor, HighThroughputExecutor)
    assert default_executor.label == "htex_default_for_cytotable"

    result = _ensure_thread_executor(cfg)
    assert len(result.executors) == 2
    assert result.executors[0] is default_executor
    assert result.executors[1].label == CYTOTABLE_THREAD_EXECUTOR_LABEL


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


def test_validate_image_export_prerequisites_requires_join_page_key(fx_tempdir: str):
    """
    Tests that image export requires page_keys['join'].
    """

    images_dir = Path(fx_tempdir) / "images"
    images_dir.mkdir()

    with pytest.raises(CytoTableException, match=r"page_keys.*join"):
        _validate_image_export_prerequisites(
            image_dir=str(images_dir),
            mask_dir=None,
            outline_dir=None,
            bbox_column_map=None,
            segmentation_file_regex=None,
            joins="SELECT 1",
            page_keys={},
        )


def test_validate_image_export_prerequisites_requires_existing_directory(
    fx_tempdir: str,
):
    """
    Tests that image export directory arguments must exist on disk.
    """

    missing_dir = str(Path(fx_tempdir) / "missing-images")

    with pytest.raises(CytoTableException, match="existing directory"):
        _validate_image_export_prerequisites(
            image_dir=missing_dir,
            mask_dir=None,
            outline_dir=None,
            bbox_column_map=None,
            segmentation_file_regex=None,
            joins="SELECT 1",
            page_keys={"join": "Metadata_ImageNumber"},
        )


def test_validate_iceberg_join_prerequisites_requires_join_page_key():
    """
    Tests that Iceberg export always requires page_keys['join'].
    """

    with pytest.raises(ValueError, match=r"page_keys.*join"):
        _validate_iceberg_join_prerequisites(joins="SELECT 1", page_keys={})


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_fails_fast_for_image_export_without_joins(
    fx_tempdir: str,
):
    """
    Tests that image export fails before staging when joins are missing.
    """

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()

    with pytest.raises(ValueError, match="requires non-empty join SQL"):
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=f"{fx_tempdir}/example_warehouse",
            preset=None,
            image_dir=str(image_dir),
            page_keys={"join": "Metadata_ImageNumber"},
        )


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_requires_joins_without_image_export(fx_tempdir: str):
    """
    Tests that Iceberg export fails before staging when joins are missing.
    """

    with pytest.raises(ValueError, match="requires non-empty join SQL"):
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=f"{fx_tempdir}/example_warehouse",
            preset=None,
            joins="",
            page_keys={"join": "Metadata_ImageNumber"},
        )


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_cleans_up_failed_build_root(fx_tempdir: str):
    """
    Tests that a failed warehouse write does not leave partial output behind.
    """

    warehouse_path = Path(fx_tempdir) / "failed_warehouse"

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
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
            "cytotable.warehouse.iceberg.parsl.load",
            side_effect=RuntimeError("parsl load failed"),
        ),
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
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
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=True),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
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
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch("cytotable.warehouse.iceberg._run_export_workflow", return_value={}),
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
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    joined_chunk = Path(fx_tempdir) / "joined_chunk.parquet"
    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), stage_parquet)
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), joined_chunk)

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            side_effect=[
                str(stage_parquet),
                [str(joined_chunk)],
            ],
        ),
        patch(
            "cytotable.warehouse.iceberg.image_crop_table_from_joined_chunk",
            return_value=pa.table(
                {"Metadata_ObjectID": pa.array([], type=pa.string())}
            ),
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
            image_dir=str(image_dir),
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    assert "profiles.joined_profiles" in list_iceberg_tables(warehouse_path)
    assert "profiles.profile_with_images" not in list_iceberg_tables(warehouse_path)
    assert "images.image_crops" not in list_iceberg_tables(warehouse_path)


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_does_not_store_compartment_tables(
    fx_tempdir: str,
):
    """
    Tests that the warehouse stores only the materialized profiles table.
    """

    warehouse_path = Path(fx_tempdir) / "profiles_only_warehouse"
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    parquet.write_table(
        pa.table({"Metadata_ImageNumber": [1], "Metadata_ObjectNumber": [1]}),
        stage_parquet,
    )

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            return_value=str(stage_parquet),
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

    tables = list_iceberg_tables(warehouse_path)
    assert "profiles.joined_profiles" in tables
    assert all(
        table not in tables
        for table in (
            "profiles.cells",
            "profiles.cytoplasm",
            "profiles.image",
            "profiles.nuclei",
        )
    )


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_supports_immediate_table_readback(
    fx_tempdir: str,
):
    """
    Tests that warehouse tables can be read back after writing.
    """

    warehouse_path = Path(fx_tempdir) / "readback_warehouse"
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    parquet.write_table(
        pa.table(
            {
                "Metadata_ImageNumber": [1],
                "Metadata_ObjectNumber": [1],
            }
        ),
        stage_parquet,
    )

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            return_value=str(stage_parquet),
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset=None,
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    table = read_iceberg_table(warehouse_path, "joined_profiles")

    assert list(table.columns) == [
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
        "Metadata_ObjectID",
    ]
    assert len(table) == 1
    assert table["Metadata_ObjectID"].notna().all()


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_records_cytotable_provenance(
    fx_tempdir: str,
):
    """
    Tests that Iceberg outputs record CytoTable producer/version metadata.
    """

    warehouse_path = Path(fx_tempdir) / "provenance_warehouse"
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    parquet.write_table(
        pa.table(
            {
                "Metadata_ImageNumber": [1],
                "Metadata_ObjectNumber": [1],
            }
        ),
        stage_parquet,
    )

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            return_value=str(stage_parquet),
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset=None,
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    bundle = catalog(warehouse_path)
    registry = loads(bundle.registry_path.read_text())
    table = bundle.load_table(("profiles", "joined_profiles"))

    assert cast(dict[str, str], registry["properties"])["data-producer"].endswith(
        "CytoTable"
    )
    assert "data-producer-version" in cast(dict[str, str], registry["properties"])
    assert table.metadata.properties["data-producer"].endswith("CytoTable")
    assert "data-producer-version" in table.metadata.properties


def test_describe_iceberg_warehouse_handles_missing_snapshot(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Tests warehouse description when a table has no current snapshot.
    """

    files = pd.DataFrame({"record_count": [2, 3]})
    table = MagicMock()
    table.inspect.files.return_value.to_pandas.return_value = files
    table.current_snapshot.return_value = None

    bundle = MagicMock()
    bundle.list_namespaces.return_value = [("profiles",)]
    bundle.list_tables.return_value = [("profiles", "joined_profiles")]
    bundle.list_views.return_value = []
    bundle.load_table.return_value = table

    monkeypatch.setattr(
        "cytotable.warehouse.iceberg.catalog", lambda *args, **kwargs: bundle
    )

    described = describe_iceberg_warehouse("example_warehouse")

    assert described.loc[0, "table"] == "profiles.joined_profiles"
    assert described.loc[0, "rows"] == 5
    assert pd.isna(described.loc[0, "snapshot_id"])


def test_describe_iceberg_warehouse_view_rows_are_none(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Views in describe() return None for row count to avoid full materialisation.
    """

    bundle = MagicMock()
    bundle.list_namespaces.return_value = [("profiles",)]
    bundle.list_tables.return_value = []
    bundle.list_views.return_value = [("profiles", "profile_with_images")]

    monkeypatch.setattr(
        "cytotable.warehouse.iceberg.catalog", lambda *args, **kwargs: bundle
    )

    described = describe_iceberg_warehouse("example_warehouse", include_views=True)

    assert len(described) == 1
    assert described.loc[0, "kind"] == "view"
    assert described.loc[0, "rows"] is None


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

    assert file_index.by_relative["example.ome.zarr"].resolve() == zarr_store.resolve()


def test_build_file_index_supports_cloud_like_paths(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Tests indexing image-like files from cloud-style directory roots.
    """

    # pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods
    class FakeCloudPath:
        def __init__(self, path: str, *, file: bool = False, directory: bool = False):
            self._path = path
            self._file = file
            self._directory = directory

        @property
        def name(self) -> str:
            return pathlib.PurePosixPath(self._path).name

        @property
        def stem(self) -> str:
            return pathlib.PurePosixPath(self._path).stem

        def is_file(self) -> bool:
            return self._file

        def is_dir(self) -> bool:
            return self._directory

        def exists(self) -> bool:
            return True

        @property
        def fspath(self) -> str:
            return "/tmp/fake-cloud-file.tiff"

        def __str__(self) -> str:
            return self._path

    root = FakeCloudPath("s3://bucket/images", directory=True)
    child = FakeCloudPath("s3://bucket/images/plate_a/cell.tiff", file=True)

    monkeypatch.setattr("cytotable.warehouse.images.CloudPath", FakeCloudPath)
    monkeypatch.setattr(
        "cytotable.warehouse.images._build_path", lambda path, **kwargs: root
    )
    monkeypatch.setattr(
        "cytotable.warehouse.images.cloud_glob", lambda start, pattern: [child]
    )

    file_index = _build_file_index("s3://bucket/images")

    assert "plate_a/cell.tiff" in file_index.by_relative
    assert file_index.by_basename["cell.tiff"] == [child]


def test_crop_ome_arrow_localizes_cloud_like_paths(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Tests that cloud-like image paths are localized before OME-Arrow reads.
    """

    scanned_paths: list[str] = []

    # pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods
    class FakeCloudPath:
        def __init__(self, path: str):
            self._path = path

        @property
        def fspath(self) -> str:
            return "/tmp/local-cache/cell.tiff"

        def __str__(self) -> str:
            return self._path

    class FakeCollected:
        def __init__(self):
            self.data = {"version": "0.0.8"}

    class FakeScanner:
        def slice_lazy(self, **_kwargs):
            return self

        def collect(self):
            return FakeCollected()

    class FakeOMEArrow:
        @staticmethod
        def scan(path: str):
            scanned_paths.append(path)
            return FakeScanner()

    monkeypatch.setattr("cytotable.warehouse.images.CloudPath", FakeCloudPath)
    monkeypatch.setattr(
        "cytotable.warehouse.images._require_ome_arrow",
        lambda: (FakeOMEArrow, pa.struct([pa.field("version", pa.string())])),
    )

    result = _crop_ome_arrow(
        FakeCloudPath("s3://bucket/images/cell.tiff"),
        {"x_min": 0, "x_max": 1, "y_min": 0, "y_max": 1},
    )

    assert scanned_paths == ["/tmp/local-cache/cell.tiff"]
    assert result["version"] == "0.0.8"


def test_build_file_index_raises_for_ambiguous_basenames(fx_tempdir: str):
    """
    Tests that basename-only lookup fails when multiple relative paths match.
    """

    image_root = Path(fx_tempdir) / "images"
    first = image_root / "plate_a" / "cell.ome.tiff"
    second = image_root / "plate_b" / "cell.ome.tiff"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.touch()
    second.touch()

    file_index = _build_file_index(str(image_root))

    with pytest.raises(ValueError, match="Ambiguous image basename"):
        _find_matching_segmentation_path(
            data_value="cell.ome.tiff",
            pattern_map=None,
            file_dir=str(image_root),
            candidate_path=Path("cell.ome.tiff"),
            file_index=file_index,
        )


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


def test_add_object_id_to_profiles_frame():
    """
    Tests adding stable object ids to joined profiles rows.
    """

    joined = pd.DataFrame(
        {
            "Metadata_ImageNumber": [1],
            "Metadata_ObjectNumber": [2],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_X": [3],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_X": [7],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_Y": [2],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_Y": [8],
        }
    )

    with_object_id = add_object_id_to_profiles_frame(joined)

    assert "Metadata_ObjectID" in with_object_id.columns
    assert with_object_id.columns.tolist()[:3] == [
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
        "Metadata_ObjectID",
    ]
    assert with_object_id.loc[0, "Metadata_ObjectID"].startswith("obj-")
    assert "Metadata_SourceBBoxXMin" in with_object_id.columns
    assert "Metadata_SourceBBoxXMax" in with_object_id.columns
    assert "Metadata_SourceBBoxYMin" in with_object_id.columns
    assert "Metadata_SourceBBoxYMax" in with_object_id.columns


def test_resolve_bbox_columns_supports_metadata_bbox_names():
    """
    Tests bbox resolution for normalized metadata bbox column names.
    """

    bbox = resolve_bbox_columns(
        [
            "Metadata_SourceBBoxXMin",
            "Metadata_SourceBBoxXMax",
            "Metadata_SourceBBoxYMin",
            "Metadata_SourceBBoxYMax",
        ]
    )

    assert bbox is not None
    assert bbox.x_min == "Metadata_SourceBBoxXMin"
    assert bbox.y_max == "Metadata_SourceBBoxYMax"


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_strip_null_fields_from_ome_arrow_schema():
    """
    Tests that null-typed OME-Arrow fields are removed for Iceberg compatibility.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    sanitized = _strip_null_fields_from_type(ome_arrow_struct)

    assert "masks" not in [field.name for field in sanitized]
    assert all(not pa.types.is_null(field.type) for field in sanitized)


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_table_from_joined_chunk(fx_tempdir: str):
    """
    Tests OME-Arrow crop export from a joined parquet chunk.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    mask_dir = Path(fx_tempdir) / "masks"
    outline_dir = Path(fx_tempdir) / "outlines"
    image_dir.mkdir()
    mask_dir.mkdir()
    outline_dir.mkdir()

    image = np.arange(100, dtype=np.uint16).reshape(10, 10)
    mask = np.zeros((10, 10), dtype=np.uint16)
    mask[1:9, 2:8] = 2
    outline = np.zeros((10, 10), dtype=np.uint16)
    outline[2:8, 3:7] = 1
    tifffile.imwrite(image_dir / "cell.tiff", image)
    tifffile.imwrite(mask_dir / "cell.tiff", mask)
    tifffile.imwrite(outline_dir / "cell.tiff", outline)

    joined_chunk = pa.table(
        {
            "Metadata_ImageNumber": [1],
            "Image_FileName_DNA": ["cell.tiff"],
            "Image_FileName_AGP": ["cell.tiff"],
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
        mask_dir=str(mask_dir),
        outline_dir=str(outline_dir),
    )

    assert crop_table.num_rows == 2
    assert IMAGE_TABLE_NAME == "image_crops"
    assert "Metadata_ObjectID" in crop_table.column_names
    assert "Metadata_ImageCropID" in crop_table.column_names
    assert "ome_arrow_image" in crop_table.column_names
    assert "ome_arrow_outline" in crop_table.column_names
    assert "ome_arrow_mask" in crop_table.column_names
    assert "ome_arrow_label" in crop_table.column_names
    assert crop_table["label_source_kind"].to_pylist() == ["outline", "outline"]
    assert len(set(crop_table["Metadata_ObjectID"].to_pylist())) == 1
    assert len(set(crop_table["Metadata_ImageCropID"].to_pylist())) == 2


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_source_image_table_from_joined_chunk(fx_tempdir: str):
    """
    Tests full OME-Arrow source image export from a joined parquet chunk.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    mask_dir = Path(fx_tempdir) / "masks"
    outline_dir = Path(fx_tempdir) / "outlines"
    image_dir.mkdir()
    mask_dir.mkdir()
    outline_dir.mkdir()

    image = np.arange(100, dtype=np.uint16).reshape(10, 10)
    mask = np.zeros((10, 10), dtype=np.uint16)
    mask[1:9, 2:8] = 2
    outline = np.zeros((10, 10), dtype=np.uint16)
    outline[2:8, 3:7] = 1
    tifffile.imwrite(image_dir / "cell.tiff", image)
    tifffile.imwrite(mask_dir / "cell.tiff", mask)
    tifffile.imwrite(outline_dir / "cell.tiff", outline)

    joined_chunk = pa.table(
        {
            "Metadata_ImageNumber": [1, 1],
            "Image_FileName_DNA": ["cell.tiff", "cell.tiff"],
            "Image_FileName_AGP": ["cell.tiff", "cell.tiff"],
        }
    )
    chunk_path = Path(fx_tempdir) / "joined_source_images.parquet"
    parquet.write_table(joined_chunk, chunk_path)

    source_table = source_image_table_from_joined_chunk(
        chunk_path=str(chunk_path),
        image_dir=str(image_dir),
        mask_dir=str(mask_dir),
        outline_dir=str(outline_dir),
    )

    assert source_table.num_rows == 2
    assert SOURCE_IMAGE_TABLE_NAME == "source_images"
    assert "Metadata_ImageID" in source_table.column_names
    assert "ome_arrow_image" in source_table.column_names
    assert "ome_arrow_outline" in source_table.column_names
    assert "ome_arrow_mask" in source_table.column_names
    assert "ome_arrow_label" in source_table.column_names
    assert source_table["label_source_kind"].to_pylist() == ["outline", "outline"]
    assert len(set(source_table["Metadata_ImageID"].to_pylist())) == 2


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_source_image_table_prefers_relative_path_matches(fx_tempdir: str):
    """
    Tests that relative image paths disambiguate duplicate basenames.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    first = image_dir / "plate_a" / "cell.tiff"
    second = image_dir / "plate_b" / "cell.tiff"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)

    tifffile.imwrite(first, np.ones((4, 4), dtype=np.uint16))
    tifffile.imwrite(second, np.full((4, 4), 2, dtype=np.uint16))

    joined_chunk = pa.table(
        {
            "Metadata_ImageNumber": [1, 2],
            "Image_FileName_DNA": ["plate_a/cell.tiff", "plate_b/cell.tiff"],
        }
    )
    chunk_path = Path(fx_tempdir) / "joined_source_images_relative.parquet"
    parquet.write_table(joined_chunk, chunk_path)

    source_table = source_image_table_from_joined_chunk(
        chunk_path=str(chunk_path),
        image_dir=str(image_dir),
    )

    assert source_table.num_rows == 2
    assert sorted(source_table["source_image_file"].to_pylist()) == [
        "plate_a/cell.tiff",
        "plate_b/cell.tiff",
    ]


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
    assert crop_table["source_bbox_x_min"].to_pylist() == [3]
    assert crop_table["source_bbox_x_max"].to_pylist() == [7]
    assert crop_table["source_bbox_y_min"].to_pylist() == [2]
    assert crop_table["source_bbox_y_max"].to_pylist() == [8]


def _write_image_crop_chunk(
    chunk_path: Path, image_names: list[str], n_rows: int
) -> None:
    """
    Write a joined chunk with ``n_rows`` rows referencing ``image_names``.

    Each row has two image columns (DNA, AGP) and CellProfiler-style bbox
    columns kept inside a 10x10 image. Used to exceed ``_CROP_PARALLEL_MIN``.
    """

    rows = []
    for i in range(n_rows):
        name = image_names[i % len(image_names)]
        x0 = i % 6
        y0 = (i * 3) % 6
        rows.append(
            {
                "Metadata_ImageNumber": 1,
                "Metadata_ObjectNumber": i + 1,
                "Image_FileName_DNA": name,
                "Image_FileName_AGP": image_names[(i + 1) % len(image_names)],
                "Cells_AreaShape_BoundingBoxMinimum_X": x0,
                "Cells_AreaShape_BoundingBoxMaximum_X": x0 + 4,
                "Cells_AreaShape_BoundingBoxMinimum_Y": y0,
                "Cells_AreaShape_BoundingBoxMaximum_Y": y0 + 4,
            }
        )
    parquet.write_table(
        pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False), chunk_path
    )


def _strip_acquisition_datetime(obj: Any) -> Any:
    """
    Recursively drop ``acquisition_datetime`` from OME-Arrow struct payloads.

    ome_arrow stamps this field with the read time on every ``from_tiff`` call,
    so it is non-deterministic across reads (and across processes). The pixel
    data and all other fields are deterministic, so removing this field yields a
    stable value for equality comparisons.
    """

    if isinstance(obj, dict):
        return {
            k: _strip_acquisition_datetime(v)
            for k, v in obj.items()
            if k != "acquisition_datetime"
        }
    if isinstance(obj, list):
        return [_strip_acquisition_datetime(v) for v in obj]
    return obj


def _table_to_normalized_pydict(table: pa.Table, sort_key: str) -> dict:
    """Sort a table by ``sort_key`` and return a deterministic pydict."""

    return _strip_acquisition_datetime(
        table.sort_by([(sort_key, "ascending")]).to_pydict()
    )


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_parallel_matches_serial(fx_tempdir: str):
    """
    The ProcessPool row-shard path produces identical output to the serial path.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    tifffile.imwrite(
        image_dir / "cell.tiff", np.arange(100, dtype=np.uint16).reshape(10, 10)
    )

    # 40 rows x 2 image columns = 80 crops >= _CROP_PARALLEL_MIN (64).
    chunk_path = Path(fx_tempdir) / "joined_parallel.parquet"
    _write_image_crop_chunk(chunk_path, ["cell.tiff"], 40)

    serial = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=1
    )
    parallel = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=4
    )

    assert serial.num_rows == 80
    assert parallel.num_rows == 80
    # Same columns/schema.
    assert serial.column_names == parallel.column_names
    # Same content, compared in a stable order (acquisition_datetime excluded
    # because ome_arrow stamps it with the read time).
    assert _table_to_normalized_pydict(serial, "Metadata_ImageCropID") == (
        _table_to_normalized_pydict(parallel, "Metadata_ImageCropID")
    )

    # Explicitly assert the cropped image payloads themselves are equivalent
    # between the two paths. The full-table comparison above already includes
    # ome_arrow_image, but this makes the image-crop equivalence obvious and
    # pinpoints pixel drift on failure (instead of a whole-table diff). Every
    # row references a resolvable image with an in-bounds bbox, so no crops are
    # null; acquisition_datetime is read-time-stamped by ome_arrow, so strip
    # that non-deterministic field before comparing the struct payloads.
    serial_sorted = serial.sort_by([("Metadata_ImageCropID", "ascending")])
    parallel_sorted = parallel.sort_by([("Metadata_ImageCropID", "ascending")])
    assert serial_sorted["ome_arrow_image"].null_count == 0
    assert parallel_sorted["ome_arrow_image"].null_count == 0
    assert _strip_acquisition_datetime(
        serial_sorted["ome_arrow_image"].to_pylist()
    ) == _strip_acquisition_datetime(parallel_sorted["ome_arrow_image"].to_pylist())


def test_extract_crop_key_field_names_includes_object_columns():
    """
    _extract_crop_key_field_names discovers CellProfiler object-identity and
    parent-linkage columns (suffixes _Object_Number / _Parent_Cells /
    _Parent_Nuclei) plus Metadata_ObjectNumber, while all-NaN columns are
    excluded. _extract_image_key_field_names (the source-image path) excludes
    object-level columns because source images are keyed per image.
    """

    data = pd.DataFrame(
        {
            "Metadata_ImageNumber": [1, 2],
            "Metadata_ObjectNumber": [1, 2],
            "Cells_Number_Object_Number": [1, 2],
            "Cytoplasm_Parent_Nuclei": [1, 2],
            "Cells_Parent_Nuclei": [1, None],  # not all-null -> included
            "Nuclei_Number_Object_Number": [None, None],  # all-null -> excluded
            "Some_Other_Column": [1, 2],
        }
    )

    assert set(_extract_crop_key_field_names(data)) == {
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
        "Cells_Number_Object_Number",
        "Cytoplasm_Parent_Nuclei",
        "Cells_Parent_Nuclei",
    }

    image_names = set(_extract_image_key_field_names(data))
    assert image_names == {"Metadata_ImageNumber"}
    assert not image_names & {
        "Metadata_ObjectNumber",
        "Cells_Number_Object_Number",
        "Cytoplasm_Parent_Nuclei",
        "Cells_Parent_Nuclei",
    }


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_preserves_object_key_columns(fx_tempdir: str):
    """
    Object-identity and parent-linkage columns survive on images.image_crops.

    Regression: the parallel path's key-field allowlist silently dropped
    CellProfiler-native columns (names ending in _Object_Number, _Parent_Cells,
    or _Parent_Nuclei) such as Cells_Number_Object_Number and
    Cytoplasm_Parent_Nuclei. Both the serial and parallel paths must carry them,
    with matching schemas.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    tifffile.imwrite(
        image_dir / "cell.tiff", np.arange(100, dtype=np.uint16).reshape(10, 10)
    )

    # 40 rows x 2 image columns = 80 crops >= _CROP_PARALLEL_MIN (64).
    n_rows = 40
    rows = []
    for i in range(n_rows):
        rows.append(
            {
                "Metadata_ImageNumber": 1,
                "Metadata_ObjectNumber": i + 1,
                "Image_FileName_DNA": "cell.tiff",
                "Image_FileName_AGP": "cell.tiff",
                "Cells_AreaShape_BoundingBoxMinimum_X": 1,
                "Cells_AreaShape_BoundingBoxMaximum_X": 5,
                "Cells_AreaShape_BoundingBoxMinimum_Y": 1,
                "Cells_AreaShape_BoundingBoxMaximum_Y": 5,
                "Cells_Number_Object_Number": i + 1,
                "Nuclei_Number_Object_Number": i + 1,
                "Cytoplasm_Number_Object_Number": i + 1,
                "Cytoplasm_Parent_Cells": i + 1,
                "Cytoplasm_Parent_Nuclei": i + 1,
                "Cells_Parent_Nuclei": i + 1,
            }
        )
    chunk_path = Path(fx_tempdir) / "joined_object_keys.parquet"
    parquet.write_table(
        pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False), chunk_path
    )

    expected_key_columns = {
        "Metadata_ObjectNumber",
        "Cells_Number_Object_Number",
        "Nuclei_Number_Object_Number",
        "Cytoplasm_Number_Object_Number",
        "Cytoplasm_Parent_Cells",
        "Cytoplasm_Parent_Nuclei",
        "Cells_Parent_Nuclei",
    }

    serial = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=1
    )
    parallel = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=4
    )

    for table in (serial, parallel):
        assert expected_key_columns.issubset(set(table.column_names))
        for col in expected_key_columns:
            # Carried through as non-null strings (see _rows_to_crop_table).
            assert table[col].null_count == 0, col

    # Serial and parallel paths produce the same schema and row count.
    assert set(serial.column_names) == set(parallel.column_names)
    assert serial.num_rows == parallel.num_rows == n_rows * 2


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_parallel_is_deterministic(fx_tempdir: str):
    """
    Repeated parallel runs produce identical output.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    tifffile.imwrite(
        image_dir / "cell.tiff", np.arange(100, dtype=np.uint16).reshape(10, 10)
    )

    chunk_path = Path(fx_tempdir) / "joined_deterministic.parquet"
    _write_image_crop_chunk(chunk_path, ["cell.tiff"], 40)

    first = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=4
    )
    second = image_crop_table_from_joined_chunk(
        chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=4
    )
    assert _table_to_normalized_pydict(first, "Metadata_ImageCropID") == (
        _table_to_normalized_pydict(second, "Metadata_ImageCropID")
    )


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_image_crop_below_threshold_stays_serial(fx_tempdir: str):
    """
    Small chunks (below _CROP_PARALLEL_MIN) do not spawn a process pool.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    tifffile.imwrite(
        image_dir / "cell.tiff", np.arange(100, dtype=np.uint16).reshape(10, 10)
    )

    # 1 row x 2 image columns = 2 crops, below the threshold.
    chunk_path = Path(fx_tempdir) / "joined_small.parquet"
    _write_image_crop_chunk(chunk_path, ["cell.tiff"], 1)

    with patch.object(images_module, "ProcessPoolExecutor") as mock_pool:
        crop_table = image_crop_table_from_joined_chunk(
            chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=4
        )
    mock_pool.assert_not_called()
    assert crop_table.num_rows == 2


def test_dedup_table_by_first_occurrence():
    """
    _dedup_table_by_first_occurrence keeps the first row per key and preserves order.
    """

    table = pa.table(
        {
            "Metadata_ImageID": ["a", "b", "a", "c", "b", None],
            "payload": pa.array(
                [
                    {"x": 1},
                    {"x": 2},
                    {"x": 3},
                    {"x": 4},
                    {"x": 5},
                    {"x": 6},
                ],
                type=pa.struct([pa.field("x", pa.int64())]),
            ),
        }
    )

    deduped = _dedup_table_by_first_occurrence(table, "Metadata_ImageID")
    assert deduped.column("Metadata_ImageID").to_pylist() == ["a", "b", "c", None]
    assert deduped.column("payload").to_pylist() == [
        {"x": 1},
        {"x": 2},
        {"x": 4},
        {"x": 6},
    ]


@pytest.mark.skipif(find_spec("ome_arrow") is None, reason="ome-arrow not installed")
def test_source_image_parallel_dedup_matches_serial(fx_tempdir: str):
    """
    The source-image parallel path deduplicates across shards and matches serial.
    """

    tifffile = pytest.importorskip("tifffile")

    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    for name in ("a.tiff", "b.tiff"):
        tifffile.imwrite(
            image_dir / name, np.arange(100, dtype=np.uint16).reshape(10, 10)
        )

    chunk_path = Path(fx_tempdir) / "joined_source_parallel.parquet"
    _write_image_crop_chunk(chunk_path, ["a.tiff", "b.tiff"], 30)

    # Lower the unique-image threshold so the 2-image chunk takes the parallel path.
    with patch.object(images_module, "_SOURCE_PARALLEL_MIN", 1):
        serial = source_image_table_from_joined_chunk(
            chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=1
        )
        parallel = source_image_table_from_joined_chunk(
            chunk_path=str(chunk_path), image_dir=str(image_dir), crop_workers=2
        )

    serial_ids = serial["Metadata_ImageID"].to_pylist()
    parallel_ids = parallel["Metadata_ImageID"].to_pylist()
    assert len(serial_ids) == len(set(serial_ids))
    assert len(parallel_ids) == len(set(parallel_ids))
    assert serial.num_rows == parallel.num_rows
    assert _table_to_normalized_pydict(serial, "Metadata_ImageID") == (
        _table_to_normalized_pydict(parallel, "Metadata_ImageID")
    )

    # Empty-shard coverage: a shard whose rows are all-unresolvable yields no
    # rows, and crop_workers exceeding the row count produces empty trailing
    # shards. Both cases must still emit the full schema so concat across
    # shards succeeds and the parallel output matches serial.
    empty_chunk = Path(fx_tempdir) / "joined_source_empty_shards.parquet"
    empty_rows = []
    # rows 0-1 reference a resolvable image in both columns; rows 2-3 reference
    # only an unresolvable image, so their shard contributes zero rows.
    for i in range(2):
        empty_rows.append(
            {
                "Metadata_ImageNumber": 1,
                "Metadata_ObjectNumber": i + 1,
                "Image_FileName_DNA": "a.tiff",
                "Image_FileName_AGP": "a.tiff",
                "Cells_AreaShape_BoundingBoxMinimum_X": 0,
                "Cells_AreaShape_BoundingBoxMaximum_X": 4,
                "Cells_AreaShape_BoundingBoxMinimum_Y": 0,
                "Cells_AreaShape_BoundingBoxMaximum_Y": 4,
            }
        )
    for i in range(2):
        empty_rows.append(
            {
                "Metadata_ImageNumber": 1,
                "Metadata_ObjectNumber": i + 3,
                "Image_FileName_DNA": "missing.tiff",
                "Image_FileName_AGP": "missing.tiff",
                "Cells_AreaShape_BoundingBoxMinimum_X": 0,
                "Cells_AreaShape_BoundingBoxMaximum_X": 4,
                "Cells_AreaShape_BoundingBoxMinimum_Y": 0,
                "Cells_AreaShape_BoundingBoxMaximum_Y": 4,
            }
        )
    parquet.write_table(
        pa.Table.from_pandas(pd.DataFrame(empty_rows), preserve_index=False),
        empty_chunk,
    )

    with patch.object(images_module, "_SOURCE_PARALLEL_MIN", 1):
        serial_empty = source_image_table_from_joined_chunk(
            chunk_path=str(empty_chunk), image_dir=str(image_dir), crop_workers=1
        )
        # crop_workers=8 >> 4 rows -> empty trailing shards; rows 2-3 form a
        # 0-output shard.
        parallel_empty = source_image_table_from_joined_chunk(
            chunk_path=str(empty_chunk), image_dir=str(image_dir), crop_workers=8
        )

    assert serial_empty.schema == parallel_empty.schema
    assert serial_empty.num_rows == parallel_empty.num_rows
    empty_parallel_ids = parallel_empty["Metadata_ImageID"].to_pylist()
    assert len(empty_parallel_ids) == len(set(empty_parallel_ids))
    assert _table_to_normalized_pydict(serial_empty, "Metadata_ImageID") == (
        _table_to_normalized_pydict(parallel_empty, "Metadata_ImageID")
    )
    # Only the resolvable image contributes rows (one per image column).
    assert serial_empty.num_rows == 2


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_writes_source_images_when_requested(
    fx_tempdir: str,
):
    """
    Tests optional full source image export into the images namespace.
    """

    warehouse_path = Path(fx_tempdir) / "source_images_warehouse"
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    joined_chunk = Path(fx_tempdir) / "joined_chunk.parquet"
    image_dir = Path(fx_tempdir) / "images"
    image_dir.mkdir()
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), stage_parquet)
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), joined_chunk)

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            side_effect=[str(stage_parquet), [str(joined_chunk)]],
        ),
        patch(
            "cytotable.warehouse.iceberg.image_crop_table_from_joined_chunk",
            return_value=pa.table(
                {"Metadata_ObjectID": pa.array(["obj-1"], type=pa.string())}
            ),
        ),
        patch(
            "cytotable.warehouse.iceberg.source_image_table_from_joined_chunk",
            return_value=pa.table(
                {"Metadata_ImageID": pa.array(["img-1"], type=pa.string())}
            ),
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
            image_dir=str(image_dir),
            include_source_images=True,
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    tables = list_iceberg_tables(warehouse_path)
    assert "images.source_images" in tables


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_deduplicates_source_images_across_chunks(
    fx_tempdir: str,
):
    """
    Tests that source image rows are deduplicated across joined chunks.
    """

    warehouse_path = Path(fx_tempdir) / "source_images_dedup_warehouse"
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    joined_chunk_a = Path(fx_tempdir) / "joined_chunk_a.parquet"
    joined_chunk_b = Path(fx_tempdir) / "joined_chunk_b.parquet"
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), stage_parquet)
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), joined_chunk_a)
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), joined_chunk_b)

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            side_effect=[
                str(stage_parquet),
                [str(joined_chunk_a), str(joined_chunk_b)],
            ],
        ),
        patch(
            "cytotable.warehouse.iceberg.image_crop_table_from_joined_chunk",
            return_value=pa.table(
                {"Metadata_ObjectID": pa.array(["obj-1"], type=pa.string())}
            ),
        ),
        patch(
            "cytotable.warehouse.iceberg.source_image_table_from_joined_chunk",
            side_effect=[
                pa.table({"Metadata_ImageID": pa.array(["img-1"], type=pa.string())}),
                pa.table({"Metadata_ImageID": pa.array(["img-1"], type=pa.string())}),
            ],
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
            image_dir=fx_tempdir,
            include_source_images=True,
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    source_images = read_iceberg_table(warehouse_path, "source_images")
    assert source_images["Metadata_ImageID"].tolist() == ["img-1"]


@pytest.mark.skipif(find_spec("pyiceberg") is None, reason="pyiceberg not installed")
def test_write_iceberg_warehouse_preserves_nullable_source_image_string_schema(
    fx_tempdir: str,
):
    """
    Tests that deduplicated source images keep nullable string columns typed.
    """

    warehouse_path = Path(fx_tempdir) / "source_images_nullable_schema_warehouse"
    stage_parquet = Path(fx_tempdir) / "joined_profiles.parquet"
    joined_chunk = Path(fx_tempdir) / "joined_chunk.parquet"
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), stage_parquet)
    parquet.write_table(pa.table({"Metadata_ImageNumber": [1]}), joined_chunk)

    with (
        patch("cytotable.warehouse.iceberg.parsl.load"),
        patch("cytotable.warehouse.iceberg.parsl.dfk") as dfk,
        patch("cytotable.warehouse.iceberg._parsl_loaded", return_value=False),
        patch(
            "cytotable.warehouse.iceberg._run_export_workflow",
            side_effect=[str(stage_parquet), [str(joined_chunk)]],
        ),
        patch(
            "cytotable.warehouse.iceberg.image_crop_table_from_joined_chunk",
            return_value=pa.table(
                {"Metadata_ObjectID": pa.array(["obj-1"], type=pa.string())}
            ),
        ),
        patch(
            "cytotable.warehouse.iceberg.source_image_table_from_joined_chunk",
            return_value=pa.table(
                {
                    "Metadata_ImageID": pa.array(["img-1"], type=pa.string()),
                    "source_image_column": pa.array(
                        ["Image_FileName_DNA"], type=pa.string()
                    ),
                    "source_image_file": pa.array(["cell.tiff"], type=pa.string()),
                    "label_source_kind": pa.array([None], type=pa.string()),
                }
            ),
        ),
    ):
        dfk.return_value.cleanup = MagicMock()
        write_iceberg_warehouse(
            source_path=f"{fx_tempdir}/missing-source",
            source_datatype="csv",
            warehouse_path=warehouse_path,
            preset="cellprofiler_csv",
            image_dir=fx_tempdir,
            include_source_images=True,
            joins="SELECT * FROM read_parquet('cells.parquet') AS cells",
            page_keys={"join": "Metadata_ImageNumber"},
        )

    source_images = read_iceberg_table(warehouse_path, "source_images")
    assert source_images["Metadata_ImageID"].tolist() == ["img-1"]
    assert source_images["label_source_kind"].isna().tolist() == [True]


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

    assert result is not None
    assert result.resolve() == segmentation.resolve()


def test_find_matching_segmentation_path_supports_cloud_like_roots(
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Tests regex-based segmentation resolution for cloud-style roots.
    """

    # pylint: disable=missing-class-docstring,missing-function-docstring,too-few-public-methods
    class FakeCloudPath:
        def __init__(self, path: str, *, file: bool = False, directory: bool = False):
            self._path = path
            self._file = file
            self._directory = directory

        @property
        def name(self) -> str:
            return pathlib.PurePosixPath(self._path).name

        @property
        def stem(self) -> str:
            return pathlib.PurePosixPath(self._path).stem

        def is_file(self) -> bool:
            return self._file

        def is_dir(self) -> bool:
            return self._directory

        def exists(self) -> bool:
            return True

        def __str__(self) -> str:
            return self._path

    root = FakeCloudPath("s3://bucket/outlines", directory=True)
    segmentation = FakeCloudPath(
        "s3://bucket/outlines/plateA_well_B03_site_1_outline.tiff", file=True
    )
    candidate = FakeCloudPath(
        "s3://bucket/images/plateA_well_B03_site_1.tiff", file=True
    )

    monkeypatch.setattr("cytotable.warehouse.images.CloudPath", FakeCloudPath)
    monkeypatch.setattr(
        "cytotable.warehouse.images._build_path", lambda path, **kwargs: root
    )

    result = _find_matching_segmentation_path(
        data_value="plateA_well_B03_site_1.tiff",
        pattern_map={
            r".*_outline\.tiff$": r"(plateA_well_B03_site_1)\.tiff$",
        },
        file_dir="s3://bucket/outlines",
        candidate_path=candidate,
        file_index=FileIndex(
            by_relative={"plateA_well_B03_site_1_outline.tiff": segmentation},
            by_basename={"plateA_well_B03_site_1_outline.tiff": [segmentation]},
            by_stem={"plateA_well_B03_site_1_outline": [segmentation]},
        ),
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

    assert result is not None
    assert result.resolve() == segmentation.resolve()


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

    assert first is not None
    assert second is not None
    assert first.resolve() == segmentation.resolve()
    assert second.resolve() == segmentation.resolve()
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
            "Metadata_ObjectID": [
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
    assert manifested.loc[0, "Metadata_ObjectID"].startswith("obj-")
    assert manifested.loc[0, "label_source_kind"] == "outline"


def test_profile_with_images_frame_skips_invalid_bbox_rows():
    """
    Tests manifest expansion skips malformed or inverted bbox rows.
    """

    joined = pd.DataFrame(
        {
            "Metadata_ImageNumber": [1, 2, 3],
            "Image_FileName_DNA": ["cell.tiff", "cell.tiff", "cell.tiff"],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_X": ["3", "bad", "7"],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_X": ["7", "8", "3"],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_Y": ["2", "2", "8"],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_Y": ["8", "8", "2"],
        }
    )
    image_rows = pd.DataFrame(
        {
            "Metadata_ObjectID": [
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
                        },
                        sort_keys=True,
                    )
                )
            ],
            "source_image_column": ["Image_FileName_DNA"],
            "source_image_file": ["cell.tiff"],
        }
    )

    manifested = profile_with_images_frame(joined_frame=joined, image_frame=image_rows)

    assert len(manifested) == 1
    assert manifested["Metadata_ImageNumber"].to_list() == [1]


def test_profile_with_images_frame_multi_row_multi_channel():
    """
    Melt-based expansion produces one row per (object, image channel) pair across
    multiple rows and multiple image columns.
    """

    joined = pd.DataFrame(
        {
            "Metadata_ImageNumber": [1, 2],
            "Image_FileName_DNA": ["dna1.tiff", "dna2.tiff"],
            "Image_FileName_AGP": ["agp1.tiff", "agp2.tiff"],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_X": [0, 0],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_X": [10, 10],
            "Cytoplasm_AreaShape_BoundingBoxMinimum_Y": [0, 0],
            "Cytoplasm_AreaShape_BoundingBoxMaximum_Y": [10, 10],
        }
    )
    image_frame = pd.DataFrame(
        {
            "Metadata_ObjectID": [],
            "source_image_column": [],
            "source_image_file": [],
        }
    )

    result = profile_with_images_frame(joined_frame=joined, image_frame=image_frame)

    # 2 rows × 2 channels = 4 expanded rows
    assert len(result) == 4
    assert set(result["source_image_column"]) == {
        "Image_FileName_DNA",
        "Image_FileName_AGP",
    }
    assert set(result["source_image_file"]) == {
        "dna1.tiff",
        "dna2.tiff",
        "agp1.tiff",
        "agp2.tiff",
    }
    # Each object gets a stable unique ID
    assert result["Metadata_ObjectID"].nunique() == 2


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

    assert "profiles.joined_profiles" in tables
    assert "images.image_crops" in tables
    assert "profiles.profile_with_images" in tables

    image_crops = read_iceberg_table(warehouse_path, "image_crops")
    profiles = read_iceberg_table(warehouse_path, "joined_profiles")
    profile_with_images = read_iceberg_table(warehouse_path, "profile_with_images")

    assert len(profiles) > 0
    assert len(image_crops) > 0
    assert len(profile_with_images) > 0
    assert "Metadata_ObjectID" in profiles.columns
    assert "Metadata_ObjectID" in image_crops.columns
    assert "Metadata_ImageCropID" in image_crops.columns
    assert "ome_arrow_image" in image_crops.columns
    assert profiles["Metadata_ObjectID"].notna().all()
    assert image_crops["Metadata_ObjectID"].notna().all()
    assert image_crops["Metadata_ImageCropID"].is_unique
    assert profile_with_images["Metadata_ObjectID"].notna().all()
    assert profile_with_images["ome_arrow_image"].notna().any()

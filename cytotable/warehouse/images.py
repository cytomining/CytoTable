"""
Helpers for exporting image crops alongside CytoTable measurement data.
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import pathlib
import re
import tempfile
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from functools import partial
from json import dumps
from typing import Any, Dict, Optional, Sequence, Union
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
from cloudpathlib import AnyPath, CloudPath

from cytotable.sources import _build_path
from cytotable.utils import cloud_glob

logger = logging.getLogger(__name__)

IMAGE_TABLE_NAME = "image_crops"
SOURCE_IMAGE_TABLE_NAME = "source_images"
PROFILE_BBOX_METADATA_COLUMNS = {
    "x_min": "Metadata_SourceBBoxXMin",
    "x_max": "Metadata_SourceBBoxXMax",
    "y_min": "Metadata_SourceBBoxYMin",
    "y_max": "Metadata_SourceBBoxYMax",
}
_IMAGE_SUFFIXES = (
    ".tif",
    ".tiff",
    ".ome.tif",
    ".ome.tiff",
    ".zarr",
    ".ome.zarr",
)


def object_id(name: str | UUID | None = None, *, prefix: str = "obj") -> str:
    """
    Return a stable string identifier with a UUID-shaped payload.
    """

    value = uuid4() if name is None else uuid5(NAMESPACE_URL, str(name))
    return f"{prefix}-{value}"


@dataclass(frozen=True)
class BBoxColumns:
    """
    Bounding box column names for cropped image export.
    """

    x_min: str
    x_max: str
    y_min: str
    y_max: str


@dataclass(frozen=True)
class FileIndex:
    """
    Relative-path-first index for image-like files in a directory tree.
    """

    by_relative: dict[str, Union[pathlib.Path, AnyPath]]
    by_basename: dict[str, list[Union[pathlib.Path, AnyPath]]]
    by_stem: dict[str, list[Union[pathlib.Path, AnyPath]]]


ImagePath = Union[pathlib.Path, AnyPath]


def _require_ome_arrow() -> tuple[Any, Any]:
    """
    Import and return OME-Arrow objects needed for crop export.
    """

    try:
        from ome_arrow import OMEArrow  # type: ignore
        from ome_arrow.meta import OME_ARROW_STRUCT  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "Image crop export requires the optional 'ome-arrow' dependency."
        ) from exc

    return OMEArrow, OME_ARROW_STRUCT


def _strip_null_fields_from_type(data_type: pa.DataType) -> pa.DataType:
    """
    Remove null-typed fields from nested Arrow types for Iceberg compatibility.
    """

    if pa.types.is_struct(data_type):
        return pa.struct(
            [
                pa.field(
                    field.name,
                    _strip_null_fields_from_type(field.type),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for field in data_type
                if not pa.types.is_null(field.type)
            ]
        )
    if pa.types.is_list(data_type):
        return pa.list_(_strip_null_fields_from_type(data_type.value_type))
    return data_type


def _strip_null_fields_from_value(value: Any, data_type: pa.DataType) -> Any:
    """
    Remove values corresponding to null-typed nested Arrow fields.
    """

    if value is None:
        return None
    if pa.types.is_struct(data_type):
        if hasattr(value, "as_py"):
            value = value.as_py()
        return {
            field.name: _strip_null_fields_from_value(value.get(field.name), field.type)
            for field in data_type
            if not pa.types.is_null(field.type)
        }
    if pa.types.is_list(data_type):
        return [
            _strip_null_fields_from_value(item, data_type.value_type) for item in value
        ]
    return value


def _normalize_file_value(value: Any) -> Optional[str]:
    """
    Normalize a file-like value to a comparable path string.
    """

    if value is None or pd.isna(value):
        return None

    normalized = str(value)
    if normalized.startswith("file:"):
        normalized = normalized[len("file:") :]
    return pathlib.PurePath(normalized).as_posix()


def _relative_index_key(path: ImagePath, root: ImagePath) -> str:
    """
    Build a normalized relative key for a file under an index root.
    """

    if isinstance(path, pathlib.Path) and isinstance(root, pathlib.Path):
        return path.relative_to(root).as_posix()

    root_str = str(root).rstrip("/")
    path_str = str(path)
    prefix = f"{root_str}/"
    if path_str.startswith(prefix):
        return path_str[len(prefix) :]
    return pathlib.PurePosixPath(path_str).name


def _local_image_io_path(path: ImagePath) -> pathlib.Path:
    """
    Return a local path for image I/O, caching cloud files when needed.
    """

    if isinstance(path, pathlib.Path):
        return path
    if isinstance(path, CloudPath):
        return pathlib.Path(path.fspath)
    return pathlib.Path(str(path))


def _build_file_index(
    file_dir: Optional[str],
    path_kwargs: Optional[Dict[str, Any]] = None,
) -> FileIndex:
    """
    Build a relative-path-first index for image-like files in a directory tree.
    """

    if file_dir is None:
        return FileIndex(by_relative={}, by_basename={}, by_stem={})

    root = _build_path(file_dir, **(path_kwargs or {}))
    if isinstance(root, pathlib.Path):
        root_exists = root.exists()
    else:
        try:
            root_exists = root.exists()
        except Exception:  # pragma: no cover - defensive for provider quirks
            root_exists = True
    if not root_exists:
        return FileIndex(by_relative={}, by_basename={}, by_stem={})

    relative_index: dict[str, ImagePath] = {}
    basename_index: dict[str, list[ImagePath]] = {}
    stem_index: dict[str, list[ImagePath]] = {}
    for path in cloud_glob(root, "**/*"):
        lowered = path.name.lower()
        is_image_path = path.is_file() or (
            path.is_dir() and lowered.endswith((".zarr", ".ome.zarr"))
        )
        if not is_image_path:
            continue
        if not lowered.endswith(_IMAGE_SUFFIXES):
            continue
        relative_key = _relative_index_key(path, root)
        relative_index[relative_key] = path
        basename_index.setdefault(path.name, []).append(path)
        stem_index.setdefault(path.stem, []).append(path)

    return FileIndex(
        by_relative=relative_index,
        by_basename=basename_index,
        by_stem=stem_index,
    )


def _resolve_indexed_path(
    normalized_value: str,
    file_index: FileIndex,
) -> Optional[ImagePath]:
    """
    Resolve a normalized path string against a relative-path-first file index.
    """

    normalized_path = pathlib.PurePosixPath(normalized_value)
    parts = normalized_path.parts

    for offset in range(len(parts)):
        candidate = pathlib.PurePosixPath(*parts[offset:]).as_posix()
        if candidate in file_index.by_relative:
            return file_index.by_relative[candidate]

    basename_matches = file_index.by_basename.get(normalized_path.name, [])
    if len(basename_matches) == 1:
        return basename_matches[0]
    if len(basename_matches) > 1:
        raise ValueError(
            f"Ambiguous image basename '{normalized_path.name}'. "
            "Provide a relative path to disambiguate."
        )

    stem_matches = file_index.by_stem.get(normalized_path.stem, [])
    if len(stem_matches) == 1:
        return stem_matches[0]
    if len(stem_matches) > 1:
        raise ValueError(
            f"Ambiguous image stem '{normalized_path.stem}'. "
            "Provide a relative path to disambiguate."
        )

    return None


def _find_matching_segmentation_path(
    data_value: str,
    pattern_map: Optional[dict[str, str]],
    file_dir: Optional[str],
    candidate_path: ImagePath,
    file_index: Optional[FileIndex] = None,
    lookup_cache: Optional[dict[str, Optional[ImagePath]]] = None,
    path_kwargs: Optional[Dict[str, Any]] = None,
) -> Optional[ImagePath]:
    """
    Resolve a matching mask/outline file path for an image value.
    """

    cache_key = None
    if lookup_cache is not None:
        cache_key = "|".join(
            [
                str(file_dir),
                str(candidate_path),
                str(data_value),
                dumps(pattern_map, sort_keys=True) if pattern_map is not None else "",
            ]
        )
        if cache_key in lookup_cache:
            return lookup_cache[cache_key]

    if file_dir is None:
        return None

    root = _build_path(file_dir, **(path_kwargs or {}))
    if isinstance(root, pathlib.Path):
        root_exists = root.exists()
    else:
        try:
            root_exists = root.exists()
        except Exception:  # pragma: no cover - defensive for provider quirks
            root_exists = True
    if not root_exists:
        return None

    indexed_files = (
        file_index
        if file_index is not None
        else _build_file_index(file_dir, path_kwargs=path_kwargs)
    )

    if pattern_map is None:
        result = _resolve_indexed_path(data_value, indexed_files)
        if lookup_cache is not None and cache_key is not None:
            lookup_cache[cache_key] = result
        return result

    indexed_paths = sorted(
        {str(path): path for path in indexed_files.by_relative.values()}.values(),
        key=lambda path: path.name,
    )

    for file_pattern, original_pattern in pattern_map.items():
        matched = re.search(original_pattern, data_value)
        if not matched:
            continue

        identifiers: list[str] = []
        identifiers.extend(
            str(group)
            for group in matched.groups()
            if isinstance(group, str) and group.strip()
        )
        identifiers.extend(
            [
                pathlib.PurePosixPath(data_value).stem,
                pathlib.PurePosixPath(str(candidate_path)).stem,
            ]
        )
        identifiers = list(
            dict.fromkeys(identifier for identifier in identifiers if identifier)
        )
        normalized_identifiers = [
            identifier.lower() for identifier in identifiers if identifier
        ]

        for file in indexed_paths:
            if not re.search(file_pattern, file.name):
                continue
            if not normalized_identifiers or any(
                identifier in file.stem.lower() for identifier in normalized_identifiers
            ):
                if lookup_cache is not None and cache_key is not None:
                    lookup_cache[cache_key] = file
                return file

    if lookup_cache is not None and cache_key is not None:
        lookup_cache[cache_key] = None
    return None


def _resolve_image_columns(data: pd.DataFrame) -> list[str]:
    """
    Find joined-table columns that look like image filename columns.
    """

    image_columns: list[str] = []
    for column in data.columns:
        if not pd.api.types.is_object_dtype(
            data[column]
        ) and not pd.api.types.is_string_dtype(data[column]):
            continue
        non_null = data[column].dropna().astype(str).head(5)
        if non_null.empty:
            continue
        if non_null.map(lambda value: value.lower().endswith(_IMAGE_SUFFIXES)).any():
            image_columns.append(str(column))
    return image_columns


def resolve_bbox_columns(
    columns: Sequence[Any],
    bbox_column_map: Optional[Dict[str, str]] = None,
) -> Optional[BBoxColumns]:
    """
    Resolve bbox columns using custom mapping, CellProfiler naming, then fallback tags.
    """

    col_by_name = {str(column): str(column) for column in columns}
    if bbox_column_map is not None:
        custom = {
            key: col_by_name.get(str(value))
            for key, value in bbox_column_map.items()
            if key in {"x_min", "x_max", "y_min", "y_max"}
        }
        if all(
            custom.get(key) is not None for key in ("x_min", "x_max", "y_min", "y_max")
        ):
            return BBoxColumns(
                x_min=custom["x_min"],  # type: ignore[arg-type]
                x_max=custom["x_max"],  # type: ignore[arg-type]
                y_min=custom["y_min"],  # type: ignore[arg-type]
                y_max=custom["y_max"],  # type: ignore[arg-type]
            )

    metadata_bbox = {
        key: col_by_name.get(value)
        for key, value in PROFILE_BBOX_METADATA_COLUMNS.items()
    }
    if all(metadata_bbox.values()):
        return BBoxColumns(
            x_min=metadata_bbox["x_min"],  # type: ignore[arg-type]
            x_max=metadata_bbox["x_max"],  # type: ignore[arg-type]
            y_min=metadata_bbox["y_min"],  # type: ignore[arg-type]
            y_max=metadata_bbox["y_max"],  # type: ignore[arg-type]
        )

    cp_prefixes = ("Cytoplasm_", "Nuclei_", "Cells_", "")
    for prefix in cp_prefixes:
        matched = {
            "x_min": col_by_name.get(f"{prefix}AreaShape_BoundingBoxMinimum_X"),
            "x_max": col_by_name.get(f"{prefix}AreaShape_BoundingBoxMaximum_X"),
            "y_min": col_by_name.get(f"{prefix}AreaShape_BoundingBoxMinimum_Y"),
            "y_max": col_by_name.get(f"{prefix}AreaShape_BoundingBoxMaximum_Y"),
        }
        if all(matched.values()):
            return BBoxColumns(
                x_min=matched["x_min"],  # type: ignore[arg-type]
                x_max=matched["x_max"],  # type: ignore[arg-type]
                y_min=matched["y_min"],  # type: ignore[arg-type]
                y_max=matched["y_max"],  # type: ignore[arg-type]
            )

    fallback = {
        "x_min": next(
            (str(column) for column in columns if "Minimum_X" in str(column)),
            None,
        ),
        "x_max": next(
            (str(column) for column in columns if "Maximum_X" in str(column)),
            None,
        ),
        "y_min": next(
            (str(column) for column in columns if "Minimum_Y" in str(column)),
            None,
        ),
        "y_max": next(
            (str(column) for column in columns if "Maximum_Y" in str(column)),
            None,
        ),
    }
    if all(fallback.values()):
        return BBoxColumns(
            x_min=fallback["x_min"],  # type: ignore[arg-type]
            x_max=fallback["x_max"],  # type: ignore[arg-type]
            y_min=fallback["y_min"],  # type: ignore[arg-type]
            y_max=fallback["y_max"],  # type: ignore[arg-type]
        )

    return None


def _extract_key_fields(row: pd.Series) -> dict[str, Any]:
    """
    Extract practical measurement key fields to carry into the image table.
    """

    preferred_columns = [
        "Metadata_TableNumber",
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
        "Image_Metadata_Well",
        "Image_Metadata_Plate",
        "Metadata_Well",
        "Metadata_Plate",
    ]
    keys = {
        column: row[column]
        for column in preferred_columns
        if column in row.index and not pd.isna(row[column])
    }
    for column in row.index:
        column_str = str(column)
        if (
            (
                column_str.endswith("_Object_Number")
                or column_str.endswith("_Parent_Cells")
                or column_str.endswith("_Parent_Nuclei")
            )
            and column_str not in keys
            and not pd.isna(row[column])
        ):
            keys[column_str] = row[column]
    return keys


def _extract_image_key_fields(row: pd.Series) -> dict[str, Any]:
    """
    Extract image-level key fields to carry into source image rows.
    """

    preferred_columns = [
        "Metadata_TableNumber",
        "Metadata_ImageNumber",
        "Image_Metadata_Well",
        "Image_Metadata_Plate",
        "Metadata_Well",
        "Metadata_Plate",
    ]
    return {
        column: row[column]
        for column in preferred_columns
        if column in row.index and not pd.isna(row[column])
    }


def _extract_image_key_field_names(data: pd.DataFrame) -> list[str]:
    """
    Return the sorted image-level key field names present in a chunk.

    Computed from the full chunk so every shard -- including ones that emit no
    rows -- assembles a table with the same key columns, which is required for
    ``pa.concat_tables`` to succeed across shards.
    """

    preferred_columns = [
        "Metadata_TableNumber",
        "Metadata_ImageNumber",
        "Image_Metadata_Well",
        "Image_Metadata_Plate",
        "Metadata_Well",
        "Metadata_Plate",
    ]
    return sorted(
        column
        for column in preferred_columns
        if column in data.columns and not data[column].isna().all()
    )


def _build_stable_object_id(
    key_fields: dict[str, Any],
    bbox: Optional[dict[str, int]] = None,
) -> str:
    """
    Build a deterministic object identifier for warehouse image rows.
    """

    payload = dumps(
        {
            "keys": key_fields,
            "bbox": bbox or {},
        },
        sort_keys=True,
        default=str,
    )
    return object_id(payload)


def _build_stable_image_crop_id(
    key_fields: dict[str, Any],
    image_column: str,
    image_name: str,
    bbox: Optional[dict[str, int]] = None,
) -> str:
    """
    Build a deterministic identifier for one object/image crop row.
    """

    payload = dumps(
        {
            "keys": key_fields,
            "bbox": bbox or {},
            "source_image_column": image_column,
            "source_image_file": image_name,
        },
        sort_keys=True,
        default=str,
    )
    return object_id(payload)


def _build_stable_source_image_id(
    key_fields: dict[str, Any],
    image_column: str,
    image_name: str,
) -> str:
    """
    Build a deterministic identifier for one source image row.
    """

    payload = dumps(
        {
            "keys": key_fields,
            "source_image_column": image_column,
            "source_image_file": image_name,
        },
        sort_keys=True,
        default=str,
    )
    return object_id(payload)


def _crop_ome_arrow(
    image_path: ImagePath,
    bbox: dict[str, int],
) -> dict[str, Any]:
    """
    Lazily crop a TIFF-backed image into an OME-Arrow struct.
    """

    OMEArrow, _ = _require_ome_arrow()
    image_path = _local_image_io_path(image_path)
    crop = (
        OMEArrow.scan(str(image_path))
        .slice_lazy(
            x_min=max(0, bbox["x_min"]),
            x_max=max(0, bbox["x_max"]),
            y_min=max(0, bbox["y_min"]),
            y_max=max(0, bbox["y_max"]),
        )
        .collect()
    )
    data = crop.data
    return data.as_py() if hasattr(data, "as_py") else data


def _read_ome_arrow(
    image_path: ImagePath,
) -> dict[str, Any]:
    """
    Lazily load a full TIFF-backed image into an OME-Arrow struct.
    """

    OMEArrow, _ = _require_ome_arrow()
    image_path = _local_image_io_path(image_path)
    image = OMEArrow.scan(str(image_path)).collect()
    data = image.data
    return data.as_py() if hasattr(data, "as_py") else data


def _validated_bbox_values(
    row: pd.Series,
    bbox_columns: BBoxColumns,
) -> Optional[dict[str, int]]:
    """
    Validate and normalize row bbox values for image cropping.
    """

    numeric_bbox: dict[str, Any] = {}
    for name, column in (
        ("x_min", bbox_columns.x_min),
        ("x_max", bbox_columns.x_max),
        ("y_min", bbox_columns.y_min),
        ("y_max", bbox_columns.y_max),
    ):
        value = pd.to_numeric(row[column], errors="coerce")
        if pd.isna(value):
            return None
        numeric_bbox[name] = int(value)

    if (
        numeric_bbox["x_min"] >= numeric_bbox["x_max"]
        or numeric_bbox["y_min"] >= numeric_bbox["y_max"]
    ):
        return None

    return {
        "x_min": numeric_bbox["x_min"],
        "x_max": numeric_bbox["x_max"],
        "y_min": numeric_bbox["y_min"],
        "y_max": numeric_bbox["y_max"],
    }


# Minimum estimated crop count before the per-chunk ProcessPool path is used.
# Below this the spawn overhead dominates, so the serial path is kept (this also
# keeps the existing small-chunk tests ProcessPool-free and deterministic).
_CROP_PARALLEL_MIN = 64


def _resolve_image_worker_count(crop_workers: Optional[int]) -> int:
    """
    Resolve the number of worker processes for per-chunk image cropping.

    ``None`` selects an automatic count (capped at 8); ``0`` or ``1`` forces the
    serial path. Negative values are clamped to 1.
    """

    if crop_workers is None:
        return min(os.cpu_count() or 4, 8)
    return max(1, int(crop_workers))


def _collect_crop_rows(
    data: pd.DataFrame,
    image_columns: Sequence[str],
    bbox_columns: BBoxColumns,
    image_dir: Optional[str],
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    image_index: FileIndex,
    mask_index: FileIndex,
    outline_index: FileIndex,
    segmentation_file_regex: Optional[Dict[str, str]],
    path_kwargs: Optional[Dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Build the list of crop-row records for one joined chunk.

    Pure with respect to ``data`` and the provided indexes, so it may run inside
    a worker process.
    """

    segmentation_cache: dict[str, Optional[ImagePath]] = {}
    rows: list[dict[str, Any]] = []
    for _, row in data.iterrows():
        bbox_values = _validated_bbox_values(row, bbox_columns)
        if bbox_values is None:
            logger.debug("Skipping image crop for invalid bounding box values.")
            continue

        key_fields = _extract_key_fields(row)
        for image_column in image_columns:
            image_name = _normalize_file_value(row.get(image_column))
            if image_name is None:
                continue
            image_path = _resolve_indexed_path(image_name, image_index)
            if image_path is None:
                logger.debug("Skipping image crop for unresolved image %s", image_name)
                continue

            outline_path = _find_matching_segmentation_path(
                data_value=image_name,
                pattern_map=segmentation_file_regex,
                file_dir=outline_dir,
                candidate_path=image_path,
                file_index=outline_index,
                lookup_cache=segmentation_cache,
                path_kwargs=path_kwargs,
            )
            mask_path = _find_matching_segmentation_path(
                data_value=image_name,
                pattern_map=segmentation_file_regex,
                file_dir=mask_dir,
                candidate_path=image_path,
                file_index=mask_index,
                lookup_cache=segmentation_cache,
                path_kwargs=path_kwargs,
            )
            label_path = outline_path or mask_path

            record = {
                **key_fields,
                "Metadata_ObjectID": _build_stable_object_id(
                    key_fields=key_fields,
                    bbox=bbox_values,
                ),
                "Metadata_ImageCropID": _build_stable_image_crop_id(
                    key_fields=key_fields,
                    image_column=image_column,
                    image_name=image_name,
                    bbox=bbox_values,
                ),
                "source_image_column": image_column,
                "source_image_file": image_name,
                "source_bbox_x_min": bbox_values["x_min"],
                "source_bbox_x_max": bbox_values["x_max"],
                "source_bbox_y_min": bbox_values["y_min"],
                "source_bbox_y_max": bbox_values["y_max"],
                "ome_arrow_image": _crop_ome_arrow(
                    image_path=image_path, bbox=bbox_values
                ),
                "ome_arrow_outline": (
                    _crop_ome_arrow(image_path=outline_path, bbox=bbox_values)
                    if outline_path is not None
                    else None
                ),
                "ome_arrow_mask": (
                    _crop_ome_arrow(image_path=mask_path, bbox=bbox_values)
                    if mask_path is not None
                    else None
                ),
                "ome_arrow_label": (
                    _crop_ome_arrow(image_path=label_path, bbox=bbox_values)
                    if label_path is not None
                    else None
                ),
                "label_source_kind": (
                    "outline"
                    if outline_path is not None
                    else "mask" if mask_path is not None else None
                ),
            }
            rows.append(record)

    return rows


def _rows_to_crop_table(
    rows: list[dict[str, Any]],
    ome_arrow_struct: pa.DataType,
    key_field_names: Sequence[str],
) -> pa.Table:
    """
    Assemble crop-row records into an Arrow table.

    ``key_field_names`` is the full chunk's key field set (see
    :func:`_extract_image_key_field_names`) so empty shard outputs carry the
    same schema as non-empty siblings and concatenate cleanly.
    """

    if not rows:
        return pa.table(
            {
                **{key: pa.array([], type=pa.string()) for key in key_field_names},
                "Metadata_ObjectID": pa.array([], type=pa.string()),
                "Metadata_ImageCropID": pa.array([], type=pa.string()),
                "source_image_column": pa.array([], type=pa.string()),
                "source_image_file": pa.array([], type=pa.string()),
                "label_source_kind": pa.array([], type=pa.string()),
                "source_bbox_x_min": pa.array([], type=pa.int64()),
                "source_bbox_x_max": pa.array([], type=pa.int64()),
                "source_bbox_y_min": pa.array([], type=pa.int64()),
                "source_bbox_y_max": pa.array([], type=pa.int64()),
                "ome_arrow_image": pa.array([], type=ome_arrow_struct),
                "ome_arrow_outline": pa.array([], type=ome_arrow_struct),
                "ome_arrow_mask": pa.array([], type=ome_arrow_struct),
                "ome_arrow_label": pa.array([], type=ome_arrow_struct),
            }
        )

    key_columns = {
        key: pa.array(
            [None if row.get(key) is None else str(row.get(key)) for row in rows],
            type=pa.string(),
        )
        for key in key_field_names
    }
    fixed_columns = {
        "Metadata_ObjectID": pa.array(
            [row["Metadata_ObjectID"] for row in rows], type=pa.string()
        ),
        "Metadata_ImageCropID": pa.array(
            [row["Metadata_ImageCropID"] for row in rows], type=pa.string()
        ),
        "source_image_column": pa.array(
            [row["source_image_column"] for row in rows], type=pa.string()
        ),
        "source_image_file": pa.array(
            [row["source_image_file"] for row in rows], type=pa.string()
        ),
        "label_source_kind": pa.array(
            [row["label_source_kind"] for row in rows], type=pa.string()
        ),
        "source_bbox_x_min": pa.array(
            [row["source_bbox_x_min"] for row in rows], type=pa.int64()
        ),
        "source_bbox_x_max": pa.array(
            [row["source_bbox_x_max"] for row in rows], type=pa.int64()
        ),
        "source_bbox_y_min": pa.array(
            [row["source_bbox_y_min"] for row in rows], type=pa.int64()
        ),
        "source_bbox_y_max": pa.array(
            [row["source_bbox_y_max"] for row in rows], type=pa.int64()
        ),
        "ome_arrow_image": pa.array(
            [
                _strip_null_fields_from_value(row["ome_arrow_image"], ome_arrow_struct)
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
        "ome_arrow_outline": pa.array(
            [
                _strip_null_fields_from_value(
                    row["ome_arrow_outline"], ome_arrow_struct
                )
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
        "ome_arrow_mask": pa.array(
            [
                _strip_null_fields_from_value(row["ome_arrow_mask"], ome_arrow_struct)
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
        "ome_arrow_label": pa.array(
            [
                _strip_null_fields_from_value(row["ome_arrow_label"], ome_arrow_struct)
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
    }
    return pa.table({**key_columns, **fixed_columns})


def _crop_shard_worker(
    chunk_path: str,
    image_dir: Optional[str],
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    image_columns: Sequence[str],
    bbox_columns: BBoxColumns,
    segmentation_file_regex: Optional[Dict[str, str]],
    path_kwargs: Optional[Dict[str, Any]],
    key_field_names: Sequence[str],
) -> pa.Table:
    """
    Process one row-shard of a joined chunk in a worker process.

    Module-level so it is importable under multiprocessing spawn. Rebuilds the
    file indexes locally (a few cheap dir walks) to avoid serializing
    ``CloudPath`` objects across the process boundary. ``key_field_names`` is
    the full chunk's key field set, passed in so empty shards still emit the
    complete schema.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    ome_arrow_struct = _strip_null_fields_from_type(ome_arrow_struct)
    data = parquet.read_table(chunk_path).to_pandas()
    image_index = _build_file_index(image_dir, path_kwargs=path_kwargs)
    mask_index = _build_file_index(mask_dir, path_kwargs=path_kwargs)
    outline_index = _build_file_index(outline_dir, path_kwargs=path_kwargs)
    rows = _collect_crop_rows(
        data=data,
        image_columns=image_columns,
        bbox_columns=bbox_columns,
        image_dir=image_dir,
        mask_dir=mask_dir,
        outline_dir=outline_dir,
        image_index=image_index,
        mask_index=mask_index,
        outline_index=outline_index,
        segmentation_file_regex=segmentation_file_regex,
        path_kwargs=path_kwargs,
    )
    return _rows_to_crop_table(rows, ome_arrow_struct, key_field_names)


def _write_row_shards(table: pa.Table, n_shards: int, tmpdir: str) -> list[str]:
    """
    Split an Arrow table into ``n_shards`` contiguous row-shard parquet files.

    Shard boundaries are contiguous and ordered so concatenating the per-shard
    results reproduces the serial row order.
    """

    n = table.num_rows
    shard_size = max(1, n // n_shards)
    shard_paths: list[str] = []
    for i in range(n_shards):
        start = i * shard_size
        end = min((i + 1) * shard_size, n) if i < n_shards - 1 else n
        if start >= end:
            break
        shard_path = os.path.join(tmpdir, f"shard_{i}.parquet")
        parquet.write_table(table.slice(start, end - start), shard_path)
        shard_paths.append(shard_path)
    return shard_paths


def _image_crop_table_parallel(
    table: pa.Table,
    image_dir: Optional[str],
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    image_columns: Sequence[str],
    bbox_columns: BBoxColumns,
    segmentation_file_regex: Optional[Dict[str, str]],
    path_kwargs: Optional[Dict[str, Any]],
    workers: int,
    key_field_names: Sequence[str],
) -> pa.Table:
    """
    Run the per-row crop work across ``workers`` processes and merge results.
    """

    with tempfile.TemporaryDirectory(prefix="cytotable_crop_shards_") as tmpdir:
        shard_paths = _write_row_shards(table, workers, tmpdir)
        worker = partial(
            _crop_shard_worker,
            image_dir=image_dir,
            mask_dir=mask_dir,
            outline_dir=outline_dir,
            image_columns=list(image_columns),
            bbox_columns=bbox_columns,
            segmentation_file_regex=segmentation_file_regex,
            path_kwargs=path_kwargs,
            key_field_names=list(key_field_names),
        )
        with ProcessPoolExecutor(
            max_workers=min(workers, len(shard_paths)),
            mp_context=multiprocessing.get_context("spawn"),
        ) as ex:
            shard_tables = list(ex.map(worker, shard_paths))
    return pa.concat_tables(shard_tables)


def image_crop_table_from_joined_chunk(
    chunk_path: str,
    image_dir: str,
    mask_dir: Optional[str] = None,
    outline_dir: Optional[str] = None,
    bbox_column_map: Optional[Dict[str, str]] = None,
    segmentation_file_regex: Optional[Dict[str, str]] = None,
    path_kwargs: Optional[Dict[str, Any]] = None,
    crop_workers: Optional[int] = None,
) -> pa.Table:
    """
    Build an Arrow table of OME-Arrow image crops from one joined parquet chunk.

    ``crop_workers`` controls per-chunk parallelism over the per-row crop loop:
    ``None`` selects an automatic count (capped at 8), while ``0``/``1`` keeps
    the serial path. Parallelism uses a process pool because the crop work
    (``slice_ome_arrow`` over a pyarrow struct) holds the GIL, so threads do not
    help. A threshold (``_CROP_PARALLEL_MIN``) keeps small chunks serial so the
    process-spawn overhead never dominates.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    ome_arrow_struct = _strip_null_fields_from_type(ome_arrow_struct)
    table = parquet.read_table(chunk_path)
    data = table.to_pandas()
    image_columns = _resolve_image_columns(data)
    bbox_columns = resolve_bbox_columns(
        data.columns.tolist(), bbox_column_map=bbox_column_map
    )

    if bbox_columns is None:
        raise ValueError(
            "Unable to identify bounding box coordinate columns for image export."
        )

    workers = _resolve_image_worker_count(crop_workers)
    estimated_crops = len(data) * max(1, len(image_columns))
    key_field_names = _extract_image_key_field_names(data)
    if workers > 1 and estimated_crops >= _CROP_PARALLEL_MIN:
        return _image_crop_table_parallel(
            table=table,
            image_dir=image_dir,
            mask_dir=mask_dir,
            outline_dir=outline_dir,
            image_columns=image_columns,
            bbox_columns=bbox_columns,
            segmentation_file_regex=segmentation_file_regex,
            path_kwargs=path_kwargs,
            workers=workers,
            key_field_names=key_field_names,
        )

    image_index = _build_file_index(image_dir, path_kwargs=path_kwargs)
    mask_index = _build_file_index(mask_dir, path_kwargs=path_kwargs)
    outline_index = _build_file_index(outline_dir, path_kwargs=path_kwargs)
    rows = _collect_crop_rows(
        data=data,
        image_columns=image_columns,
        bbox_columns=bbox_columns,
        image_dir=image_dir,
        mask_dir=mask_dir,
        outline_dir=outline_dir,
        image_index=image_index,
        mask_index=mask_index,
        outline_index=outline_index,
        segmentation_file_regex=segmentation_file_regex,
        path_kwargs=path_kwargs,
    )
    return _rows_to_crop_table(rows, ome_arrow_struct, key_field_names)


# Minimum number of *unique* source images before the source-image ProcessPool
# path is used. Source-image work is dominated by one full read per unique image
# (rows are deduplicated by Metadata_ImageID), so the threshold is on unique
# images rather than row count.
_SOURCE_PARALLEL_MIN = 16


def _dedup_table_by_first_occurrence(table: pa.Table, key_column: str) -> pa.Table:
    """
    Drop rows with a repeated ``key_column`` value, keeping the first occurrence.

    Preserves row order and the table schema (including nested struct columns).
    """

    if table.num_rows == 0:
        return table
    keys = table.column(key_column).to_pylist()
    seen: set[Any] = set()
    indices: list[int] = []
    for i, key in enumerate(keys):
        if key is not None:
            if key in seen:
                continue
            seen.add(key)
        indices.append(i)
    if len(indices) == table.num_rows:
        return table
    return table.take(indices)


def _collect_source_rows(
    data: pd.DataFrame,
    image_columns: Sequence[str],
    image_dir: Optional[str],
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    image_index: FileIndex,
    mask_index: FileIndex,
    outline_index: FileIndex,
    segmentation_file_regex: Optional[Dict[str, str]],
    path_kwargs: Optional[Dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Build the deduplicated list of source-image rows for one joined chunk.

    Rows are deduplicated by ``Metadata_ImageID`` (first occurrence wins), so a
    shard returns at most one row per source image it touches. Pure with respect
    to ``data`` and the provided indexes, so it may run in a worker process.
    """

    segmentation_cache: dict[str, Optional[ImagePath]] = {}
    rows_by_id: dict[str, dict[str, Any]] = {}

    for _, row in data.iterrows():
        key_fields = _extract_image_key_fields(row)
        for image_column in image_columns:
            image_name = _normalize_file_value(row.get(image_column))
            if image_name is None:
                continue
            image_path = _resolve_indexed_path(image_name, image_index)
            if image_path is None:
                logger.debug(
                    "Skipping source image export for unresolved image %s", image_name
                )
                continue

            source_image_id = _build_stable_source_image_id(
                key_fields=key_fields,
                image_column=image_column,
                image_name=image_name,
            )
            if source_image_id in rows_by_id:
                continue

            outline_path = _find_matching_segmentation_path(
                data_value=image_name,
                pattern_map=segmentation_file_regex,
                file_dir=outline_dir,
                candidate_path=image_path,
                file_index=outline_index,
                lookup_cache=segmentation_cache,
                path_kwargs=path_kwargs,
            )
            mask_path = _find_matching_segmentation_path(
                data_value=image_name,
                pattern_map=segmentation_file_regex,
                file_dir=mask_dir,
                candidate_path=image_path,
                file_index=mask_index,
                lookup_cache=segmentation_cache,
                path_kwargs=path_kwargs,
            )
            label_path = outline_path or mask_path

            rows_by_id[source_image_id] = {
                **key_fields,
                "Metadata_ImageID": source_image_id,
                "source_image_column": image_column,
                "source_image_file": image_name,
                "ome_arrow_image": _read_ome_arrow(image_path),
                "ome_arrow_outline": (
                    _read_ome_arrow(outline_path) if outline_path is not None else None
                ),
                "ome_arrow_mask": (
                    _read_ome_arrow(mask_path) if mask_path is not None else None
                ),
                "ome_arrow_label": (
                    _read_ome_arrow(label_path) if label_path is not None else None
                ),
                "label_source_kind": (
                    "outline"
                    if outline_path is not None
                    else "mask" if mask_path is not None else None
                ),
            }

    return list(rows_by_id.values())


def _rows_to_source_table(
    rows: list[dict[str, Any]],
    ome_arrow_struct: pa.DataType,
    key_field_names: Sequence[str],
) -> pa.Table:
    """
    Assemble source-image rows into an Arrow table.

    ``key_field_names`` is the full chunk's key field set (see
    :func:`_extract_image_key_field_names`) so empty shard outputs carry the
    same schema as non-empty siblings and concatenate cleanly.
    """

    if not rows:
        return pa.table(
            {
                **{key: pa.array([], type=pa.string()) for key in key_field_names},
                "Metadata_ImageID": pa.array([], type=pa.string()),
                "source_image_column": pa.array([], type=pa.string()),
                "source_image_file": pa.array([], type=pa.string()),
                "label_source_kind": pa.array([], type=pa.string()),
                "ome_arrow_image": pa.array([], type=ome_arrow_struct),
                "ome_arrow_outline": pa.array([], type=ome_arrow_struct),
                "ome_arrow_mask": pa.array([], type=ome_arrow_struct),
                "ome_arrow_label": pa.array([], type=ome_arrow_struct),
            }
        )

    key_columns = {
        key: pa.array(
            [None if row.get(key) is None else str(row.get(key)) for row in rows],
            type=pa.string(),
        )
        for key in key_field_names
    }
    fixed_columns = {
        "Metadata_ImageID": pa.array(
            [row["Metadata_ImageID"] for row in rows], type=pa.string()
        ),
        "source_image_column": pa.array(
            [row["source_image_column"] for row in rows], type=pa.string()
        ),
        "source_image_file": pa.array(
            [row["source_image_file"] for row in rows], type=pa.string()
        ),
        "label_source_kind": pa.array(
            [row["label_source_kind"] for row in rows], type=pa.string()
        ),
        "ome_arrow_image": pa.array(
            [
                _strip_null_fields_from_value(row["ome_arrow_image"], ome_arrow_struct)
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
        "ome_arrow_outline": pa.array(
            [
                _strip_null_fields_from_value(
                    row["ome_arrow_outline"], ome_arrow_struct
                )
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
        "ome_arrow_mask": pa.array(
            [
                _strip_null_fields_from_value(row["ome_arrow_mask"], ome_arrow_struct)
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
        "ome_arrow_label": pa.array(
            [
                _strip_null_fields_from_value(row["ome_arrow_label"], ome_arrow_struct)
                for row in rows
            ],
            type=ome_arrow_struct,
        ),
    }
    return pa.table({**key_columns, **fixed_columns})


def _source_shard_worker(
    chunk_path: str,
    image_dir: Optional[str],
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    image_columns: Sequence[str],
    segmentation_file_regex: Optional[Dict[str, str]],
    path_kwargs: Optional[Dict[str, Any]],
    key_field_names: Sequence[str],
) -> pa.Table:
    """
    Process one row-shard of a joined chunk for source-image export.

    Module-level so it is importable under multiprocessing spawn. Rebuilds the
    file indexes locally to avoid serializing ``CloudPath`` across the process
    boundary. ``key_field_names`` is the full chunk's key field set, passed in
    so empty shards still emit the complete schema.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    ome_arrow_struct = _strip_null_fields_from_type(ome_arrow_struct)
    data = parquet.read_table(chunk_path).to_pandas()
    image_index = _build_file_index(image_dir, path_kwargs=path_kwargs)
    mask_index = _build_file_index(mask_dir, path_kwargs=path_kwargs)
    outline_index = _build_file_index(outline_dir, path_kwargs=path_kwargs)
    rows = _collect_source_rows(
        data=data,
        image_columns=image_columns,
        image_dir=image_dir,
        mask_dir=mask_dir,
        outline_dir=outline_dir,
        image_index=image_index,
        mask_index=mask_index,
        outline_index=outline_index,
        segmentation_file_regex=segmentation_file_regex,
        path_kwargs=path_kwargs,
    )
    return _rows_to_source_table(rows, ome_arrow_struct, key_field_names)


def _source_image_table_parallel(
    table: pa.Table,
    image_dir: Optional[str],
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    image_columns: Sequence[str],
    segmentation_file_regex: Optional[Dict[str, str]],
    path_kwargs: Optional[Dict[str, Any]],
    workers: int,
    key_field_names: Sequence[str],
) -> pa.Table:
    """
    Run source-image export across ``workers`` processes and merge results.

    Each shard deduplicates internally by ``Metadata_ImageID``; the merged table
    is deduplicated again across shards (first occurrence wins) so an image that
    appears in more than one shard is exported exactly once. ``key_field_names``
    is the full chunk's key field set, passed to shards so empty shards still
    emit the complete schema.
    """

    with tempfile.TemporaryDirectory(prefix="cytotable_source_shards_") as tmpdir:
        shard_paths = _write_row_shards(table, workers, tmpdir)
        worker = partial(
            _source_shard_worker,
            image_dir=image_dir,
            mask_dir=mask_dir,
            outline_dir=outline_dir,
            image_columns=list(image_columns),
            segmentation_file_regex=segmentation_file_regex,
            path_kwargs=path_kwargs,
            key_field_names=list(key_field_names),
        )
        with ProcessPoolExecutor(
            max_workers=min(workers, len(shard_paths)),
            mp_context=multiprocessing.get_context("spawn"),
        ) as ex:
            shard_tables = list(ex.map(worker, shard_paths))
    merged = pa.concat_tables(shard_tables)
    return _dedup_table_by_first_occurrence(merged, "Metadata_ImageID")


def _count_unique_source_image_names(
    data: pd.DataFrame, image_columns: Sequence[str]
) -> int:
    """
    Estimate the number of distinct source images referenced by a chunk.
    """

    names: set[str] = set()
    for column in image_columns:
        column_values = data[column].dropna().astype(str) if column in data else None
        if column_values is not None and not column_values.empty:
            names.update(column_values.unique().tolist())
    return len(names)


def source_image_table_from_joined_chunk(
    chunk_path: str,
    image_dir: str,
    mask_dir: Optional[str] = None,
    outline_dir: Optional[str] = None,
    segmentation_file_regex: Optional[Dict[str, str]] = None,
    path_kwargs: Optional[Dict[str, Any]] = None,
    crop_workers: Optional[int] = None,
) -> pa.Table:
    """
    Build an Arrow table of full OME-Arrow source images from one joined chunk.

    ``crop_workers`` controls per-chunk parallelism over the per-image reads,
    mirroring ``image_crop_table_from_joined_chunk``: ``None`` selects an
    automatic count (capped at 8), ``0``/``1`` keeps the serial path. Because
    source-image rows are deduplicated by ``Metadata_ImageID``, parallelism is
    only used when the chunk references at least ``_SOURCE_PARALLEL_MIN``
    distinct images.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    ome_arrow_struct = _strip_null_fields_from_type(ome_arrow_struct)
    table = parquet.read_table(chunk_path)
    data = table.to_pandas()
    image_columns = _resolve_image_columns(data)

    workers = _resolve_image_worker_count(crop_workers)
    unique_image_count = _count_unique_source_image_names(data, image_columns)
    key_field_names = _extract_image_key_field_names(data)
    if workers > 1 and unique_image_count >= _SOURCE_PARALLEL_MIN:
        return _source_image_table_parallel(
            table=table,
            image_dir=image_dir,
            mask_dir=mask_dir,
            outline_dir=outline_dir,
            image_columns=image_columns,
            segmentation_file_regex=segmentation_file_regex,
            path_kwargs=path_kwargs,
            workers=workers,
            key_field_names=key_field_names,
        )

    image_index = _build_file_index(image_dir, path_kwargs=path_kwargs)
    mask_index = _build_file_index(mask_dir, path_kwargs=path_kwargs)
    outline_index = _build_file_index(outline_dir, path_kwargs=path_kwargs)
    rows = _collect_source_rows(
        data=data,
        image_columns=image_columns,
        image_dir=image_dir,
        mask_dir=mask_dir,
        outline_dir=outline_dir,
        image_index=image_index,
        mask_index=mask_index,
        outline_index=outline_index,
        segmentation_file_regex=segmentation_file_regex,
        path_kwargs=path_kwargs,
    )
    return _rows_to_source_table(rows, ome_arrow_struct, key_field_names)


def add_object_id_to_profiles_frame(
    joined_frame: pd.DataFrame,
    bbox_column_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Add a stable object identifier column to a joined profiles frame.
    """

    bbox_columns = resolve_bbox_columns(
        joined_frame.columns.tolist(), bbox_column_map=bbox_column_map
    )
    frame = joined_frame.copy()

    def _generate_id(row: pd.Series) -> str:
        return _build_stable_object_id(
            key_fields=_extract_key_fields(row),
            bbox=(
                _validated_bbox_values(row, bbox_columns)
                if bbox_columns is not None
                else None
            ),
        )

    if "Metadata_ObjectID" not in frame.columns:
        object_ids = frame.apply(_generate_id, axis=1).tolist()
        metadata_columns = [
            column
            for column in frame.columns
            if str(column).lower().startswith("metadata_")
        ]
        insert_at = len(metadata_columns)
        frame.insert(insert_at, "Metadata_ObjectID", object_ids)
    else:
        null_mask = frame["Metadata_ObjectID"].isna()
        if null_mask.any():
            frame.loc[null_mask, "Metadata_ObjectID"] = frame.loc[null_mask].apply(
                _generate_id, axis=1
            )

    if bbox_columns is not None:
        rename_map = {
            getattr(bbox_columns, axis): alias
            for axis, alias in PROFILE_BBOX_METADATA_COLUMNS.items()
            if getattr(bbox_columns, axis) != alias and alias not in frame.columns
        }
        if rename_map:
            frame = frame.rename(columns=rename_map)

    return frame


def profile_with_images_frame(
    joined_frame: pd.DataFrame,
    image_frame: pd.DataFrame,
    bbox_column_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Expand joined measurement rows into stable object/image references and merge crops.
    """

    image_columns = _resolve_image_columns(joined_frame)
    bbox_columns = resolve_bbox_columns(
        joined_frame.columns.tolist(), bbox_column_map=bbox_column_map
    )
    if bbox_columns is None or not image_columns:
        return joined_frame.copy()

    # Vectorized bbox filter — coerce all four coordinates at once and keep
    # only rows where both axes have a positive non-null span.
    x_min = pd.to_numeric(joined_frame[bbox_columns.x_min], errors="coerce")
    x_max = pd.to_numeric(joined_frame[bbox_columns.x_max], errors="coerce")
    y_min = pd.to_numeric(joined_frame[bbox_columns.y_min], errors="coerce")
    y_max = pd.to_numeric(joined_frame[bbox_columns.y_max], errors="coerce")
    valid_bbox = (
        x_min.notna()
        & x_max.notna()
        & y_min.notna()
        & y_max.notna()
        & (x_min < x_max)
        & (y_min < y_max)
    )
    valid = joined_frame[valid_bbox].copy()
    if valid.empty:
        return joined_frame.copy()

    # Stamp object IDs with apply — still per-row but avoids constructing a
    # growing Python list and is tighter in the CPython call overhead.
    def _generate_valid_id(row: pd.Series) -> str:
        return _build_stable_object_id(
            key_fields=_extract_key_fields(row),
            bbox=_validated_bbox_values(row, bbox_columns),
        )

    if "Metadata_ObjectID" not in valid.columns:
        valid["Metadata_ObjectID"] = valid.apply(_generate_valid_id, axis=1)
    else:
        null_mask = valid["Metadata_ObjectID"].isna()
        if null_mask.any():
            valid.loc[null_mask, "Metadata_ObjectID"] = valid.loc[null_mask].apply(
                _generate_valid_id, axis=1
            )

    # Expand one row per image column with melt instead of accumulating a
    # dict-per-row list — pandas handles the reshape in C without building
    # intermediate Python objects for every cell.
    non_image_cols = [c for c in valid.columns if c not in image_columns]
    melted = valid.melt(
        id_vars=non_image_cols,
        value_vars=image_columns,
        var_name="source_image_column",
        value_name="source_image_file",
    )
    melted["source_image_file"] = melted["source_image_file"].apply(
        _normalize_file_value
    )
    melted = melted[melted["source_image_file"].notna()].reset_index(drop=True)
    if melted.empty:
        return joined_frame.copy()

    merge_columns = [
        column
        for column in (
            "Metadata_ObjectID",
            "source_image_column",
            "source_image_file",
        )
        if column in image_frame.columns
    ]
    image_columns_to_add = [
        column
        for column in image_frame.columns
        if column not in joined_frame.columns or column in merge_columns
    ]
    return melted.merge(
        image_frame[image_columns_to_add],
        on=merge_columns,
        how="left",
        suffixes=("", "_image"),
    )

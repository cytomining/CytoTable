"""
Helpers for exporting image crops alongside CytoTable measurement data.
"""

from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass
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


def image_crop_table_from_joined_chunk(
    chunk_path: str,
    image_dir: str,
    mask_dir: Optional[str] = None,
    outline_dir: Optional[str] = None,
    bbox_column_map: Optional[Dict[str, str]] = None,
    segmentation_file_regex: Optional[Dict[str, str]] = None,
    path_kwargs: Optional[Dict[str, Any]] = None,
) -> pa.Table:
    """
    Build an Arrow table of OME-Arrow image crops from one joined parquet chunk.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    ome_arrow_struct = _strip_null_fields_from_type(ome_arrow_struct)
    data = parquet.read_table(chunk_path).to_pandas()
    image_columns = _resolve_image_columns(data)
    bbox_columns = resolve_bbox_columns(
        data.columns.tolist(), bbox_column_map=bbox_column_map
    )

    if bbox_columns is None:
        raise ValueError(
            "Unable to identify bounding box coordinate columns for image export."
        )

    image_index = _build_file_index(image_dir, path_kwargs=path_kwargs)
    mask_index = _build_file_index(mask_dir, path_kwargs=path_kwargs)
    outline_index = _build_file_index(outline_dir, path_kwargs=path_kwargs)
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

    key_field_names = sorted(
        {key for row in rows for key in row.keys()}
        - {
            "source_image_column",
            "source_image_file",
            "label_source_kind",
            "Metadata_ObjectID",
            "Metadata_ImageCropID",
            "source_bbox_x_min",
            "source_bbox_x_max",
            "source_bbox_y_min",
            "source_bbox_y_max",
            "ome_arrow_image",
            "ome_arrow_outline",
            "ome_arrow_mask",
            "ome_arrow_label",
        }
    )

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


def source_image_table_from_joined_chunk(
    chunk_path: str,
    image_dir: str,
    mask_dir: Optional[str] = None,
    outline_dir: Optional[str] = None,
    segmentation_file_regex: Optional[Dict[str, str]] = None,
    path_kwargs: Optional[Dict[str, Any]] = None,
) -> pa.Table:
    """
    Build an Arrow table of full OME-Arrow source images from one joined chunk.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    ome_arrow_struct = _strip_null_fields_from_type(ome_arrow_struct)
    data = parquet.read_table(chunk_path).to_pandas()
    image_columns = _resolve_image_columns(data)

    image_index = _build_file_index(image_dir, path_kwargs=path_kwargs)
    mask_index = _build_file_index(mask_dir, path_kwargs=path_kwargs)
    outline_index = _build_file_index(outline_dir, path_kwargs=path_kwargs)
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

    rows = list(rows_by_id.values())
    key_field_names = sorted(
        {key for row in rows for key in row.keys()}
        - {
            "Metadata_ImageID",
            "source_image_column",
            "source_image_file",
            "label_source_kind",
            "ome_arrow_image",
            "ome_arrow_outline",
            "ome_arrow_mask",
            "ome_arrow_label",
        }
    )

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
    if "Metadata_ObjectID" not in frame.columns:
        object_ids: list[Optional[str]] = []
        for _, row in joined_frame.iterrows():
            key_fields = _extract_key_fields(row)
            bbox_values = (
                _validated_bbox_values(row, bbox_columns)
                if bbox_columns is not None
                else None
            )
            object_ids.append(
                _build_stable_object_id(key_fields=key_fields, bbox=bbox_values)
            )

        metadata_columns = [
            column
            for column in frame.columns
            if str(column).lower().startswith("metadata_")
        ]
        insert_at = len(metadata_columns)
        frame.insert(insert_at, "Metadata_ObjectID", object_ids)

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

    expanded_rows: list[dict[str, Any]] = []
    for _, row in joined_frame.iterrows():
        bbox = _validated_bbox_values(row, bbox_columns)
        if bbox is None:
            continue
        key_fields = _extract_key_fields(row)
        row_dict = row.to_dict()
        stable_object_id = (
            str(row["Metadata_ObjectID"])
            if "Metadata_ObjectID" in row.index
            and not pd.isna(row["Metadata_ObjectID"])
            else _build_stable_object_id(key_fields=key_fields, bbox=bbox)
        )
        for image_column in image_columns:
            image_name = _normalize_file_value(row.get(image_column))
            if image_name is None:
                continue
            expanded_rows.append(
                {
                    **row_dict,
                    "Metadata_ObjectID": stable_object_id,
                    "source_image_column": image_column,
                    "source_image_file": image_name,
                }
            )

    if not expanded_rows:
        return joined_frame.copy()

    expanded = pd.DataFrame(expanded_rows)
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
    return expanded.merge(
        image_frame[image_columns_to_add],
        on=merge_columns,
        how="left",
        suffixes=("", "_image"),
    )

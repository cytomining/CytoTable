"""
Helpers for exporting image crops alongside CytoTable measurement data.
"""

from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass
from json import dumps
from typing import Any, Dict, Optional, Sequence
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet

logger = logging.getLogger(__name__)

IMAGE_TABLE_NAME = "image_crops"
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


def _normalize_file_value(value: Any) -> Optional[str]:
    """
    Normalize a file-like value to a comparable basename.
    """

    if value is None or pd.isna(value):
        return None

    normalized = str(value)
    if normalized.startswith("file:"):
        normalized = normalized[len("file:") :]
    return pathlib.Path(normalized).name


def _build_file_index(file_dir: Optional[str]) -> dict[str, pathlib.Path]:
    """
    Build a basename and stem index for image-like files in a directory tree.
    """

    if file_dir is None:
        return {}

    root = pathlib.Path(file_dir)
    if not root.exists():
        return {}

    basename_index: dict[str, pathlib.Path] = {}
    stem_candidates: dict[str, list[pathlib.Path]] = {}
    for path in root.rglob("*"):
        lowered = path.name.lower()
        is_image_path = path.is_file() or (
            path.is_dir() and lowered.endswith((".zarr", ".ome.zarr"))
        )
        if not is_image_path:
            continue
        if not lowered.endswith(_IMAGE_SUFFIXES):
            continue
        basename_index[path.name] = path
        stem_candidates.setdefault(path.stem, []).append(path)

    for stem, paths in stem_candidates.items():
        if len(paths) == 1 and stem not in basename_index:
            basename_index[stem] = paths[0]

    return basename_index


def _find_matching_segmentation_path(
    data_value: str,
    pattern_map: Optional[dict[str, str]],
    file_dir: Optional[str],
    candidate_path: pathlib.Path,
    file_index: Optional[dict[str, pathlib.Path]] = None,
    lookup_cache: Optional[dict[str, Optional[pathlib.Path]]] = None,
) -> Optional[pathlib.Path]:
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

    root = pathlib.Path(file_dir)
    if not root.exists():
        return None

    indexed_files = (
        file_index if file_index is not None else _build_file_index(file_dir)
    )

    if pattern_map is None:
        result = indexed_files.get(
            pathlib.Path(candidate_path).name
        ) or indexed_files.get(pathlib.Path(candidate_path).stem)
        if lookup_cache is not None and cache_key is not None:
            lookup_cache[cache_key] = result
        return result

    indexed_paths = sorted(
        {path.resolve(): path for path in indexed_files.values()}.values(),
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
                pathlib.Path(data_value).stem,
                pathlib.Path(candidate_path).stem,
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


def _build_stable_object_id(
    key_fields: dict[str, Any],
    image_column: str,
    image_name: str,
    bbox: dict[str, int],
) -> str:
    """
    Build a deterministic object identifier for warehouse image rows.
    """

    payload = dumps(
        {
            "keys": key_fields,
            "source_image_column": image_column,
            "source_image_file": image_name,
            "bbox": bbox,
        },
        sort_keys=True,
        default=str,
    )
    return object_id(payload)


def _crop_ome_arrow(
    image_path: pathlib.Path,
    bbox: BBoxColumns,
    row: pd.Series,
) -> dict[str, Any]:
    """
    Lazily crop a TIFF-backed image into an OME-Arrow struct.
    """

    OMEArrow, _ = _require_ome_arrow()
    crop = (
        OMEArrow.scan(str(image_path))
        .slice_lazy(
            x_min=max(0, int(row[bbox.x_min])),
            x_max=max(0, int(row[bbox.x_max])),
            y_min=max(0, int(row[bbox.y_min])),
            y_max=max(0, int(row[bbox.y_max])),
        )
        .collect()
    )
    data = crop.data
    return data.as_py() if hasattr(data, "as_py") else data


def image_crop_table_from_joined_chunk(
    chunk_path: str,
    image_dir: str,
    mask_dir: Optional[str] = None,
    outline_dir: Optional[str] = None,
    bbox_column_map: Optional[Dict[str, str]] = None,
    segmentation_file_regex: Optional[Dict[str, str]] = None,
) -> pa.Table:
    """
    Build an Arrow table of OME-Arrow image crops from one joined parquet chunk.
    """

    _, ome_arrow_struct = _require_ome_arrow()
    data = parquet.read_table(chunk_path).to_pandas()
    image_columns = _resolve_image_columns(data)
    bbox_columns = resolve_bbox_columns(
        data.columns.tolist(), bbox_column_map=bbox_column_map
    )

    if bbox_columns is None:
        raise ValueError(
            "Unable to identify bounding box coordinate columns for image export."
        )

    image_index = _build_file_index(image_dir)
    mask_index = _build_file_index(mask_dir)
    outline_index = _build_file_index(outline_dir)
    segmentation_cache: dict[str, Optional[pathlib.Path]] = {}

    rows: list[dict[str, Any]] = []
    for _, row in data.iterrows():
        if any(
            pd.isna(row[column])
            for column in (
                bbox_columns.x_min,
                bbox_columns.x_max,
                bbox_columns.y_min,
                bbox_columns.y_max,
            )
        ):
            continue

        key_fields = _extract_key_fields(row)
        for image_column in image_columns:
            image_name = _normalize_file_value(row.get(image_column))
            if image_name is None:
                continue
            image_path = image_index.get(image_name)
            if image_path is None:
                logger.debug("Skipping image crop for unresolved image %s", image_name)
                continue

            label_path = _find_matching_segmentation_path(
                data_value=image_name,
                pattern_map=segmentation_file_regex,
                file_dir=outline_dir,
                candidate_path=image_path,
                file_index=outline_index,
                lookup_cache=segmentation_cache,
            ) or _find_matching_segmentation_path(
                data_value=image_name,
                pattern_map=segmentation_file_regex,
                file_dir=mask_dir,
                candidate_path=image_path,
                file_index=mask_index,
                lookup_cache=segmentation_cache,
            )

            record = {
                **key_fields,
                "object_id": _build_stable_object_id(
                    key_fields=key_fields,
                    image_column=image_column,
                    image_name=image_name,
                    bbox={
                        "x_min": int(row[bbox_columns.x_min]),
                        "x_max": int(row[bbox_columns.x_max]),
                        "y_min": int(row[bbox_columns.y_min]),
                        "y_max": int(row[bbox_columns.y_max]),
                    },
                ),
                "source_image_column": image_column,
                "source_image_file": image_name,
                "bbox_x_min": int(row[bbox_columns.x_min]),
                "bbox_x_max": int(row[bbox_columns.x_max]),
                "bbox_y_min": int(row[bbox_columns.y_min]),
                "bbox_y_max": int(row[bbox_columns.y_max]),
                "ome_image": _crop_ome_arrow(
                    image_path=image_path, bbox=bbox_columns, row=row
                ),
                "ome_label": (
                    _crop_ome_arrow(image_path=label_path, bbox=bbox_columns, row=row)
                    if label_path is not None
                    else None
                ),
                "label_source_kind": (
                    "outline"
                    if (
                        label_path is not None
                        and outline_dir is not None
                        and pathlib.Path(outline_dir)
                        in pathlib.Path(label_path).parents
                    )
                    else "mask" if label_path is not None else None
                ),
            }
            rows.append(record)

    key_field_names = sorted(
        {key for row in rows for key in row.keys()}
        - {
            "source_image_column",
            "source_image_file",
            "label_source_kind",
            "object_id",
            "bbox_x_min",
            "bbox_x_max",
            "bbox_y_min",
            "bbox_y_max",
            "ome_image",
            "ome_label",
        }
    )

    if not rows:
        return pa.table(
            {
                **{key: pa.array([], type=pa.string()) for key in key_field_names},
                "object_id": pa.array([], type=pa.string()),
                "source_image_column": pa.array([], type=pa.string()),
                "source_image_file": pa.array([], type=pa.string()),
                "label_source_kind": pa.array([], type=pa.string()),
                "bbox_x_min": pa.array([], type=pa.int64()),
                "bbox_x_max": pa.array([], type=pa.int64()),
                "bbox_y_min": pa.array([], type=pa.int64()),
                "bbox_y_max": pa.array([], type=pa.int64()),
                "ome_image": pa.array([], type=ome_arrow_struct),
                "ome_label": pa.array([], type=ome_arrow_struct),
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
        "object_id": pa.array([row["object_id"] for row in rows], type=pa.string()),
        "source_image_column": pa.array(
            [row["source_image_column"] for row in rows], type=pa.string()
        ),
        "source_image_file": pa.array(
            [row["source_image_file"] for row in rows], type=pa.string()
        ),
        "label_source_kind": pa.array(
            [row["label_source_kind"] for row in rows], type=pa.string()
        ),
        "bbox_x_min": pa.array([row["bbox_x_min"] for row in rows], type=pa.int64()),
        "bbox_x_max": pa.array([row["bbox_x_max"] for row in rows], type=pa.int64()),
        "bbox_y_min": pa.array([row["bbox_y_min"] for row in rows], type=pa.int64()),
        "bbox_y_max": pa.array([row["bbox_y_max"] for row in rows], type=pa.int64()),
        "ome_image": pa.array(
            [row["ome_image"] for row in rows], type=ome_arrow_struct
        ),
        "ome_label": pa.array(
            [row["ome_label"] for row in rows], type=ome_arrow_struct
        ),
    }
    return pa.table({**key_columns, **fixed_columns})


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
        if any(
            pd.isna(row[column])
            for column in (
                bbox_columns.x_min,
                bbox_columns.x_max,
                bbox_columns.y_min,
                bbox_columns.y_max,
            )
        ):
            continue
        key_fields = _extract_key_fields(row)
        bbox = {
            "x_min": int(row[bbox_columns.x_min]),
            "x_max": int(row[bbox_columns.x_max]),
            "y_min": int(row[bbox_columns.y_min]),
            "y_max": int(row[bbox_columns.y_max]),
        }
        row_dict = row.to_dict()
        for image_column in image_columns:
            image_name = _normalize_file_value(row.get(image_column))
            if image_name is None:
                continue
            expanded_rows.append(
                {
                    **row_dict,
                    "object_id": _build_stable_object_id(
                        key_fields=key_fields,
                        image_column=image_column,
                        image_name=image_name,
                        bbox=bbox,
                    ),
                    "source_image_column": image_column,
                    "source_image_file": image_name,
                }
            )

    if not expanded_rows:
        return joined_frame.copy()

    expanded = pd.DataFrame(expanded_rows)
    merge_columns = [
        column
        for column in ("object_id", "source_image_column", "source_image_file")
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

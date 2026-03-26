"""
Validation helpers for CytoTable public APIs.
"""

from typing import Dict, Literal, Optional

from cytotable.exceptions import CytoTableException, DatatypeException


def validate_convert_backend_options(
    *,
    dest_backend: Literal["parquet", "iceberg"],
    dest_datatype: Literal["parquet", "anndata_h5ad", "anndata_zarr"],
    image_dir: Optional[str],
    include_source_images: bool,
    mask_dir: Optional[str],
    outline_dir: Optional[str],
    bbox_column_map: Optional[Dict[str, str]],
    segmentation_file_regex: Optional[Dict[str, str]],
    concat: bool,
    join: bool,
    drop_null: bool,
) -> None:
    """
    Validate backend-specific convert() options before dispatch.
    """

    if dest_backend not in ["parquet", "iceberg"]:
        raise DatatypeException(
            f"Invalid dest_backend provided: {dest_backend}. "
            "Valid options are 'parquet' or 'iceberg'."
        )

    image_export_requested = any(
        (
            image_dir is not None,
            include_source_images,
            mask_dir is not None,
            outline_dir is not None,
            bool(bbox_column_map),
            bool(segmentation_file_regex),
        )
    )
    if image_export_requested and image_dir is None:
        raise CytoTableException(
            "Image export options require image_dir to be provided."
        )
    if image_export_requested and dest_backend != "iceberg":
        raise CytoTableException("Image export requires dest_backend='iceberg'.")
    if image_export_requested and not join:
        raise CytoTableException(
            "Image export requires join=True so bounding boxes can be resolved from joined rows."
        )

    if dest_backend == "iceberg":
        if dest_datatype != "parquet":
            raise DatatypeException(
                "Iceberg backend currently supports only dest_datatype='parquet' "
                "for normalized table staging."
            )
        if not concat:
            raise CytoTableException(
                "Iceberg backend does not support concat=False. "
                "It always stages normalized tables using concatenated logical outputs."
            )
        if not join:
            raise CytoTableException(
                "Iceberg backend does not support join=False. "
                "Use the default join=True behavior when writing an Iceberg warehouse."
            )
        if drop_null:
            raise CytoTableException(
                "Iceberg backend does not support drop_null=True. "
                "This parquet join-time filter is unavailable for warehouse writes."
            )

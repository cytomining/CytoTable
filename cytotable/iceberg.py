"""
Utilities for reading and writing local Iceberg warehouses with CytoTable.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union, cast

import pandas as pd
import parsl
import pyarrow as pa
import pyarrow.parquet as parquet

from cytotable.convert import _run_export_workflow
from cytotable.exceptions import CytoTableException
from cytotable.images import (
    IMAGE_TABLE_NAME,
    image_crop_table_from_joined_chunk,
    profile_with_images_frame,
)
from cytotable.presets import config
from cytotable.utils import _default_parsl_config, _expand_path, _parsl_loaded

logger = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "analytics"
DEFAULT_REGISTRY_FILE = "catalog.json"
DEFAULT_WAREHOUSE_DIR = "warehouse"
DEFAULT_JOINED_VIEW = "cytotable_joined"
DEFAULT_PROFILE_WITH_IMAGES_VIEW = "profile_with_images"

try:
    from pyiceberg.catalog import Catalog, MetastoreCatalog, PropertiesUpdateSummary
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.serializers import FromInputFile
    from pyiceberg.table import CommitTableResponse, Table
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
    from pyiceberg.table.update import TableRequirement, TableUpdate
    from pyiceberg.typedef import EMPTY_DICT, Identifier, Properties
except ImportError as import_error:
    _PYICEBERG_IMPORT_ERROR: Optional[ImportError] = import_error
else:
    _PYICEBERG_IMPORT_ERROR = None


def _require_pyiceberg() -> None:
    """
    Raise an informative error when pyiceberg is unavailable.
    """

    if _PYICEBERG_IMPORT_ERROR is not None:
        raise ImportError(
            "Iceberg support requires the optional 'pyiceberg' dependency."
        ) from _PYICEBERG_IMPORT_ERROR


def _qualify(name: str, namespace: str) -> str:
    """
    Return a fully qualified Iceberg identifier.
    """

    return name if "." in name else f"{namespace}.{name}"


def _warehouse_dir(path: Union[str, Path], registry_file: str) -> Path:
    """
    Return the directory that stores Iceberg metadata and data files.
    """

    root = Path(path)
    return root if (root / registry_file).exists() else root / DEFAULT_WAREHOUSE_DIR


def _rewrite_join_sql_for_warehouse(joins: str, source_names: Dict[str, str]) -> str:
    """
    Replace parquet reads in join SQL with registered DuckDB relation names.
    """

    rewritten = joins
    for source_name in source_names:
        rewritten = rewritten.replace(
            f"read_parquet('{source_name}.parquet')",
            source_names[source_name],
        )
    return rewritten


def _resolve_convert_config(
    *,
    preset: Optional[str],
    metadata: Optional[Tuple[str, ...] | list[str]],
    compartments: Optional[Tuple[str, ...] | list[str]],
    identifying_columns: Optional[Tuple[str, ...] | list[str]],
    joins: Optional[str],
    chunk_size: Optional[int],
    page_keys: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    """
    Resolve CytoTable conversion settings with preset defaults applied.
    """

    if preset is not None:
        metadata = (
            cast(Tuple[str, ...], config[preset]["CONFIG_NAMES_METADATA"])
            if metadata is None
            else metadata
        )
        compartments = (
            cast(Tuple[str, ...], config[preset]["CONFIG_NAMES_COMPARTMENTS"])
            if compartments is None
            else compartments
        )
        identifying_columns = (
            cast(Tuple[str, ...], config[preset]["CONFIG_IDENTIFYING_COLUMNS"])
            if identifying_columns is None
            else identifying_columns
        )
        joins = cast(str, config[preset]["CONFIG_JOINS"]) if joins is None else joins
        chunk_size = (
            cast(int, config[preset]["CONFIG_CHUNK_SIZE"])
            if chunk_size is None
            else chunk_size
        )
        page_keys = (
            cast(Dict[str, str], config[preset]["CONFIG_PAGE_KEYS"])
            if page_keys is None
            else page_keys
        )

    return {
        "metadata": tuple(metadata or ()),
        "compartments": tuple(compartments or ()),
        "identifying_columns": tuple(identifying_columns or ()),
        "joins": joins or "",
        "chunk_size": chunk_size,
        "page_keys": dict(page_keys or {}),
        "preset": preset,
    }


if _PYICEBERG_IMPORT_ERROR is None:

    class TinyCatalog(MetastoreCatalog):
        """
        Tiny filesystem-backed catalog for local CytoTable Iceberg warehouses.
        """

        def __init__(
            self,
            warehouse_root: Path,
            *,
            default_namespace: str = DEFAULT_NAMESPACE,
            registry_file: str = DEFAULT_REGISTRY_FILE,
        ) -> None:
            self.default_namespace = default_namespace
            self.registry_path = warehouse_root / registry_file
            warehouse_root.mkdir(parents=True, exist_ok=True)
            super().__init__("local", warehouse=warehouse_root.resolve().as_uri())

        def _read_registry(self) -> dict[str, object]:
            if not self.registry_path.exists():
                return {
                    "namespaces": [self.default_namespace],
                    "tables": {},
                    "views": {},
                }
            registry = json.loads(self.registry_path.read_text())
            registry.setdefault("views", {})
            return registry

        def _write_registry(self, registry: dict[str, object]) -> None:
            self.registry_path.write_text(
                json.dumps(registry, indent=2, sort_keys=True)
            )

        def create_namespace(
            self, namespace: str | Identifier, properties: Properties = EMPTY_DICT
        ) -> None:
            del properties
            registry = self._read_registry()
            names = set(cast(list[str], registry["namespaces"]))
            names.add(Catalog.namespace_to_string(namespace))
            registry["namespaces"] = sorted(names)
            self._write_registry(registry)

        def load_namespace_properties(
            self, namespace: str | Identifier
        ) -> dict[str, str]:
            name = Catalog.namespace_to_string(namespace)
            if name not in cast(list[str], self._read_registry()["namespaces"]):
                raise NoSuchNamespaceError(name)
            return {}

        def list_namespaces(
            self, namespace: str | Identifier = ()
        ) -> list[tuple[str, ...]]:
            del namespace
            return [
                tuple(name.split("."))
                for name in cast(list[str], self._read_registry()["namespaces"])
            ]

        def list_tables(self, namespace: str | Identifier) -> list[tuple[str, ...]]:
            prefix = f"{Catalog.namespace_to_string(namespace)}."
            return [
                tuple(name.split("."))
                for name in sorted(
                    cast(dict[str, str], self._read_registry()["tables"])
                )
                if name.startswith(prefix)
            ]

        def load_table(self, identifier: str | Identifier) -> Table:
            name = ".".join(Catalog.identifier_to_tuple(identifier))
            metadata_location = cast(
                dict[str, str], self._read_registry()["tables"]
            ).get(name)
            if metadata_location is None:
                raise NoSuchTableError(name)
            io = self._load_file_io(location=metadata_location)
            metadata = FromInputFile.table_metadata(io.new_input(metadata_location))
            return Table(
                Catalog.identifier_to_tuple(identifier),
                metadata,
                metadata_location,
                io,
                self,
            )

        def register_table(
            self, identifier: str | Identifier, metadata_location: str
        ) -> Table:
            registry = self._read_registry()
            cast(dict[str, str], registry["tables"])[
                ".".join(Catalog.identifier_to_tuple(identifier))
            ] = metadata_location
            self._write_registry(registry)
            return self.load_table(identifier)

        def commit_table(
            self,
            table: Table,
            requirements: tuple[TableRequirement, ...],
            updates: tuple[TableUpdate, ...],
        ) -> CommitTableResponse:
            identifier = Catalog.identifier_to_tuple(table.name())
            try:
                current = self.load_table(identifier)
            except NoSuchTableError:
                current = None
            staged = self._update_and_stage_table(
                current, identifier, requirements, updates
            )
            self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
            registry = self._read_registry()
            cast(dict[str, str], registry["tables"])[
                ".".join(identifier)
            ] = staged.metadata_location
            self._write_registry(registry)
            return CommitTableResponse(
                metadata=staged.metadata, metadata_location=staged.metadata_location
            )

        def create_table(  # noqa: PLR0913
            self,
            identifier: str | Identifier,
            schema: Schema | pa.Schema,
            location: str | None = None,
            partition_spec: PartitionSpec = UNPARTITIONED_PARTITION_SPEC,
            sort_order: SortOrder = UNSORTED_SORT_ORDER,
            properties: Properties = EMPTY_DICT,
        ) -> Table:
            return self.create_table_transaction(
                identifier, schema, location, partition_spec, sort_order, properties
            ).commit_transaction()

        def table_exists(self, identifier: str | Identifier) -> bool:
            return ".".join(Catalog.identifier_to_tuple(identifier)) in cast(
                dict[str, str], self._read_registry()["tables"]
            )

        def view_exists(self, identifier: str | Identifier) -> bool:
            return ".".join(Catalog.identifier_to_tuple(identifier)) in cast(
                dict[str, dict[str, object]], self._read_registry()["views"]
            )

        def list_views(self, namespace: str | Identifier) -> list[tuple[str, ...]]:
            prefix = f"{Catalog.namespace_to_string(namespace)}."
            return [
                tuple(name.split("."))
                for name in sorted(cast(dict[str, str], self._read_registry()["views"]))
                if name.startswith(prefix)
            ]

        def drop_view(self, _identifier: str | Identifier) -> None:
            raise NotImplementedError

        def drop_table(self, _identifier: str | Identifier) -> None:
            raise NotImplementedError

        def rename_table(
            self, _from_identifier: str | Identifier, _to_identifier: str | Identifier
        ) -> Table:
            raise NotImplementedError

        def drop_namespace(self, _namespace: str | Identifier) -> None:
            raise NotImplementedError

        def update_namespace_properties(
            self,
            _namespace: str | Identifier,
            _removals: set[str] | None = None,
            _updates: Properties = EMPTY_DICT,
        ) -> PropertiesUpdateSummary:
            raise NotImplementedError

else:

    class TinyCatalog:  # type: ignore[no-redef]
        """
        Placeholder catalog when pyiceberg is unavailable.
        """


def catalog(
    warehouse_path: Union[str, Path],
    *,
    default_namespace: str = DEFAULT_NAMESPACE,
    registry_file: str = DEFAULT_REGISTRY_FILE,
) -> TinyCatalog:
    """
    Open a local Iceberg warehouse and return its tiny catalog.
    """

    _require_pyiceberg()
    return TinyCatalog(
        _warehouse_dir(warehouse_path, registry_file),
        default_namespace=default_namespace,
        registry_file=registry_file,
    )


def write_iceberg_warehouse(  # noqa: PLR0913
    source_path: str,
    warehouse_path: Union[str, Path],
    source_datatype: Optional[str] = None,
    metadata: Optional[Tuple[str, ...] | list[str]] = None,
    compartments: Optional[Tuple[str, ...] | list[str]] = None,
    identifying_columns: Optional[Tuple[str, ...] | list[str]] = None,
    joins: Optional[str] = None,
    chunk_size: Optional[int] = None,
    infer_common_schema: bool = True,
    data_type_cast_map: Optional[Dict[str, str]] = None,
    add_tablenumber: Optional[bool] = None,
    page_keys: Optional[Dict[str, str]] = None,
    sort_output: bool = True,
    preset: Optional[str] = "cellprofiler_csv",
    image_dir: Optional[str] = None,
    mask_dir: Optional[str] = None,
    outline_dir: Optional[str] = None,
    bbox_column_map: Optional[Dict[str, str]] = None,
    segmentation_file_regex: Optional[Dict[str, str]] = None,
    default_namespace: str = DEFAULT_NAMESPACE,
    registry_file: str = DEFAULT_REGISTRY_FILE,
    joined_view_name: Optional[str] = DEFAULT_JOINED_VIEW,
    profile_with_images_view_name: Optional[str] = DEFAULT_PROFILE_WITH_IMAGES_VIEW,
    parsl_config: Optional[parsl.Config] = None,
    **kwargs,
) -> str:
    """
    Normalize raw source data and store the resulting logical tables in Iceberg.
    """

    _require_pyiceberg()

    root = Path(_expand_path(str(warehouse_path)))
    if root.exists():
        raise CytoTableException(
            f"An existing file or directory was provided as warehouse_path: '{root}'."
        )

    resolved = _resolve_convert_config(
        preset=preset,
        metadata=metadata,
        compartments=compartments,
        identifying_columns=identifying_columns,
        joins=joins,
        chunk_size=chunk_size,
        page_keys=page_keys,
    )

    root.mkdir(parents=True, exist_ok=False)
    stage_dir = Path(tempfile.mkdtemp(prefix="cytotable-iceberg-", dir=str(root)))

    if not _parsl_loaded():
        parsl.load(parsl_config or _default_parsl_config())
    else:
        logger.warning("Reusing previously loaded Parsl configuration.")

    try:
        staged = cast(
            Dict[str, list[dict[str, Any]]],
            _run_export_workflow(
                source_path=source_path,
                dest_path=str(stage_dir),
                source_datatype=source_datatype,
                metadata=list(cast(Tuple[str, ...], resolved["metadata"])),
                compartments=list(cast(Tuple[str, ...], resolved["compartments"])),
                identifying_columns=list(
                    cast(Tuple[str, ...], resolved["identifying_columns"])
                ),
                concat=True,
                join=False,
                joins=cast(str, resolved["joins"]),
                chunk_size=cast(Optional[int], resolved["chunk_size"]),
                infer_common_schema=infer_common_schema,
                drop_null=False,
                sort_output=sort_output,
                page_keys=cast(Dict[str, str], resolved["page_keys"]),
                data_type_cast_map=data_type_cast_map,
                add_tablenumber=add_tablenumber,
                **kwargs,
            ),
        )

        bundle = catalog(
            root,
            default_namespace=default_namespace,
            registry_file=registry_file,
        )
        bundle.create_namespace(default_namespace)

        source_names: Dict[str, str] = {}
        for source_group_name, group in staged.items():
            table_name = Path(source_group_name).stem.lower()
            parquet_path = Path(group[0]["table"][0])
            arrow_table = parquet.read_table(parquet_path)
            if bundle.table_exists((default_namespace, table_name)):
                table = bundle.load_table((default_namespace, table_name))
            else:
                table = bundle.create_table(
                    (default_namespace, table_name),
                    arrow_table.schema,
                )
            table.append(arrow_table)
            source_names[table_name] = table_name

        if joined_view_name and resolved["joins"]:
            registry = bundle._read_registry()
            cast(dict[str, dict[str, object]], registry["views"])[
                _qualify(joined_view_name, default_namespace)
            ] = {
                "kind": "sql",
                "tables": sorted(source_names.values()),
                "sql": _rewrite_join_sql_for_warehouse(
                    cast(str, resolved["joins"]), source_names
                ),
                "page_keys": cast(Dict[str, str], resolved["page_keys"]),
                "preset": resolved["preset"],
            }
            bundle._write_registry(registry)

        if image_dir is not None:
            joined_chunk_paths = cast(
                list[str],
                _run_export_workflow(
                    source_path=source_path,
                    dest_path=str(stage_dir / "joined"),
                    source_datatype=source_datatype,
                    metadata=list(cast(Tuple[str, ...], resolved["metadata"])),
                    compartments=list(cast(Tuple[str, ...], resolved["compartments"])),
                    identifying_columns=list(
                        cast(Tuple[str, ...], resolved["identifying_columns"])
                    ),
                    concat=False,
                    join=True,
                    joins=cast(str, resolved["joins"]),
                    chunk_size=cast(Optional[int], resolved["chunk_size"]),
                    infer_common_schema=infer_common_schema,
                    drop_null=False,
                    sort_output=sort_output,
                    page_keys=cast(Dict[str, str], resolved["page_keys"]),
                    data_type_cast_map=data_type_cast_map,
                    add_tablenumber=add_tablenumber,
                    **kwargs,
                ),
            )
            image_table: Optional[Table] = None
            for chunk_path in joined_chunk_paths:
                crop_table = image_crop_table_from_joined_chunk(
                    chunk_path=chunk_path,
                    image_dir=image_dir,
                    mask_dir=mask_dir,
                    outline_dir=outline_dir,
                    bbox_column_map=bbox_column_map,
                    segmentation_file_regex=segmentation_file_regex,
                )
                if crop_table.num_rows == 0:
                    continue
                if image_table is None:
                    image_table = (
                        bundle.load_table((default_namespace, IMAGE_TABLE_NAME))
                        if bundle.table_exists((default_namespace, IMAGE_TABLE_NAME))
                        else bundle.create_table(
                            (default_namespace, IMAGE_TABLE_NAME), crop_table.schema
                        )
                    )
                image_table.append(crop_table)

            if joined_view_name and profile_with_images_view_name:
                registry = bundle._read_registry()
                cast(dict[str, dict[str, object]], registry["views"])[
                    _qualify(profile_with_images_view_name, default_namespace)
                ] = {
                    "kind": "profile_with_images",
                    "base_view": _qualify(joined_view_name, default_namespace),
                    "image_table": _qualify(IMAGE_TABLE_NAME, default_namespace),
                    "bbox_column_map": bbox_column_map or {},
                }
                bundle._write_registry(registry)

    finally:
        if _parsl_loaded():
            parsl.dfk().cleanup()
        shutil.rmtree(stage_dir, ignore_errors=True)

    return str(root)


def _read_sql_view(bundle: TinyCatalog, view_name: str) -> pd.DataFrame:
    """
    Read a saved SQL view by materializing Iceberg tables into DuckDB.
    """

    from cytotable.utils import _duckdb_reader

    registry = bundle._read_registry()
    spec = cast(dict[str, Any], cast(dict[str, Any], registry["views"])[view_name])
    sql = cast(str, spec["sql"])

    with _duckdb_reader() as reader:
        for table_name in cast(list[str], spec["tables"]):
            qualified = _qualify(table_name, bundle.default_namespace)
            arrow_table = (
                bundle.load_table(tuple(qualified.split("."))).scan().to_arrow()
            )
            reader.register(table_name, arrow_table)
        return reader.execute(sql).fetch_arrow_table().to_pandas()


def _read_profile_with_images_view(bundle: TinyCatalog, view_name: str) -> pd.DataFrame:
    """
    Read a saved profile/image manifest view from warehouse tables.
    """

    registry = bundle._read_registry()
    spec = cast(dict[str, Any], cast(dict[str, Any], registry["views"])[view_name])
    joined_frame = _read_registered_view(bundle, cast(str, spec["base_view"]))
    image_frame = (
        bundle.load_table(tuple(cast(str, spec["image_table"]).split(".")))
        .scan()
        .to_arrow()
        .to_pandas()
    )
    return profile_with_images_frame(
        joined_frame=joined_frame,
        image_frame=image_frame,
        bbox_column_map=cast(Dict[str, str], spec.get("bbox_column_map") or {}),
    )


def _read_registered_view(bundle: TinyCatalog, view_name: str) -> pd.DataFrame:
    """
    Read a saved registry-backed warehouse view.
    """

    registry = bundle._read_registry()
    spec = cast(dict[str, Any], cast(dict[str, Any], registry["views"])[view_name])
    kind = cast(str, spec["kind"])
    if kind == "sql":
        return _read_sql_view(bundle, view_name)
    if kind == "profile_with_images":
        return _read_profile_with_images_view(bundle, view_name)
    raise CytoTableException(f"Unsupported warehouse view kind: {kind}")


def read_iceberg_table(
    warehouse_path: Union[str, Path],
    table_name: str,
    *,
    default_namespace: str = DEFAULT_NAMESPACE,
    registry_file: str = DEFAULT_REGISTRY_FILE,
) -> pd.DataFrame:
    """
    Read an Iceberg table or saved SQL view from a local warehouse.
    """

    bundle = catalog(
        warehouse_path,
        default_namespace=default_namespace,
        registry_file=registry_file,
    )
    qualified_name = _qualify(table_name, bundle.default_namespace)
    if bundle.view_exists(tuple(qualified_name.split("."))):
        return _read_registered_view(bundle, qualified_name)
    return (
        bundle.load_table(tuple(qualified_name.split(".")))
        .scan()
        .to_arrow()
        .to_pandas()
    )


def list_iceberg_tables(
    warehouse_path: Union[str, Path],
    include_views: bool = True,
    *,
    default_namespace: str = DEFAULT_NAMESPACE,
    registry_file: str = DEFAULT_REGISTRY_FILE,
) -> list[str]:
    """
    List fully qualified tables and optional views in a local Iceberg warehouse.
    """

    bundle = catalog(
        warehouse_path,
        default_namespace=default_namespace,
        registry_file=registry_file,
    )
    names = [
        ".".join(identifier)
        for namespace in bundle.list_namespaces()
        for identifier in bundle.list_tables(namespace)
    ]
    if include_views:
        names.extend(
            ".".join(identifier)
            for namespace in bundle.list_namespaces()
            for identifier in bundle.list_views(namespace)
        )
    return sorted(names)


def describe_iceberg_warehouse(
    warehouse_path: Union[str, Path],
    include_views: bool = True,
    *,
    default_namespace: str = DEFAULT_NAMESPACE,
    registry_file: str = DEFAULT_REGISTRY_FILE,
) -> pd.DataFrame:
    """
    Summarize tables and saved views within a local Iceberg warehouse.
    """

    bundle = catalog(
        warehouse_path,
        default_namespace=default_namespace,
        registry_file=registry_file,
    )
    rows: list[dict[str, object]] = []
    for namespace in bundle.list_namespaces():
        for identifier in bundle.list_tables(namespace):
            table = bundle.load_table(identifier)
            files = table.inspect.files().to_pandas()
            rows.append(
                {
                    "table": ".".join(identifier),
                    "rows": int(files["record_count"].sum()),
                    "data_files": len(files),
                    "snapshot_id": table.current_snapshot().snapshot_id,
                    "kind": "table",
                }
            )
        if include_views:
            for identifier in bundle.list_views(namespace):
                view_name = ".".join(identifier)
                rows.append(
                    {
                        "table": view_name,
                        "rows": len(_read_registered_view(bundle, view_name)),
                        "data_files": 0,
                        "snapshot_id": None,
                        "kind": "view",
                    }
                )
    return pd.DataFrame(rows).sort_values("table").reset_index(drop=True)


__all__ = [
    "DEFAULT_JOINED_VIEW",
    "DEFAULT_NAMESPACE",
    "DEFAULT_PROFILE_WITH_IMAGES_VIEW",
    "DEFAULT_REGISTRY_FILE",
    "TinyCatalog",
    "catalog",
    "describe_iceberg_warehouse",
    "list_iceberg_tables",
    "read_iceberg_table",
    "write_iceberg_warehouse",
]

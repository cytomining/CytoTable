"""
Infer CellProfiler-style joins for ``convert(..., joins="infer")``.

Recognize image/table keys, object numbers, and Parent_* references in
converted parquet schemas. Validate parent references against actual values,
then build and bind a LEFT JOIN tree and check its row count. Unsupported
relationships require manual SQL; no fuzzy schema matching is attempted.

See cytomining/CytoTable#522.
"""

from __future__ import annotations

import logging
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from cytotable.exceptions import CytoTableException

logger = logging.getLogger(__name__)

# Sentinel value for the convert() `joins` parameter which requests
# join SQL inference instead of manually provided SQL.
INFER_JOINS_SENTINEL = "infer"

# lowercased canonical shared key column names, in priority order
_IMAGE_KEY_NAMES: Tuple[str, ...] = ("metadata_imagenumber", "imagenumber")
_TABLENUMBER_KEY_NAMES: Tuple[str, ...] = ("metadata_tablenumber", "tablenumber")
# regexes for table-specific shared key columns, for ex.
# "Cells_ImageNumber" or "Image_TableNumber"
_STEM_IMAGE_KEY_REGEX = re.compile(r"^(?:metadata_)?[a-z0-9]+_imagenumber$")
_STEM_TABLENUMBER_REGEX = re.compile(r"^(?:metadata_)?[a-z0-9]+_tablenumber$")
# lowercased generic object number column names, in priority order
_OBJECTNUMBER_NAMES: Tuple[str, ...] = (
    "metadata_objectnumber",
    "objectnumber",
    "number_object_number",
)
# regex for compartment-prefixed object number columns, for ex.
# "Cells_Number_Object_Number" or "Metadata_Cells_Number_Object_Number"
_COMPARTMENT_OBJECTNUMBER_TEMPLATE = (
    r"^(?:metadata_)?{compartment}_number_object_number$"
)
# regex for parent reference columns, for ex. "Parent_Cells",
# "Metadata_Cytoplasm_Parent_Cells", or "Cells_Parent_Nuclei"
_PARENT_REFERENCE_REGEX = re.compile(
    r"^(?:metadata_)?(?:[a-z0-9]+_)?parent_(?P<target>[a-z0-9]+)$"
)
# minimum fraction of child rows which must reference an existing parent
# object for a parent-reference edge to be trusted without a warning
_TRUSTED_MATCH_RATE = 0.5
# duckdb storage type tokens which indicate numeric (pagination-safe) columns
_NUMERIC_TYPE_TOKENS: Tuple[str, ...] = (
    "INT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "NUMERIC",
)


@dataclass(frozen=True)
class InferredJoin:
    """
    Result of join SQL inference from source table data.

    Attributes:
        sql (str):
            DuckDB join SQL using ``'<table_stem>.parquet'`` read_parquet
            placeholders, as expected by ``_prepare_join_sql``.
        page_key (str):
            Column name suggested as the ``page_keys["join"]`` pagination
            key for the joined result.
        base_table (str):
            Table alias used as the join tree base.
        edges (Tuple[Tuple[str, str], ...]):
            Accepted (child, parent) table alias pairs used for the joins.
    """

    sql: str
    page_key: str
    base_table: str
    edges: Tuple[Tuple[str, str], ...]


@dataclass(frozen=True)
class _TableInfo:
    """
    Schema-level information about a converted source table group.
    """

    group_name: str
    alias: str
    compartment: Optional[str]
    is_metadata: bool
    paths: Tuple[str, ...]
    columns: Tuple[str, ...]
    image_key: Optional[str] = None
    tablenumber_key: Optional[str] = None
    objectnumber_key: Optional[str] = None


@dataclass(frozen=True)
class _Edge:
    """
    A candidate or validated join edge from a child to a parent table.
    """

    child: str
    parent: str
    reference_column: str
    parent_key: str


@dataclass(frozen=True)
class _JoinStep:
    """
    A single LEFT JOIN step within the inferred join tree.
    """

    table: _TableInfo
    conditions: Tuple[str, ...]


def is_infer_joins_sentinel(joins: Optional[str]) -> bool:
    """
    Determine whether a joins value is the inference sentinel.

    Join SQL inference occurs only when this returns True for the
    ``joins`` value; inference is always an explicit choice.

    Args:
        joins (Optional[str]):
            The joins value to check.

    Returns:
        bool:
            True if the value references join SQL inference.
    """
    return joins is not None and joins.strip().lower() == INFER_JOINS_SENTINEL


def infer_join_sql(
    sources: Dict[str, List[Dict[str, Any]]],
    metadata: Optional[Sequence[str]] = None,
    compartments: Optional[Sequence[str]] = None,
) -> InferredJoin:
    """
    Infer DuckDB join SQL by inspecting converted source table data.

    Return SQL with ``'<table_stem>.parquet'`` placeholders for
    ``_prepare_join_sql``. Validation scans parent-reference values and
    counts the full joined result before returning.

    Args:
        sources (Dict[str, List[Dict[str, Any]]]):
            Grouped converted sources which each include ``table`` keys
            with parquet filepath lists, as produced by the export
            workflow's concatenation stage.
        metadata (Optional[Sequence[str]]):
            Metadata source table names used to classify source groups.
        compartments (Optional[Sequence[str]]):
            Compartment source table names used to classify source groups.

    Returns:
        InferredJoin:
            The inferred join SQL and related details.

    Raises:
        CytoTableException:
            Raised when no tables are provided, when no defensible join
            can be inferred for one or more tables, when no pagination
            key can be determined, or when the rendered SQL fails
            validation against the source data.
    """

    tables = _collect_tables(
        sources=sources, metadata=metadata, compartments=compartments
    )
    if not tables:
        raise CytoTableException("No source tables available for join SQL inference.")

    edges = _validate_edges(
        edges=_find_reference_edges(tables=tables),
        tables={table.alias: table for table in tables},
    )
    base = _select_base_table(tables=tables, edges=edges)
    steps = _build_join_tree(base=base, tables=tables, edges=edges)
    sql, page_key = _render_join_sql(base=base, steps=steps)
    _validate_rendered_sql(base=base, steps=steps, sql=sql)

    return InferredJoin(
        sql=sql,
        page_key=page_key,
        base_table=base.alias,
        edges=tuple(sorted((edge.child, edge.parent) for edge in edges)),
    )


def _collect_tables(
    *,
    sources: Dict[str, List[Dict[str, Any]]],
    metadata: Optional[Sequence[str]],
    compartments: Optional[Sequence[str]],
) -> List[_TableInfo]:
    """
    Gather schema-level information for each converted source table group.

    Args:
        sources (Dict[str, List[Dict[str, Any]]]):
            Grouped converted sources with parquet ``table`` paths.
        metadata (Optional[Sequence[str]]):
            Metadata source table names.
        compartments (Optional[Sequence[str]]):
            Compartment source table names.

    Returns:
        List[_TableInfo]:
            Table information sorted by alias for deterministic output.
    """

    import pyarrow.parquet as parquet  # pylint: disable=import-outside-toplevel

    tables = []
    for group_name, source_group_vals in sources.items():
        stem = pathlib.Path(group_name).stem.lower()
        paths = tuple(str(path) for path in source_group_vals[0]["table"])
        columns = tuple(parquet.read_schema(paths[0]).names)
        compartment, is_metadata = _classify_group(
            stem=stem, metadata=metadata, compartments=compartments
        )
        image_key, tablenumber_key, objectnumber_key = _detect_key_columns(
            columns=columns, compartment=compartment
        )
        tables.append(
            _TableInfo(
                group_name=group_name,
                alias=stem,
                compartment=compartment,
                is_metadata=is_metadata,
                paths=paths,
                columns=columns,
                image_key=image_key,
                tablenumber_key=tablenumber_key,
                objectnumber_key=objectnumber_key,
            )
        )

    return sorted(tables, key=lambda table: table.alias)


def _classify_group(
    *,
    stem: str,
    metadata: Optional[Sequence[str]],
    compartments: Optional[Sequence[str]],
) -> Tuple[Optional[str], bool]:
    """
    Classify a source group stem as a compartment or a metadata table.

    Args:
        stem (str):
            Lowercased source group filename stem.
        metadata (Optional[Sequence[str]]):
            Metadata source table names.
        compartments (Optional[Sequence[str]]):
            Compartment source table names.

    Returns:
        Tuple[Optional[str], bool]:
            The matched compartment or metadata name (lowercased) and
            whether the group classifies as a metadata table.
    """

    for name in compartments or ():
        if name.lower() in stem:
            return name.lower(), False
    for name in metadata or ():
        if name.lower() in stem:
            return name.lower(), True
    return None, False


def _detect_key_columns(
    *, columns: Sequence[str], compartment: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Detect shared image/tablenumber key and object number columns.

    Args:
        columns (Sequence[str]):
            Column names of the table.
        compartment (Optional[str]):
            Lowercased compartment name of the table, if any.

    Returns:
        Tuple[Optional[str], Optional[str], Optional[str]]:
            The image key, tablenumber key, and object number column
            names, where detectable.
    """

    lowered = {column.lower(): column for column in columns}

    image_key = _pick_column(
        lowered=lowered, exact_names=_IMAGE_KEY_NAMES, regex=_STEM_IMAGE_KEY_REGEX
    )
    tablenumber_key = _pick_column(
        lowered=lowered,
        exact_names=_TABLENUMBER_KEY_NAMES,
        regex=_STEM_TABLENUMBER_REGEX,
    )
    objectnumber_key = None
    if compartment is not None:
        objectnumber_key = _pick_column(
            lowered=lowered,
            exact_names=(),
            regex=re.compile(
                _COMPARTMENT_OBJECTNUMBER_TEMPLATE.format(compartment=compartment)
            ),
        )
    if objectnumber_key is None:
        objectnumber_key = _pick_column(
            lowered=lowered,
            exact_names=_OBJECTNUMBER_NAMES,
            regex=None,
        )

    return image_key, tablenumber_key, objectnumber_key


def _pick_column(
    *,
    lowered: Dict[str, str],
    exact_names: Tuple[str, ...],
    regex: Optional[re.Pattern[str]],
    allowed: Optional[set[str]] = None,
) -> Optional[str]:
    """
    Select a column by exact lowercased names, then by regex, in order.

    Args:
        lowered (Dict[str, str]):
            Mapping of lowercased column names to original column names.
        exact_names (Tuple[str, ...]):
            Lowercased names to seek exactly, in priority order.
        regex (Optional[re.Pattern[str]]):
            Optional regex applied to lowercased names as a fallback.
        allowed (Optional[set[str]]):
            Optional set of lowercased names which are eligible for
            selection, for ex. those of numeric columns.

    Returns:
        Optional[str]:
            The original column name which matched, if any.
    """

    for name in exact_names:
        if name in lowered and (allowed is None or name in allowed):
            return lowered[name]
    if regex is not None:
        for name in sorted(lowered):
            if (allowed is None or name in allowed) and regex.match(name):
                return lowered[name]
    return None


def _find_reference_edges(*, tables: List[_TableInfo]) -> List[_Edge]:
    """
    Detect candidate parent reference join edges between compartment tables.

    Args:
        tables (List[_TableInfo]):
            Table information gathered from converted sources.

    Returns:
        List[_Edge]:
            Candidate edges sorted by (child, parent, reference column).
    """

    table_by_compartment = {
        table.compartment: table for table in tables if table.compartment
    }
    edges = []
    for table in tables:
        if table.is_metadata:
            continue
        for column in table.columns:
            match = _PARENT_REFERENCE_REGEX.match(column.lower())
            if match is None:
                continue
            parent = _find_referenced_table(
                target=match.group("target"),
                tables=tables,
                table_by_compartment=table_by_compartment,
            )
            if (
                parent is None
                or parent.alias == table.alias
                or parent.objectnumber_key is None
                # a shared image-level key is required so that object
                # numbers are not matched across unrelated images
                or table.image_key is None
                or parent.image_key is None
            ):
                continue
            edges.append(
                _Edge(
                    child=table.alias,
                    parent=parent.alias,
                    reference_column=column,
                    parent_key=parent.objectnumber_key,
                )
            )

    # dedupe multiple references between the same table pair,
    # keeping the first reference column deterministically
    deduped: Dict[Tuple[str, str], _Edge] = {}
    for edge in sorted(
        edges, key=lambda edge: (edge.child, edge.parent, edge.reference_column)
    ):
        deduped.setdefault((edge.child, edge.parent), edge)

    return [deduped[key] for key in sorted(deduped)]


def _find_referenced_table(
    *,
    target: str,
    tables: List[_TableInfo],
    table_by_compartment: Dict[str, _TableInfo],
) -> Optional[_TableInfo]:
    """
    Find the table which a parent reference column points to.

    Args:
        target (str):
            Lowercased compartment name referenced by the column.
        tables (List[_TableInfo]):
            Table information gathered from converted sources.
        table_by_compartment (Dict[str, _TableInfo]):
            Tables keyed by lowercased compartment name.

    Returns:
        Optional[_TableInfo]:
            The referenced table, if one is detected.
    """

    if target in table_by_compartment:
        return table_by_compartment[target]
    # fall back to alias matching for tables whose group name is
    # compartment-prefixed, for ex. 'per_cells' referencing 'cells'
    for table in tables:
        if table.alias == target or table.alias.endswith(f"_{target}"):
            return table
    return None


def _validate_edges(
    *, edges: List[_Edge], tables: Dict[str, _TableInfo]
) -> List[_Edge]:
    """
    Validate candidate edges against the actual table data.

    Measures the fraction of child rows whose parent reference matches
    an existing parent object. Edges which match nothing are rejected,
    and edges with low match rates are kept with a warning.

    Args:
        edges (List[_Edge]):
            Candidate join edges.
        tables (Dict[str, _TableInfo]):
            Table information by alias.

    Returns:
        List[_Edge]:
            Edges with at least one matching row, or an empty child table.
    """

    validated = []
    for edge in edges:
        child_rows, matched_rows = _measure_edge_match(
            edge=edge, child=tables[edge.child], parent=tables[edge.parent]
        )
        if child_rows > 0 and matched_rows == 0:
            logger.info(
                "Dropping inferred join edge '%s'.'%s' -> '%s': no values "
                "match the referenced '%s' objects.",
                edge.child,
                edge.reference_column,
                edge.parent,
                edge.parent,
            )
            continue
        match_rate = matched_rows / child_rows if child_rows > 0 else 1.0
        if match_rate < _TRUSTED_MATCH_RATE:
            logger.warning(
                "Inferred join edge '%s'.'%s' -> '%s' matches only "
                "%d of %d child rows. Rows without matches will "
                "retain null values from the left join.",
                edge.child,
                edge.reference_column,
                edge.parent,
                matched_rows,
                child_rows,
            )
        logger.info(
            "Validated inferred join edge '%s'.'%s' -> '%s': "
            "%d of %d child rows match (rate=%.3f).",
            edge.child,
            edge.reference_column,
            edge.parent,
            matched_rows,
            child_rows,
            match_rate,
        )
        validated.append(edge)

    return validated


def _measure_edge_match(
    *, edge: _Edge, child: _TableInfo, parent: _TableInfo
) -> Tuple[int, int]:
    """
    Measure how many child rows reference existing parent objects.

    Args:
        edge (_Edge):
            The join edge to measure.
        child (_TableInfo):
            The referencing (child) table.
        parent (_TableInfo):
            The referenced (parent) table.

    Returns:
        Tuple[int, int]:
            The total child row count and the count of child rows
            which reference an existing parent object.
    """

    from cytotable.utils import (  # pylint: disable=import-outside-toplevel
        _duckdb_reader,
    )

    conditions = _edge_conditions(edge=edge, child=child, parent=parent)

    query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (
                WHERE EXISTS (
                    SELECT 1
                    FROM {_paths_sql(parent.paths)} AS "{parent.alias}"
                    WHERE {" AND ".join(conditions)}
                )
            ) AS matched_rows
        FROM {_paths_sql(child.paths)} AS "{child.alias}"
    """

    with _duckdb_reader() as ddb_reader:
        result = ddb_reader.execute(query).fetchone() or (0, 0)

    return int(result[0]), int(result[1])


def _shared_key_pairs(*, left: _TableInfo, right: _TableInfo) -> List[Tuple[str, str]]:
    """
    Gather shared key column pairs (left, right) between two tables.

    Args:
        left (_TableInfo):
            One table of the pair.
        right (_TableInfo):
            The other table of the pair.

    Returns:
        List[Tuple[str, str]]:
            Column name pairs which both tables share.
    """

    pairs = []
    if left.image_key is not None and right.image_key is not None:
        pairs.append((left.image_key, right.image_key))
    if left.tablenumber_key is not None and right.tablenumber_key is not None:
        pairs.append((left.tablenumber_key, right.tablenumber_key))
    return pairs


def _edge_conditions(
    *, edge: _Edge, child: _TableInfo, parent: _TableInfo
) -> Tuple[str, ...]:
    """
    Render the ON conditions for a parent reference join edge.

    Args:
        edge (_Edge):
            The join edge to render conditions for.
        child (_TableInfo):
            The referencing (child) table.
        parent (_TableInfo):
            The referenced (parent) table.

    Returns:
        Tuple[str, ...]:
            SQL condition strings relating the two tables.
    """

    conditions = [
        f'"{child.alias}"."{child_column}" = "{parent.alias}"."{parent_column}"'
        for child_column, parent_column in _shared_key_pairs(left=child, right=parent)
    ]
    conditions.append(
        f'"{child.alias}"."{edge.reference_column}" = '
        f'"{parent.alias}"."{edge.parent_key}"'
    )

    return tuple(conditions)


def _select_base_table(*, tables: List[_TableInfo], edges: List[_Edge]) -> _TableInfo:
    """
    Select the table to use as the base of the inferred join tree.

    The compartment with the most parent references is preferred
    (canonically the cytoplasm compartment), then cytoplasm-named
    tables, then tables with more columns, then alphabetical order.

    Args:
        tables (List[_TableInfo]):
            Table information gathered from converted sources.
        edges (List[_Edge]):
            Validated join edges.

    Returns:
        _TableInfo:
            The selected base table.
    """

    candidates = [table for table in tables if not table.is_metadata]
    if not candidates:
        candidates = list(tables)

    reference_counts: Dict[str, int] = {}
    for edge in edges:
        reference_counts[edge.child] = reference_counts.get(edge.child, 0) + 1
    most_references = max(reference_counts.values(), default=0)

    finalists = [
        table
        for table in candidates
        if reference_counts.get(table.alias, 0) == most_references
    ]

    return sorted(
        finalists,
        key=lambda table: (
            0 if "cytoplasm" in table.alias or table.compartment == "cytoplasm" else 1,
            -len(table.columns),
            table.alias,
        ),
    )[0]


def _build_join_tree(
    *, base: _TableInfo, tables: List[_TableInfo], edges: List[_Edge]
) -> List[_JoinStep]:
    """
    Build a spanning left-join tree which joins every table exactly once.

    Args:
        base (_TableInfo):
            The base table to join from.
        tables (List[_TableInfo]):
            All table information.
        edges (List[_Edge]):
            Validated join edges.

    Returns:
        List[_JoinStep]:
            Ordered join steps describing each LEFT JOIN.

    Raises:
        CytoTableException:
            Raised when one or more tables cannot be joined to the tree.
    """

    table_by_alias = {table.alias: table for table in tables}
    joined = {base.alias}
    steps: List[_JoinStep] = []
    remaining = sorted(edges, key=lambda e: (e.child, e.parent))

    # attach tables reachable through parent reference edges
    progress = True
    while remaining and progress:
        progress = False
        for edge in list(remaining):
            table = None
            if edge.child in joined and edge.parent not in joined:
                # joining the referenced parent table from the child
                table = table_by_alias[edge.parent]
            elif edge.parent in joined and edge.child not in joined:
                # joining the referencing child table from the parent
                table = table_by_alias[edge.child]
            if table is None:
                continue
            steps.append(
                _JoinStep(
                    table=table,
                    conditions=_edge_conditions(
                        edge=edge,
                        child=table_by_alias[edge.child],
                        parent=table_by_alias[edge.parent],
                    ),
                )
            )
            joined.add(table.alias)
            remaining.remove(edge)
            progress = True
            break

    # attach metadata tables to the base table through shared keys
    for table in tables:
        if table.alias in joined:
            continue
        pairs = _shared_key_pairs(left=base, right=table)
        if not pairs:
            continue
        conditions = [
            f'"{table.alias}"."{table_column}" = ' f'"{base.alias}"."{base_column}"'
            for base_column, table_column in pairs
        ]
        steps.append(_JoinStep(table=table, conditions=tuple(conditions)))
        joined.add(table.alias)

    unjoined = [table.alias for table in tables if table.alias not in joined]
    if unjoined:
        raise CytoTableException(
            "Unable to infer a join for table(s): "
            f"{', '.join(sorted(unjoined))}. No compartment parent "
            "reference or shared key relationship with the other tables "
            "was detected and validated against the data. Please provide "
            "join SQL manually through the 'joins' parameter."
        )

    return steps


def _render_join_sql(*, base: _TableInfo, steps: List[_JoinStep]) -> Tuple[str, str]:
    """
    Render the inferred join SQL and its suggested pagination key.

    Args:
        base (_TableInfo):
            The base table to join from.
        steps (List[_JoinStep]):
            Ordered join steps describing each LEFT JOIN.

    Returns:
        Tuple[str, str]:
            The rendered SQL (with parquet read placeholders) and the
            suggested join pagination key column name.

    Raises:
        CytoTableException:
            Raised when no pagination key column can be determined.
    """

    # track columns already selected so duplicate column names from
    # joined tables are excluded (mirroring the join presets)
    seen = set(base.columns)
    select_parts = [f'"{base.alias}".*']
    for step in steps:
        excluded = [column for column in step.table.columns if column in seen]
        select_part = f'"{step.table.alias}".*'
        if excluded:
            select_part += " EXCLUDE ({})".format(
                ", ".join(f'"{column}"' for column in excluded)
            )
        select_parts.append(select_part)
        seen.update(step.table.columns)

    from_parts = [f"read_parquet('{base.alias}.parquet') AS \"{base.alias}\""]
    for step in steps:
        from_parts.append(
            f"LEFT JOIN read_parquet('{step.table.alias}.parquet') "
            f'AS "{step.table.alias}"\n    ON {" AND ".join(step.conditions)}'
        )

    sql = (
        "SELECT\n    "
        + ",\n    ".join(select_parts)
        + "\nFROM "
        + "\n".join(from_parts)
    )

    page_key = base.objectnumber_key or base.image_key
    if page_key is None:
        raise CytoTableException(
            f"Unable to determine a join pagination key for base table "
            f"'{base.alias}': no object number or image number column "
            "was detected. Please provide join SQL manually through the "
            "'joins' parameter."
        )

    return sql, page_key


def _paths_sql(paths: Tuple[str, ...]) -> str:
    """
    Render a read_parquet source for one or more parquet paths.

    Args:
        paths (Tuple[str, ...]):
            Parquet filepaths to read.

    Returns:
        str:
            DuckDB read_parquet expression for the paths.
    """

    if len(paths) == 1:
        return f"read_parquet('{paths[0]}')"
    return "read_parquet([{}])".format(", ".join(f"'{path}'" for path in paths))


def _validate_rendered_sql(
    *, base: _TableInfo, steps: List[_JoinStep], sql: str
) -> None:
    """
    Validate the rendered SQL by binding and counting joined rows.

    Substitute real paths, bind without materializing rows, and compare
    joined and base row counts to warn about fan-out.

    Args:
        base (_TableInfo):
            The base table of the join.
        steps (List[_JoinStep]):
            Ordered join steps of the join.
        sql (str):
            The rendered SQL with parquet read placeholders.

    Raises:
        CytoTableException:
            Raised when the SQL fails to bind or execute against the
            data, or when the joined result has fewer rows than the base
            table (which left joins cannot produce).
    """

    from cytotable.utils import (  # pylint: disable=import-outside-toplevel
        _duckdb_reader,
    )

    rendered = sql
    for table in [base] + [step.table for step in steps]:
        rendered = rendered.replace(
            f"read_parquet('{table.alias}.parquet')",
            _paths_sql(table.paths),
        )

    with _duckdb_reader() as ddb_reader:
        try:
            # bind the statement without materializing results
            ddb_reader.execute(f"EXPLAIN {rendered}")
            base_rows = (
                ddb_reader.execute(
                    f"SELECT COUNT(*) FROM {_paths_sql(base.paths)}"
                ).fetchone()
                or (0,)
            )[0]
            joined_rows = (
                ddb_reader.execute(f"SELECT COUNT(*) FROM ({rendered})").fetchone()
                or (0,)
            )[0]
        except Exception as e:  # pylint: disable=broad-exception-caught
            raise CytoTableException(
                "Inferred join SQL failed validation against the source "
                f"data: {e}. Please provide join SQL manually through "
                "the 'joins' parameter."
            ) from e

    if joined_rows < base_rows:
        raise CytoTableException(
            f"Inferred join produced {joined_rows} rows from a base table "
            f"with {base_rows} rows, which left joins cannot do. Please "
            "provide join SQL manually through the 'joins' parameter."
        )
    if joined_rows > base_rows:
        logger.warning(
            "Inferred join produces %d rows from a base table with %d "
            "rows: joined tables contain duplicated key values which "
            "multiply rows. Verify whether this is expected.",
            joined_rows,
            base_rows,
        )


def _default_identifying_columns(
    *, compartments: Optional[Sequence[str]] = None
) -> Tuple[str, ...]:
    """
    Build default identifying columns for join SQL inference runs.

    Preserve canonical keys and parent references during column renaming.

    Args:
        compartments (Optional[Sequence[str]]):
            Compartment source table names.

    Returns:
        Tuple[str, ...]:
            Default identifying column names.
    """

    columns = ["ImageNumber", "ObjectNumber", "TableNumber"]
    for name in compartments or ():
        columns.append(f"Parent_{str(name).capitalize()}")

    return tuple(columns)


def _infer_source_page_keys(
    *,
    sources: Dict[str, List[Dict[str, Any]]],
    page_keys: Dict[str, str],
    compartments: Optional[Sequence[str]] = None,
) -> Dict[str, str]:
    """
    Infer pagination keys for source table groups which lack one.

    Preserve explicit keys. Inspect each remaining group's first source,
    preferring numeric object, image, and table numbers, then any numeric column.

    Args:
        sources (Dict[str, List[Dict[str, Any]]]):
            Grouped sources gathered for conversion work.
        page_keys (Dict[str, str]):
            User-provided pagination keys, if any.
        compartments (Optional[Sequence[str]]):
            Compartment source table names.

    Returns:
        Dict[str, str]:
            Updated pagination keys including inferred entries.
    """

    updated = dict(page_keys or {})
    for group_name, source_group_vals in sources.items():
        # skip groups already covered by an existing pagination key
        if any(key.lower() in group_name.lower() for key in updated):
            continue
        # skip npz sources, which use the passthrough pagination case
        if all(
            str(source["source_path"].suffix).lower() == ".npz"
            for source in source_group_vals
        ):
            continue
        page_key = _detect_source_page_key(
            source=source_group_vals[0],
            group_name=group_name,
            compartments=compartments,
        )
        updated[pathlib.Path(group_name).stem.lower()] = page_key
        logger.info(
            "Inferred pagination key '%s' for source group '%s'.",
            page_key,
            group_name,
        )

    return updated


def _detect_source_page_key(
    *,
    source: Dict[str, Any],
    group_name: str,
    compartments: Optional[Sequence[str]] = None,
) -> str:
    """
    Detect a numeric pagination key column for a source table group.

    Args:
        source (Dict[str, Any]):
            A source table entry from the group to detect a key for.
        group_name (str):
            The source group name the source belongs to.
        compartments (Optional[Sequence[str]]):
            Compartment source table names.

    Returns:
        str:
            The detected pagination key column name.

    Raises:
        CytoTableException:
            Raised when no numeric pagination key column can be
            detected for the source.
    """

    from cytotable.convert import (  # pylint: disable=import-outside-toplevel
        _get_table_columns_and_types,
    )

    columns = _get_table_columns_and_types(
        source=dict(source), sort_output=True
    ).result()

    if not columns or columns[0] is None:
        raise CytoTableException(
            f"Unable to infer a pagination key for source group "
            f"'{group_name}': the source table could not be inspected. "
            "Please provide pagination keys through the 'page_keys' "
            "parameter."
        )

    # map lowercased column names to original column names
    lowered = {
        str(column["column_name"]).lower(): str(column["column_name"])
        for column in columns
        if column is not None
    }
    # map lowercased column names to duckdb storage types
    dtypes = {
        str(column["column_name"]).lower(): str(column["column_dtype"])
        for column in columns
        if column is not None
    }
    # only numeric columns are eligible for keyset pagination
    numeric = {
        name
        for name, dtype in dtypes.items()
        if any(token in dtype.upper() for token in _NUMERIC_TYPE_TOKENS)
    }

    compartment, _ = _classify_group(
        stem=pathlib.Path(group_name).stem.lower(),
        metadata=None,
        compartments=compartments,
    )
    candidates = [
        # compartment-prefixed object number, for ex.
        # "Cells_Number_Object_Number"
        (
            (),
            (
                re.compile(
                    _COMPARTMENT_OBJECTNUMBER_TEMPLATE.format(compartment=compartment)
                )
                if compartment is not None
                else None
            ),
        ),
        # generic object number names, for ex. "ObjectNumber"
        (_OBJECTNUMBER_NAMES, None),
        # image and table number keys
        (_IMAGE_KEY_NAMES, _STEM_IMAGE_KEY_REGEX),
        (_TABLENUMBER_KEY_NAMES, _STEM_TABLENUMBER_REGEX),
    ]
    for exact_names, regex in candidates:
        page_key = _pick_column(
            lowered=lowered,
            exact_names=exact_names,
            regex=regex,
            allowed=numeric,
        )
        if page_key is not None:
            return page_key

    # fall back to the first numeric column, if one exists
    for name in sorted(numeric):
        return lowered[name]

    raise CytoTableException(
        f"Unable to infer a pagination key for source group "
        f"'{group_name}': no numeric key column was detected. Please "
        "provide pagination keys through the 'page_keys' parameter."
    )

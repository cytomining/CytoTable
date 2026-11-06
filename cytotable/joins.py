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
    numeric_columns: frozenset[str] = frozenset()
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

    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.parquet as parquet  # pylint: disable=import-outside-toplevel

    tables = []
    for group_name, source_group_vals in sources.items():
        stem = pathlib.Path(group_name).stem.lower()
        paths = tuple(str(path) for path in source_group_vals[0]["table"])
        schema = parquet.read_schema(paths[0])
        columns = tuple(schema.names)
        numeric_columns = frozenset(
            name
            for name, field_type in zip(schema.names, schema.types)
            if pa.types.is_integer(field_type) or pa.types.is_floating(field_type)
        )
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
                numeric_columns=numeric_columns,
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
            Candidate edges, unvalidated and not yet deduplicated by
            table pair (a table pair may have more than one candidate
            reference column).
    """

    table_by_compartment: Dict[str, List[_TableInfo]] = {}
    for table in tables:
        if table.compartment:
            table_by_compartment.setdefault(table.compartment, []).append(table)
    table_by_alias = {table.alias: table for table in tables}

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
                table_by_alias=table_by_alias,
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

    return sorted(
        edges, key=lambda edge: (edge.child, edge.parent, edge.reference_column)
    )


def _find_referenced_table(
    *,
    target: str,
    table_by_alias: Dict[str, _TableInfo],
    table_by_compartment: Dict[str, List[_TableInfo]],
) -> Optional[_TableInfo]:
    """
    Find the table which a parent reference column points to.

    An exact alias match (for ex. a table whose group name stem is
    literally the target, such as ``'per_cells'`` for target
    ``'cells'``) is preferred. Otherwise, the target must resolve to
    exactly one compartment-classified table; a target which matches
    more than one table's compartment (for ex. both ``cells`` and
    ``cells_summary`` classifying as compartment ``cells``) is
    ambiguous and is not resolved, so the reference is not used.

    Args:
        target (str):
            Lowercased compartment name referenced by the column.
        table_by_alias (Dict[str, _TableInfo]):
            Tables keyed by alias.
        table_by_compartment (Dict[str, List[_TableInfo]]):
            Tables keyed by lowercased compartment name.

    Returns:
        Optional[_TableInfo]:
            The referenced table, if exactly one is detected.
    """

    if target in table_by_alias:
        return table_by_alias[target]
    # fall back to alias matching for tables whose group name is
    # compartment-prefixed, for ex. 'per_cells' referencing 'cells'
    alias_matches = [
        table for table in table_by_alias.values() if table.alias.endswith(f"_{target}")
    ]
    if len(alias_matches) == 1:
        return alias_matches[0]

    candidates = table_by_compartment.get(target, [])
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.info(
            "Ambiguous parent reference target '%s': multiple tables "
            "(%s) classify as this compartment. Skipping this reference "
            "rather than guessing.",
            target,
            ", ".join(sorted(table.alias for table in candidates)),
        )
    return None


def _validate_edges(
    *, edges: List[_Edge], tables: Dict[str, _TableInfo]
) -> List[_Edge]:
    """
    Validate candidate edges against the actual table data, then keep
    the best-matching edge for each (child, parent) table pair.

    Measures the fraction of child rows whose parent reference matches
    an existing parent object. Edges which match nothing are rejected
    before deduplication, so a table pair with multiple candidate
    reference columns is not resolved by an arbitrary (and possibly
    invalid) column picked before validation. Edges with low match
    rates are kept with a warning. Edges where any child row matches
    more than one parent row are rejected, since such a reference
    would multiply rows or produce ambiguous parent associations.

    Args:
        edges (List[_Edge]):
            Candidate join edges.
        tables (Dict[str, _TableInfo]):
            Table information by alias.

    Returns:
        List[_Edge]:
            At most one validated edge per (child, parent) table pair.

    Raises:
        CytoTableException:
            Raised when a candidate edge references duplicate parent
            keys, which would produce ambiguous or multiplied rows.
    """

    validated = []
    for edge in edges:
        child_rows, matched_rows, max_parent_matches = _measure_edge_match(
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
        if max_parent_matches > 1:
            raise CytoTableException(
                f"Inferred join edge '{edge.child}'.'{edge.reference_column}' "
                f"-> '{edge.parent}' matches more than one '{edge.parent}' row "
                "for at least one child row, which would produce ambiguous "
                "or multiplied results. Please provide join SQL manually "
                "through the 'joins' parameter."
            )
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
        validated.append((edge, match_rate))

    # keep only the best-matching edge for each (child, parent) pair,
    # now that every candidate has been validated against real data
    best_by_pair: Dict[Tuple[str, str], Tuple[_Edge, float]] = {}
    for edge, match_rate in validated:
        pair = (edge.child, edge.parent)
        current = best_by_pair.get(pair)
        if current is None or match_rate > current[1]:
            best_by_pair[pair] = (edge, match_rate)
        elif (
            match_rate == current[1]
            and edge.reference_column < current[0].reference_column
        ):
            best_by_pair[pair] = (edge, match_rate)

    return [best_by_pair[pair][0] for pair in sorted(best_by_pair)]


def _measure_edge_match(
    *, edge: _Edge, child: _TableInfo, parent: _TableInfo
) -> Tuple[int, int, int]:
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
        Tuple[int, int, int]:
            The total child row count, the count of child rows which
            reference an existing parent object, and the maximum
            number of parent rows a single child row matches (used to
            detect ambiguous references which would multiply rows).
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
            ) AS matched_rows,
            MAX(
                (
                    SELECT COUNT(*)
                    FROM {_paths_sql(parent.paths)} AS "{parent.alias}"
                    WHERE {" AND ".join(conditions)}
                )
            ) AS max_parent_matches
        FROM {_paths_sql(child.paths)} AS "{child.alias}"
    """

    with _duckdb_reader() as ddb_reader:
        result = ddb_reader.execute(query).fetchone() or (0, 0, 0)

    return int(result[0]), int(result[1]), int(result[2] or 0)


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

    # attach metadata tables to the base table through shared keys;
    # compartment tables must be joined through a validated parent
    # reference edge above (object-level relationship) and are never
    # attached by shared image/table keys alone, since that would
    # cross-join every object pair sharing an image (see
    # cytomining/CytoTable#522 review discussion)
    for table in tables:
        if table.alias in joined or not table.is_metadata:
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

    # prefer the object number key, but only when it is numeric
    # (required for keyset pagination); fall back to the numeric
    # image key otherwise
    page_key = (
        base.objectnumber_key
        if base.objectnumber_key is not None
        and base.objectnumber_key in base.numeric_columns
        else None
    ) or (
        base.image_key
        if base.image_key is not None and base.image_key in base.numeric_columns
        else None
    )
    if page_key is None:
        raise CytoTableException(
            f"Unable to determine a join pagination key for base table "
            f"'{base.alias}': no numeric object number or image number "
            "column was detected. Please provide join SQL manually "
            "through the 'joins' parameter."
        )

    return sql, page_key


def _sql_string_literal(value: str) -> str:
    """
    Render a value as a DuckDB SQL string literal, escaping apostrophes.

    Args:
        value (str):
            The value to render.

    Returns:
        str:
            A quoted SQL string literal, with embedded apostrophes
            doubled so DuckDB does not misread them as closing the
            literal (which would otherwise be parsed as an identifier).
    """

    return "'" + value.replace("'", "''") + "'"


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
        return f"read_parquet({_sql_string_literal(paths[0])})"
    return "read_parquet([{}])".format(
        ", ".join(_sql_string_literal(path) for path in paths)
    )


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

    Preserve explicit keys. For each remaining group, select a key
    which is numeric and free of null values across every source file
    in the group, preferring object, image, and table numbers, then
    any other common numeric column.

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
    # the 'join' entry is reserved for the final joined-table
    # pagination key and never refers to a raw source group
    explicit_keys = [key for key in updated if key.lower() != "join"]
    for group_name, source_group_vals in sources.items():
        # skip groups already covered by an existing pagination key
        if any(key.lower() in group_name.lower() for key in explicit_keys):
            continue
        # skip npz sources, which use the passthrough pagination case
        if all(
            str(source["source_path"].suffix).lower() == ".npz"
            for source in source_group_vals
        ):
            continue
        page_key = _detect_source_page_key(
            sources=source_group_vals,
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


def _raw_source_select_sql(source: Dict[str, Any]) -> str:
    """
    Render a DuckDB FROM-clause source for a raw (unconverted) source.

    Args:
        source (Dict[str, Any]):
            A source table entry with a ``source_path`` and, for
            SQLite sources, a ``table_name``.

    Returns:
        str:
            A DuckDB table function expression reading the source.
    """

    source_path = source["source_path"]
    if str(source_path.suffix).lower() == ".csv":
        return f"read_csv_auto({_sql_string_literal(str(source_path))})"
    return (
        f"sqlite_scan({_sql_string_literal(str(source_path))}, "
        f"{_sql_string_literal(str(source['table_name']))})"
    )


def _column_has_nulls(*, sources: List[Dict[str, Any]], column: str) -> bool:
    """
    Check whether a column contains null values in any of the sources.

    Args:
        sources (List[Dict[str, Any]]):
            Raw source table entries to check.
        column (str):
            The column name to check for null values.

    Returns:
        bool:
            True if the column contains a null value in any source,
            or if the column could not be queried (treated as unsafe
            to use for pagination rather than silently dropping rows).
    """

    from cytotable.utils import (  # pylint: disable=import-outside-toplevel
        _duckdb_reader,
    )

    for source in sources:
        query = (
            f"SELECT 1 FROM {_raw_source_select_sql(source)} "
            f'WHERE "{column}" IS NULL LIMIT 1'
        )
        try:
            with _duckdb_reader() as ddb_reader:
                if ddb_reader.execute(query).fetchone() is not None:
                    return True
        except Exception:  # pylint: disable=broad-exception-caught
            # treat an unqueryable column as unsafe rather than assuming
            # it is null-free
            return True
    return False


def _detect_source_page_key(
    *,
    sources: List[Dict[str, Any]],
    group_name: str,
    compartments: Optional[Sequence[str]] = None,
) -> str:
    """
    Detect a numeric, null-free pagination key column for a source group.

    The key must be present, numeric, and free of null values in
    every source file within the group: keyset pagination filters rows
    with ``WHERE page_key BETWEEN ...``, which silently excludes rows
    with a null key, and a column present in only some files would
    fail to query for the others.

    Args:
        sources (List[Dict[str, Any]]):
            All source table entries within the group to detect a key
            for.
        group_name (str):
            The source group name the sources belong to.
        compartments (Optional[Sequence[str]]):
            Compartment source table names.

    Returns:
        str:
            The detected pagination key column name.

    Raises:
        CytoTableException:
            Raised when no numeric, null-free pagination key column
            common to every source file can be detected for the group.
    """

    from cytotable.convert import (  # pylint: disable=import-outside-toplevel
        _get_table_columns_and_types,
    )

    # gather per-source numeric columns, then intersect so only
    # columns present (and numeric) in every file remain eligible
    common_lowered: Dict[str, str] = {}
    common_numeric: set[str] = set()
    for index, source in enumerate(sources):
        columns = _get_table_columns_and_types(
            source=dict(source), sort_output=True
        ).result()
        if not columns or columns[0] is None:
            raise CytoTableException(
                f"Unable to infer a pagination key for source group "
                f"'{group_name}': the source table could not be "
                "inspected. Please provide pagination keys through "
                "the 'page_keys' parameter."
            )
        lowered = {
            str(column["column_name"]).lower(): str(column["column_name"])
            for column in columns
            if column is not None
        }
        dtypes = {
            str(column["column_name"]).lower(): str(column["column_dtype"])
            for column in columns
            if column is not None
        }
        numeric = {
            name
            for name, dtype in dtypes.items()
            if any(token in dtype.upper() for token in _NUMERIC_TYPE_TOKENS)
        }
        if index == 0:
            common_lowered, common_numeric = lowered, numeric
        else:
            common_lowered = {
                name: original
                for name, original in common_lowered.items()
                if name in lowered
            }
            common_numeric &= numeric

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
    tried = []
    for exact_names, regex in candidates:
        page_key = _pick_column(
            lowered=common_lowered,
            exact_names=exact_names,
            regex=regex,
            allowed=common_numeric,
        )
        if page_key is None:
            continue
        tried.append(page_key)
        if not _column_has_nulls(sources=sources, column=page_key):
            return page_key

    # fall back to any other common numeric column
    for name in sorted(common_numeric):
        page_key = common_lowered[name]
        if page_key in tried:
            continue
        tried.append(page_key)
        if not _column_has_nulls(sources=sources, column=page_key):
            return page_key

    raise CytoTableException(
        f"Unable to infer a pagination key for source group "
        f"'{group_name}': no numeric column free of null values and "
        "common to every source file in the group was detected "
        f"(considered: {', '.join(tried) if tried else 'none'}). Please "
        "provide pagination keys through the 'page_keys' parameter."
    )

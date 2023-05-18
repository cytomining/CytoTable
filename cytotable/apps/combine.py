"""
cytotable.apps.combine : work related to combining table data together (concatenation, joins, etc)
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from parsl.app.app import python_app

logger = logging.getLogger(__name__)


@python_app
def _concat_source_group(
    source_group_name: str,
    source_group: List[Dict[str, Any]],
    dest_path: str = ".",
    common_schema: Optional[List[Tuple[str, str]]] = None,
) -> List[Dict[str, Any]]:
    """
    Concatenate group of source data together as single file.

    For a reference to data concatenation within Arrow see the following:
    https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html

    Notes: this function presumes a multi-directory, multi-file common data
    structure for compartments and other data. For example:

    Source (file tree):

    .. code-block:: bash

        root
        ├── subdir_1
        │  └── Cells.csv
        └── subdir_2
            └── Cells.csv


    Becomes:

    .. code-block:: python

        # earlier data read into parquet chunks from multiple
        # data source files.
        read_data = [
            {"table": ["cells-1.parquet", "cells-2.parquet"]},
            {"table": ["cells-1.parquet", "cells-2.parquet"]},
        ]

        # focus of this function
        concatted = [{"table": ["cells.parquet"]}]


    Args:
        source_group_name: str
            Name of data source source group (for common compartments, etc).
        source_group: List[Dict[str, Any]]:
            Data structure containing grouped data for concatenation.
        dest_path: Optional[str] (Default value = None)
            Optional destination path for concatenated sources.
        common_schema: List[Tuple[str, str]] (Default value = None)
            Common schema to use for concatenation amongst arrow tables
            which may have slightly different but compatible schema.

    Returns:
        List[Dict[str, Any]]
            Updated dictionary containing concatenated sources.
    """

    import pathlib

    import pyarrow as pa
    import pyarrow.parquet as parquet

    from cytotable.exceptions import SchemaException

    # check whether we already have a file as dest_path
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink(missing_ok=True)

    # build a result placeholder
    concatted: List[Dict[str, Any]] = [
        {
            # source path becomes parent's parent dir with the same filename
            "source_path": pathlib.Path(
                (
                    f"{source_group[0]['source_path'].parent.parent}"
                    f"/{source_group[0]['source_path'].stem}"
                )
            )
        }
    ]

    # build destination path for file to land
    destination_path = pathlib.Path(
        (
            f"{dest_path}/{str(pathlib.Path(source_group_name).stem).lower()}/"
            f"{str(pathlib.Path(source_group_name).stem).lower()}.parquet"
        )
    )

    # if there's already a file remove it
    destination_path.unlink(missing_ok=True)

    # ensure the parent directories exist:
    destination_path.parent.mkdir(parents=True, exist_ok=True)

    # build the schema for concatenation writer
    writer_schema = pa.schema(common_schema)

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    with parquet.ParquetWriter(str(destination_path), writer_schema) as writer:
        for source in source_group:
            for table in [table for table in source["table"]]:
                # if we haven't inferred the common schema
                # check that our file matches the expected schema, otherwise raise an error
                if common_schema is None and not writer_schema.equals(
                    parquet.read_schema(table)
                ):
                    raise SchemaException(
                        (
                            f"Detected mismatching schema for target concatenation group members:"
                            f" {str(source_group[0]['table'])} and {str(table)}"
                        )
                    )

                # read the file from the list and write to the concatted parquet file
                # note: we pass column order based on the first chunk file to help ensure schema
                # compatibility for the writer
                writer.write_table(parquet.read_table(table, schema=writer_schema))
                # remove the file which was written in the concatted parquet file (we no longer need it)
                pathlib.Path(table).unlink()

            # attempt to clean up dir containing original table(s) only if it's empty
            try:
                pathlib.Path(pathlib.Path(source["table"][0]).parent).rmdir()
            except OSError as os_err:
                # raise only if we don't have a dir not empty errno
                if os_err.errno != 66:
                    raise

    # return the concatted parquet filename
    concatted[0]["table"] = [destination_path]

    return concatted


@python_app
def _get_join_chunks(
    sources: Dict[str, List[Dict[str, Any]]],
    metadata: Union[List[str], Tuple[str, ...]],
    chunk_columns: Union[List[str], Tuple[str, ...]],
    chunk_size: int,
) -> List[List[Dict[str, Any]]]:
    """
    Build groups of join keys for later join operations

    Args:
        sources: Dict[List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        chunk_columns: Union[List[str], Tuple[str, ...]]:
            Column names which appear in all compartments to use when performing join
        chunk_size: int:
            Size of join chunks which is used to limit data size during join ops

    Returns:
        List[List[Dict[str, Any]]]]:
            A list of lists with at most chunk size length that contain join keys
    """

    import pathlib

    import pyarrow.parquet as parquet

    # fetch the compartment concat result as the basis for join groups
    for key, source in sources.items():
        if any(name.lower() in pathlib.Path(key).stem.lower() for name in metadata):
            first_result = source
            break

    # gather the workflow result for basis if it's not yet returned
    basis = first_result

    # read only the table's chunk_columns
    join_column_rows = parquet.read_table(
        source=basis[0]["table"], columns=list(chunk_columns)
    ).to_pylist()

    # build and return the chunked join column rows
    return [
        join_column_rows[i : i + chunk_size]
        for i in range(0, len(join_column_rows), chunk_size)
    ]


@python_app
def _join_source_chunk(
    sources: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    joins: str,
    join_group: List[Dict[str, Any]],
    drop_null: bool,
) -> str:
    """
    Join sources based on join group keys (group of specific join column values)

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        dest_path: str:
            Destination path to write file-based content.
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join
            operations using the join_group keys as a reference.
        join_group: List[Dict[str, Any]]:
            Group of joinable keys to be used as "chunked" filter
            of overall dataset.
        drop_null: bool:
            Whether to drop rows with null values within the resulting
            joined data.

    Returns:
        str
            Path to joined file which is created as a result of this function.
    """

    import pathlib
    import uuid

    import pyarrow.parquet as parquet

    from cytotable.utils import _column_sort, _duckdb_reader

    # replace with real location of sources for join sql
    for key, val in sources.items():
        if pathlib.Path(key).stem.lower() in joins.lower():
            joins = joins.replace(
                f"'{str(pathlib.Path(key).stem.lower())}.parquet'",
                str([str(table) for table in val[0]["table"]]),
            )

    # update the join groups to include unique values per table
    updated_join_group = []
    for key in sources.keys():
        updated_join_group.extend(
            [
                {
                    f"{str(pathlib.Path(key).stem)}.{join_key}": val
                    for join_key, val in chunk.items()
                }
                for chunk in join_group
            ]
        )

    # form where clause for sql joins to filter the results
    joins += (
        "WHERE ("
        + ") OR (".join(
            [
                " AND ".join(
                    [
                        # create groups of join column filters where values always
                        # are expected to equal those within the join_group together
                        f"{join_column} = {join_column_value}"
                        if not isinstance(join_column_value, str)
                        # account for string values
                        else (f"{join_column} = " f"'{join_column_value}'")
                        for join_column, join_column_value in chunk.items()
                    ]
                )
                for chunk in updated_join_group
            ]
        )
        + ")"
    )

    # perform compartment joins using duckdb over parquet files
    result = _duckdb_reader().execute(joins).arrow()

    # drop nulls if specified
    if drop_null:
        result = result.drop_null()

    # account for duplicate column names from joins
    cols = []
    # reversed order column check as col removals will change index order
    for i, colname in reversed(list(enumerate(result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            result = result.remove_column(i)

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    result = result.select(sorted(sorted(result.column_names), key=_column_sort))

    result_file_path = (
        # store the result in the parent of the dest_path
        f"{str(pathlib.Path(dest_path).parent)}/"
        # use the dest_path stem in the name
        f"{str(pathlib.Path(dest_path).stem)}-"
        # give the join chunk result a unique to arbitrarily
        # differentiate from other chunk groups which are mapped
        # and before they are brought together as one dataset
        f"{str(uuid.uuid4().hex)}.parquet"
    )

    # write the result
    parquet.write_table(
        table=result,
        where=result_file_path,
    )

    return result_file_path


@python_app
def _concat_join_sources(
    sources: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    join_sources: List[str],
) -> str:
    """
    Concatenate join sources from parquet-based chunks.

    For a reference to data concatenation within Arrow see the following:
    https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        dest_path: str:
            Destination path to write file-based content.
        join_sources: List[str]:
            List of local filepath destination for join source chunks
            which will be concatenated.

    Returns:
        str
            Path to concatenated file which is created as a result of this function.
    """

    import itertools
    import pathlib
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as parquet

    # remove the unjoined concatted compartments to prepare final dest_path usage
    # (we now have joined results)
    flattened_sources = list(itertools.chain(*list(sources.values())))
    for source in flattened_sources:
        for table in source["table"]:
            pathlib.Path(table).unlink(missing_ok=True)

    # remove dir if we have it
    if pathlib.Path(dest_path).is_dir():
        shutil.rmtree(path=dest_path)

    # also remove any pre-existing files which may already be at file destination
    pathlib.Path(dest_path).unlink(missing_ok=True)

    # write the concatted result as a parquet file
    parquet.write_table(
        table=pa.concat_tables(
            tables=[parquet.read_table(table_path) for table_path in join_sources]
        ),
        where=dest_path,
    )

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    writer_schema = parquet.read_schema(join_sources[0])
    with parquet.ParquetWriter(str(dest_path), writer_schema) as writer:
        for table_path in join_sources:
            writer.write_table(parquet.read_table(table_path, schema=writer_schema))
            # remove the file which was written in the concatted parquet file (we no longer need it)
            pathlib.Path(table_path).unlink()

    # return modified sources format to indicate the final result
    # and retain the other source data for reference as needed
    return dest_path


@python_app
def _infer_source_group_common_schema(
    source_group: List[Dict[str, Any]]
) -> List[Tuple[str, str]]:
    """
    Infers a common schema for group of parquet files which may have
    similar but slightly different schema or data. Intended to assist with
    data concatenation and other operations.

    Args:
        source_group: List[Dict[str, Any]]:
            Group of one or more data sources which includes metadata about
            path to parquet data.

    Returns:
        List[Tuple[str, str]]
            A list of tuples which includes column name and PyArrow datatype.
            This data will later be used as the basis for forming a PyArrow schema.
    """

    import pyarrow as pa
    import pyarrow.parquet as parquet

    from cytotable.exceptions import SchemaException

    # read first file for basis of schema and column order for all others
    common_schema = parquet.read_schema(source_group[0]["table"][0])

    # infer common basis of schema and column order for all others
    for schema in [
        parquet.read_schema(table)
        for source in source_group
        for table in source["table"]
    ]:
        # account for completely equal schema
        if schema.equals(common_schema):
            continue

        # gather field names from schema
        schema_field_names = [item.name for item in schema]

        # reversed enumeration because removing indexes ascendingly changes schema field order
        for index, field in reversed(list(enumerate(common_schema))):
            # check whether field name is contained within writer basis, remove if not
            # note: because this only checks for naming, we defer to initially detected type
            if field.name not in schema_field_names:
                common_schema = common_schema.remove(index)

            # check if we have a nulltype and non-nulltype conflict, deferring to non-nulltype
            elif pa.types.is_null(field.type) and not pa.types.is_null(
                schema.field(field.name).type
            ):
                common_schema = common_schema.set(
                    index, field.with_type(schema.field(field.name).type)
                )

            # check if we have an integer to float challenge and enable later casting
            elif pa.types.is_integer(field.type) and pa.types.is_floating(
                schema.field(field.name).type
            ):
                common_schema = common_schema.set(index, field.with_type(pa.float64()))

    if len(list(common_schema.names)) == 0:
        raise SchemaException(
            (
                "No common schema basis to perform concatenation for source group."
                " All columns mismatch one another within the group."
            )
        )

    # return a python-native list of tuples with column names and str types
    return list(
        zip(
            common_schema.names,
            [str(schema_type) for schema_type in common_schema.types],
        )
    )

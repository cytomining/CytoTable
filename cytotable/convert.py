"""
CytoTable: convert - transforming data for use with pyctyominer.
"""


import itertools
import logging
import uuid
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

import parsl
import pyarrow as pa
from parsl.app.app import join_app, python_app

from cytotable.presets import config
from cytotable.utils import _column_sort, _default_parsl_config

logger = logging.getLogger(__name__)


@python_app
def _get_table_chunk_offsets(
    source: Dict[str, Any],
    chunk_size: int,
) -> Union[List[int], None]:
    """
    Get table data chunk offsets for later use in capturing segments
    of values. This work also provides a chance to catch problematic
    input data which will be ignored with warnings.

    Args:
        source: Dict[str, Any]
            Contains the source data to be chunked. Represents a single
            file or table of some kind.
        chunk_size: int
            The size in rowcount of the chunks to create

    Returns:
        List[int]
            List of integers which represent offsets to use for reading
            the data later on.
    """

    import logging
    import pathlib

    import duckdb
    from cloudpathlib import AnyPath

    from cytotable.exceptions import NoInputDataException
    from cytotable.utils import _duckdb_reader

    logger = logging.getLogger(__name__)

    table_name = source["table_name"] if "table_name" in source.keys() else None
    source_path = source["source_path"]
    source_type = str(pathlib.Path(source_path).suffix).lower()

    try:
        # for csv's, check that we have more than one row (a header and data values)
        if (
            source_type == ".csv"
            and sum(1 for _ in AnyPath(source_path).open("r")) <= 1
        ):
            raise NoInputDataException(
                f"Data file has 0 rows of values. Error in file: {source_path}"
            )

        # gather the total rowcount from csv or sqlite data input sources
        rowcount = int(
            _duckdb_reader()
            .execute(
                # nosec
                f"SELECT COUNT(*) from read_csv_auto('{source_path}')"
                if source_type == ".csv"
                else f"SELECT COUNT(*) from sqlite_scan('{source_path}', '{table_name}')"
            )
            .fetchone()[0]
        )

    # catch input errors which will result in skipped files
    except (duckdb.InvalidInputException, NoInputDataException) as invalid_input_exc:
        logger.warning(
            msg=f"Skipping file due to input file errors: {str(invalid_input_exc)}"
        )

        return None

    return list(
        range(
            0,
            # gather rowcount from table and use as maximum for range
            rowcount,
            # step through using chunk size
            chunk_size,
        )
    )


@python_app
def _source_chunk_to_parquet(
    source_group_name: str,
    source: Dict[str, Any],
    chunk_size: int,
    offset: int,
    dest_path: str,
) -> str:
    """
    Export source data to chunked parquet file using chunk size and offsets.

    Args:
        source_group_name: str
            Name of the source group (for ex. compartment or metadata table name)
        source: Dict[str, Any]
            Contains the source data to be chunked. Represents a single
            file or table of some kind along with collected information about table.
        chunk_size: int
            Row count to use for chunked output
        offset: int
            The offset for chunking the data from source.
        dest_path: str
            Path to store the output data.

    Returns:
        str
            A string of the output filepath
    """

    import pathlib

    from cloudpathlib import AnyPath

    from cytotable.utils import _duckdb_reader

    # attempt to build dest_path
    source_dest_path = (
        f"{dest_path}/{str(pathlib.Path(source_group_name).stem).lower()}/"
        f"{str(pathlib.Path(source['source_path']).parent.name).lower()}"
    )
    pathlib.Path(source_dest_path).mkdir(parents=True, exist_ok=True)

    # build output query and filepath base
    # (chunked output will append offset to keep output paths unique)
    if str(AnyPath(source["source_path"]).suffix).lower() == ".csv":
        base_query = f"""SELECT * from read_csv_auto('{str(source["source_path"])}')"""
        result_filepath_base = f"{source_dest_path}/{str(source['source_path'].stem)}"
    elif str(AnyPath(source["source_path"]).suffix).lower() == ".sqlite":
        base_query = f"""
                SELECT * from sqlite_scan('{str(source["source_path"])}', '{str(source["table_name"])}')
                """
        result_filepath_base = f"{source_dest_path}/{str(source['source_path'].stem)}.{source['table_name']}"

    result_filepath = f"{result_filepath_base}-{offset}.parquet"

    # isolate using new connection to read data with chunk size + offset
    # and export directly to parquet via duckdb (avoiding need to return data to python)
    _duckdb_reader().execute(
        f"""
        COPY (
            {base_query}
            LIMIT {chunk_size} OFFSET {offset}
        ) TO '{result_filepath}'
        (FORMAT PARQUET);
        """
    )

    # return the filepath for the chunked output file
    return result_filepath


@python_app
def _prepend_column_name(
    table_path: str,
    source_group_name: str,
    identifying_columns: List[str],
    metadata: Union[List[str], Tuple[str, ...]],
    compartments: List[str],
) -> str:
    """
    Rename columns using the source group name, avoiding identifying columns.

    Notes:
    * A source_group_name represents a filename referenced as part of what
    is specified within targets.

    Args:
        table_path: str:
            Path to a parquet file which will be modified.
        source_group_name: str:
            Name of data source source group (for common compartments, etc).
        identifying_columns: List[str]:
            Column names which are used as ID's and as a result need to be
            treated differently when renaming.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        compartments: List[str]:
            List of source data names which are used as compartments

    Returns:
        str
            Path to the modified file
    """

    import pathlib

    import pyarrow.parquet as parquet

    targets = tuple(metadata) + tuple(compartments)

    table = parquet.read_table(source=table_path)

    # stem of source group name
    # for example:
    #   targets: ['cytoplasm']
    #   source_group_name: 'Per_Cytoplasm.sqlite'
    #   source_group_name_stem: 'Cytoplasm'
    source_group_name_stem = targets[
        # return first result from generator below as index to targets
        next(
            i
            for i, val in enumerate(targets)
            # compare if value from targets in source_group_name stem
            if val.lower() in str(pathlib.Path(source_group_name).stem).lower()
        )
        # capitalize the result
    ].capitalize()

    # capture updated column names as new variable
    updated_column_names = []

    for column_name in table.column_names:
        # if-condition for prepending source_group_name_stem to column name
        # where colname is not in identifying_columns parameter values
        # and where the column is not already prepended with source_group_name_stem
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'AreaShape_Area'
        #   updated_column_name: 'Cells_AreaShape_Area'
        if column_name not in identifying_columns and not column_name.startswith(
            source_group_name_stem.capitalize()
        ):
            updated_column_names.append(f"{source_group_name_stem}_{column_name}")
        # if-condition for prepending 'Metadata_' to column name
        # where colname is in identifying_columns parameter values
        # and where the column is already prepended with source_group_name_stem
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'Cells_Number_Object_Number'
        #   updated_column_name: 'Metadata_Cells_Number_Object_Number'
        elif column_name in identifying_columns and column_name.startswith(
            source_group_name_stem.capitalize()
        ):
            updated_column_names.append(f"Metadata_{column_name}")
        # if-condition for prepending 'Metadata' and source_group_name_stem to column name
        # where colname is in identifying_columns parameter values
        # and where the colname does not already start with 'Metadata_'
        # and colname not in metadata list
        # and colname does not include 'ObjectNumber'
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'Parent_Nuclei'
        #   updated_column_name: 'Metadata_Cells_Parent_Nuclei'
        elif (
            column_name in identifying_columns
            and not column_name.startswith("Metadata_")
            and not any(item.capitalize() in column_name for item in metadata)
            and not "ObjectNumber" in column_name
        ):
            updated_column_names.append(
                f"Metadata_{source_group_name_stem}_{column_name}"
            )
        # if-condition for prepending 'Metadata' to column name
        # where colname doesn't already start with 'Metadata_'
        # and colname is in identifying_columns parameter values
        # for example:
        #   column_name: 'ObjectNumber'
        #   updated_column_name: 'Metadata_ObjectNumber'
        elif (
            not column_name.startswith("Metadata_")
            and column_name in identifying_columns
        ):
            updated_column_names.append(f"Metadata_{column_name}")
        # else we add the existing colname to the updated list as-is
        else:
            updated_column_names.append(column_name)

    # perform table column name updates
    parquet.write_table(
        table=table.rename_columns(updated_column_names), where=table_path
    )

    return table_path


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

    import pyarrow.parquet as parquet

    from cytotable.utils import _duckdb_reader

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

    import pathlib
    import shutil

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


@python_app
def _return_future(input: Any) -> Any:
    """
    This is a simple wrapper python_app to allow
    the return of join_app-compliant output (must be a Parsl future)

    Args:
        input: Any
            Any input which will be used within the context of a
            Parsl join_app future return.

    Returns:
        Any
            Returns the input as provided wrapped within the context
            of a python_app for the purpose of a join_app.
    """

    return input


@join_app
def _to_parquet(  # pylint: disable=too-many-arguments, too-many-locals
    source_path: str,
    dest_path: str,
    source_datatype: Optional[str],
    metadata: Union[List[str], Tuple[str, ...]],
    compartments: Union[List[str], Tuple[str, ...]],
    identifying_columns: Union[List[str], Tuple[str, ...]],
    concat: bool,
    join: bool,
    joins: Optional[str],
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
    chunk_size: Optional[int],
    infer_common_schema: bool,
    drop_null: bool,
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
    """
    Export data to parquet.

    Args:
        source_path: str:
            str reference to read source files from.
            Note: may be local or remote object-storage
            location using convention "s3://..." or similar.
        dest_path: str:
            Path to write files to.
            Note: this may only be a local path.
        source_datatype: Optional[str]: (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, ...]]: (Default value = None)
            Compartment names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be
            ignored with regards to renaming.
        concat: bool:
            Whether to concatenate similar files together.
        join: bool:
            Whether to join the compartment data together into one dataset
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
            Column names which appear in all compartments to use when performing join
        chunk_size: Optional[int],
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool:  (Default value = True)
            Whether to infer a common schema when concatenating sources.
        drop_null: bool:
            Whether to drop null results.
        **kwargs: Any:
            Keyword args used for gathering source data, primarily relevant for
            Cloudpathlib cloud-based client configuration.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]:
            Grouped sources which include metadata about destination filepath
            where parquet file was written or a string filepath for the joined
            result.
    """

    import pathlib

    from cytotable.convert import (
        _concat_join_sources,
        _concat_source_group,
        _get_join_chunks,
        _get_table_chunk_offsets,
        _infer_source_group_common_schema,
        _join_source_chunk,
        _prepend_column_name,
        _return_future,
        _source_chunk_to_parquet,
    )
    from cytotable.sources import _gather_sources

    # gather sources to be processed
    sources = _gather_sources(
        source_path=source_path,
        source_datatype=source_datatype,
        targets=list(metadata) + list(compartments),
        **kwargs,
    ).result()

    # if we already have a file in dest_path, remove it
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink()

    # prepare offsets for chunked data export from source tables
    offsets_prepared = {
        source_group_name: [
            dict(
                source,
                **{
                    "offsets": _get_table_chunk_offsets(
                        source=source,
                        chunk_size=chunk_size,
                    ).result()
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in sources.items()
    }

    # if offsets is none and we haven't halted, remove the file as there
    # were input formatting errors which will create challenges downstream
    invalid_files_dropped = {
        source_group_name: [
            # ensure we have offsets
            source
            for source in source_group_vals
            if source["offsets"] is not None
        ]
        for source_group_name, source_group_vals in offsets_prepared.items()
        # ensure we have source_groups with at least one source table
        if len(source_group_vals) > 0
    }

    results = {
        source_group_name: [
            dict(
                source,
                **{
                    "table": [
                        # perform column renaming and create potential return result
                        _prepend_column_name(
                            # perform chunked data export to parquet using offsets
                            table_path=_source_chunk_to_parquet(
                                source_group_name=source_group_name,
                                source=source,
                                chunk_size=chunk_size,
                                offset=offset,
                                dest_path=dest_path,
                            ),
                            source_group_name=source_group_name,
                            identifying_columns=identifying_columns,
                            metadata=metadata,
                            compartments=compartments,
                        ).result()
                        for offset in source["offsets"]
                    ]
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in invalid_files_dropped.items()
    }

    # if we're concatting or joining and need to infer the common schema
    if (concat or join) and infer_common_schema:
        # create a common schema for concatenation work
        common_schema_determined = {
            source_group_name: {
                "sources": source_group_vals,
                "common_schema": _infer_source_group_common_schema(
                    source_group=source_group_vals
                ),
            }
            for source_group_name, source_group_vals in results.items()
        }

    # if concat or join, concat the source groups
    # note: join implies a concat, but concat does not imply a join
    # We concat to join in order to create a common schema for join work
    # performed after concatenation.
    if concat or join:
        # create a potential return result for concatenation output
        results = {
            source_group_name: _concat_source_group(
                source_group_name=source_group_name,
                source_group=source_group_vals["sources"],
                dest_path=dest_path,
                common_schema=source_group_vals["common_schema"],
            ).result()
            for source_group_name, source_group_vals in common_schema_determined.items()
        }

    # conditional section for merging
    # note: join implies a concat, but concat does not imply a join
    if join:
        # map joined results based on the join groups gathered above
        # note: after mapping we end up with a list of strings (task returns str)
        join_sources_result = [
            _join_source_chunk(
                # gather the result of concatted sources prior to
                # join group merging as each mapped task run will need
                # full concat results
                sources=results,
                dest_path=dest_path,
                joins=joins,
                # get merging chunks by join columns
                join_group=join_group,
                drop_null=drop_null,
            ).result()
            # create join group for querying the concatenated
            # data in order to perform memory-safe joining
            # per user chunk size specification.
            for join_group in _get_join_chunks(
                sources=results,
                chunk_columns=chunk_columns,
                chunk_size=chunk_size,
                metadata=metadata,
            ).result()
        ]

        # concat our join chunks together as one cohesive dataset
        # return results in common format which includes metadata
        # for lineage and debugging
        results = _concat_join_sources(
            dest_path=dest_path,
            join_sources=join_sources_result,
            sources=results,
        ).result()

    # wrap the final result as a future and return
    return _return_future(results)


def convert(  # pylint: disable=too-many-arguments,too-many-locals
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    metadata: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_METADATA"]
    ),
    compartments: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_COMPARTMENTS"]
    ),
    identifying_columns: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_IDENTIFYING_COLUMNS"]
    ),
    concat: bool = True,
    join: bool = True,
    joins: Optional[str] = cast(str, config["cellprofiler_csv"]["CONFIG_JOINS"]),
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_CHUNK_COLUMNS"]
    ),
    chunk_size: Optional[int] = cast(
        int, config["cellprofiler_csv"]["CONFIG_CHUNK_SIZE"]
    ),
    infer_common_schema: bool = True,
    drop_null: bool = True,
    preset: Optional[str] = None,
    parsl_config: Optional[parsl.Config] = None,
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
    """
    Convert file-based data from various sources to Pycytominer-compatible standards.

    Note: source paths may be local or remote object-storage location
    using convention "s3://..." or similar.

    Args:
        source_path: str:
            str reference to read source files from.
            Note: may be local or remote object-storage location
            using convention "s3://..." or similar.
        dest_path: str:
            Path to write files to.
            Note: this may only be a local path.
        dest_datatype: Literal["parquet"]:
            Destination datatype to write to.
        source_datatype: Optional[str]:  (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, str, str, str]]:
            (Default value = DEFAULT_COMPARTMENTS)
            Compartment names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be
            ignored with regards to renaming.
        concat: bool:  (Default value = True)
            Whether to concatenate similar files together.
        join: bool:  (Default value = True)
            Whether to join the compartment data together into one dataset
        joins: str: (Default value = presets.config["cellprofiler_csv"]["CONFIG_JOINS"]):
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_columns: Optional[Union[List[str], Tuple[str, ...]]]
            (Default value = DEFAULT_CHUNK_COLUMNS)
            Column names which appear in all compartments to use when performing join
        chunk_size: Optional[int] (Default value = DEFAULT_CHUNK_SIZE)
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool: (Default value = True)
            Whether to infer a common schema when concatenating sources.
        drop_null: bool (Default value = True)
            Whether to drop nan/null values from results
        preset: str (Default value = None)
            an optional group of presets to use based on common configurations
        parsl_config: Optional[parsl.Config] (Default value = None)
            Optional Parsl configuration to use for running CytoTable operations.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]
            Grouped sources which include metadata about destination filepath
            where parquet file was written or str of joined result filepath.

    Example:

        .. code-block:: python

            from cytotable import convert

            # using a local path with cellprofiler csv presets
            convert(
                source_path="./tests/data/cellprofiler/csv_single",
                source_datatype="csv",
                dest_path=".",
                dest_datatype="parquet",
                preset="cellprofiler_csv",
            )

            # using an s3-compatible path with no signature for client
            # and cellprofiler csv presets
            convert(
                source_path="s3://s3path",
                source_datatype="csv",
                dest_path=".",
                dest_datatype="parquet",
                concat=True,
                preset="cellprofiler_csv",
                no_sign_request=True,
            )

            # using local path with cellprofiler sqlite presets
            convert(
                source_path="example.sqlite",
                dest_path="example.parquet",
                dest_datatype="parquet",
                preset="cellprofiler_sqlite",
            )
    """

    # attempt to load parsl configuration
    try:
        # if we don't have a parsl configuration provided, load the default
        if parsl_config is None:
            parsl.load(_default_parsl_config())
        else:
            # else we attempt to load the given parsl configuration
            parsl.load(parsl_config)
    except RuntimeError as runtime_exc:
        # catch cases where parsl has already been loaded and defer to
        # previously loaded configuration with a warning
        if str(runtime_exc) == "Config has already been loaded":
            logger.warning(str(runtime_exc))

            # if we're supplying a new config, attempt to clear current config
            # and use the new configuration instead. Otherwise, use existing.
            if parsl_config is not None:
                # clears the existing parsl configuration
                parsl.clear()
                # then load the supplied configuration
                parsl.load(parsl_config)

        # for other potential runtime errors besides config already being loaded
        else:
            raise

    # optionally load preset configuration for arguments
    # note: defer to overrides from parameters whose values
    # are not None (allows intermixing of presets and overrides)
    if preset is not None:
        metadata = (
            cast(list, config[preset]["CONFIG_NAMES_METADATA"])
            if metadata is None or preset != "cellprofiler_csv"
            else metadata
        )
        compartments = (
            cast(list, config[preset]["CONFIG_NAMES_COMPARTMENTS"])
            if compartments is None or preset != "cellprofiler_csv"
            else compartments
        )
        identifying_columns = (
            cast(list, config[preset]["CONFIG_IDENTIFYING_COLUMNS"])
            if identifying_columns is None or preset != "cellprofiler_csv"
            else identifying_columns
        )
        joins = (
            cast(str, config[preset]["CONFIG_JOINS"])
            if joins is None or preset != "cellprofiler_csv"
            else joins
        )
        chunk_columns = (
            cast(list, config[preset]["CONFIG_CHUNK_COLUMNS"])
            if chunk_columns is None or preset != "cellprofiler_csv"
            else chunk_columns
        )
        chunk_size = (
            cast(int, config[preset]["CONFIG_CHUNK_SIZE"])
            if chunk_size is None or preset != "cellprofiler_csv"
            else chunk_size
        )

    # send sources to be written to parquet if selected
    if dest_datatype == "parquet":
        output = _to_parquet(
            source_path=source_path,
            dest_path=dest_path,
            source_datatype=source_datatype,
            metadata=metadata,
            compartments=compartments,
            identifying_columns=identifying_columns,
            concat=concat,
            join=join,
            joins=joins,
            chunk_columns=chunk_columns,
            chunk_size=chunk_size,
            infer_common_schema=infer_common_schema,
            drop_null=drop_null,
            **kwargs,
        ).result()

    return output

"""
CytoTable: convert - transforming data for use with pyctyominer.
"""

import itertools
import logging
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

import parsl
from parsl.app.app import python_app

from cytotable.exceptions import CytoTableException
from cytotable.presets import config
from cytotable.sources import _gather_sources
from cytotable.utils import (
    _column_sort,
    _default_parsl_config,
    _expand_path,
    _parsl_loaded,
    evaluate_futures,
)

logger = logging.getLogger(__name__)


@python_app
def _get_table_columns_and_types(
    source: Dict[str, Any], sort_output: bool
) -> List[Dict[str, str]]:
    """
    Gather column data from table through duckdb.

    Args:
        source: Dict[str, Any]
            Contains source data details. Represents a single
            file or table of some kind.
        sort_output:
            Specifies whether to sort cytotable output or not.

    Returns:
        List[Dict[str, str]]
            list of dictionaries which each include column level information
    """

    import duckdb

    from cytotable.utils import _duckdb_reader, _sqlite_mixed_type_query_to_parquet

    source_path = source["source_path"]
    source_type = str(source_path.suffix).lower()

    # prepare the data source in the form of a duckdb query
    select_source = (
        f"read_csv_auto('{source_path}')"
        if source_type == ".csv"
        else f"sqlite_scan('{source_path}', '{source['table_name']}')"
    )

    # Query top 5 results from table and use pragma_storage_info() to
    # gather duckdb interpreted data typing. We gather 5 values for
    # each column to help with type inferences (where smaller sets
    # may yield lower data type accuracy for the full table).
    select_query = """
        /* we create an in-mem table for later use with the pragma_storage_info call
        as this call only functions with materialized tables and not views or related */
        CREATE TABLE column_details AS
            (SELECT *
            FROM &select_source
            LIMIT 5
            );

        /* selects specific column metadata from pragma_storage_info */
        SELECT DISTINCT
            column_id,
            column_name,
            segment_type as column_dtype
        FROM pragma_storage_info('column_details')
        /* avoid duplicate entries in the form of VALIDITY segment_types */
        WHERE segment_type != 'VALIDITY'
        /* explicitly order the columns by their id to avoid inconsistent results */
        ORDER BY column_id ASC;
        """

    # attempt to read the data to parquet from duckdb
    # with exception handling to read mixed-type data
    # using sqlite3 and special utility function
    try:
        # isolate using new connection to read data based on pageset
        # and export directly to parquet via duckdb (avoiding need to return data to python)
        # perform the query and create a list of dictionaries with the column data for table
        with _duckdb_reader() as ddb_reader:
            return (
                ddb_reader.execute(
                    select_query.replace("&select_source", select_source)
                )
                .arrow()
                .to_pylist()
            )

    except duckdb.Error as e:
        # if we see a mismatched type error
        # run a more nuanced query through sqlite
        # to handle the mixed types
        if "Mismatch Type Error" in str(e) and source_type == ".sqlite":
            arrow_data_tbl = _sqlite_mixed_type_query_to_parquet(
                source_path=str(source["source_path"]),
                table_name=str(source["table_name"]),
                page_key=source["page_key"],
                pageset=source["pagesets"][0],
                sort_output=sort_output,
            )
            with _duckdb_reader() as ddb_reader:
                return (
                    ddb_reader.execute(
                        select_query.replace("&select_source", "arrow_data_tbl")
                    )
                    .arrow()
                    .to_pylist()
                )
        else:
            raise


@python_app
def _prep_cast_column_data_types(
    columns: List[Dict[str, str]], data_type_cast_map: Dict[str, str]
) -> List[Dict[str, str]]:
    """
    Cast data types per what is received in cast_map.

    Example:
    - columns: [{"column_id":0, "column_name":"colname", "column_dtype":"DOUBLE"}]
    - data_type_cast_map: {"float": "float32"}

    The above passed through this function will set the "column_dtype" value
    to a "REAL" dtype ("REAL" in duckdb is roughly equivalent to "float32")

    Args:
        table_path: str:
            Path to a parquet file which will be modified.
        data_type_cast_map: Dict[str, str]
            A dictionary mapping data type groups to specific types.
            Roughly to eventually align with DuckDB types:
            https://duckdb.org/docs/sql/data_types/overview

            Note: includes synonym matching for common naming convention
            use in Pandas and/or PyArrow via cytotable.utils.DATA_TYPE_SYNONYMS

    Returns:
        List[Dict[str, str]]
            list of dictionaries which each include column level information
    """

    from functools import partial

    from cytotable.utils import _arrow_type_cast_if_specified

    if data_type_cast_map is not None:
        return list(
            # map across all columns
            map(
                partial(
                    # attempts to cast the columns provided
                    _arrow_type_cast_if_specified,
                    # set static data_type_case_map arg for
                    # use with all fields
                    data_type_cast_map=data_type_cast_map,
                ),
                columns,
            )
        )

    return columns


@python_app
def _get_table_keyset_pagination_sets(
    chunk_size: int,
    page_key: str,
    source: Optional[Dict[str, Any]] = None,
    sql_stmt: Optional[str] = None,
) -> Union[List[Tuple[Union[int, float], Union[int, float]]], None]:
    """
    Get table data chunk keys for later use in capturing segments
    of values. This work also provides a chance to catch problematic
    input data which will be ignored with warnings.

    Args:
        source: Dict[str, Any]
            Contains the source data to be chunked. Represents a single
            file or table of some kind.
        chunk_size: int
            The size in rowcount of the chunks to create.
        page_key: str
            The column name to be used to identify pagination chunks.
            Expected to be of numeric type (int, float) for ordering.
        sql_stmt:
            Optional sql statement to form the pagination set from.
            Default behavior extracts pagination sets from the full
            data source.

    Returns:
        List[Any]
            List of keys to use for reading the data later on.
    """

    import logging
    import sqlite3
    from contextlib import closing

    import duckdb

    from cytotable.exceptions import NoInputDataException
    from cytotable.utils import _duckdb_reader, _generate_pagesets

    logger = logging.getLogger(__name__)

    if source is not None:
        table_name = source["table_name"] if "table_name" in source.keys() else None
        source_path = source["source_path"]
        source_type = str(source_path.suffix).lower()

        try:
            with _duckdb_reader() as ddb_reader:
                if source_type == ".csv":
                    sql_query = f"SELECT {page_key} FROM read_csv_auto('{source_path}', header=TRUE, delim=',') ORDER BY {page_key}"
                else:
                    sql_query = f"SELECT {page_key} FROM sqlite_scan('{source_path}', '{table_name}') ORDER BY {page_key}"

                page_keys = [
                    results[0] for results in ddb_reader.execute(sql_query).fetchall()
                ]

        # exception case for when we have mixed types
        # (i.e. integer col with string and ints) in a sqlite column
        except duckdb.TypeMismatchException:
            with closing(sqlite3.connect(source_path)) as cx:
                with cx:
                    page_keys = [
                        key[0]
                        for key in cx.execute(
                            f"SELECT {page_key} FROM {table_name} ORDER BY {page_key};"
                        ).fetchall()
                        if isinstance(key[0], (int, float))
                    ]

        except (
            duckdb.InvalidInputException,
            NoInputDataException,
        ) as invalid_input_exc:
            logger.warning(
                msg=f"Skipping file due to input file errors: {str(invalid_input_exc)}"
            )

            return None

    elif sql_stmt is not None:
        with _duckdb_reader() as ddb_reader:
            sql_query = f"SELECT {page_key} FROM ({sql_stmt}) ORDER BY {page_key}"
            page_keys = ddb_reader.execute(sql_query).fetchall()
            page_keys = [key[0] for key in page_keys]

    return _generate_pagesets(page_keys, chunk_size)


@python_app
def _source_pageset_to_parquet(
    source_group_name: str,
    source: Dict[str, Any],
    pageset: Tuple[Union[int, float], Union[int, float]],
    dest_path: str,
    sort_output: bool,
) -> str:
    """
    Export source data to chunked parquet file using chunk size and offsets.

    Args:
        source_group_name: str
            Name of the source group (for ex. compartment or metadata table name).
        source: Dict[str, Any]
            Contains the source data to be chunked. Represents a single
            file or table of some kind along with collected information about table.
        pageset: Tuple[int, int]
            The pageset for chunking the data from source.
        dest_path: str
            Path to store the output data.
        sort_output: bool
            Specifies whether to sort cytotable output or not.

    Returns:
        str
            A string of the output filepath.
    """

    import pathlib

    import duckdb
    from cloudpathlib import AnyPath

    from cytotable.utils import (
        _duckdb_reader,
        _sqlite_mixed_type_query_to_parquet,
        _write_parquet_table_with_metadata,
    )

    # attempt to build dest_path
    source_dest_path = (
        f"{dest_path}/{str(AnyPath(source_group_name).stem).lower()}/"
        f"{str(source['source_path'].parent.name).lower()}"
    )
    pathlib.Path(source_dest_path).mkdir(parents=True, exist_ok=True)

    # add source table columns
    casted_source_cols = [
        # here we cast the column to the specified type ensure the colname remains the same
        f"CAST(\"{column['column_name']}\" AS {column['column_dtype']}) AS \"{column['column_name']}\""
        for column in source["columns"]
    ]

    # create selection statement from lists above
    select_columns = ",".join(
        # if we should sort the output, add the metadata_cols
        casted_source_cols
        if sort_output
        else casted_source_cols
    )

    # build output query and filepath base
    # (chunked output will append offset to keep output paths unique)
    if str(source["source_path"].suffix).lower() == ".csv":
        base_query = f"SELECT {select_columns} FROM read_csv_auto('{str(source['source_path'])}', header=TRUE, delim=',')"
        result_filepath_base = f"{source_dest_path}/{str(source['source_path'].stem)}"

    elif str(source["source_path"].suffix).lower() == ".sqlite":
        base_query = f"SELECT {select_columns} FROM sqlite_scan('{str(source['source_path'])}', '{str(source['table_name'])}')"
        result_filepath_base = f"{source_dest_path}/{str(source['source_path'].stem)}.{source['table_name']}"

    # form a filepath which indicates the pageset
    result_filepath = f"{result_filepath_base}-{pageset[0]}-{pageset[1]}.parquet"

    # Attempt to read the data to parquet file
    # using duckdb for extraction and pyarrow for
    # writing data to a parquet file.
    try:
        # read data with chunk size + offset
        # and export to parquet
        with _duckdb_reader() as ddb_reader:
            _write_parquet_table_with_metadata(
                table=ddb_reader.execute(
                    f"""
                    {base_query}
                    WHERE {source['page_key']} BETWEEN {pageset[0]} AND {pageset[1]}
                    /* optional ordering per pageset */
                    {"ORDER BY " + source['page_key'] if sort_output else ""};
                    """
                ).arrow(),
                where=result_filepath,
            )
    # Include exception handling to read mixed-type data
    # using sqlite3 and special utility function.
    except duckdb.Error as e:
        # if we see a mismatched type error
        # run a more nuanced query through sqlite
        # to handle the mixed types
        if (
            "Mismatch Type Error" in str(e)
            and str(source["source_path"].suffix).lower() == ".sqlite"
        ):
            _write_parquet_table_with_metadata(
                # here we use sqlite instead of duckdb to extract
                # data for special cases where column and value types
                # may not align (which is valid functionality in SQLite).
                table=_sqlite_mixed_type_query_to_parquet(
                    source_path=str(source["source_path"]),
                    table_name=str(source["table_name"]),
                    page_key=source["page_key"],
                    pageset=pageset,
                    sort_output=sort_output,
                ),
                where=result_filepath,
            )
        else:
            raise

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
            List of source data names which are used as metadata.
        compartments: List[str]:
            List of source data names which are used as compartments.

    Returns:
        str
            Path to the modified file.
    """

    import logging
    import pathlib

    import pyarrow.parquet as parquet

    from cytotable.constants import CYTOTABLE_ARROW_USE_MEMORY_MAPPING
    from cytotable.utils import _write_parquet_table_with_metadata

    logger = logging.getLogger(__name__)

    targets = tuple(metadata) + tuple(compartments)

    # if we have no targets or metadata to work from, return the table unchanged
    if len(targets) == 0:
        logger.warning(
            msg=(
                "Skipping column name prepend operations "
                "because no compartments or metadata were provided."
            )
        )
        return table_path

    table = parquet.read_table(
        source=table_path, memory_map=CYTOTABLE_ARROW_USE_MEMORY_MAPPING
    )

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
        # and colname does not include 'ObjectNumber' or 'TableNumber'
        # (which are specially treated column names in this context)
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'Parent_Nuclei'
        #   updated_column_name: 'Metadata_Cells_Parent_Nuclei'
        elif (
            column_name in identifying_columns
            and not column_name.startswith("Metadata_")
            and not any(item.capitalize() in column_name for item in metadata)
            and not any(item in column_name for item in ["ObjectNumber", "TableNumber"])
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
    _write_parquet_table_with_metadata(
        table=table.rename_columns(updated_column_names), where=table_path
    )

    return table_path


@python_app
def _concat_source_group(
    source_group_name: str,
    source_group: List[Dict[str, Any]],
    dest_path: str,
    common_schema: Optional[List[Tuple[str, str]]] = None,
    sort_output: bool = True,
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
        sort_output: bool
            Specifies whether to sort cytotable output or not.

    Returns:
        List[Dict[str, Any]]
            Updated dictionary containing concatenated sources.
    """

    import errno
    import pathlib

    import pyarrow as pa
    import pyarrow.parquet as parquet

    from cytotable.constants import (
        CYTOTABLE_ARROW_USE_MEMORY_MAPPING,
        CYTOTABLE_DEFAULT_PARQUET_METADATA,
    )
    from cytotable.exceptions import SchemaException
    from cytotable.utils import _natural_sort

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
    writer_schema = pa.schema(common_schema).with_metadata(
        CYTOTABLE_DEFAULT_PARQUET_METADATA
    )

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    with parquet.ParquetWriter(str(destination_path), writer_schema) as writer:
        for source in source_group:
            tables = [table for table in source["table"]]
            if sort_output:
                tables = _natural_sort(tables)
            for table in tables:
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
                writer.write_table(
                    parquet.read_table(
                        table,
                        schema=writer_schema,
                        memory_map=CYTOTABLE_ARROW_USE_MEMORY_MAPPING,
                    )
                )
                # remove the file which was written in the concatted parquet file (we no longer need it)
                pathlib.Path(table).unlink()

            # attempt to clean up dir containing original table(s) only if it's empty
            try:
                pathlib.Path(pathlib.Path(source["table"][0]).parent).rmdir()
            except OSError as os_err:
                # raise only if we don't have a dir not empty errno
                if os_err.errno != errno.ENOTEMPTY:
                    raise

    # return the concatted parquet filename
    concatted[0]["table"] = [destination_path]

    return concatted


@python_app()
def _prepare_join_sql(
    sources: Dict[str, List[Dict[str, Any]]],
    joins: str,
) -> str:
    """
    Prepare join SQL statement with actual locations of data based on the sources.

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join
            operations using the join_group keys as a reference.
        sort_output: bool
            Specifies whether to sort cytotable output or not.

    Returns:
        str:
            String representing the SQL to be used in later join work.
    """
    import pathlib

    # replace with real location of sources for join sql
    order_by_tables = []
    for key, val in sources.items():
        if pathlib.Path(key).stem.lower() in joins.lower():
            table_name = str(pathlib.Path(key).stem.lower())
            joins = joins.replace(
                f"'{table_name}.parquet'",
                str([str(table) for table in val[0]["table"]]),
            )
            order_by_tables.append(table_name)

    # add the order by statements to the join
    return joins


@python_app
def _join_source_pageset(
    dest_path: str,
    joins: str,
    page_key: str,
    pageset: Tuple[int, int],
    sort_output: bool,
    drop_null: bool,
) -> str:
    """
    Join sources based on join group keys (group of specific join column values)

    Args:
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

    from cytotable.utils import _duckdb_reader, _write_parquet_table_with_metadata

    with _duckdb_reader() as ddb_reader:
        result = ddb_reader.execute(
            f"""
            WITH joined AS (
                {joins}
            )
            SELECT *
            FROM joined
            WHERE {page_key} BETWEEN {pageset[0]} AND {pageset[1]}
            /* optional sorting per pagset */
            {"ORDER BY " + page_key if sort_output else ""};
            """
        ).arrow()

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
        # add the pageset indication to the filename
        f"{pageset[0]}-{pageset[1]}.parquet"
    )

    # write the result
    _write_parquet_table_with_metadata(
        table=result,
        where=result_file_path,
    )

    return result_file_path


@python_app
def _concat_join_sources(
    sources: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    join_sources: List[str],
    sort_output: bool = True,
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
        sort_output: bool
            Specifies whether to sort cytotable output or not.

    Returns:
        str
            Path to concatenated file which is created as a result of this function.
    """

    import pathlib
    import shutil

    import pyarrow.parquet as parquet

    from cytotable.constants import (
        CYTOTABLE_ARROW_USE_MEMORY_MAPPING,
        CYTOTABLE_DEFAULT_PARQUET_METADATA,
    )
    from cytotable.utils import _natural_sort

    # remove the unjoined concatted compartments to prepare final dest_path usage
    # (we now have joined results)
    flattened_sources = list(itertools.chain(*list(sources.values())))
    for source in flattened_sources:
        for table in source["table"]:
            pathlib.Path(table).unlink(missing_ok=True)

    # remove dir if we have it
    if pathlib.Path(dest_path).is_dir():
        shutil.rmtree(path=dest_path)

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    writer_schema = parquet.read_schema(join_sources[0]).with_metadata(
        CYTOTABLE_DEFAULT_PARQUET_METADATA
    )
    with parquet.ParquetWriter(str(dest_path), writer_schema) as writer:
        for table_path in (
            join_sources
            if not sort_output
            else _natural_sort(list_to_sort=join_sources)
        ):
            writer.write_table(
                parquet.read_table(
                    table_path,
                    schema=writer_schema,
                    memory_map=CYTOTABLE_ARROW_USE_MEMORY_MAPPING,
                )
            )
            # remove the file which was written in the concatted parquet file (we no longer need it)
            pathlib.Path(table_path).unlink()

    # return modified sources format to indicate the final result
    # and retain the other source data for reference as needed
    return dest_path


@python_app
def _infer_source_group_common_schema(
    source_group: List[Dict[str, Any]],
    data_type_cast_map: Optional[Dict[str, str]] = None,
) -> List[Tuple[str, str]]:
    """
    Infers a common schema for group of parquet files which may have
    similar but slightly different schema or data. Intended to assist with
    data concatenation and other operations.

    Args:
        source_group: List[Dict[str, Any]]:
            Group of one or more data sources which includes metadata about
            path to parquet data.
        data_type_cast_map: Optional[Dict[str, str]], default None
            A dictionary mapping data type groups to specific types.
            Roughly includes Arrow data types language from:
            https://arrow.apache.org/docs/python/api/datatypes.html

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
                common_schema = common_schema.set(
                    index,
                    field.with_type(
                        # use float64 as a default here if we aren't casting floats
                        pa.float64()
                        if data_type_cast_map is None
                        or "float" not in data_type_cast_map.keys()
                        # otherwise use the float data type cast type
                        else pa.type_for_alias(data_type_cast_map["float"])
                    ),
                )

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


def _to_parquet(  # pylint: disable=too-many-arguments, too-many-locals
    source_path: str,
    dest_path: str,
    source_datatype: Optional[str],
    metadata: Optional[Union[List[str], Tuple[str, ...]]],
    compartments: Optional[Union[List[str], Tuple[str, ...]]],
    identifying_columns: Optional[Union[List[str], Tuple[str, ...]]],
    concat: bool,
    join: bool,
    joins: Optional[str],
    chunk_size: Optional[int],
    infer_common_schema: bool,
    drop_null: bool,
    sort_output: bool,
    page_keys: Dict[str, str],
    data_type_cast_map: Optional[Dict[str, str]] = None,
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
            Path to write files to. This path will be used for
            intermediary data work and must be a new file or directory path.
            This parameter will result in a directory on `join=False`.
            This parameter will result in a single file on `join=True`.
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
            Whether to join the compartment data together into one dataset.
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_size: Optional[int],
            Size of join chunks which is used to limit data size during join ops.
        infer_common_schema: bool:  (Default value = True)
            Whether to infer a common schema when concatenating sources.
        drop_null: bool:
            Whether to drop null results.
        sort_output: bool
            Specifies whether to sort cytotable output or not.
        page_keys: Dict[str, str]
            A dictionary which defines which column names are used for keyset pagination
            in order to perform data extraction.
        data_type_cast_map: Dict[str, str]
            A dictionary mapping data type groups to specific types.
            Roughly includes Arrow data types language from:
            https://arrow.apache.org/docs/python/api/datatypes.html
        **kwargs: Any:
            Keyword args used for gathering source data, primarily relevant for
            Cloudpathlib cloud-based client configuration.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]:
            Grouped sources which include metadata about destination filepath
            where parquet file was written or a string filepath for the joined
            result.
    """

    # gather sources to be processed
    sources = _gather_sources(
        source_path=source_path,
        source_datatype=source_datatype,
        targets=(
            list(metadata) + list(compartments)
            if metadata is not None and compartments is not None
            else []
        ),
        **kwargs,
    )

    # expand the destination path
    expanded_dest_path = _expand_path(path=dest_path)

    # check that each source group name has a pagination key
    for source_group_name in sources.keys():
        matching_keys = [
            key for key in page_keys.keys() if key.lower() in source_group_name.lower()
        ]
        if not matching_keys:
            raise CytoTableException(
                f"No matching key found in page_keys for source_group_name: {source_group_name}."
                "Please include a pagination key based on a column name from the table."
            )

    # prepare pagesets for chunked data export from source tables
    pagesets_prepared = {
        source_group_name: [
            dict(
                source,
                **{
                    "page_key": (
                        page_key := [
                            value
                            for key, value in page_keys.items()
                            if key.lower() in source_group_name.lower()
                        ][0]
                    ),
                    "pagesets": _get_table_keyset_pagination_sets(
                        source=source,
                        chunk_size=chunk_size,
                        page_key=page_key,
                    ),
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in sources.items()
    }

    # if pagesets is none and we haven't halted, remove the file as there
    # were input formatting errors which will create challenges downstream
    invalid_files_dropped = {
        source_group_name: [
            # ensure we have pagesets
            source
            for source in source_group_vals
            if source["pagesets"] is not None
        ]
        for source_group_name, source_group_vals in evaluate_futures(
            pagesets_prepared
        ).items()
        # ensure we have source_groups with at least one source table
        if len(source_group_vals) > 0
    }

    # gather column names and types from source tables
    column_names_and_types_gathered = {
        source_group_name: [
            dict(
                source,
                **{
                    "columns": _prep_cast_column_data_types(
                        columns=_get_table_columns_and_types(
                            source=source, sort_output=sort_output
                        ),
                        data_type_cast_map=data_type_cast_map,
                    )
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in invalid_files_dropped.items()
    }

    results = {
        source_group_name: [
            dict(
                source,
                **{
                    "table": [
                        # perform column renaming and create potential return result
                        _prepend_column_name(
                            # perform chunked data export to parquet using pagesets
                            table_path=_source_pageset_to_parquet(
                                source_group_name=source_group_name,
                                source=source,
                                pageset=pageset,
                                dest_path=expanded_dest_path,
                                sort_output=sort_output,
                            ),
                            source_group_name=source_group_name,
                            identifying_columns=identifying_columns,
                            metadata=metadata,
                            compartments=compartments,
                        )
                        for pageset in source["pagesets"]
                    ]
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in evaluate_futures(
            column_names_and_types_gathered
        ).items()
    }

    # if we're concatting or joining and need to infer the common schema
    if (concat or join) and infer_common_schema:
        # create a common schema for concatenation work
        common_schema_determined = {
            source_group_name: [
                {
                    "sources": source_group_vals,
                    "common_schema": _infer_source_group_common_schema(
                        source_group=source_group_vals,
                        data_type_cast_map=data_type_cast_map,
                    ),
                }
            ]
            for source_group_name, source_group_vals in evaluate_futures(
                results
            ).items()
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
                source_group=source_group_vals[0]["sources"],
                dest_path=expanded_dest_path,
                common_schema=source_group_vals[0]["common_schema"],
                sort_output=sort_output,
            )
            for source_group_name, source_group_vals in evaluate_futures(
                common_schema_determined
            ).items()
        }

    # conditional section for merging
    # note: join implies a concat, but concat does not imply a join
    if join:
        # evaluate the results as they're used multiple times below
        evaluated_results = evaluate_futures(results)

        prepared_joins_sql = _prepare_join_sql(
            sources=evaluated_results, joins=joins
        ).result()

        page_key_join = [
            value for key, value in page_keys.items() if key.lower() == "join"
        ][0]

        # map joined results based on the join groups gathered above
        # note: after mapping we end up with a list of strings (task returns str)
        join_sources_result = [
            _join_source_pageset(
                # gather the result of concatted sources prior to
                # join group merging as each mapped task run will need
                # full concat results
                dest_path=expanded_dest_path,
                joins=prepared_joins_sql,
                page_key=page_key_join,
                pageset=pageset,
                sort_output=sort_output,
                drop_null=drop_null,
            )
            # create join group for querying the concatenated
            # data in order to perform memory-safe joining
            # per user chunk size specification.
            for pageset in _get_table_keyset_pagination_sets(
                sql_stmt=prepared_joins_sql,
                chunk_size=chunk_size,
                page_key=page_key_join,
            ).result()
        ]

        # concat our join chunks together as one cohesive dataset
        # return results in common format which includes metadata
        # for lineage and debugging
        results = _concat_join_sources(
            dest_path=expanded_dest_path,
            join_sources=[join.result() for join in join_sources_result],
            sources=evaluated_results,
            sort_output=sort_output,
        )

    # wrap the final result as a future and return
    return evaluate_futures(results)


def convert(  # pylint: disable=too-many-arguments,too-many-locals
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    metadata: Optional[Union[List[str], Tuple[str, ...]]] = None,
    compartments: Optional[Union[List[str], Tuple[str, ...]]] = None,
    identifying_columns: Optional[Union[List[str], Tuple[str, ...]]] = None,
    concat: bool = True,
    join: bool = True,
    joins: Optional[str] = None,
    chunk_size: Optional[int] = None,
    infer_common_schema: bool = True,
    drop_null: bool = False,
    data_type_cast_map: Optional[Dict[str, str]] = None,
    page_keys: Optional[Dict[str, str]] = None,
    sort_output: bool = True,
    preset: Optional[str] = "cellprofiler_csv",
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
            Path to write files to. This path will be used for
            intermediary data work and must be a new file or directory path.
            This parameter will result in a directory on `join=False`.
            This parameter will result in a single file on `join=True`.
            Note: this may only be a local path.
        dest_datatype: Literal["parquet"]:
            Destination datatype to write to.
        source_datatype: Optional[str]:  (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, str, str, str]]:
            (Default value = None)
            Compartment names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be
            ignored with regards to renaming.
        concat: bool:  (Default value = True)
            Whether to concatenate similar files together.
        join: bool:  (Default value = True)
            Whether to join the compartment data together into one dataset
        joins: str: (Default value = None):
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_size: Optional[int] (Default value = None)
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool (Default value = True)
            Whether to infer a common schema when concatenating sources.
        data_type_cast_map: Dict[str, str], (Default value = None)
            A dictionary mapping data type groups to specific types.
            Roughly includes Arrow data types language from:
            https://arrow.apache.org/docs/python/api/datatypes.html
        page_keys: str:
            The table and column names to be used for key pagination.
            Uses the form: {"table_name":"column_name"}.
            Expects columns to include numeric data (ints or floats).
            Interacts with the `chunk_size` parameter to form
            pages of `chunk_size`.
        sort_output: bool (Default value = True)
            Specifies whether to sort cytotable output or not.
        drop_null: bool (Default value = False)
            Whether to drop nan/null values from results
        preset: str (Default value = "cellprofiler_csv")
            an optional group of presets to use based on common configurations
        parsl_config: Optional[parsl.Config] (Default value = None)
            Optional Parsl configuration to use for running CytoTable operations.
            Note: when using CytoTable multiple times in the same process,
            CytoTable will use the first provided configuration for all runs.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]
            Grouped sources which include metadata about destination filepath
            where parquet file was written or str of joined result filepath.

    Example:

        .. code-block:: python

            from cytotable import convert

            # using a local path with cellprofiler csv presets
            convert(
                source_path="./tests/data/cellprofiler/ExampleHuman",
                source_datatype="csv",
                dest_path="ExampleHuman.parquet",
                dest_datatype="parquet",
                preset="cellprofiler_csv",
            )

            # using an s3-compatible path with no signature for client
            # and cellprofiler csv presets
            convert(
                source_path="s3://s3path",
                source_datatype="csv",
                dest_path="s3_local_result",
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

    # Raise an exception if an existing path is provided as the destination
    # to avoid removing existing data or unrelated data removal.
    if _expand_path(dest_path).exists():
        raise CytoTableException(
            (
                "An existing file or directory was provided as dest_path: "
                f"'{dest_path}'. Please use a new path for this parameter."
            )
        )

    # attempt to load parsl configuration if we didn't already load one
    if not _parsl_loaded():
        # if we don't have a parsl configuration provided, load the default
        if parsl_config is None:
            parsl.load(_default_parsl_config())
        else:
            # else we attempt to load the given parsl configuration
            parsl.load(parsl_config)
    else:
        # otherwise warn the user about previous config.
        logger.warning("Reusing previously loaded Parsl configuration.")

    # optionally load preset configuration for arguments
    # note: defer to overrides from parameters whose values
    # are not None (allows intermixing of presets and overrides)
    if preset is not None:
        metadata = (
            cast(list, config[preset]["CONFIG_NAMES_METADATA"])
            if metadata is None
            else metadata
        )
        compartments = (
            cast(list, config[preset]["CONFIG_NAMES_COMPARTMENTS"])
            if compartments is None
            else compartments
        )
        identifying_columns = (
            cast(list, config[preset]["CONFIG_IDENTIFYING_COLUMNS"])
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
            cast(dict, config[preset]["CONFIG_PAGE_KEYS"])
            if page_keys is None
            else page_keys
        )

    # Raise an exception for scenarios where one configures CytoTable to join
    # but does not provide a pagination key for the joins.
    if join and (page_keys is None or "join" not in page_keys.keys()):
        raise CytoTableException(
            (
                "When using join=True one must pass a 'join' pagination key "
                "in the page_keys parameter. The 'join' pagination key is a column "
                "name found within the joined results based on the SQL provided from "
                "the joins parameter. This special key is required as not all columns "
                "from the source tables might not be included."
            )
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
            chunk_size=chunk_size,
            infer_common_schema=infer_common_schema,
            drop_null=drop_null,
            data_type_cast_map=data_type_cast_map,
            sort_output=sort_output,
            page_keys=cast(dict, page_keys),
            **kwargs,
        )

    return output

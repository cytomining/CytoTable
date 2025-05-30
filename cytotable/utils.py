"""
Utility functions for CytoTable
"""

import logging
import os
import pathlib
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import duckdb
import parsl
import pyarrow as pa
from cloudpathlib import AnyPath, CloudPath
from cloudpathlib.exceptions import InvalidPrefixError
from parsl.app.app import AppBase
from parsl.config import Config
from parsl.errors import NoDataFlowKernelError
from parsl.executors import HighThroughputExecutor

logger = logging.getLogger(__name__)

# reference the original init
original_init = AppBase.__init__


def Parsl_AppBase_init_for_docs(self, func, *args, **kwargs):
    """
    A function to extend Parsl.app.app.AppBase with
    docstring from decorated functions rather than
    the decorators from Parsl. Used for
    Sphinx documentation purposes.
    """
    original_init(self, func, *args, **kwargs)
    # add function doc as the app doc
    self.__doc__ = func.__doc__


# set the AppBase to the new init for the docstring.
AppBase.__init__ = Parsl_AppBase_init_for_docs


def _parsl_loaded() -> bool:
    """
    Checks whether Parsl configuration has already been loaded.
    """

    try:
        # try to reference Parsl dataflowkernel
        parsl.dfk()
    except NoDataFlowKernelError:
        # if we detect a Parsl NoDataFlowKernelError
        # return false to indicate parsl config has not yet been loaded.
        return False

    # otherwise we indicate parsl config has already been loaded
    return True


def _default_parsl_config():
    """
    Return a default Parsl configuration for use with CytoTable.
    """
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_default_for_cytotable",
            )
        ]
    )


# custom sort for resulting columns
def _column_sort(value: str):
    """
    A custom sort for column values as a list.
    To be used with sorted and Pyarrow tables.
    """

    # lowercase str which will be used for comparisons
    # to avoid any capitalization challenges
    value_lower = value.lower()

    # first sorted values (by list index)
    sort_first = [
        "tablenumber",
        "metadata_tablenumber",
        "imagenumber",
        "metadata_imagenumber",
        "objectnumber",
        "object_number",
    ]

    # middle sort value
    sort_middle = "metadata"

    # sorted last (by list order enumeration)
    sort_later = [
        "image",
        "cytoplasm",
        "cells",
        "nuclei",
    ]

    # if value is in the sort_first list
    # return the index from that list
    if value_lower in sort_first:
        return sort_first.index(value_lower)

    # if sort_middle is anywhere in value return
    # next index value after sort_first values
    if sort_middle in value_lower:
        return len(sort_first)

    # if any sort_later are found as the first part of value
    # return enumerated index of sort_later value (starting from
    # relative len based on the above conditionals and lists)
    if any(value_lower.startswith(val) for val in sort_later):
        for _k, _v in enumerate(sort_later, start=len(sort_first) + 1):
            if value_lower.startswith(_v):
                return _k

    # else we return the total length of all sort values
    return len(sort_first) + len(sort_later) + 1


def _duckdb_reader() -> duckdb.DuckDBPyConnection:
    """
    Creates a DuckDB connection with the
    sqlite_scanner installed and loaded.

    Note: using this function assumes implementation will
    close the subsequently created DuckDB connection using
    `_duckdb_reader().close()` or using a context manager,
    for ex., using: `with _duckdb_reader() as ddb_reader:`

    Returns:
        duckdb.DuckDBPyConnection
    """

    import duckdb

    from cytotable.constants import MAX_THREADS

    return duckdb.connect().execute(
        # note: we use an f-string here to
        # dynamically configure threads as appropriate
        f"""
        /* Install and load sqlite plugin for duckdb */
        INSTALL sqlite_scanner;
        LOAD sqlite_scanner;

        /* Install httpfs plugin to avoid error
        https://github.com/duckdb/duckdb/issues/3243 */
        INSTALL httpfs;

        /*
        Set threads available to duckdb
        See the following for more information:
        https://duckdb.org/docs/sql/pragmas#memory_limit-threads
        */
        PRAGMA threads={MAX_THREADS};

        /*
        Allow unordered results for performance increase possibilities
        See the following for more information:
        https://duckdb.org/docs/sql/configuration#configuration-reference
        */
        PRAGMA preserve_insertion_order=FALSE;
        """,
    )


def _sqlite_mixed_type_query_to_parquet(
    source_path: str,
    table_name: str,
    page_key: str,
    pageset: Tuple[Union[int, float], Union[int, float]],
    sort_output: bool,
    tablenumber: Optional[int] = None,
) -> str:
    """
    Performs SQLite table data extraction where one or many
    columns include data values of potentially mismatched type
    such that the data may be exported to Arrow for later use.

    Args:
        source_path: str:
            A str which is a path to a SQLite database file.
        table_name: str:
            The name of the table being queried.
        page_key: str:
            The column name to be used to identify pagination chunks.
        pageset: Tuple[Union[int, float], Union[int, float]]:
            The range for values used for paginating data from source.
        sort_output: bool
            Specifies whether to sort cytotable output or not.
        add_cytotable_meta: bool, default=False:
            Whether to add CytoTable metadata fields or not
        tablenumber: Optional[int], default=None:
            An optional table number to append to the results.
            Defaults to None.

    Returns:
        pyarrow.Table:
           The resulting arrow table for the data
    """
    import sqlite3

    import pyarrow as pa

    from cytotable.constants import SQLITE_AFFINITY_DATA_TYPE_SYNONYMS
    from cytotable.exceptions import DatatypeException

    # open sqlite3 connection
    with sqlite3.connect(source_path) as conn:
        cursor = conn.cursor()

        # Gather table column details including datatype.
        # Note: uses SQLite pragma for table information.
        # See the following for more information:
        # https://sqlite.org/pragma.html#pragma_table_info
        cursor.execute(
            """
            SELECT :table_name as table_name,
                    name as column_name,
                    type as column_type
            FROM pragma_table_info(:table_name)
            /* explicit column ordering by 'cid' */
            ORDER BY cid ASC;
            """,
            {"table_name": table_name},
        )

        # gather column metadata details as list of dictionaries
        column_info = [
            dict(zip([desc[0] for desc in cursor.description], row))
            for row in cursor.fetchall()
        ]

        def _sqlite_affinity_data_type_lookup(col_type: str) -> str:
            # seek the translated type from SQLITE_AFFINITY_DATA_TYPE_SYNONYMS
            translated_type = [
                key
                for key, values in SQLITE_AFFINITY_DATA_TYPE_SYNONYMS.items()
                if col_type in values
            ]

            # if we're unable to find a synonym for the type, raise an error
            if not translated_type:
                raise DatatypeException(
                    f"Unable to find SQLite data type synonym for {col_type}."
                )

            # return the translated type for use in SQLite
            return translated_type[0]

        # build tablenumber segment addition (if necessary)
        tablenumber_sql = (
            # to become tablenumber in sql select later with integer
            f"CAST({tablenumber} AS INTEGER) as TableNumber, "
            if tablenumber is not None
            # if we don't have a tablenumber value, don't introduce the column
            else ""
        )

        # create cases for mixed-type handling in each column discovered above
        query_parts = tablenumber_sql + ", ".join(
            [
                f"""
            CASE
                /* when the storage class type doesn't match the column, return nulltype */
                WHEN typeof({col['column_name']}) !=
                '{_sqlite_affinity_data_type_lookup(col['column_type'].lower())}' THEN NULL
                /* else, return the normal value */
                ELSE {col['column_name']}
            END AS {col['column_name']}
            """
                for col in column_info
            ]
        )

        # perform the select using the cases built above and using chunksize + offset
        sql_stmt = f"""
            SELECT
                {query_parts}
            FROM {table_name}
            WHERE {page_key} BETWEEN {pageset[0]} AND {pageset[1]}
            {"ORDER BY " + page_key if sort_output else ""};
            """

        # execute the sql stmt
        cursor.execute(sql_stmt)
        # collect the results and include the column name with values
        results = [
            dict(zip([desc[0] for desc in cursor.description], row))
            for row in cursor.fetchall()
        ]

        # close the sqlite3 cursor
        cursor.close()

    # close the sqlite3 connection
    # note: context manager does not automatically close the connection
    # as per notes found under:
    # https://docs.python.org/3/library/sqlite3.html#sqlite3-connection-context-manager
    conn.close()

    # return arrow table with results
    return pa.Table.from_pylist(results)


def _cache_cloudpath_to_local(path: AnyPath) -> pathlib.Path:
    """
    Takes a cloudpath and uses cache to convert to a local copy
    for use in scenarios where remote work is not possible (sqlite).

    Args:
        path: Union[str, AnyPath]
            A filepath which will be checked and potentially
            converted to a local filepath.

    Returns:
        pathlib.Path
            A local pathlib.Path to cached version of cloudpath file.
    """

    # check that the path is a file (caching won't work with a dir)
    # and check that the file is of sqlite type
    # (other file types will be handled remotely in cloud)
    if (
        isinstance(path, CloudPath)
        and path.is_file()
        and path.suffix.lower() in [".sqlite", ".npz"]
    ):
        try:
            # update the path to be the local filepath for reference in CytoTable ops
            # note: incurs a data read which will trigger caching of the file
            path = pathlib.Path(path.fspath)
        except InvalidPrefixError:
            # share information about not finding a cloud path
            logger.info(
                "Did not detect a cloud path based on prefix. Defaulting to use local path operations."
            )

    return path


def _arrow_type_cast_if_specified(
    column: Dict[str, str], data_type_cast_map: Dict[str, str]
) -> Dict[str, str]:
    """
    Attempts to cast data types for an PyArrow field using provided a data_type_cast_map.

    Args:
        column: Dict[str, str]:
            Dictionary which includes a column idx, name, and dtype
        data_type_cast_map: Dict[str, str]
            A dictionary mapping data type groups to specific types.
            Roughly includes Arrow data types language from:
            https://arrow.apache.org/docs/python/api/datatypes.html
            Example: {"float": "float32"}

    Returns:
        Dict[str, str]
            A potentially data type updated dictionary of column information
    """

    from cytotable.constants import DDB_DATA_TYPE_SYNONYMS

    # for casting to new float type
    if "float" in data_type_cast_map.keys() and column["column_dtype"] in [
        "REAL",
        "DOUBLE",
    ]:
        return {
            "column_id": column["column_id"],
            "column_name": column["column_name"],
            "column_dtype": [
                key
                for key, value in DDB_DATA_TYPE_SYNONYMS.items()
                if data_type_cast_map["float"] in value
            ][0],
        }

    # for casting to new int type
    elif "integer" in data_type_cast_map.keys() and column["column_dtype"] in [
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
    ]:
        return {
            "column_id": column["column_id"],
            "column_name": column["column_name"],
            "column_dtype": [
                key
                for key, value in DDB_DATA_TYPE_SYNONYMS.items()
                if data_type_cast_map["integer"] in value
            ][0],
        }

    # else we retain the existing data field type
    return column


def _expand_path(
    path: Union[str, pathlib.Path, AnyPath],
) -> Union[pathlib.Path, AnyPath]:
    """
    Expands "~" user directory references with the user's home directory, and expands variable references with values from the environment. After user/variable expansion, the path is resolved and an absolute path is returned.

    Args:
        path: Union[str, pathlib.Path, CloudPath]:
            Path to expand.

    Returns:
        Union[pathlib.Path, Any]
            A local pathlib.Path or Cloudpathlib.AnyPath type path.
    """

    import os
    import pathlib

    from cloudpathlib import AnyPath

    # expand environment variables and resolve the path as absolute
    modifed_path = AnyPath(os.path.expandvars(path))

    # note: we use pathlib.Path here to help expand local paths (~, etc)
    if isinstance(modifed_path, pathlib.Path):
        modifed_path = modifed_path.expanduser()

    return modifed_path.resolve()


def _get_cytotable_version() -> str:
    """
    Seeks the current version of CytoTable using either pkg_resources
    or dunamai to determine the current version being used.

    Returns:
        str
            A string representing the version of CytoTable currently being used.
    """

    try:
        # attempt to gather the development version from dunamai
        # for scenarios where cytotable from source is used.
        import dunamai

        return dunamai.Version.from_any_vcs().serialize()
    except (RuntimeError, ModuleNotFoundError):
        # else grab a static version from __init__.py
        # for scenarios where the built/packaged cytotable is used.
        import cytotable

        return cytotable.__version__


def _write_parquet_table_with_metadata(table: pa.Table, **kwargs) -> None:
    """
    Adds metadata to parquet output from CytoTable.
    Note: this mostly wraps pyarrow.parquet.write_table
    https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html

    Args:
        table: pa.Table:
            Pyarrow table to be serialized as parquet table.
        **kwargs: Any:
            kwargs provided to this function roughly align with
            pyarrow.parquet.write_table. The following might be
            examples of what to expect here:
            - where: str or pyarrow.NativeFile
    """

    from pyarrow import parquet

    from cytotable.constants import CYTOTABLE_DEFAULT_PARQUET_METADATA
    from cytotable.utils import _get_cytotable_version

    parquet.write_table(
        table=table.replace_schema_metadata(
            metadata=CYTOTABLE_DEFAULT_PARQUET_METADATA
        ),
        **kwargs,
    )


def _gather_tablenumber_checksum(pathname: str, buffer_size: int = 1048576) -> int:
    """
    Build and return a checksum for use as a unique identifier across datasets
    referenced from cytominer-database:
    https://github.com/cytomining/cytominer-database/blob/master/cytominer_database/ingest_variable_engine.py#L129

    Args:
        pathname: str:
            A path to a file with which to generate the checksum on.
        buffer_size: int:
            Buffer size to use for reading data.

    Returns:
        int
            an integer representing the checksum of the pathname file.
    """

    import os
    import zlib

    # check whether the buffer size is larger than the file_size
    file_size = os.path.getsize(pathname)
    if file_size < buffer_size:
        buffer_size = file_size

    # open file
    with open(str(pathname), "rb") as stream:
        # begin result formation
        result = zlib.crc32(bytes(0))
        while True:
            # read data from stream using buffer size
            buffer = stream.read(buffer_size)
            if not buffer:
                # if we have no more data to use, break while loop
                break
            # use buffer read data to form checksum
            result = zlib.crc32(buffer, result)

    return result & 0xFFFFFFFF


def _unwrap_value(val: Union[parsl.dataflow.futures.AppFuture, Any]) -> Any:
    """
    Helper function to unwrap futures from values or return values
    where there are no futures.

    Args:
        val: Union[parsl.dataflow.futures.AppFuture, Any]
            A value which may or may not be a Parsl future which
            needs to be evaluated.

    Returns:
        Any
            Returns the value as-is if there's no future, the future
            result if Parsl futures are encountered.
    """

    # if we have a future value, evaluate the result
    if isinstance(val, parsl.dataflow.futures.AppFuture):
        return val.result()
    elif isinstance(val, list):
        # if we have a list of futures, return the results
        if isinstance(val[0], parsl.dataflow.futures.AppFuture):
            return [elem.result() for elem in val]
    # otherwise return the value
    return val


def _unwrap_source(
    source: Union[
        Dict[str, Union[parsl.dataflow.futures.AppFuture, Any]],
        Union[parsl.dataflow.futures.AppFuture, Any],
    ],
) -> Union[Dict[str, Any], Any]:
    """
    Helper function to unwrap futures from sources.

    Args:
        source: Union[
            Dict[str, Union[parsl.dataflow.futures.AppFuture, Any]],
            Union[parsl.dataflow.futures.AppFuture, Any],
        ]
            A source is a portion of an internal data structure used by
            CytoTable for processing and organizing data results.
    Returns:
        Union[Dict[str, Any], Any]
            An evaluated dictionary or other value type.
    """
    # if we have a dictionary, unwrap any values which may be futures
    if isinstance(source, dict):
        return {key: _unwrap_value(val) for key, val in source.items()}
    else:
        # otherwise try to unwrap the source as-is without dictionary nesting
        return _unwrap_value(source)


def evaluate_futures(
    sources: Union[Dict[str, List[Dict[str, Any]]], List[Any], str],
) -> Any:
    """
    Evaluates any Parsl futures for use within other tasks.
    This enables a pattern of Parsl app usage as "tasks" and delayed
    future result evaluation for concurrency.

    Args:
        sources: Union[Dict[str, List[Dict[str, Any]]], List[Any], str]
            Sources are an internal data structure used by CytoTable for
            processing and organizing data results. They may include futures
            which require asynchronous processing through Parsl, so we
            process them through this function.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]
            A data structure which includes evaluated futures where they were found.
    """

    return (
        {
            source_group_name: [
                # unwrap sources into future results
                _unwrap_source(source)
                for source in (
                    source_group_vals.result()
                    # if we have a future, return the result
                    if isinstance(source_group_vals, parsl.dataflow.futures.AppFuture)
                    # otherwise return the value
                    else source_group_vals
                )
            ]
            for source_group_name, source_group_vals in sources.items()
            # if we have a dict, use the above, otherwise unwrap the value in case of future
        }
        if isinstance(sources, dict)
        else _unwrap_value(sources)
    )


def _generate_pagesets(
    keys: List[Union[int, float]], chunk_size: int
) -> List[Tuple[Union[int, float], Union[int, float]]]:
    """
    Generate a pageset (keyset pagination) from a list of keys.

    Parameters:
        keys List[Union[int, float]]:
            List of keys to paginate.
        chunk_size int:
            Size of each chunk/page.

    Returns:
        List[Tuple[Union[int, float], Union[int, float]]]:
            List of (start_key, end_key) tuples representing each page.
    """

    # Initialize an empty list to store the chunks/pages
    chunks = []

    # Start index for iteration through the keys
    i = 0

    while i < len(keys):
        # Get the start key for the current chunk
        start_key = keys[i]

        # Calculate the end index for the current chunk
        end_index = min(i + chunk_size, len(keys)) - 1

        # Get the end key for the current chunk
        end_key = keys[end_index]

        # Ensure non-overlapping by incrementing the start of the next range if there are duplicates
        while end_index + 1 < len(keys) and keys[end_index + 1] == end_key:
            end_index += 1

        # Append the current chunk (start_key, end_key) to the list of chunks
        chunks.append((start_key, end_key))

        # Update the index to start from the next chunk
        i = end_index + 1

    # Return the list of chunks/pages
    return chunks


def _natural_sort(list_to_sort):
    """
    Sorts the given iterable using natural sort adapted from approach
    provided by the following link:
    https://stackoverflow.com/a/4836734

    Args:
      list_to_sort: List:
        The list to sort.

    Returns:
      List: The sorted list.
    """
    import re

    return sorted(
        list_to_sort,
        # use a custom key to sort the list
        key=lambda key: [
            # use integer of c if it's a digit, otherwise str
            int(c) if c.isdigit() else c
            # Split the key into parts, separating numbers from alphabetic characters
            for c in re.split("([0-9]+)", str(key))
        ],
    )


def _extract_npz_to_parquet(
    source_path: str,
    dest_path: str,
    tablenumber: Optional[int] = None,
) -> str:
    """
    Extract data from an .npz file created by DeepProfiler
    as a tabular dataset and write to parquet.

    DeepProfiler creates datasets which look somewhat like this:
    Keys in the .npz file: ['features', 'metadata', 'locations']

    Variable: features
    Shape: (229, 6400)
    Data type: float32

    Variable: locations
    Shape: (229, 2)
    Data type: float64

    Variable: metadata
    Shape: ()
    Data type: object
    Whole object: {
    'Metadata_Plate': 'SQ00014812',
    'Metadata_Well': 'A01',
    'Metadata_Site': 1,
    'Plate_Map_Name': 'C-7161-01-LM6-022',
    'RNA': 'SQ00014812/r01c01f01p01-ch3sk1fk1fl1.png',
    'ER': 'SQ00014812/r01c01f01p01-ch2sk1fk1fl1.png',
    'AGP': 'SQ00014812/r01c01f01p01-ch4sk1fk1fl1.png',
    'Mito': 'SQ00014812/r01c01f01p01-ch5sk1fk1fl1.png',
    'DNA': 'SQ00014812/r01c01f01p01-ch1sk1fk1fl1.png',
    'Treatment_ID': 0,
    'Treatment_Replicate': 1,
    'Treatment': 'DMSO@NA',
    'Compound': 'DMSO',
    'Concentration': '',
    'Split': 'Training',
    'Metadata_Model': 'efficientnet'
    }

    Args:
        source_path: str
            Path to the .npz file.
        dest_path: str
            Destination path for the parquet file.
        tablenumber: Optional[int]
            Optional tablenumber to be added to the data.

    Returns:
        str
            Path to the exported parquet file.
    """

    import pathlib

    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as parquet

    # Load features from the .npz file
    with open(source_path, "rb") as data:
        loaded_npz = np.load(file=data, allow_pickle=True)
        # find the shape of the features, which will help structure
        # data which doesn't yet conform to the same shape (by row count).
        rows = loaded_npz["features"].shape[0]
        # note: we use [()] to load the numpy array as a python dict
        metadata = loaded_npz["metadata"][()]
        # fetch the metadata model name, falling back to "DP" if not found
        feature_prefix = metadata.get("Metadata_Model", "DP")
        # we transpose the feature data for more efficient
        # columnar-focused access
        feature_data = loaded_npz["features"].T

        npz_as_pydict = {
            # add metadata to the table
            # note: metadata within npz files corresponds to a dictionary of
            # various keys and values related to the feature and location data.
            "Metadata_TableNumber": pa.array([tablenumber] * rows, type=pa.int64()),
            "Metadata_NPZSource": pa.array(
                [pathlib.Path(source_path).name] * rows, type=pa.string()
            ),
            **{key: [metadata[key]] * rows for key in metadata.keys()},
            # add locations data to the table
            "Location_Center_X": [loaded_npz["locations"][i][0] for i in range(rows)],
            "Location_Center_Y": [loaded_npz["locations"][i][1] for i in range(rows)],
            # add features data to the table
            **{
                f"{feature_prefix}_{feature_idx + 1}": feature_data[feature_idx]
                for feature_idx in range(feature_data.shape[0])
            },
        }

    # convert the numpy arrays to a PyArrow table and write to parquet
    parquet.write_table(pa.Table.from_pydict(npz_as_pydict), dest_path)

    return dest_path


def map_pyarrow_type(
    field_type: pa.DataType, data_type_cast_map: Optional[Dict[str, str]]
) -> pa.DataType:
    """
    Map PyArrow types dynamically to handle nested types and casting.

    This function takes a PyArrow `field_type` and dynamically maps
    it to a valid PyArrow type, handling nested types (e.g., lists,
    structs) and resolving type conflicts (e.g., integer to float).
    It also supports custom type casting using the
    `data_type_cast_map` parameter.

    Args:
        field_type: pa.DataType
            The PyArrow data type to be mapped.
            This can include simple types (e.g., int, float, string)
            or nested types (e.g., list, struct).
        data_type_cast_map: Optional[Dict[str, str]], default None
            A dictionary mapping data type groups to specific types.
            This allows for custom type casting.
            For example:
            - {"float": "float32"} maps
            floating-point types to `float32`.
            - {"int": "int64"} maps integer
            types to `int64`.
            If `data_type_cast_map` is
            None, default PyArrow types are used.

    Returns:
        pa.DataType
            The mapped PyArrow data type.
            If no mapping is needed, the original
            `field_type` is returned.
    """

    if pa.types.is_list(field_type):
        # Handle list types (e.g., list<element: float>)
        return pa.list_(
            map_pyarrow_type(
                field_type=field_type.value_type, data_type_cast_map=data_type_cast_map
            )
        )
    elif pa.types.is_struct(field_type):
        # Handle struct types recursively
        return pa.struct(
            [
                (
                    field.name,
                    map_pyarrow_type(
                        field_type=field.type, data_type_cast_map=data_type_cast_map
                    ),
                )
                for field in field_type
            ]
        )
    elif pa.types.is_floating(field_type):
        # Handle floating-point types
        if data_type_cast_map and "float" in data_type_cast_map:
            return pa.type_for_alias(data_type_cast_map["float"])
        return pa.float64()  # Default to float64 if no mapping is provided
    elif pa.types.is_integer(field_type):
        # Handle integer types
        if data_type_cast_map and "integer" in data_type_cast_map:
            return pa.type_for_alias(data_type_cast_map["integer"])
        return pa.int64()  # Default to int64 if no mapping is provided
    elif pa.types.is_string(field_type):
        # Handle string types
        return pa.string()
    elif pa.types.is_null(field_type):
        # Handle null types
        return pa.null()
    else:
        # Default to the original type if no mapping is needed
        return field_type

"""
cytotable.apps.source : work related to source data (CSV's, SQLite tables, etc)
"""


import logging
from typing import Any, Dict, List, Optional, Union

from parsl.app.app import python_app

logger = logging.getLogger(__name__)


@python_app
def _gather_tablenumber(
    source_group_name: str, source: Dict[str, Any]
) -> Optional[int]:
    """
    Gathers a "TableNumber" for the table which is a unique identifier intended
    to help differentiate between imagenumbers to create distinct results.

    We use the following steps for this process:
    1. Check if a TableNumber already exists.
    - If it does, we return None (indicating no additional action necessary)
    - If it does not, we proceed below.
    2. Build a checksum based on the table data.
    3. Return this checksum for later use.

    Args:
        source_group_name: str
            Name of the source group (for ex. compartment or metadata table name)
        source: Dict[str, Any]
            Contains the source data to be chunked. Represents a single
            file or table of some kind along with collected information about table.

    Returns:
        str or None
            If string, a checksum of the table
    """

    import zlib

    from cloudpathlib import AnyPath

    from cytotable.utils import _duckdb_reader

    BUFFER_SIZE = 65536

    # select column names from table
    if str(AnyPath(source["source_path"]).suffix).lower() == ".csv":
        query = f"""
            SELECT *
            FROM read_csv_auto('{str(source["source_path"])}')
            LIMIT 1
            """

    elif str(AnyPath(source["source_path"]).suffix).lower() == ".sqlite":
        query = f"""
            SELECT *
            FROM sqlite_scan('{str(source["source_path"])}', '{str(source["table_name"])}')
            LIMIT 1
            """
    # determine if TableNumber is already in the result
    if "TableNumber" in _duckdb_reader().execute(query).arrow().column_names:
        return None

    # build and return a checksum
    # referenced from cytominer-database:
    # https://github.com/cytomining/cytominer-database/blob/master/cytominer_database/ingest_variable_engine.py#L129
    with open(str(source["source_path"]), "rb") as stream:
        result = zlib.crc32(bytes(0))
        while True:
            buffer = stream.read(BUFFER_SIZE)
            if not buffer:
                break
            result = zlib.crc32(buffer, result)

    return result & 0xFFFFFFFF


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
                f"SELECT COUNT(*) from read_csv_auto('{source_path}', parallel=TRUE)"
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

    import duckdb
    from cloudpathlib import AnyPath

    from cytotable.utils import _duckdb_reader, _sqlite_mixed_type_query_to_parquet

    # attempt to build dest_path
    source_dest_path = (
        f"{dest_path}/{str(pathlib.Path(source_group_name).stem).lower()}/"
        f"{str(pathlib.Path(source['source_path']).parent.name).lower()}"
    )
    pathlib.Path(source_dest_path).mkdir(parents=True, exist_ok=True)

    # build tablenumber segment addition (if necessary)
    tablenumber_sql = (
        # to become tablenumber in sql select later with bigint (8-byte integer)
        # we cast here to bigint to avoid concat or join conflicts later due to
        # misaligned automatic data typing.
        f"CAST({source['tablenumber']} AS BIGINT) as TableNumber, "
        if source["tablenumber"] is not None
        # if we don't have a tablenumber value, don't introduce the column
        else ""
    )

    # build output query and filepath base
    # (chunked output will append offset to keep output paths unique)
    if str(AnyPath(source["source_path"]).suffix).lower() == ".csv":
        base_query = f"""SELECT {tablenumber_sql} * from read_csv_auto('{str(source["source_path"])}', parallel=TRUE)"""
        result_filepath_base = f"{source_dest_path}/{str(source['source_path'].stem)}"
    elif str(AnyPath(source["source_path"]).suffix).lower() == ".sqlite":
        base_query = f"""
                SELECT {tablenumber_sql} * from sqlite_scan('{str(source["source_path"])}', '{str(source["table_name"])}')
                """
        result_filepath_base = f"{source_dest_path}/{str(source['source_path'].stem)}.{source['table_name']}"

    result_filepath = f"{result_filepath_base}-{offset}.parquet"

    # attempt to read the data to parquet from duckdb
    # with exception handling to read mixed-type data
    # using sqlite3 and special utility function
    try:
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
    except duckdb.Error as e:
        # if we see a mismatched type error
        # run a more nuanced query through sqlite
        # to handle the mixed types
        if (
            "Mismatch Type Error" in str(e)
            and str(AnyPath(source["source_path"]).suffix).lower() == ".sqlite"
        ):
            result_filepath = _sqlite_mixed_type_query_to_parquet(
                source_path=str(source["source_path"]),
                table_name=str(source["table_name"]),
                chunk_size=chunk_size,
                offset=offset,
                result_filepath=result_filepath,
            )

    # return the filepath for the chunked output file
    return result_filepath

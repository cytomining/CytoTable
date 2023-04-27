"""
Utility functions for CytoTable
"""

import asyncio
import contextlib
import logging
import multiprocessing
import os
import pathlib
import socket
import subprocess
import sys
from typing import Callable, Union

import duckdb

import pyarrow as pa
from cloudpathlib import AnyPath, CloudPath
from cloudpathlib.exceptions import InvalidPrefixError
from prefect.client import get_client
from prefect.settings import get_current_settings


logger = logging.getLogger(__name__)


# referenced from:
# https://github.com/pytest-dev/pytest-asyncio/blob/8755fb5a3883d1f0225188d61cab89ae70a6ba41/pytest_asyncio/plugin.py#L583
def _unused_port(socket_type: int) -> int:
    """Find an unused localhost port from 1024-65535 and return it."""
    with contextlib.closing(socket.socket(type=socket_type)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def unused_tcp_port_factory() -> Callable[[], int]:
    """A factory function, producing different unused TCP ports."""
    produced = set()

    def factory():
        """Return an unused port."""
        port = _unused_port(socket.SOCK_STREAM)

        while port in produced:
            port = _unused_port(socket.SOCK_STREAM)

        produced.add(port)

        return port

    return factory


# referenced from
# https://github.com/PrefectHQ/prefect/blob/779406bcb3d5ea630b28e2062c62d495a64ea9ff/src/prefect/testing/fixtures.py#L40
@contextlib.contextmanager
def hosted_api_server():
    """
    Runs an instance of the Prefect API server in a subprocess instead of the using the
    ephemeral application.

    Uses the same database as the rest of the tests.

    Yields:
        The API URL
    """
    port = unused_tcp_port_factory()()

    # Will connect to the same database as normal test clients
    with subprocess.Popen(
        [
            "uvicorn",
            "--factory",
            "prefect.server.api.server:create_app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "info",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
        env={**os.environ, **get_current_settings().to_environment_variables()},
    ) as process:
        api_url = f"http://localhost:{port}/api"

        # Yield to the consuming tests
        yield api_url

        # Then shutdown the process
        process.terminate()


async def _set_prefect_concurrency_limit() -> asyncio.coroutine:
    async with get_client() as client:
        # set a concurrency limit of 10 on the 'small_instance' tag
        await client.create_concurrency_limit(
            tag="chunk_concurrency", concurrency_limit=5
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

    Returns:
        duckdb.DuckDBPyConnection
    """

    return duckdb.connect().execute(
        # note: we use an f-string here to
        # dynamically configure threads as appropriate
        f"""
        /* Install and load sqlite plugin for duckdb */
        INSTALL sqlite_scanner;
        LOAD sqlite_scanner;

        /*
        Set threads available to duckdb 
        See the following for more information:
        https://duckdb.org/docs/sql/pragmas#memory_limit-threads
        */
        PRAGMA threads={multiprocessing.cpu_count()};

        /* 
        Allow unordered results for performance increase possibilities
        See the following for more information:
        https://duckdb.org/docs/sql/configuration#configuration-reference
        */
        PRAGMA preserve_insertion_order=FALSE;

        /* 
        Allow parallel csv reads for performance increase possibilities
        See the following for more information:
        https://duckdb.org/docs/sql/configuration#configuration-reference
        */
        PRAGMA experimental_parallel_csv=TRUE;
        """,
    )


def _cache_cloudpath_to_local(path: Union[str, AnyPath]) -> pathlib.Path:
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

    candidate_path = AnyPath(path)

    # check that the path is a file (caching won't work with a dir)
    # and check that the file is of sqlite type
    # (other file types will be handled remotely in cloud)
    if candidate_path.is_file() and candidate_path.suffix.lower() == ".sqlite":
        try:
            # update the path to be the local filepath for reference in CytoTable ops
            # note: incurs a data read which will trigger caching of the file
            path = CloudPath(path).fspath
        except InvalidPrefixError:
            # share information about not finding a cloud path
            logger.info(
                "Did not detect a cloud path based on prefix. Defaulting to use local path operations."
            )

    # cast the result as a pathlib.Path
    return pathlib.Path(path)

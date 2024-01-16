"""
conftest.py for pytest
"""

# pylint: disable=line-too-long,unused-argument

import pathlib
import shutil
import socket
import sqlite3
import subprocess
import tempfile
from contextlib import closing
from typing import Any, Dict, Generator, List, Tuple

import boto3
import boto3.session
import duckdb
import pandas as pd
import parsl
import pyarrow as pa
import pytest
from moto import mock_s3
from moto.server import ThreadedMotoServer
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from pyarrow import csv, parquet
from pycytominer.cyto_utils.cells import SingleCells

from cytotable.utils import _column_sort, _default_parsl_config, _parsl_loaded


@pytest.fixture(name="clear_parsl_config", scope="module")
def fixture_clear_parsl_config() -> None:
    """
    Fixture for clearing previously set parsl configurations.

    This is primarily used with the load_parsl_* fixtures in order to avoid
    issues with overlapping and sometimes mixed sequence test execution.
    """

    # check for previously loaded configuration
    if _parsl_loaded():
        # clear the previous config
        parsl.clear()


@pytest.fixture(name="load_parsl_threaded", scope="module")
def fixture_load_parsl_threaded(clear_parsl_config: None) -> None:
    """
    Fixture for loading parsl ThreadPoolExecutor for testing

    See the following for more details.
    https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html

    Note: we use the threadpoolexecutor in some occasions to avoid issues
    with multiprocessing in moto / mocked S3 environments.
    See here for more: https://docs.getmoto.org/en/latest/docs/faq.html#is-moto-concurrency-safe
    """

    parsl.load(
        Config(executors=[ThreadPoolExecutor(label="tpe_for_cytotable_testing")])
    )


@pytest.fixture(name="load_parsl_default", scope="module")
def fixture_load_parsl_default(clear_parsl_config: None) -> None:
    """
    Fixture for loading default cytotable parsl config for tests

    This leverages Parsl's HighThroughputExecutor.
    See here for more: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html
    """

    config = _default_parsl_config()
    # note: we add the debug option here for testing from the default
    # referenced in configuration to help observe testing issues
    config.executors[0].worker_debug = True

    parsl.load(config)


# note: we use name here to avoid pylint flagging W0621
@pytest.fixture(name="fx_tempdir")
def fixture_get_tempdir() -> Generator:
    """
    Provide temporary directory for testing
    """

    tmpdir = tempfile.mkdtemp()

    yield tmpdir

    shutil.rmtree(path=tmpdir, ignore_errors=True)


@pytest.fixture(name="data_dir_cellprofiler")
def fixture_data_dir_cellprofiler() -> str:
    """
    Provide a data directory for cellprofiler test data
    """

    return f"{pathlib.Path(__file__).parent}/data/cellprofiler"


@pytest.fixture(name="data_dir_cytominerdatabase")
def fixture_data_dir_cytominerdatabase() -> str:
    """
    Provide a data directory for cytominerdatabase test data
    """

    return f"{pathlib.Path(__file__).parent}/data/cytominer-database"


@pytest.fixture(name="data_dir_cellprofiler_sqlite_nf1")
def fixture_data_dir_cellprofiler_sqlite_nf1(data_dir_cellprofiler: str) -> str:
    """
    Provide a data directory for cellprofiler sqlite data from
    NF1 SchwannCell Data project.
    """

    return f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"


@pytest.fixture(name="data_dirs_cytominerdatabase")
def fixture_data_dirs_cytominerdatabase(data_dir_cytominerdatabase: str) -> List[str]:
    """
    Provide a data directory for cytominer-database test data
    """

    return [
        f"{data_dir_cytominerdatabase}/data_a",
        f"{data_dir_cytominerdatabase}/data_b",
    ]


@pytest.fixture(name="data_dirs_in_carta")
def fixture_data_dir_in_carta() -> List[str]:
    """
    Provide data directories for in-carta test data
    """

    return [f"{pathlib.Path(__file__).parent}/data/in-carta/colas-lab"]


@pytest.fixture(name="cytominerdatabase_sqlite")
def fixture_cytominerdatabase_sqlite(
    fx_tempdir: str,
    data_dirs_cytominerdatabase: List[str],
) -> List[str]:
    """
    Processed cytominer-database test data as sqlite data
    """

    output_paths = []
    for data_dir in data_dirs_cytominerdatabase:
        # example command for reference as subprocess below
        # cytominer-database ingest source_directory sqlite:///backend.sqlite -c ingest_config.ini
        output_path = f"sqlite:///{fx_tempdir}/{pathlib.Path(data_dir).name}.sqlite"

        # run cytominer-database as command-line call
        subprocess.call(
            [
                "cytominer-database",
                "ingest",
                data_dir,
                output_path,
                "-c",
                f"{data_dir}/config_SQLite.ini",
            ]
        )
        # store the sqlite output file within list to be returned
        output_paths.append(output_path)

    return output_paths


@pytest.fixture()
def cytominerdatabase_to_pycytominer_merge_single_cells_parquet(
    fx_tempdir: str,
    cytominerdatabase_sqlite: List[str],
) -> List[str]:
    """
    Processed cytominer-database test sqlite data as
    pycytominer merged single cell parquet files
    """

    output_paths = []
    for sqlite_file in cytominerdatabase_sqlite:
        # build SingleCells from database and merge single cells into parquet file
        output_paths.append(
            SingleCells(
                sqlite_file,
                strata=["Metadata_Well"],
                image_cols=["TableNumber", "ImageNumber"],
            ).merge_single_cells(
                sc_output_file=f"{fx_tempdir}/{pathlib.Path(sqlite_file).name}.parquet",
                output_type="parquet",
                join_on=["Image_Metadata_Well"],
            )
        )

    return output_paths


@pytest.fixture(name="example_tables")
def fixture_example_tables() -> Tuple[pa.Table, ...]:
    """
    Provide static example tables
    """

    table_image = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Image_Metadata_Plate": pa.array(["001", "001", "002", "002"]),
            "Image_Metadata_Well": pa.array(["A1", "A1", "A2", "A2"]),
        }
    )
    table_cytoplasm = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Cytoplasm_ObjectNumber": pa.array([1, 2, 1, 2]),
            "Cytoplasm_Feature_X": pa.array([0.1, 0.2, 0.1, 0.2]),
        }
    )
    table_cells = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Cells_ObjectNumber": pa.array([1, 2, 1, 2]),
            "Cells_Feature_Y": pa.array([0.01, 0.02, 0.01, 0.02]),
        }
    )
    table_nuclei_1 = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(
                [
                    "1",
                    "1",
                ]
            ),
            "Nuclei_ObjectNumber": pa.array(
                [
                    1,
                    2,
                ]
            ),
            "Nuclei_Feature_Z": pa.array(
                [
                    0.001,
                    0.002,
                ]
            ),
        }
    )

    table_nuclei_2 = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["2", "2"]),
            "Nuclei_ObjectNumber": pa.array([1, 2]),
            "Nuclei_Feature_Z": pa.array([0.001, 0.002]),
        }
    )

    return table_image, table_cytoplasm, table_cells, table_nuclei_1, table_nuclei_2


@pytest.fixture(name="example_local_sources")
def fixture_example_local_sources(
    fx_tempdir: str,
    example_tables: Tuple[pa.Table, ...],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Provide an example source
    """

    for table, number, name in zip(
        example_tables,
        range(0, len(example_tables)),
        ["image", "cytoplasm", "cells", "nuclei", "nuclei"],
    ):
        # build paths for output to land
        pathlib.Path(f"{fx_tempdir}/example/{number}").mkdir(
            parents=True, exist_ok=True
        )
        pathlib.Path(f"{fx_tempdir}/example_dest/{name}/{number}").mkdir(
            parents=True, exist_ok=True
        )
        # write example input
        csv.write_csv(table, f"{fx_tempdir}/example/{number}/{name}.csv")
        # write example output
        parquet.write_table(
            table, f"{fx_tempdir}/example_dest/{name}/{number}/{name}.parquet"
        )

    return {
        "image.csv": [
            {
                "source_path": pathlib.Path(f"{fx_tempdir}/example/0/image.csv"),
                "table": [
                    pathlib.Path(f"{fx_tempdir}/example_dest/image/0/image.parquet")
                ],
            },
        ],
        "cytoplasm.csv": [
            {
                "source_path": pathlib.Path(f"{fx_tempdir}/example/1/cytoplasm.csv"),
                "table": [
                    pathlib.Path(
                        f"{fx_tempdir}/example_dest/cytoplasm/1/cytoplasm.parquet"
                    )
                ],
            }
        ],
        "cells.csv": [
            {
                "source_path": pathlib.Path(f"{fx_tempdir}/example/2/cells.csv"),
                "table": [
                    pathlib.Path(f"{fx_tempdir}/example_dest/cells/2/cells.parquet")
                ],
            }
        ],
        "nuclei.csv": [
            {
                "source_path": pathlib.Path(f"{fx_tempdir}/example/3/nuclei.csv"),
                "table": [
                    pathlib.Path(f"{fx_tempdir}/example_dest/nuclei/3/nuclei.parquet")
                ],
            },
            {
                "source_path": pathlib.Path(f"{fx_tempdir}/example/4/nuclei.csv"),
                "table": [
                    pathlib.Path(f"{fx_tempdir}/example_dest/nuclei/4/nuclei.parquet")
                ],
            },
        ],
    }


@pytest.fixture(name="cellprofiler_merged_examplehuman")
def fixture_cellprofiler_merged_examplehuman(
    data_dir_cellprofiler: str,
) -> pa.Table:
    """
    Fixture for manually configured merged/joined result from
    CellProfiler ExampleHuman CSV Data
    """

    def col_renames(name: str, table: pa.Table):
        """
        Helper function to rename columns appropriately
        """
        return table.rename_columns(
            [
                f"Metadata_{colname}"
                if colname in ["ImageNumber", "ObjectNumber"]
                else f"Metadata_{name}_{colname}"
                if any(name in colname for name in ["Parent_Cells", "Parent_Nuclei"])
                else f"{name}_{colname}"
                if not (colname.startswith(name) or colname.startswith("Metadata_"))
                else colname
                for colname in table.column_names
            ]
        )

    # prepare simulated merge result from convert
    image_table = csv.read_csv(
        f"{data_dir_cellprofiler}/ExampleHuman/Image.csv"
    ).select(["ImageNumber"])
    cytoplasm_table = csv.read_csv(
        f"{data_dir_cellprofiler}/ExampleHuman/Cytoplasm.csv"
    )
    cells_table = csv.read_csv(f"{data_dir_cellprofiler}/ExampleHuman/Cells.csv")
    nuclei_table = csv.read_csv(f"{data_dir_cellprofiler}/ExampleHuman/Nuclei.csv")
    image_table = col_renames(name="Image", table=image_table)
    cytoplasm_table = col_renames(name="Cytoplasm", table=cytoplasm_table)
    cells_table = col_renames(name="Cells", table=cells_table)
    nuclei_table = col_renames(name="Nuclei", table=nuclei_table)

    control_result = (
        duckdb.connect()
        .execute(
            """
            SELECT
                *
            FROM
                image_table AS image
            LEFT JOIN cytoplasm_table AS cytoplasm ON
                cytoplasm.Metadata_ImageNumber = image.Metadata_ImageNumber
            LEFT JOIN cells_table AS cells ON
                cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND cells.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
            LEFT JOIN nuclei_table AS nuclei ON
                nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND nuclei.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
        """
        )
        .arrow()
    )

    # reversed order column check as col removals will change index order
    cols = []
    for i, colname in reversed(list(enumerate(control_result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            control_result = control_result.remove_column(i)

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    control_result = control_result.select(
        sorted(sorted(control_result.column_names), key=_column_sort)
    )

    return control_result


@pytest.fixture(name="cellprofiler_merged_nf1data")
def fixture_cellprofiler_merged_nf1data(
    data_dir_cellprofiler: str,
) -> pa.Table:
    """
    Fixture for manually configured merged/joined result from
    CellProfiler NF1_SchwannCell SQLite Data
    """

    control_result = (
        duckdb.connect()
        # segmented executes below are used to parameterize the sqlite source
        # without using the same parameter in the select query (unnecessary after CALL)
        .execute(
            """
            /* install and load sqlite plugin for duckdb */
            INSTALL sqlite_scanner;
            LOAD sqlite_scanner;

            /* attach sqlite db to duckdb for full table awareness */
            CALL sqlite_attach(?);
            """,
            parameters=[
                f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"
            ],
        )
        .execute(
            """
            /* perform query on sqlite tables through duckdb */
            WITH Per_Image_Filtered AS (
                SELECT
                    ImageNumber,
                    Image_Metadata_Well,
                    Image_Metadata_Plate
                FROM Per_Image
            )
            SELECT *
            FROM Per_Image_Filtered image
            LEFT JOIN Per_Cytoplasm cytoplasm ON
                image.ImageNumber = cytoplasm.ImageNumber
            LEFT JOIN Per_Cells cells ON
                cells.ImageNumber = cytoplasm.ImageNumber
                AND cells.Cells_Number_Object_Number = cytoplasm.Cytoplasm_Parent_Cells
            LEFT JOIN Per_Nuclei nuclei ON
                nuclei.ImageNumber = cytoplasm.ImageNumber
                AND nuclei.Nuclei_Number_Object_Number = cytoplasm.Cytoplasm_Parent_Nuclei
        """
        )
        .arrow()
        .drop_null()
    )

    # reversed order column check as col removals will change index order
    cols = []
    for i, colname in reversed(list(enumerate(control_result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            control_result = control_result.remove_column(i)

    control_result = control_result.rename_columns(
        [
            colname if colname != "ImageNumber" else "Metadata_ImageNumber"
            for colname in control_result.column_names
        ]
    )

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    control_result = control_result.select(
        sorted(sorted(control_result.column_names), key=_column_sort)
    )

    return control_result


@pytest.fixture(name="")
def fixture_cytominerdatabase_merged_cellhealth(
    data_dir_cytominerdatabase: str,
) -> pa.Table:
    """
    Fixture for manually configured merged/joined result from
    CellProfiler -> Cytominer-database Cell-Health SQLite data
    """

    sql_stmt = """
        WITH Image_Filtered AS (
            SELECT
                TableNumber,
                ImageNumber,
                Image_Metadata_Well,
                Image_Metadata_Plate
            FROM Image
        ),
        /* gather unique objectnumber column names from each
        compartment so as to retain differentiation */
        Cytoplasm_renamed AS (
            SELECT
                ObjectNumber AS Cytoplasm_ObjectNumber,
                *
            FROM Cytoplasm
        ),
        Cells_renamed AS (
            SELECT
                ObjectNumber AS Cells_ObjectNumber,
                *
            FROM Cells
        ),
        Nuclei_renamed AS (
            SELECT
                ObjectNumber AS Nuclei_ObjectNumber,
                *
            FROM Nuclei
        )
        SELECT DISTINCT *
        FROM Image_Filtered image
        LEFT JOIN Cytoplasm_renamed cytoplasm ON
            image.ImageNumber = cytoplasm.ImageNumber
        LEFT JOIN Cells_renamed cells ON
            cells.ImageNumber = cytoplasm.ImageNumber
            AND cells.Cells_Number_Object_Number = cytoplasm.Cytoplasm_Parent_Cells
        LEFT JOIN Nuclei_renamed nuclei ON
            nuclei.ImageNumber = cytoplasm.ImageNumber
            AND nuclei.Nuclei_Number_Object_Number = cytoplasm.Cytoplasm_Parent_Nuclei
    """

    # extract a pandas df
    df_from_sqlite = (
        pd.read_sql(
            sql=sql_stmt,
            con=f"sqlite:///{data_dir_cytominerdatabase}/Cell-Health/test-SQ00014613.sqlite",
        )
        # replacing 'nan' strings with None
        .replace(to_replace="nan", value=None)
        # renaming columns as appropriate
        .rename(
            columns={
                "ImageNumber": "Metadata_ImageNumber",
                "TableNumber": "Metadata_TableNumber",
                "Cytoplasm_Parent_Cells": "Metadata_Cytoplasm_Parent_Cells",
                "Cytoplasm_Parent_Nuclei": "Metadata_Cytoplasm_Parent_Nuclei",
            }
            # drop generic objectnumber column gathered from each compartment
            # (we'll rely on the compartment prefixed name instead for comparisons)
        )
        # drop ObjectNumber entirely (we have no reference as to which compartment object)
    ).drop(columns="ObjectNumber")
    # Drop duplicated column names which come from each compartment table
    # in preparation for conversion to arrow.
    # for ex: Metadata_ImageNumber and Metadata_TableNumber
    df_from_sqlite = df_from_sqlite.loc[
        :, ~df_from_sqlite.columns.duplicated(keep="first")
    ]

    # convert to arrow table using the pandas df
    control_result = pa.Table.from_pandas(df=df_from_sqlite)

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    control_result = control_result.select(
        sorted(sorted(control_result.column_names), key=_column_sort)
    )

    return control_result


@pytest.fixture(scope="session", name="infer_open_port")
def fixture_infer_open_port() -> int:
    """
    Infers an open port for use with tests.
    """

    # Referenced with modifications from https://stackoverflow.com/a/45690594/22216869.
    # Note: this implementation opens, temporarily uses an available port, and returns
    # that same port for use in tests. The contextlib.closing context relieves the use
    # of the returned available port.

    # Context for a socket which is opened and automatically closed
    # using family=AF_INET (internet address family socket default)
    # and type=SOCK_STREAM (a socket stream)
    with closing(
        socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    ) as open_socket:
        # Bind the socket to address of format (hostname, port),
        # in this case, localhost and port 0.
        # Using 0 indicates to use an available open port for this work.
        # see: https://docs.python.org/3/library/socket.html#socket-families
        open_socket.bind(("localhost", 0))

        # Set the value of 1 to SO_REUSEADDR as a socket option.
        # see bottom of: https://docs.python.org/3/library/socket.html
        # "The SO_REUSEADDR flag tells the kernel to reuse a local socket in TIME_WAIT state,
        #  without waiting for its natural timeout to expire."
        open_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Return the port value of the socket address of format (hostname, port).
        return open_socket.getsockname()[1]


@pytest.fixture(scope="session", name="s3_session")
def fixture_s3_session(
    infer_open_port: int,
) -> Generator[Tuple[boto3.session.Session, int], None, None]:
    """
    Yield a mocked boto session for s3 tests.
    Return includes port related to session.

    Referenced from:
    https://docs.getmoto.org/en/latest/docs/getting_started.html
    and
    https://docs.getmoto.org/en/latest/docs/server_mode.html#start-within-python
    """

    # start a moto server for use in testing
    server = ThreadedMotoServer(port=infer_open_port)
    server.start()

    with mock_s3():
        yield boto3.session.Session(), infer_open_port


@pytest.fixture()
def example_s3_endpoint(
    s3_session: Tuple[boto3.session.Session, int],
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    data_dir_cellprofiler_sqlite_nf1: str,
) -> str:
    """
    Create an mocked bucket which includes example sources

    Referenced with changes from:
    https://docs.getmoto.org/en/latest/docs/getting_started.html
    """
    # s3 is a fixture defined above that yields a boto3 s3 client.
    endpoint_url = f"http://localhost:{s3_session[1]}"
    bucket_name = "example"

    # create s3 client
    s3_client = s3_session[0].client("s3", endpoint_url=endpoint_url)

    # create a bucket for content to land in
    s3_client.create_bucket(Bucket=bucket_name)

    # upload each example file to the mock bucket
    for source_path in [
        source["source_path"]
        for group in example_local_sources.values()
        for source in group
    ]:
        s3_client.upload_file(
            Filename=str(source_path),
            Bucket=bucket_name,
            # mock nested directory structure within bucket per each file's parent
            Key=f"{source_path.parent.name}/{source_path.name}",
        )

    # upload sqlite example
    s3_client.upload_file(
        Filename=data_dir_cellprofiler_sqlite_nf1,
        Bucket=bucket_name,
        # mock nested directory structure within bucket
        Key=f"nf1/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}",
    )

    # return endpoint url for use in testing
    return endpoint_url


@pytest.fixture()
def example_sqlite_mixed_types_database(
    fx_tempdir: str,
) -> Generator:
    """
    Creates a database which includes mixed type columns
    for testing specific functionality within CytoTable
    """

    # create a temporary sqlite connection
    filepath = f"{fx_tempdir}/example_mixed_types.sqlite"

    # statements for creating database with simple structure
    create_stmts = [
        "DROP TABLE IF EXISTS tbl_a;",
        """
        CREATE TABLE tbl_a (
        col_integer INTEGER NOT NULL
        ,col_text TEXT
        ,col_blob BLOB
        /* note: here we use DOUBLE instead of REAL
        to help test scenarios where the column type
        does not align with values SQLite yields from
        SQL function `typeof()`. In this example,
        SQLite will have a column with type of DOUBLE
        and floating-point values in that column
        will have a type of REAL. */
        ,col_real DOUBLE
        );
        """,
    ]

    # some example values to insert into the database
    insert_vals = [1, "sample", b"sample_blob", 0.5]
    err_values = ["nan", "sample", b"another_blob", "nan"]

    # create the database and insert some data into it
    with sqlite3.connect(filepath) as connection:
        for stmt in create_stmts:
            connection.execute(stmt)

        connection.execute(
            (
                "INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)"
                "VALUES (?, ?, ?, ?);"
            ),
            insert_vals,
        )
        connection.execute(
            (
                "INSERT INTO tbl_a (col_integer, col_text, col_blob, col_real)"
                "VALUES (?, ?, ?, ?);"
            ),
            err_values,
        )

    try:
        yield filepath
    finally:
        # after completing the tests, remove the file
        pathlib.Path(filepath).unlink()

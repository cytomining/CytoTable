"""
conftest.py for pytest
"""

# pylint: disable=line-too-long,unused-argument

import pathlib
import shutil
import sqlite3
import subprocess
import tempfile
from typing import Any, Dict, Generator, List, Tuple

import duckdb
import pandas as pd
import parsl
import pyarrow as pa
import pytest
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from pyarrow import csv, parquet
from pycytominer.cyto_utils.cells import SingleCells
from sqlalchemy.util import deprecations

from cytotable.constants import CYOTABLE_META_COLUMN_TYPES
from cytotable.utils import _column_sort, _default_parsl_config, _parsl_loaded

# filters sqlalchemy 2.0 uber warning
# referenced from: https://stackoverflow.com/a/76308286
deprecations.SILENCE_UBER_WARNING = True


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
    Provide data directories for IN Carta test data
    """

    return [f"{pathlib.Path(__file__).parent}/data/in-carta/colas-lab"]


@pytest.fixture(name="cytominerdatabase_sqlite", scope="function")
def fixture_cytominerdatabase_sqlite(
    tmp_path: str,
    data_dirs_cytominerdatabase: List[str],
) -> List[str]:
    """
    Processed cytominer-database test data as sqlite data
    """

    output_paths = []
    for data_dir in data_dirs_cytominerdatabase:
        # example command for reference as subprocess below
        # cytominer-database ingest source_directory sqlite:///backend.sqlite -c ingest_config.ini
        output_path = f"sqlite:///{tmp_path}/{pathlib.Path(data_dir).name}.sqlite"

        # run cytominer-database as command-line call
        subprocess.run(
            args=[
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

    for sqlite_file in cytominerdatabase_sqlite:
        print(pathlib.Path(sqlite_file.replace("sqlite:///", "")))
        print(pathlib.Path(sqlite_file.replace("sqlite:///", "")).is_file())

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
            "cytotable_meta_source_path": pa.array(
                ["image.csv", "image.csv", "image.csv", "image.csv"]
            ),
            "cytotable_meta_offset": pa.array([50, 50, 100, 100]),
            "cytotable_meta_rownum": pa.array([1, 2, 3, 4]),
        }
    )
    table_cytoplasm = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Cytoplasm_ObjectNumber": pa.array([1, 2, 1, 2]),
            "Cytoplasm_Parent_Cells": pa.array([1, 2, 1, 2]),
            "Cytoplasm_Parent_Nuclei": pa.array([1, 2, 1, 2]),
            "Cytoplasm_Feature_X": pa.array([0.1, 0.2, 0.1, 0.2]),
            "cytotable_meta_source_path": pa.array(
                ["cytoplasm.csv", "cytoplasm.csv", "cytoplasm.csv", "cytoplasm.csv"]
            ),
            "cytotable_meta_offset": pa.array([50, 50, 100, 100]),
            "cytotable_meta_rownum": pa.array([1, 2, 3, 4]),
        }
    )
    table_cells = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["1", "1", "2", "2"]),
            "Cells_ObjectNumber": pa.array([1, 2, 1, 2]),
            "Cells_Feature_Y": pa.array([0.01, 0.02, 0.01, 0.02]),
            "cytotable_meta_source_path": pa.array(
                ["cells.csv", "cells.csv", "cells.csv", "cells.csv"]
            ),
            "cytotable_meta_offset": pa.array([50, 50, 100, 100]),
            "cytotable_meta_rownum": pa.array([1, 2, 3, 4]),
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
            "cytotable_meta_source_path": pa.array(["nuclei_1.csv", "nuclei_1.csv"]),
            "cytotable_meta_offset": pa.array([50, 50]),
            "cytotable_meta_rownum": pa.array([1, 2]),
        }
    )

    table_nuclei_2 = pa.Table.from_pydict(
        {
            "ImageNumber": pa.array(["2", "2"]),
            "Nuclei_ObjectNumber": pa.array([1, 2]),
            "Nuclei_Feature_Z": pa.array([0.001, 0.002]),
            "cytotable_meta_source_path": pa.array(["nuclei_1.csv", "nuclei_1.csv"]),
            "cytotable_meta_offset": pa.array([50, 50]),
            "cytotable_meta_rownum": pa.array([1, 2]),
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
        csv.write_csv(
            # we remove simulated cytotable metadata columns to be more realistic
            # (incoming sources would not usually contain these)
            table.select(
                [
                    column
                    for column in table.column_names
                    if column not in CYOTABLE_META_COLUMN_TYPES
                ]
            ),
            f"{fx_tempdir}/example/{number}/{name}.csv",
        )
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
                (
                    f"Metadata_{colname}"
                    if colname in ["ImageNumber", "ObjectNumber"]
                    else (
                        f"Metadata_{name}_{colname}"
                        if any(
                            name in colname
                            for name in ["Parent_Cells", "Parent_Nuclei"]
                        )
                        else (
                            f"{name}_{colname}"
                            if not (
                                colname.startswith(name)
                                or colname.startswith("Metadata_")
                            )
                            else colname
                        )
                    )
                )
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
            SELECT
                image.ImageNumber,
                image.Image_Metadata_Well,
                image.Image_Metadata_Plate,
                cytoplasm.*,
                cells.*,
                nuclei.*
            FROM Per_Cytoplasm cytoplasm
            LEFT JOIN Per_Cells cells USING (ImageNumber)
            LEFT JOIN Per_Nuclei nuclei USING (ImageNumber)
            LEFT JOIN Per_Image image USING (ImageNumber)
            WHERE
                cells.Cells_Number_Object_Number = cytoplasm.Cytoplasm_Parent_Cells
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
        WITH image_filtered AS (
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
        FROM image_filtered
        LEFT JOIN Cytoplasm_renamed cytoplasm ON
            image_filtered.ImageNumber = cytoplasm.ImageNumber
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


@pytest.fixture(name="example_s3_path_csv_jump")
def fixture_example_s3_path_csv_jump() -> str:
    """
    Provides an example s3 endpoint for use with tests
    """

    return (
        "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/"
        "workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1"
    )


@pytest.fixture(scope="session", name="example_s3_path_sqlite_jump")
def fixture_example_s3_path() -> str:
    """
    Provides an example s3 endpoint for use with tests
    """

    return (
        "s3://cellpainting-gallery/cpg0016-jump/source_4/"
        "workspace/backend/2021_08_23_Batch12/BR00126114"
        "/BR00126114.sqlite"
    )

"""
Shrink datasets from Colas Lab from IN Carta provided as collection of CSV's.

Note: built to be run from CytoTable poetry dev environment from project base, e.g.:
`poetry run python tests/data/in-carta/colas-lab/shrink_colas_lab_data_for_tests.py`
"""

import pathlib

import duckdb
from pyarrow import csv

# set a path for local and target data dir
SOURCE_DATA_DIR = "tests/data/in-carta/colas-lab/data"
TARGET_DATA_DIR = "tests/data/in-carta/colas-lab"


# build a collection of schema
schema_collection = []
for data_file in pathlib.Path(SOURCE_DATA_DIR).rglob("*.csv"):
    with duckdb.connect() as ddb:
        # read the csv file as a pyarrow table and extract detected schema
        schema_collection.append(
            {
                "file": data_file,
                "schema": ddb.execute(
                    f"""
                    SELECT *
                    FROM read_csv_auto('{data_file}')
                    """
                )
                .arrow()
                .schema,
            }
        )

# determine if the schema are exactly alike
for schema in schema_collection:
    for schema_to_compare in schema_collection:
        # compare every schema to all others
        if schema["file"] != schema_to_compare["file"]:
            if not schema["schema"].equals(schema_to_compare["schema"]):
                # if we detect that the schema are inequal, raise an exception
                raise TypeError("Inequal schema detected.")


for idx, data_file in enumerate(pathlib.Path(SOURCE_DATA_DIR).rglob("*.csv")):
    with duckdb.connect() as ddb:
        # Read the csv file with SQL-based filters
        # as a pyarrow table then output to a new and
        # smaller csv for testing purposes.
        csv.write_csv(
            # we use duckdb to filter the original dataset in SQL
            data=ddb.execute(
                f"""
                SELECT *
                FROM read_csv_auto('{data_file}') as data_file
                /* select only the first three objects to limit the dataset */
                WHERE data_file."OBJECT ID" in (1,2,3)
                /* select rows C and D to limit the dataset */
                AND data_file."ROW" in ('C', 'D')
                """
            ).arrow(),
            # output the filtered data as a CSV to a new location
            output_file=f"{TARGET_DATA_DIR}/Test 0 Day{idx} Test Test_2024_Jan-0{idx+1}-{idx+12}-12-12_Test.csv",
        )

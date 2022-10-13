"""
conftest.py for pytest
"""
import os
import pathlib
import tempfile
from typing import Any, Dict, List, Tuple

import boto3
import boto3.session
import pyarrow as pa
import pytest
from moto import mock_s3
from moto.server import ThreadedMotoServer
from pyarrow import csv, parquet


# note: we use name here to avoid pylint flagging W0621
@pytest.fixture(name="get_tempdir")
def fixture_get_tempdir() -> str:
    """
    Provide temporary directory for testing
    """

    return tempfile.gettempdir()


@pytest.fixture()
def data_dir_cellprofiler() -> str:
    """
    Provide a data directory for cellprofiler
    """

    return f"{os.path.dirname(__file__)}/data/cellprofiler"


@pytest.fixture(name="example_tables")
def fixture_example_tables() -> Tuple[pa.Table, pa.Table, pa.Table]:
    """
    Provide example tables
    """
    table_a = pa.Table.from_pydict(
        {
            "n_legs": pa.array(
                [
                    2,
                    4,
                ]
            ),
            "animals": pa.array(
                [
                    "Flamingo",
                    "Horse",
                ]
            ),
            "has_feathers": pa.array(
                [
                    True,
                    False,
                ]
            ),
        }
    )
    table_b = pa.Table.from_pydict(
        {
            "n_legs": pa.array([5.0, 100.0]),
            "animals": pa.array(["Brittle stars", "Centipede"]),
        }
    )
    table_c = pa.Table.from_pydict(
        {
            "color": pa.array(["blue", "red", "green", "orange"]),
        }
    )

    return table_a, table_b, table_c


@pytest.fixture(name="example_local_records")
def fixture_example_local_records(
    get_tempdir: str, example_tables: Tuple[pa.Table, pa.Table, pa.Table]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Provide an example record
    """

    # gather example tables
    table_a, table_b, table_c = example_tables

    # build paths for output to land
    pathlib.Path(f"{get_tempdir}/animals/a").mkdir(parents=True, exist_ok=True)
    pathlib.Path(f"{get_tempdir}/animals/b").mkdir(parents=True, exist_ok=True)
    pathlib.Path(f"{get_tempdir}/animals/c").mkdir(parents=True, exist_ok=True)

    # write mocked input
    csv.write_csv(table_a, f"{get_tempdir}/animals/a/animal_legs.csv")
    csv.write_csv(table_b, f"{get_tempdir}/animals/b/animal_legs.csv")
    csv.write_csv(table_c, f"{get_tempdir}/animals/c/colors.csv")

    # write mocked output
    parquet.write_table(table_a, f"{get_tempdir}/animals/a.animal_legs.parquet")
    parquet.write_table(table_b, f"{get_tempdir}/animals/b.animal_legs.parquet")
    parquet.write_table(table_c, f"{get_tempdir}/animals/colors.parquet")

    return {
        "animal_legs.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/animals/a/animal_legs.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/animals/a.animal_legs.parquet"
                ),
            },
            {
                "source_path": pathlib.Path(f"{get_tempdir}/animals/b/animal_legs.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/animals/b.animal_legs.parquet"
                ),
            },
        ],
        "colors.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/animals/c/colors.csv"),
                "destination_path": pathlib.Path(
                    f"{get_tempdir}/animals/colors.parquet"
                ),
            }
        ],
    }


@pytest.fixture(scope="session", name="s3_session")
def fixture_s3_session() -> boto3.session.Session:
    """
    Yield a mocked boto session for s3 tests.

    Referenced from:
    https://docs.getmoto.org/en/latest/docs/getting_started.html
    and
    https://docs.getmoto.org/en/latest/docs/server_mode.html#start-within-python
    """

    # start a moto server for use in testing
    server = ThreadedMotoServer()
    server.start()

    with mock_s3():
        yield boto3.session.Session()


@pytest.fixture()
def example_s3_endpoint(
    s3_session: boto3.session.Session,
    example_local_records: Dict[str, List[Dict[str, Any]]],
) -> str:
    """
    Create an mocked bucket which includes example records

    Referenced with changes from:
    https://docs.getmoto.org/en/latest/docs/getting_started.html
    """
    # s3 is a fixture defined above that yields a boto3 s3 client.
    # Feel free to instantiate another boto3 S3 client -- Keep note of the region though.
    endpoint_url = "http://localhost:5000"
    bucket_name = "example"

    # create s3 client
    s3_client = s3_session.client("s3", endpoint_url=endpoint_url)

    # create a bucket for content to land in
    s3_client.create_bucket(Bucket=bucket_name)

    # upload each example file to the mock bucket
    for source_path in [
        record["source_path"]
        for group in example_local_records.values()
        for record in group
    ]:
        s3_client.upload_file(
            Filename=str(source_path),
            Bucket=bucket_name,
            # mock nested directory structure within bucket per each file's parent
            Key=f"{source_path.parent.name}/{source_path.name}",
        )

    # return endpoint url for use in testing
    return endpoint_url

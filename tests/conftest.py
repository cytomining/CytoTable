"""
conftest.py for pytest
"""
import os
import pathlib
import tempfile
from typing import Any, Dict, List, Tuple

import pyarrow as pa
import pytest
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


@pytest.fixture()
def example_records(
    get_tempdir: str, example_tables: Tuple[pa.Table, pa.Table, pa.Table]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Provide an example record
    """
    table_a, table_b, table_c = example_tables

    pathlib.Path(f"{get_tempdir}/animals/a").mkdir(parents=True, exist_ok=True)
    pathlib.Path(f"{get_tempdir}/animals/b").mkdir(parents=True, exist_ok=True)
    pathlib.Path(f"{get_tempdir}/animals/c").mkdir(parents=True, exist_ok=True)
    csv.write_csv(table_a, f"{get_tempdir}/animals/a/animal_legs.csv")
    csv.write_csv(table_b, f"{get_tempdir}/animals/b/animal_legs.csv")
    csv.write_csv(table_c, f"{get_tempdir}/animals/c/colors.csv")
    parquet.write_table(table_a, f"{get_tempdir}/animals/a.animal_legs.parquet")
    parquet.write_table(table_b, f"{get_tempdir}/animals/b.animal_legs.parquet")
    parquet.write_table(table_c, f"{get_tempdir}/animals/c.colors.parquet")

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
                    f"{get_tempdir}/animals/c.colors.parquet"
                ),
            }
        ],
    }

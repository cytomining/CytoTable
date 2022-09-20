"""
conftest.py for pytest
"""
import os
import pathlib
import tempfile
from typing import Any, Dict, List

import pyarrow as pa
import pytest


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


@pytest.fixture()
def example_records(get_tempdir: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Provide an example record
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
        }
    )
    table_b = pa.Table.from_pydict(
        {
            "n_legs": pa.array([5, 100]),
            "animals": pa.array(["Brittle stars", "Centipede"]),
        }
    )
    table_c = pa.Table.from_pydict(
        {
            "color": pa.array(["blue", "red", "green", "orange"]),
        }
    )

    return {
        "animal_legs.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/animals/a/animal_legs.csv"),
                "table": table_a,
            },
            {
                "source_path": pathlib.Path(f"{get_tempdir}/animals/b/animal_legs.csv"),
                "table": table_b,
            },
        ],
        "colors.csv": [
            {
                "source_path": pathlib.Path(f"{get_tempdir}/animals/c/colors.csv"),
                "table": table_c,
            }
        ],
    }

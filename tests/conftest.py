"""
conftest.py for pytest
"""
import os
import tempfile

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

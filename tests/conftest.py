"""
conftest.py for pytest
"""
import tempfile

import pytest


# note: we use name here to avoid pylint flagging W0621
@pytest.fixture(name="get_tempdir")
def fixture_get_tempdir() -> str:
    """
    Provide temporary directory for testing
    """

    return tempfile.gettempdir()

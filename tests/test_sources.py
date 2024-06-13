"""
Testing for cytotable/sources.py
"""

import tempfile
import pathlib
import pytest
from cytotable.sources import _file_is_more_than_one_line
from cytotable.exceptions import NoInputDataException

def test_file_is_more_than_one_line():

    # zero lines test
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file_path = pathlib.Path(tmp_file.name)
    with pytest.raises(NoInputDataException):
        _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test file with one line
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmp_file:
        tmp_file.write("This is the only line in the file.")
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert not _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test file with more than one line
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmp_file:
        tmp_file.write("First line.\nSecond line.")
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test multiple line file
    with tempfile.NamedTemporaryFile(delete=False, mode='w') as tmp_file:
        tmp_file.write("First line.\nSecond line.\nThird line.")
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test sqlite file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as tmp_file:
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

"""
Testing for cytotable/sources.py
"""

import pathlib
import tempfile

from cytotable.sources import _file_is_more_than_one_line, _get_source_filepaths


def test_file_is_more_than_one_line():
    """
    Tests for _file_is_more_than_one_line
    """
    # zero lines test
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert not _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test file with one line
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_file:
        tmp_file.write("This is the only line in the file.")
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert not _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test file with more than one line
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_file:
        tmp_file.write("First line.\nSecond line.")
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test multiple line file
    with tempfile.NamedTemporaryFile(delete=False, mode="w") as tmp_file:
        tmp_file.write("First line.\nSecond line.\nThird line.")
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()

    # test sqlite file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite") as tmp_file:
        tmp_file_path = pathlib.Path(tmp_file.name)
    assert _file_is_more_than_one_line(tmp_file_path)
    tmp_file_path.unlink()


def test_get_source_filepaths_with_npz():
    """
    Tests for _get_source_filepaths with combinations of .npz files.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = pathlib.Path(tmp_dir)

        # Create temporary .npz file
        npz_file = tmp_dir_path / "test_file.npz"
        with open(npz_file, mode="wb", encoding="utf-8") as f:
            f.write(b"dummy binary content")

        # Call _get_source_filepaths
        result = _get_source_filepaths(path=tmp_dir_path, source_datatype="npz")

        # Verify that both .npz files are included in the result
        assert len(result) == 1  # One group
        assert any(
            "test_file.npz" in str(file["source_path"])
            for file in result[next(iter(result))]
        )

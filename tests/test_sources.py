"""
Testing for cytotable/sources.py
"""

import os
import pathlib
import shutil
import sqlite3
import tempfile
from contextlib import closing

import pytest

from cytotable.exceptions import SQLiteReadOnlyException
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
        with open(npz_file, mode="wb") as f:
            f.write(b"dummy binary content")

        # Call _get_source_filepaths
        result = _get_source_filepaths(path=tmp_dir_path, source_datatype="npz")

        # Verify that both .npz files are included in the result
        assert len(result) == 1  # One group
        assert any(
            "test_file.npz" in str(file["source_path"])
            for file in result[next(iter(result))]
        )


def test_get_source_filepaths_with_wal_mode_readonly_sqlite():
    """
    Tests that _get_source_filepaths raises a helpful SQLiteReadOnlyException
    (instead of an opaque duckdb.Error) when a source .sqlite file is left in
    WAL journal mode and is missing its '-wal'/'-shm' companion files while
    the file and its directory are read-only. This reproduces the "attempt to
    write a readonly database" error which SQLite raises even for read-only
    queries against such a file.

    This is a meaningful regression check, not a vacuous one: without the
    os.chmod calls below to make the file/directory read-only, no exception
    is raised at all (SQLite is able to create the missing '-wal'/'-shm'
    files on the fly), and if _raise_if_sqlite_readonly_error stops
    converting the underlying duckdb.Error, this test fails because a plain
    duckdb.Error (not a SQLiteReadOnlyException) propagates instead.

    Note this test is skipped when running as root, since root can bypass
    the read-only file permission bits set below, which would prevent the
    "attempt to write a readonly database" error from being reproduced.
    """
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        pytest.skip(
            "root bypasses file permission bits, so read-only setup below"
            " would not reproduce the target error."
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir_path = pathlib.Path(tmp_dir)

        # create a WAL-mode sqlite source with a single table
        source_dir = tmp_dir_path / "source"
        source_dir.mkdir()
        source_path = source_dir / "example.sqlite"
        with closing(sqlite3.connect(source_path)) as conn:
            with conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("CREATE TABLE Image (ImageNumber INTEGER);")
                conn.execute("INSERT INTO Image VALUES (1);")
                conn.commit()

        # copy only the main db file (omitting -wal/-shm companions),
        # simulating a copy/sync which dropped the companion files
        readonly_dir = tmp_dir_path / "readonly"
        readonly_dir.mkdir()
        readonly_path = readonly_dir / "example.sqlite"
        shutil.copy(source_path, readonly_path)

        # make the file and its directory read-only
        os.chmod(readonly_path, 0o444)
        os.chmod(readonly_dir, 0o555)

        try:
            with pytest.raises(SQLiteReadOnlyException, match="journal_mode=DELETE"):
                _get_source_filepaths(
                    path=readonly_dir,
                    targets=["image"],
                )
        finally:
            # restore write permissions so TemporaryDirectory cleanup succeeds
            os.chmod(readonly_dir, 0o755)
            os.chmod(readonly_path, 0o644)

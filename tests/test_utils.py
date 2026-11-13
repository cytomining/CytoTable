"""
Testing CytoTable utility functions found within util.py
"""

# pylint: disable=C0301

import fcntl
import inspect
import pathlib
import subprocess
import sys
import textwrap
from typing import List

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from botocore.exceptions import EndpointConnectionError

from cytotable.utils import (
    _duckdb_reader,
    _generate_pagesets,
    _natural_sort,
    _remove_empty_dirs,
    cloud_glob,
    find_anndata_metadata_field_names,
    map_pyarrow_type,
)


def test_duckdb_reader_uses_no_install_statements():
    """
    Test that _duckdb_reader does not issue INSTALL/LOAD extension statements.

    DuckDB's INSTALL takes an exclusive lock on the extension .info file,
    even when the extension is already installed. When many CytoTable
    threads each open a connection on a filesystem with unreliable file
    locking (for ex. NFS), concurrent INSTALL statements fail with
    "Could not set lock on file" (see cytomining/CytoTable#525). DuckDB's
    autoload setting (enabled by default) loads sqlite_scanner and httpfs
    on demand without any INSTALL/LOAD statements, so none are needed.
    """

    # the executed setup SQL must not contain INSTALL or LOAD
    # statements at all, so inspect the function source
    source = inspect.getsource(_duckdb_reader)
    # isolate the executed SQL from the explanatory docstring
    body = source.split('"""', 2)[2]
    assert "INSTALL " not in body
    assert "LOAD " not in body

    # the reader must still work and keep its pragmas
    with _duckdb_reader() as reader:
        assert (
            reader.execute(
                "SELECT current_setting('preserve_insertion_order')"
            ).fetchone()[0]
            is False
        )
        assert reader.execute("SELECT 41 + 1").fetchone()[0] == 42


def test_duckdb_reader_ignores_locked_extension_info_file(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
):
    """
    Test that a locked extension .info file does not break connections.

    This emulates the NFS conditions from cytomining/CytoTable#525:
    another process holds an exclusive lock on the sqlite_scanner
    extension .info file. If _duckdb_reader issued INSTALL statements,
    the connection would fail while trying to set its own lock. With
    INSTALL removed, the connection must succeed regardless of the lock.
    """

    # point DuckDB's extension directory at a temporary home;
    # monkeypatch restores the original value afterwards
    monkeypatch.setenv("HOME", str(tmp_path))

    # install the extension once so the .info file exists
    with duckdb.connect() as bootstrap:
        bootstrap.execute("INSTALL sqlite_scanner;")

    info_files = list(
        (tmp_path / ".duckdb" / "extensions").glob(
            "**/sqlite_scanner.duckdb_extension.info"
        )
    )
    # old DuckDB versions (for ex. 0.10.1) install without a .info file
    if not info_files:
        pytest.skip("this DuckDB version does not create extension .info files")

    # child process holds an exclusive POSIX record lock on the .info
    # file. lockf matches the locking mechanism DuckDB itself uses
    # (fcntl F_SETLK); plain flock locks do not conflict with record
    # locks on Linux, which would make this test pass vacuously.
    child = subprocess.Popen(  # pylint: disable=consider-using-with
        [
            sys.executable,
            "-c",
            textwrap.dedent(f"""
                import fcntl, time
                handle = open({str(info_files[0])!r}, "a+b")
                fcntl.lockf(handle.fileno(), fcntl.LOCK_EX)
                print("locked", flush=True)
                time.sleep(30)
                """),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "locked"

        # negative control: a conflicting record lock from this process
        # must fail, proving the child really holds an exclusive lock
        with open(info_files[0], "rb") as probe:
            with pytest.raises(OSError):
                fcntl.lockf(probe.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)

        # the connection must succeed while the .info file is locked
        with _duckdb_reader() as reader:
            assert reader.execute("SELECT 41 + 1").fetchone()[0] == 42
    finally:
        child.terminate()
        child.wait()


def test_remove_empty_dirs(tmp_path: pathlib.Path):
    """
    Test that _remove_empty_dirs removes empty subdirectories bottom-up while
    preserving the root and any directory that still contains files.
    """

    # nested empty dirs which should be fully removed
    (tmp_path / "a" / "b" / "c").mkdir(parents=True)
    # a sibling empty dir which should be removed
    (tmp_path / "empty").mkdir()
    # a dir directly containing a file which should be preserved
    (tmp_path / "keep").mkdir()
    kept_file = tmp_path / "keep" / "data.txt"
    kept_file.write_text("x")
    # a dir whose only content is a nested file: both levels preserved
    (tmp_path / "outer" / "inner").mkdir(parents=True)
    nested_file = tmp_path / "outer" / "inner" / "data.txt"
    nested_file.write_text("y")

    _remove_empty_dirs(tmp_path)

    # the root itself is always preserved
    assert tmp_path.is_dir()
    # empty trees are removed entirely
    assert not (tmp_path / "a").exists()
    assert not (tmp_path / "empty").exists()
    # dirs holding files (directly or nested) are preserved
    assert kept_file.is_file()
    assert nested_file.is_file()
    assert (tmp_path / "outer" / "inner").is_dir()


def test_remove_empty_dirs_noop_for_missing_or_file(tmp_path: pathlib.Path):
    """
    Test that _remove_empty_dirs is a no-op for non-existent paths and files.
    """

    # a non-existent path does not raise
    _remove_empty_dirs(tmp_path / "does-not-exist")

    # a file path does not raise and the file is left intact
    a_file = tmp_path / "a_file.txt"
    a_file.write_text("x")
    _remove_empty_dirs(a_file)
    assert a_file.is_file()


def test_generate_pageset():  # pylint: disable=too-many-statements
    """
    Test the generate_pageset function with various scenarios.
    """

    # Test case with a single element
    keys = [1]
    chunk_size = 3
    expected = [(1, 1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case when chunk size is larger than the list
    keys = [1, 2, 3]
    chunk_size = 10
    expected = [(1, 3)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with all elements being the same
    keys = [1, 1, 1, 1, 1]
    chunk_size = 2
    expected = [(1, 1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with one duplicate of chunk size and others
    keys = [1, 1, 1, 2, 3, 4]
    chunk_size = 3
    expected = [(1, 1), (2, 4)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with a chunk size of one
    keys = [1, 2, 3, 4, 5]
    chunk_size = 1
    expected = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with no duplicates
    keys = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunk_size = 3
    expected = [(1, 3), (4, 6), (7, 9), (10, 10)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with non-continuous keys
    keys = [1, 3, 5, 7, 9, 12, 14]
    chunk_size = 2
    expected = [(1, 3), (5, 7), (9, 12), (14, 14)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with inconsistent duplicates
    keys = [1, 1, 3, 4, 5, 5, 8, 8, 8]
    chunk_size = 3
    expected = [(1, 3), (4, 5), (8, 8)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Bigger test case with inconsistent duplicates
    keys = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 12, 12, 12]
    chunk_size = 3
    expected = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (12, 12)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with no duplicates
    keys = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1]
    chunk_size = 3
    expected = [(1.1, 3.3), (4.4, 6.6), (7.7, 9.9), (10.1, 10.1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with non-continuous float keys
    keys = [1.1, 3.3, 5.5, 7.7, 9.9, 12.12, 14.14]
    chunk_size = 2
    expected = [(1.1, 3.3), (5.5, 7.7), (9.9, 12.12), (14.14, 14.14)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with inconsistent duplicates
    keys = [1.1, 1.1, 3.3, 4.4, 5.5, 5.5, 8.8, 8.8, 8.8]
    chunk_size = 3
    expected = [(1.1, 3.3), (4.4, 5.5), (8.8, 8.8)]
    assert _generate_pagesets(keys, chunk_size) == expected


@pytest.mark.parametrize(
    "input_list, expected",
    [
        ([], []),
        (["a1"], ["a1"]),
        (["a1", "a10", "a2", "a3"], ["a1", "a2", "a3", "a10"]),
        (["1", "10", "2", "11", "21", "20"], ["1", "2", "10", "11", "20", "21"]),
        (["b1", "a1", "b2", "a2"], ["a1", "a2", "b1", "b2"]),
        (["apple1", "Apple10", "apple2"], ["Apple10", "apple1", "apple2"]),
        (["a1", "A1", "a10", "A10"], ["A1", "A10", "a1", "a10"]),
        (
            ["a-1", "a-10", "b-2", "B-1", "b-3", "a-2", "A-3"],
            ["A-3", "B-1", "a-1", "a-2", "a-10", "b-2", "b-3"],
        ),
    ],
)
def test_natural_sort(input_list, expected):
    """
    Tests for _natural_sort
    """
    assert _natural_sort(input_list) == expected


def test_map_pyarrow_type():
    """
    Testing map_pyarrow_type
    """
    # Test simple types
    assert map_pyarrow_type(pa.float32(), None) == pa.float64()
    assert map_pyarrow_type(pa.int32(), None) == pa.int64()
    assert map_pyarrow_type(pa.string(), None) == pa.string()
    assert map_pyarrow_type(pa.null(), None) == pa.null()

    # Test nested list types
    assert map_pyarrow_type(pa.list_(pa.float32()), None) == pa.list_(pa.float64())
    assert map_pyarrow_type(pa.list_(pa.int32()), None) == pa.list_(pa.int64())

    # Test custom type casting with data_type_cast_map
    data_type_cast_map = {"float": "float32", "integer": "int32"}
    assert map_pyarrow_type(pa.float64(), data_type_cast_map) == pa.float32()
    assert map_pyarrow_type(pa.int64(), data_type_cast_map) == pa.int32()
    assert map_pyarrow_type(pa.list_(pa.float64()), data_type_cast_map) == pa.list_(
        pa.float32()
    )
    assert map_pyarrow_type(pa.list_(pa.int64()), data_type_cast_map) == pa.list_(
        pa.int32()
    )

    # Test unsupported types (should return the original type)
    unsupported_type = pa.binary()
    assert map_pyarrow_type(unsupported_type, None) == unsupported_type

    # Test nested struct types
    struct_type = pa.struct([("field1", pa.float32()), ("field2", pa.int32())])
    expected_struct_type = pa.struct([("field1", pa.float64()), ("field2", pa.int64())])
    assert map_pyarrow_type(struct_type, None) == expected_struct_type

    # Test custom type casting with nested struct types
    custom_struct_type = pa.struct([("field1", pa.float64()), ("field2", pa.int64())])
    expected_custom_struct_type = pa.struct(
        [("field1", pa.float32()), ("field2", pa.int32())]
    )
    assert (
        map_pyarrow_type(custom_struct_type, data_type_cast_map)
        == expected_custom_struct_type
    )


def test_find_anndata_metadata_field_names(tmp_path: pathlib.Path):
    """
    Testing find_anndata_metadata_field_names
    """
    # Test with a simple schema

    # export a dataframe to parquet with numeric
    # and non-numeric columns
    pd.DataFrame(
        {
            "feature1": [1.0, 2.0, 3.0],
            "feature2": [4.0, 5.0, 6.0],
            "feature3": ["a", "b", "c"],
            "obs1": ["x", "y", "z"],
            "obs2": [True, False, True],
        }
    ).to_parquet((tmp_file := tmp_path / "test.parquet"))

    numeric_colnames, nonnumeric_colnames = find_anndata_metadata_field_names(tmp_file)

    assert numeric_colnames == ["feature1", "feature2"]
    assert nonnumeric_colnames == ["feature3", "obs1", "obs2"]


@pytest.mark.parametrize(
    "root_path, max_matches, expected_result",
    [
        # local data
        (
            "tests/data/cellprofiler/ExampleHuman",
            None,
            [
                "tests/data/cellprofiler/ExampleHuman/Cells.csv",
                "tests/data/cellprofiler/ExampleHuman/Cytoplasm.csv",
                "tests/data/cellprofiler/ExampleHuman/Experiment.csv",
                "tests/data/cellprofiler/ExampleHuman/Image.csv",
                "tests/data/cellprofiler/ExampleHuman/Nuclei.csv",
                "tests/data/cellprofiler/ExampleHuman/PH3.csv",
            ],
        ),
        # large directory of nested results on AWS S3
        (
            "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/",
            10,
            [
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/BR00116991_A01_1/load_data_Cells_BoundingBox_Overlap.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/BR00116991_A01_1/load_data_Nuclei_BoundingBox_Overlap.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_BoundingBox_First.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_BoundingBox_Overlap.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_Cellpose3.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_Donut_Large.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_Donut_Small.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_HighThresh.csv",
                "s3://cellpainting-gallery/cpg0043-segmentation/broad/workspace/analysis/2025_08_21_Batch1/BR00116991/analysis/BR00116991-A01-1/Cells_LowThresh.csv",
            ],
        ),
        # small directory of flat results (only the root contains csv's) on AWS S3
        (
            "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/",
            None,
            [
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Cells.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Cytoplasm.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Experiment.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Image.csv",
                "s3://cellpainting-gallery/cpg0000-jump-pilot/source_4/workspace/analysis/2020_11_04_CPJUMP1/BR00116991/analysis/BR00116991-A01-1/Nuclei.csv",
            ],
        ),
    ],
)
def test_cloud_glob(root_path: str, max_matches: int, expected_result: List[str]):
    """
    Testing cloud_glob utility function.
    """

    # check that our results match expected
    try:
        result = sorted(
            [
                str(p)
                for p in cloud_glob(
                    start=root_path, pattern="**/*.csv", max_matches=max_matches
                )
            ]
        )
    except EndpointConnectionError as exc:
        pytest.skip(
            f"Skipping live cloud listing test because the endpoint is unavailable: {exc}"
        )

    assert result == sorted(expected_result)


@pytest.mark.parametrize(
    "input_kind, pattern, filter_to_files",
    [
        ("path", "**/*.csv", False),
        ("str", "**/*.csv", False),
        # ``**/*`` also yields directories, so filter to files before comparing.
        ("path", "**/*", True),
        ("str", "**/*", True),
    ],
)
def test_cloud_glob_follows_symlinked_directories(
    tmp_path: pathlib.Path,
    input_kind: str,
    pattern: str,
    filter_to_files: bool,
):
    """
    Regression test for https://github.com/cytomining/CytoTable/issues/440.

    Per-site CSVs live in a real directory and are exposed under a separate
    parent through a symlinked subdirectory. cloud_glob must discover the
    CSVs via the symlinked path for both ``Path`` and ``str`` inputs and
    for both extension-anchored and unrestricted glob patterns.
    """
    real = tmp_path / "real" / "analysis"
    real.mkdir(parents=True)
    expected_names = ["Cells.csv", "Cytoplasm.csv", "Image.csv", "Nuclei.csv"]
    for name in expected_names:
        (real / name).write_text("ImageNumber,ObjectNumber\n1,1\n")

    staged = tmp_path / "staged" / "1"
    staged.mkdir(parents=True)
    (staged / "analysis").symlink_to(real, target_is_directory=True)

    staged_root = tmp_path / "staged"
    start = staged_root if input_kind == "path" else str(staged_root)

    results = cloud_glob(start=start, pattern=pattern)
    if filter_to_files:
        results = (p for p in results if pathlib.Path(p).is_file())
    names = sorted(pathlib.Path(p).name for p in results)
    assert names == sorted(expected_names)


@pytest.mark.parametrize(
    "pattern, expected",
    [
        # Anchored, no '**' anywhere -- only the top-level CSV under analysis/.
        ("analysis/*.csv", ["analysis/Cells.csv"]),
        # '**' in the middle of the pattern.
        (
            "analysis/**/*.csv",
            ["analysis/Cells.csv", "analysis/nested/Cells.csv"],
        ),
    ],
)
def test_cloud_glob_supports_non_double_star_prefixed_patterns(
    tmp_path: pathlib.Path,
    pattern: str,
    expected: List[str],
):
    """
    cloud_glob must honour pathlib-glob semantics for patterns that do not
    start with ``**/`` even on Python versions that lack
    ``Path.glob(recurse_symlinks=True)``.
    """
    real = tmp_path / "real" / "analysis"
    real.mkdir(parents=True)
    (real / "Cells.csv").write_text("a,b\n1,2\n")
    (real / "notes.txt").write_text("ignore me")
    nested = real / "nested"
    nested.mkdir()
    (nested / "Cells.csv").write_text("a,b\n3,4\n")

    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "analysis").symlink_to(real, target_is_directory=True)

    results = sorted(
        str(p.relative_to(staged)) for p in cloud_glob(start=staged, pattern=pattern)
    )
    assert results == expected


def test_cloud_glob_deduplicates_paths_with_same_real_target(
    tmp_path: pathlib.Path,
):
    """
    When the same file is reachable through both a real path and a symlink
    inside the search tree, cloud_glob must yield it only once.
    """
    real = tmp_path / "real"
    real.mkdir()
    (real / "Cells.csv").write_text("a,b\n1,2\n")

    # 'mirror' points back at the real directory inside the same search tree,
    # so a naive walk would yield 'real/Cells.csv' and 'mirror/Cells.csv'.
    (tmp_path / "mirror").symlink_to(real, target_is_directory=True)

    results = list(cloud_glob(start=tmp_path, pattern="**/*.csv"))
    assert len(results) == 1
    assert pathlib.Path(results[0]).name == "Cells.csv"


def test_cloud_glob_terminates_on_cyclic_symlinks(tmp_path: pathlib.Path):
    """
    A symlink cycle inside the search tree must not cause cloud_glob to hang
    or recurse without bound.
    """
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "Cells.csv").write_text("a,b\n1,2\n")
    # 'loop' under sub points back at the search root, creating a cycle:
    # tmp_path -> sub -> loop -> tmp_path -> sub -> ...
    (sub / "loop").symlink_to(tmp_path, target_is_directory=True)

    results = list(cloud_glob(start=tmp_path, pattern="**/*.csv"))
    assert [pathlib.Path(p).name for p in results] == ["Cells.csv"]

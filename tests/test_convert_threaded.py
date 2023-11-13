"""
ThreadPoolExecutor-based tests for CytoTable.convert and related.
"""

# pylint: disable=no-member,too-many-lines,unused-argument,line-too-long


import pathlib
from typing import Any, Dict, List, cast

import parsl
import pyarrow as pa
import pytest
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor
from pyarrow import parquet

from cytotable.convert import convert
from cytotable.sources import _get_source_filepaths


def test_convert_tpe_cellprofiler_csv(
    load_parsl_threaded: None,
    fx_tempdir: str,
    data_dir_cellprofiler: str,
    cellprofiler_merged_examplehuman: pa.Table,
):
    """
    Tests non-S3 convert with Parsl ThreadPoolExecutor
    """

    control_result = cellprofiler_merged_examplehuman

    test_result = parquet.read_table(
        convert(
            source_path=f"{data_dir_cellprofiler}/ExampleHuman",
            dest_path=f"{fx_tempdir}/ExampleHuman",
            dest_datatype="parquet",
            source_datatype="csv",
            preset="cellprofiler_csv",
        )
    )

    # sort all values by the same columns
    # we do this due to the potential for inconsistently ordered results
    control_result = control_result.sort_by(
        [(colname, "ascending") for colname in control_result.column_names]
    )
    test_result = test_result.sort_by(
        [(colname, "ascending") for colname in test_result.column_names]
    )

    assert test_result.shape == control_result.shape
    assert test_result.equals(control_result)

    # clean up the parsl config for other tests
    parsl.clear()


def test_convert_s3_path_csv(
    load_parsl_threaded: None,
    fx_tempdir: str,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
    example_s3_endpoint: str,
):
    """
    Tests convert with mocked csv s3 object storage endpoint
    """

    multi_dir_nonconcat_s3_result = convert(
        source_path="s3://example/",
        dest_path=f"{fx_tempdir}/s3_test",
        dest_datatype="parquet",
        concat=False,
        join=False,
        joins=None,
        source_datatype="csv",
        compartments=["cytoplasm", "cells"],
        metadata=["image"],
        identifying_columns=["imagenumber"],
        # endpoint_url here will be used with cloudpathlib client(**kwargs)
        endpoint_url=example_s3_endpoint,
        parsl_config=Config(
            executors=[
                ThreadPoolExecutor(
                    label="tpe_for_cytotable_testing_moto_s3",
                )
            ]
        ),
    )

    # compare each of the results using files from the source
    for control_path, test_path in zip(
        [
            source["table"]
            for group in cast(Dict, multi_dir_nonconcat_s3_result).values()
            for source in group
        ],
        [
            source["table"]
            for group in example_local_sources.values()
            for source in group
        ],
    ):
        parquet_control = parquet.ParquetDataset(path_or_paths=control_path).read()
        parquet_result = parquet.ParquetDataset(
            path_or_paths=test_path, schema=parquet_control.schema
        ).read()

        assert parquet_result.schema.equals(parquet_control.schema)
        assert parquet_result.shape == parquet_control.shape


def test_convert_s3_path_sqlite(
    load_parsl_threaded: None,
    fx_tempdir: str,
    data_dir_cellprofiler_sqlite_nf1: str,
    example_s3_endpoint: str,
):
    """
    Tests convert with mocked sqlite s3 object storage endpoint

    Note: we use a dedicated tmpdir for work in this test to avoid
    race conditions with nested pytest fixture post-yield deletions.
    """

    # local sqlite read
    local_cytotable_table = parquet.read_table(
        source=convert(
            source_path=data_dir_cellprofiler_sqlite_nf1,
            dest_path=(
                f"{fx_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.local.parquet"
            ),
            dest_datatype="parquet",
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
            # note: we use the threadpoolexecutor to avoid issues with multiprocessing
            # in moto / mocked S3 environments.
            # See here for more: https://docs.getmoto.org/en/latest/docs/faq.html#is-moto-concurrency-safe
            parsl_config=Config(
                executors=[
                    ThreadPoolExecutor(
                        label="tpe_for_cytotable_testing_moto_s3",
                    )
                ]
            ),
        )
    )

    # s3 sqlite read with single and directly referenced file
    s3_cytotable_table = parquet.read_table(
        source=convert(
            source_path=f"s3://example/nf1/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}",
            dest_path=(
                f"{fx_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.mocks3.direct.parquet"
            ),
            dest_datatype="parquet",
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
            endpoint_url=example_s3_endpoint,
            # use explicit cache to avoid temp cache removal / overlaps with
            # sequential s3 SQLite files. See below for more information
            # https://cloudpathlib.drivendata.org/stable/caching/#automatically
            local_cache_dir=f"{fx_tempdir}/sqlite_s3_cache/1",
        )
    )

    # s3 sqlite read with nested sqlite file
    s3_cytotable_table_nested = parquet.read_table(
        source=convert(
            source_path="s3://example/nf1/",
            dest_path=(
                f"{fx_tempdir}/{pathlib.Path(data_dir_cellprofiler_sqlite_nf1).name}"
                ".cytotable.mocks3.nested.parquet"
            ),
            dest_datatype="parquet",
            chunk_size=100,
            preset="cellprofiler_sqlite_pycytominer",
            endpoint_url=example_s3_endpoint,
            # use explicit cache to avoid temp cache removal / overlaps with
            # sequential s3 SQLite files. See below for more information
            # https://cloudpathlib.drivendata.org/stable/caching/#automatically
            local_cache_dir=f"{fx_tempdir}/sqlite_s3_cache/2",
        )
    )

    assert local_cytotable_table.sort_by(
        [(name, "ascending") for name in local_cytotable_table.schema.names]
    ).equals(
        s3_cytotable_table.sort_by(
            [(name, "ascending") for name in s3_cytotable_table.schema.names]
        )
    )
    assert local_cytotable_table.sort_by(
        [(name, "ascending") for name in local_cytotable_table.schema.names]
    ).equals(
        s3_cytotable_table_nested.sort_by(
            [(name, "ascending") for name in s3_cytotable_table_nested.schema.names]
        )
    )


def test_get_source_filepaths(
    load_parsl_threaded: None, fx_tempdir: str, data_dir_cellprofiler: str
):
    """
    Tests _get_source_filepaths
    """

    # test that no sources raises an exception
    empty_dir = pathlib.Path(f"{fx_tempdir}/temp")
    empty_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(Exception):
        single_dir_result = _get_source_filepaths(
            path=empty_dir,
            targets=["image", "cells", "nuclei", "cytoplasm"],
        ).result()

    # check that single sqlite file is returned as desired
    single_file_result = _get_source_filepaths(
        path=pathlib.Path(
            f"{data_dir_cellprofiler}/NF1_SchwannCell_data/all_cellprofiler.sqlite"
        ),
        targets=["cells"],
    ).result()
    assert len(set(single_file_result.keys())) == 1

    # check that single csv file is returned as desired
    single_file_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman/Cells.csv"),
        targets=["cells"],
    ).result()
    assert len(set(single_file_result.keys())) == 1

    single_dir_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["cells"],
    ).result()
    # test that the single dir structure includes 1 unique key (for cells)
    assert len(set(single_dir_result.keys())) == 1

    single_dir_result = _get_source_filepaths(
        path=pathlib.Path(f"{data_dir_cellprofiler}/ExampleHuman"),
        targets=["image", "cells", "nuclei", "cytoplasm"],
    ).result()
    # test that the single dir structure includes 4 unique keys
    assert len(set(single_dir_result.keys())) == 4

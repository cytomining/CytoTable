"""
Tests for cytotable.apps.workflow
"""

# pylint: disable=unused-argument

import itertools
from typing import Any, Dict, List, cast

from pyarrow import parquet

from cytotable.apps.workflow import _to_parquet
from cytotable.utils import _duckdb_reader


def test_to_parquet(
    load_parsl: None,
    get_tempdir: str,
    example_local_sources: Dict[str, List[Dict[str, Any]]],
):
    """
    Tests _to_parquet
    """

    flattened_example_sources = list(
        itertools.chain(*list(example_local_sources.values()))
    )

    # note: we cast here for mypy linting (dict and str treatment differ)
    result: Dict[str, List[Dict[str, Any]]] = cast(
        dict,
        _to_parquet(
            source_path=str(
                example_local_sources["image.csv"][0]["source_path"].parent
            ),
            dest_path=get_tempdir,
            source_datatype=None,
            compartments=["cytoplasm", "cells", "nuclei"],
            metadata=["image"],
            identifying_columns=["imagenumber"],
            concat=False,
            join=False,
            joins=None,
            chunk_columns=None,
            chunk_size=4,
            infer_common_schema=False,
            drop_null=True,
            add_tablenumber=False,
        ).result(),
    )

    flattened_results = list(itertools.chain(*list(result.values())))
    for i, flattened_result in enumerate(flattened_results):
        parquet_result = parquet.ParquetDataset(
            path_or_paths=flattened_result["table"]
        ).read()
        csv_source = (
            _duckdb_reader()
            .execute(
                f"""
                select * from
                read_csv_auto('{str(flattened_example_sources[i]["source_path"])}',
                ignore_errors=TRUE)
                """
            )
            .arrow()
        )
        assert parquet_result.schema.equals(csv_source.schema)
        assert parquet_result.shape == csv_source.shape

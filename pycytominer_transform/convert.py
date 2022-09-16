"""
pycytominer-transform: convert - transforming CellProfiler data for
by use with pyctyominer.
"""

import pathlib
from typing import Dict, List, Literal, Optional

import pyarrow as pa
from prefect import flow, task
from pyarrow import csv, parquet


@task
def get_source_filepaths(path: str, targets: List[str]) -> Dict[str, List[Dict]]:
    """

    Args:
      path: str:
      targets: List[str]:

    Returns:

    """

    records = []
    for file in pathlib.Path(path).glob("**/*"):
        if file.is_file and (str(file.stem).lower() in targets or targets is None):
            records.append({"source_path": file})

    if len(records) < 1:
        raise Exception(
            f"No input data to process at path: {str(pathlib.Path(path).resolve())}"
        )

    grouped_records = {}
    for unique_source in set(source["source_path"].name for source in records):
        grouped_records[unique_source] = [
            source for source in records if source["source_path"].name == unique_source
        ]

    return grouped_records


@task
def read_csv(record: Dict) -> Dict:
    """

    Args:
      record: Dict:

    Returns:

    """

    table = csv.read_csv(input_file=record["source_path"])
    record["table"] = table

    return record


@task
def concat_tables(records: Dict[str, List[Dict]]) -> Dict[str, List[Dict]]:
    """

    Args:
      records: List[Dict[str, Any]]:

    Returns:

    """

    for group in records:
        if len(records[group]) < 2:
            continue
        records[group] = [
            {
                "source_path": records[group][0]["source_path"].parent,
                "table": pa.concat_tables(
                    [record["table"] for record in records[group]]
                ),
            }
        ]

    return records


@task
def write_parquet(record: Dict, dest_path: str = "", unique_name: bool = False) -> Dict:
    """

    Args:
      record: Dict:
      unique_name: bool:

    Returns:

    """

    destination_path = f"{dest_path}/{str(record['source_path'].stem)}.parquet"

    if unique_name:
        destination_path = (
            f"{str(record['source_path'].parent.name)}.{destination_path}"
        )

    parquet.write_table(table=record["table"], where=destination_path)

    record["destination_path"] = destination_path

    return record


@task
def infer_source_datatype(records: Dict[str, List[Dict]]) -> str:
    """

    Args:
      records: List[Dict]:

    Returns:

    """

    suffixes = list(set(group.split(".")[-1] for group in records))

    if len(suffixes) > 1:
        raise Exception(
            f"Detected more than one inferred datatypes from source path: {suffixes}"
        )

    return suffixes[0]


@flow
def to_arrow(
    path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    concat: bool = True,
):
    """

    Args:
      path: str:
      source_datatype: Optional[str]:  (Default value = None)
      targets: List[str]:  (Default value = None)
      concat: bool: (Default value = True)

    Returns:

    """

    if targets is None:
        targets = ["image", "cells", "nuclei", "cytoplasm"]

    records = get_source_filepaths(path=path, targets=targets)

    if source_datatype is None:
        source_datatype = infer_source_datatype(records=records)

    for group in records:  # pylint: disable=consider-using-dict-items
        if source_datatype == "csv":
            tables_map = read_csv.map(record=records[group])
        records[group] = [table.wait().result() for table in tables_map]

    if concat:
        records = concat_tables(records=records)

    return records


@flow
def to_parquet(records: Dict[str, List[Dict]], dest_path: str = ""):
    """

    Args:
      records: List[Dict]:

    Returns:

    """

    for group in records:
        if len(records[group]) > 2:
            destinations = write_parquet.map(
                record=records[group], dest_path=dest_path, unique_name=True
            )
        else:
            destinations = write_parquet.map(
                record=records[group], dest_path=dest_path, unique_name=False
            )

        records[group] = [destination.wait().result() for destination in destinations]

    return records


def convert(
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
):
    """

    Args:
      path: str:
      source_datatype: str:
      dest_datatype: str:
      targets: List[str]:  (Default value = None):

    Returns:

    """
    if targets is None:
        targets = ["image", "cells", "nuclei", "cytoplasm"]

    records = to_arrow(
        path=source_path, source_datatype=source_datatype, targets=targets
    )

    if dest_datatype == "parquet":
        output = to_parquet(records=records, dest_path=dest_path)

    return output

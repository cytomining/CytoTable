"""
pycytominer-transform: convert - transforming CellProfiler data for
by use with pyctyominer.
"""

import pathlib
from typing import Dict, List, Optional

from pyarrow import csv, parquet
from prefect import flow, task, get_run_logger


@task
def get_source_filepaths(path: str, targets: List[str]) -> list[Dict]:
    """

    Args:
      path: str:
      targets: List[str]:

    Returns:

    """

    get_run_logger().info("INFO level log message from a task.")

    records = []
    for file in pathlib.Path(path).glob("**/*"):
        if file.is_file and str(file.stem).lower() in targets:
            records.append({"source_path": file})

    if len(records) < 1:
        raise Exception(
            "No input data to process at path: %s" % str(pathlib.Path(path).resolve())
        )

    return records


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
def write_parquet(record: Dict) -> Dict:
    """

    Args:
      record: Dict:

    Returns:

    """

    destination_path = str(record["source_path"].stem) + ".parquet"

    get_run_logger().info(destination_path)
    parquet.write_table(table=record["table"], where=destination_path)

    record["destination_path"] = destination_path

    return record


@task
def infer_source_datatype(records: List[Dict]) -> str:
    """

    Args:
      records: List[Dict]:

    Returns:

    """

    suffixes = list(
        set([(str(record["source_path"].suffix)).lower() for record in records])
    )

    if len(suffixes) > 1:
        raise Exception(
            "Detected more than one inferred datatypes from source path: %s" % suffixes
        )
    else:
        return suffixes[0]


@flow
def to_arrow(
    path: str,
    source_datatype: Optional[str] = None,
    targets: List[str] = ["image", "cells", "nuclei", "cytoplasm"],
):
    """

    Args:
      path: str:
      source_datatype: Optional[str]:  (Default value = None)
      targets: List[str]:  (Default value = ["image")
      "cells":
      "nuclei":
      "cytoplasm"]:

    Returns:

    """

    paths = get_source_filepaths(path=path, targets=targets)

    if source_datatype is None:
        source_datatype = infer_source_datatype(paths=paths)

    if source_datatype == "csv":
        tables = read_csv.map(record=paths)

    result = [table.wait().result() for table in tables]

    return result


@flow
def to_parquet(records: List[Dict]):
    """

    Args:
      records: List[Dict]:

    Returns:

    """

    destinations = write_parquet.map(record=records)

    result = [destination.wait().result() for destination in destinations]

    return result


def convert(
    path: str,
    source_datatype: str,
    dest_datatype: str,
    targets: List[str] = ["image", "cells", "nuclei", "cytoplasm"],
):
    """

    Args:
      path: str:
      source_datatype: str:
      dest_datatype: str:
      targets: List[str]:  (Default value = ["image")
      "cells":
      "nuclei":
      "cytoplasm"]:

    Returns:

    """

    records = to_arrow(path=path, source_datatype=source_datatype, targets=targets)

    if dest_datatype == "parquet":
        output = to_parquet(records=records)

    return output

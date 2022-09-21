"""
pycytominer-transform: convert - transforming data for use with pyctyominer.
"""

import pathlib
from typing import Any, Dict, List, Literal, Optional

import pyarrow as pa
from prefect import flow, task
from prefect.task_runners import BaseTaskRunner, ConcurrentTaskRunner
from pyarrow import csv, parquet

DEFAULT_TARGETS = ["image", "cells", "nuclei", "cytoplasm"]


@task
def get_source_filepaths(
    path: str, targets: List[str]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gather dataset of filepaths from a provided directory path.

    Args:
      path: str:
        Path to seek filepaths within.
      targets: List[str]:
        Target filenames to seek within the provided path.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the targets.
    """

    records = []

    # gathers files from provided path using targets as a filter
    for file in pathlib.Path(path).glob("**/*"):
        if file.is_file() and (str(file.stem).lower() in targets or targets is None):
            records.append({"source_path": file})

    # if we collected no files above, raise exception
    if len(records) < 1:
        raise Exception(
            f"No input data to process at path: {str(pathlib.Path(path).resolve())}"
        )

    grouped_records = {}

    # group files together by similar filename for potential concatenation later
    for unique_source in set(source["source_path"].name for source in records):
        grouped_records[unique_source] = [
            source for source in records if source["source_path"].name == unique_source
        ]

    return grouped_records


@task
def read_csv(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read csv file from record.

    Args:
      record: Dict[str, Any]:
        Data containing filepath to csv file

    Returns:
      Dict[str, Any]
        Updated dictionary with CSV data in-memory
    """

    # read csv using pyarrow lib
    table = csv.read_csv(input_file=record["source_path"])

    # attach table data to record
    record["table"] = table

    return record


@task
def concat_tables(
    records: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Concatenate similar tables together as unified dataset.

    Args:
      records: Dict[str, List[Dict[str, Any]]]:
        Data structure containing potentially grouped data for concatenation.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Updated dictionary with CSV data in-memory
    """

    for group in records:
        # if we have less than 2 records, no need to concat
        if len(records[group]) < 2:
            continue

        # build a new record group
        records[group] = [
            {
                # source path becomes parent's parent dir with the same filename
                "source_path": pathlib.Path(
                    (
                        f"{records[group][0]['source_path'].parent.parent}"
                        f"/{records[group][0]['source_path'].name}"
                    )
                ),
                # table becomes the result of concatted tables
                "table": pa.concat_tables(
                    [record["table"] for record in records[group]]
                ),
            }
        ]

    return records


@task
def write_parquet(
    record: Dict[str, Any], dest_path: str = ".", unique_name: bool = False
) -> Dict[str, Any]:
    """
    Write parquet data using in-memory data.

    Args:
      record: Dict:
        Dictionary including in-memory data which will be written to parquet.
      dest_path: str:  (Default value = ".")
        Destination path to write the parquet file to.
      unique_name: bool:  (Default value = False)
        Determines whether a unique name is necessary for the file.

    Returns:
      Dict[str, Any]
        Updated dictionary containing the destination path where parquet file
        was written.

    """

    # make the dest_path dir if it doesn't already exist
    pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)

    # build a default destination path for the parquet output
    destination_path = pathlib.Path(
        f"{dest_path}/{str(record['source_path'].stem)}.parquet"
    )

    # build unique names to avoid overlaps
    if unique_name:
        destination_path = pathlib.Path(
            (
                f"{dest_path}/{str(record['source_path'].parent.name)}"
                f".{str(record['source_path'].stem)}.parquet"
            )
        )

    # write the table to destination path output
    parquet.write_table(table=record["table"], where=destination_path)

    # update the record to include the destination path
    record["destination_path"] = destination_path

    return record


@task
def infer_source_datatype(
    records: Dict[str, List[Dict[str, Any]]], target_datatype: Optional[str] = None
) -> str:
    """
    Infers and optionally validates datatype (extension) of files.

    Args:
      records: Dict[str, List[Dict[str, Any]]]:
        Grouped datasets of files which will be used by other functions.
      target_datatype: Optional[str]:  (Default value = None)
        Optional target datatype to validate within the context of
        detected datatypes.

    Returns:
      str
        A string of the datatype detected or validated target_datatype.
    """

    # gather file extension suffixes
    suffixes = list(set((group.split(".")[-1]).lower() for group in records))

    # if we don't have a target datatype and have more than one suffix
    # we can't infer which file type to read.
    if target_datatype is None and len(suffixes) > 1:
        raise Exception(
            f"Detected more than one inferred datatypes from source path: {suffixes}"
        )

    # if we have a target datatype and the target isn't within the detected suffixes
    # we will have no files to process.
    if target_datatype is not None and target_datatype not in suffixes:
        raise Exception(
            (
                f"Unable to find targeted datatype {target_datatype} "
                "within files. Detected datatypes: {suffixes}"
            )
        )

    # if we haven't set a target datatype and need to rely on the inferred one
    # set it so it may be returned
    if target_datatype is None:
        target_datatype = suffixes[0]

    return target_datatype


@flow
def to_arrow(
    path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    concat: bool = True,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gather Arrow tables from file-based datasets provided by path.

    Args:
      path: str:
        Where to gather file-based data from.
      source_datatype: Optional[str]:  (Default value = None)
        The source datatype (extension) to use for reading the tables.
      targets: Optional[List[str]]:  (Default value = None)
        The source file names to target within the provided path.
      concat: bool:  (Default value = True)
        Whether to concatenate similar files together as unified
        datasets.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Grouped records which include metadata and table data related
        to files which were read.
    """

    # if we have no targets, set the defaults
    if targets is None:
        targets = DEFAULT_TARGETS

    # gather filepaths which will be used as the basis for this work
    records = get_source_filepaths(path=path, targets=targets)

    # infer or validate the source datatype based on source filepaths
    source_datatype = infer_source_datatype(
        records=records, target_datatype=source_datatype
    )

    for group in records:  # pylint: disable=consider-using-dict-items
        # if the source datatype is csv, read it as mapped records
        if source_datatype == "csv":
            tables_map = read_csv.map(record=records[group])

        # recollect the group of mapped read records
        records[group] = [table.wait().result() for table in tables_map]

    if concat:
        # concat grouped records
        records = concat_tables(records=records)

    return records


@flow
def to_parquet(
    records: Dict[str, List[Dict[str, Any]]], dest_path: str = "."
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Export Arrow data to parquet from dataset groups.

    Args:
      records: Dict[str, List[Dict[str, Any]]]:
        Grouped records which include metadata and table data related
        to files which were read.
      dest_path: str:  (Default value = ".")
        Destination where parquet files will be written.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Grouped records which include metadata about destination filepath
        where parquet file was written.
    """

    # for each group of records, map writing parquet per file
    for group in records:
        # if the record group has more than one file we will require a unique name
        # for each file so it's not overwritten.
        if len(records[group]) > 2:
            destinations = write_parquet.map(
                record=records[group], dest_path=dest_path, unique_name=True
            )
        else:
            destinations = write_parquet.map(
                record=records[group], dest_path=dest_path, unique_name=False
            )

        # recollect the group of mapped written records
        records[group] = [destination.wait().result() for destination in destinations]

    return records


def convert(  # pylint: disable=too-many-arguments
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    concat: bool = True,
    task_runner: BaseTaskRunner = ConcurrentTaskRunner,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert file-based data from various sources to Pycytominer-compatible standards.

    Args:
      source_path: str:
        Path to read source files from.
      dest_path: str:
        Path to write files to.
      dest_datatype: Literal["parquet"]:
        Destination datatype to write to.
      source_datatype: Optional[str]:  (Default value = None)
        Source datatype to focus on during conversion.
      targets: Optional[List[str]]:  (Default value = None)
        Target filenames to use for conversion.
      concat: bool:  (Default value = True)
        Whether to concatenate similar files together.
      task_runner: BaseTaskRunner (Default value = ConcurrentTaskRunner)
        Prefect task runner to use with flows.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Grouped records which include metadata about destination filepath
        where parquet file was written.
    """

    # if we have no targets, set the defaults
    if targets is None:
        targets = DEFAULT_TARGETS

    # collect arrow data from source path
    records = to_arrow.with_options(task_runner=task_runner)(
        path=source_path,
        source_datatype=source_datatype,
        targets=targets,
        concat=concat,
    )

    # send records to be written to parquet if selected
    if dest_datatype == "parquet":
        output = to_parquet.with_options(task_runner=task_runner)(
            records=records, dest_path=dest_path
        )

    return output

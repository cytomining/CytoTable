"""
pycytominer-transform: convert - transforming data for use with pyctyominer.
"""

import pathlib
from typing import Any, Dict, List, Literal, Optional, Union

import pyarrow as pa
from cloudpathlib import AnyPath, CloudPath
from prefect import flow, task, unmapped
from prefect.futures import PrefectFuture
from prefect.task_runners import BaseTaskRunner, ConcurrentTaskRunner
from pyarrow import csv, parquet

DEFAULT_TARGETS = ["image", "cells", "nuclei", "cytoplasm"]


@task
def build_path(
    path: Union[str, pathlib.Path, AnyPath], **kwargs
) -> Union[pathlib.Path, Any]:
    """
    Build a path client
    """

    processed_path = AnyPath(path)

    # set the client for a CloudPath
    if isinstance(processed_path, CloudPath):
        processed_path.client = processed_path.client.__class__(**kwargs)

    return processed_path


@task
def get_source_filepaths(
    path: Union[pathlib.Path, AnyPath], targets: Optional[List[str]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gather dataset of filepaths from a provided directory path.

    Args:
      path: Union[pathlib.Path, Any]:
        Path to seek filepaths within.
      targets: List[str]:
        Target filenames to seek within the provided path.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the targets.
    """

    # gathers files from provided path using targets as a filter
    records = [
        {"source_path": file}
        for file in path.glob("**/*")
        if file.is_file()
        and (
            targets is None
            or str(file.stem).lower() in [target.lower() for target in targets]
        )
    ]

    # if we collected no files above, raise exception
    if len(records) < 1:
        raise Exception(f"No input data to process at path: {str(path)}")

    grouped_records = {}

    # group files together by similar filename for potential concatenation later
    for unique_source in set(source["source_path"].name for source in records):
        grouped_records[unique_source] = [
            source for source in records if source["source_path"].name == unique_source
        ]

    return grouped_records


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


@task
def filter_source_filepaths(
    records: Dict[str, List[Dict[str, Any]]], target_datatype: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Filter source filepaths based on provided target_datatype

    Args:
      records: Dict[str, List[Dict[str, Any]]]
        Grouped datasets of files which will be used by other functions.
      target_datatype: str
        Target datatype to use for filtering the dataset.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the targets.
    """

    return {
        filegroup: [
            file
            for file in files
            # ensure the filesize is greater than 0
            if file["source_path"].stat().st_size > 0
            # ensure the datatype matches the target datatype
            and file["source_path"].suffix == f".{target_datatype}"
        ]
        for filegroup, files in records.items()
    }


@flow
def gather_records(
    source_path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:

    """
    Flow for gathering records for conversion

    Args:
      source_path: str:
        Where to gather file-based data from.
      source_datatype: Optional[str]:  (Default value = None)
        The source datatype (extension) to use for reading the tables.
      targets: Optional[List[str]]:  (Default value = None)
        The source file names to target within the provided path.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the targets.
    """

    source_path = build_path(path=source_path, **kwargs)

    # gather filepaths which will be used as the basis for this work
    records = get_source_filepaths(path=source_path, targets=targets)

    # infer or validate the source datatype based on source filepaths
    source_datatype = infer_source_datatype(
        records=records, target_datatype=source_datatype
    )

    # filter source filepaths to inferred or targeted datatype
    return filter_source_filepaths(records=records, target_datatype=source_datatype)


@task
def read_file(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read csv file from record.

    Args:
      record: Dict[str, Any]:
        Data containing filepath to csv file

    Returns:
      Dict[str, Any]
        Updated dictionary with CSV data in-memory
    """

    if AnyPath(record["source_path"]).suffix == ".csv":  # pylint: disable=no-member
        # define invalid row handler for rows which may be
        # somehow erroneous. See below for more details:
        # https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow-csv-parseoptions
        def skip_erroneous_colcount(row):
            if row.actual_columns != row.expected_columns:
                return "skip"
            return "error"

        # setup parse options
        parse_options = csv.ParseOptions(invalid_row_handler=skip_erroneous_colcount)

        # read csv using pyarrow lib and attach table data to record
        record["table"] = csv.read_csv(
            input_file=record["source_path"],
            parse_options=parse_options,
        )

    return record


@task
def concat_record_group(
    record_group: List[Dict[str, Any]],
    dest_path: Optional[str] = None,
    infer_common_schema: bool = True,
) -> List[Dict[str, Any]]:
    """
    Concatenate group of records together as unified dataset.

    Args:
      records: List[Dict[str, Any]]:
        Data structure containing grouped data for concatenation.
      dest_path: Optional[str] (Default value = None)
        Optional destination path for concatenated records.
      infer_common_schema: bool (Default value = True)
        Whether to infer a common schema when concatenating records.

    Returns:
      List[Dict[str, Any]]
        Updated dictionary containing concatted records
    """

    # if we have nothing to concat, return the record group
    if len(record_group) < 2:
        return record_group

    concatted = [
        {
            # source path becomes parent's parent dir with the same filename
            "source_path": pathlib.Path(
                (
                    f"{record_group[0]['source_path'].parent.parent}"
                    f"/{record_group[0]['source_path'].stem}"
                )
            )
        }
    ]

    destination_path = pathlib.Path(
        (
            f"{dest_path}"
            f"/{record_group[0]['source_path'].parent.parent.name}"
            f".{record_group[0]['source_path'].stem}"
            ".parquet"
        )
    )

    # if there's already a file remove it
    destination_path.unlink(missing_ok=True)

    # read first file for basis of schema and column order for all others
    writer_basis_schema = parquet.read_schema(record_group[0]["destination_path"])

    if infer_common_schema:
        # infer common basis of schema and column order for all others
        for schema in [
            parquet.read_schema(record["destination_path"]) for record in record_group
        ]:

            # account for completely equal schema
            if schema.equals(writer_basis_schema):
                continue

            # gather field names from schema
            schema_field_names = [item.name for item in schema]

            # reversed enumeration because removing indexes ascendingly changes schema field order
            for index, field in reversed(list(enumerate(writer_basis_schema))):

                # check whether field name is contained within writer basis, remove if not
                # note: because this only checks for naming, we defer to initially detected type
                if field.name not in schema_field_names:

                    writer_basis_schema = writer_basis_schema.remove(index)

                # check if we have an integer to float challenge and enable later casting
                elif pa.types.is_integer(field.type) and pa.types.is_floating(
                    schema.field(field.name).type
                ):
                    writer_basis_schema = writer_basis_schema.set(
                        index, field.with_type(pa.float64())
                    )

    if len(list(writer_basis_schema.names)) == 0:
        raise Exception(
            (
                "No common schema basis to perform concatenation for record group."
                " All columns mismatch one another within the group."
            )
        )

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    writer = parquet.ParquetWriter(str(destination_path), writer_basis_schema)

    for table in [record["destination_path"] for record in record_group]:

        # if we haven't inferred the common schema
        # check that our file matches the expected schema, otherwise raise an error
        if not infer_common_schema and not writer_basis_schema.equals(
            parquet.read_schema(table)
        ):
            raise Exception(
                (
                    f"Detected mismatching schema for target concatenation group members:"
                    f" {str(record_group[0]['destination_path'])} and {str(table)}"
                )
            )

        # read the file from the list and write to the concatted parquet file
        # note: we pass column order based on the first chunk file to help ensure schema
        # compatibility for the writer
        writer.write_table(parquet.read_table(table, schema=writer_basis_schema))
        # remove the file which was written in the concatted parquet file (we no longer need it)
        pathlib.Path(table).unlink()

    # close the single concatted parquet file writer
    writer.close()

    # return the concatted parquet filename
    concatted[0]["destination_path"] = destination_path
    return concatted


@task
def write_parquet(
    record: Dict[str, Any], dest_path: str, unique_name: bool = False
) -> Dict[str, Any]:
    """
    Write parquet data using in-memory data.

    Args:
      record: Dict:
        Dictionary including in-memory data which will be written to parquet.
      dest_path: str:
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

    # unset table
    del record["table"]

    # update the record to include the destination path
    record["destination_path"] = destination_path

    return record


@flow
def to_parquet(  # pylint: disable=too-many-arguments
    source_path: Union[str, pathlib.Path, Any],
    dest_path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    concat: bool = True,
    infer_common_schema: bool = True,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Export Arrow data to parquet from dataset groups.

    Args:
      records: Dict[str, List[Dict[str, Any]]]:
        Grouped records which include metadata and table data related
        to files which were read.
      dest_path: str:
        Destination where parquet files will be written.
      concat: bool (Default value = True)
        Whether to concatenate similar records together as one.
      infer_common_schema: bool (Default value = True)
        Whether to infer a common schema when concatenating records.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Grouped records which include metadata about destination filepath
        where parquet file was written.
    """

    # gather records to be processed
    records = gather_records(
        source_path=source_path,
        source_datatype=source_datatype,
        targets=targets,
        **kwargs,
    )

    if not isinstance(records, Dict):
        records = records.result()

    results = {}
    # for each group of records, map writing parquet per file
    for record_group_name, record_group in records.items():

        # read files
        record_group = read_file.map(record=record_group)

        # if the record group has more than one record, we will need a unique name
        unique_name = False
        if len(record_group) >= 2:
            unique_name = True

        # map for writing parquet files with list of files via records
        destinations = write_parquet.map(
            record=record_group,
            dest_path=unmapped(dest_path),
            unique_name=unmapped(unique_name),
        )

        if concat:
            # build a new record group
            results[record_group_name] = concat_record_group.submit(
                record_group=destinations,
                dest_path=dest_path,
                infer_common_schema=infer_common_schema,
            )
        else:
            results[record_group_name] = destinations

    return {
        key: value.result()
        if isinstance(value, PrefectFuture)
        else [inner_result.result() for inner_result in value]
        if not isinstance(value, Dict)
        else value
        for key, value in results.items()
    }


def convert(  # pylint: disable=too-many-arguments
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    default_targets: bool = False,
    concat: bool = True,
    infer_common_schema: bool = True,
    task_runner: BaseTaskRunner = ConcurrentTaskRunner,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert file-based data from various sources to Pycytominer-compatible standards.

    Note: source paths may be object-storage locations using common "s3://..." or similar.

    Args:
      source_path: Union[str, pathlib.Path, AnyPath]:
        str or Path-like reference to read source files from.
      dest_path: str:
        Path to write files to.
      dest_datatype: Literal["parquet"]:
        Destination datatype to write to.
      source_datatype: Optional[str]:  (Default value = None)
        Source datatype to focus on during conversion.
      targets: Optional[List[str]]:  (Default value = None)
        Target filenames to use for conversion.
      default_targets: bool: (Default value = False)
        Whether to use DEFAULT_TARGETS as a reference for targets
      concat: bool:  (Default value = True)
        Whether to concatenate similar files together.
      infer_common_schema: bool (Default value = True)
        Whether to infer a common schema when concatenating records.
      task_runner: BaseTaskRunner (Default value = ConcurrentTaskRunner)
        Prefect task runner to use with flows.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Grouped records which include metadata about destination filepath
        where parquet file was written.


    Example:

      .. code-block:: python

        from pycytominer_transform import convert

        # using an local path
        convert(
            source_path="./tests/data/cellprofiler/csv_single",
            source_datatype="csv",
            dest_path=".",
            default_targets=True,
            dest_datatype="parquet",
        )

        # using an s3-compatible path with no signature for client
        convert(
            source_path="s3://s3path",
            source_datatype="csv",
            dest_path=".",
            dest_datatype="parquet",
            concat=True,
            default_targets=True,
            no_sign_request=True,
        )
    """

    # raise an alert if we have default_targets set and have tried to set targets
    if default_targets and targets is not None:
        raise Exception("Default targets set to True and targets provided.")

    # set the defaults
    if default_targets:
        targets = DEFAULT_TARGETS

    # send records to be written to parquet if selected
    if dest_datatype == "parquet":
        output = to_parquet.with_options(task_runner=task_runner)(
            source_path=source_path,
            source_datatype=source_datatype,
            targets=targets,
            concat=concat,
            dest_path=dest_path,
            infer_common_schema=infer_common_schema,
            **kwargs,
        )

    return output

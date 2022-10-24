"""
pycytominer-transform: convert - transforming data for use with pyctyominer.
"""

import itertools
import pathlib
import uuid
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pyarrow as pa
from cloudpathlib import AnyPath, CloudPath
from prefect import flow, task, unmapped
from prefect.futures import PrefectFuture
from prefect.task_runners import BaseTaskRunner, ConcurrentTaskRunner
from pyarrow import csv, parquet

from pycytominer_transform.exceptions import (
    DatatypeException,
    NoInputDataException,
    SchemaException,
)

DEFAULT_NAMES_COMPARTMENTS = ("cells", "nuclei", "cytoplasm")
DEFAULT_NAMES_METADATA = ("image",)
DEFAULT_MERGE_COLUMNS_COMPARTMENTS = ("ImageNumber", "ObjectNumber")
DEFAULT_MERGE_COLUMNS_METADATA = ("ImageNumber",)
DEFAULT_MERGE_CHUNK_SIZE = 1000


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
      compartments: List[str]:
        Compartment filenames to seek within the provided path.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the compartments.
    """

    # gathers files from provided path using compartments as a filter
    records = [
        {"source_path": file}
        for file in path.glob("**/*")
        if file.is_file()
        and (
            targets is None
            or str(file.stem).lower()
            in [compartment.lower() for compartment in targets]
        )
    ]

    # if we collected no files above, raise exception
    if len(records) < 1:
        raise NoInputDataException(f"No input data to process at path: {str(path)}")

    grouped_records = {}

    # group files together by similar filename for potential concatenation later
    for unique_source in set(source["source_path"].name for source in records):
        grouped_records[unique_source] = [
            source for source in records if source["source_path"].name == unique_source
        ]

    return grouped_records


@task
def infer_source_datatype(
    records: Dict[str, List[Dict[str, Any]]], source_datatype: Optional[str] = None
) -> str:
    """
    Infers and optionally validates datatype (extension) of files.

    Args:
      records: Dict[str, List[Dict[str, Any]]]:
        Grouped datasets of files which will be used by other functions.
      source_datatype: Optional[str]:  (Default value = None)
        Optional source datatype to validate within the context of
        detected datatypes.

    Returns:
      str
        A string of the datatype detected or validated source_datatype.
    """

    # gather file extension suffixes
    suffixes = list(set((group.split(".")[-1]).lower() for group in records))

    # if we don't have a source datatype and have more than one suffix
    # we can't infer which file type to read.
    if source_datatype is None and len(suffixes) > 1:
        raise DatatypeException(
            f"Detected more than one inferred datatypes from source path: {suffixes}"
        )

    # if we have a source datatype and it isn't within the detected suffixes
    # we will have no files to process.
    if source_datatype is not None and source_datatype not in suffixes:
        raise DatatypeException(
            (
                f"Unable to find source datatype {source_datatype} "
                "within files. Detected datatypes: {suffixes}"
            )
        )

    # if we haven't set a source datatype and need to rely on the inferred one
    # set it so it may be returned
    if source_datatype is None:
        source_datatype = suffixes[0]

    return source_datatype


@task
def filter_source_filepaths(
    records: Dict[str, List[Dict[str, Any]]], source_datatype: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Filter source filepaths based on provided source_datatype

    Args:
      records: Dict[str, List[Dict[str, Any]]]
        Grouped datasets of files which will be used by other functions.
      source_datatype: str
        Source datatype to use for filtering the dataset.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the datatype.
    """

    return {
        filegroup: [
            file
            for file in files
            # ensure the filesize is greater than 0
            if file["source_path"].stat().st_size > 0
            # ensure the datatype matches the source datatype
            and file["source_path"].suffix == f".{source_datatype}"
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
      compartments: Optional[List[str]]:  (Default value = None)
        The source file names to target within the provided path.

    Returns:
      Dict[str, List[Dict[str, Any]]]
        Data structure which groups related files based on the compartments.
    """

    source_path = build_path(path=source_path, **kwargs)

    # gather filepaths which will be used as the basis for this work
    records = get_source_filepaths(path=source_path, targets=targets)

    # infer or validate the source datatype based on source filepaths
    source_datatype = infer_source_datatype(
        records=records, source_datatype=source_datatype
    )

    # filter source filepaths to inferred or source datatype
    return filter_source_filepaths(records=records, source_datatype=source_datatype)


@task
def read_file(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read csv file from record.

    Args:
      record: Dict[str, Any]:
        Data containing filepath to csv file

    Returns:
      record: Dict[str, Any]
        Updated record (Dict[str, Any]) with CSV data in-memory
    """

    if AnyPath(record["source_path"]).suffix == ".csv":  # pylint: disable=no-member
        # define invalid row handler for rows which may be
        # somehow erroneous. See below for more details:
        # https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow-csv-parseoptions
        def skip_erroneous_colcount(row):
            return "skip" if row.actual_columns != row.expected_columns else "error"

        # setup parse options
        parse_options = csv.ParseOptions(invalid_row_handler=skip_erroneous_colcount)

        # read csv using pyarrow lib and attach table data to record
        record["table"] = csv.read_csv(
            input_file=record["source_path"],
            parse_options=parse_options,
        )

    return record


@task
def prepend_column_name(
    record: Dict[str, Any],
    record_group_name: str,
    merge_columns: Optional[Union[List[str], Tuple[str, ...]]] = None,
) -> Dict[str, Any]:
    """
    Rename columns using the record group name
    """

    record["table"] = record["table"].rename_columns(
        [
            f"{str(pathlib.Path(record_group_name).stem)}_{column_name}"
            if merge_columns and column_name not in merge_columns
            else column_name
            for column_name in record["table"].column_names
        ]
    )

    return record


@task
def concat_record_group(
    record_group: List[Dict[str, Any]],
    dest_path: str = ".",
    common_schema: List[Tuple[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    Concatenate group of records together as unified dataset.

    Args:
      record_group: List[Dict[str, Any]]:
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

    # check whether we already have a file as dest_path
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink(missing_ok=True)

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

    if common_schema is not None:
        writer_schema = pa.schema(common_schema)

    # build a parquet file writer which will be used to append files
    # as a single concatted parquet file, referencing the first file's schema
    # (all must be the same schema)
    with parquet.ParquetWriter(str(destination_path), writer_schema) as writer:

        for table in [record["destination_path"] for record in record_group]:
            # if we haven't inferred the common schema
            # check that our file matches the expected schema, otherwise raise an error
            if common_schema is None and not writer_schema.equals(
                parquet.read_schema(table)
            ):
                raise SchemaException(
                    (
                        f"Detected mismatching schema for target concatenation group members:"
                        f" {str(record_group[0]['destination_path'])} and {str(table)}"
                    )
                )

            # read the file from the list and write to the concatted parquet file
            # note: we pass column order based on the first chunk file to help ensure schema
            # compatibility for the writer
            writer.write_table(parquet.read_table(table, schema=writer_schema))
            # remove the file which was written in the concatted parquet file (we no longer need it)
            pathlib.Path(table).unlink()

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

    # unlink the file if it exists
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink()

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


@task
def get_merge_chunks(
    records: Dict[str, List[Dict[str, Any]]],
    metadata: Union[List[str], Tuple[str, ...]],
    merge_columns_compartments: Union[List[str], Tuple[str, ...]],
    merge_chunk_size: int,
) -> List[List[Dict[str, Any]]]:
    """
    Reads first record as reference for building merge chunks by merge_columns

    records: Dict[List[Dict[str, Any]]]:
      Grouped datasets of files which will be used by other functions.
    merge_columns: Union[List[str], Tuple[str, ...]]:
      Columns to use to form merge chunk result
    merge_chunk_size: int:
      Size of chunks to use for merge chunk result

    Returns:
      List[List[Dict[str, Any]]]]:
        A list of lists with at most chunk size length that contain merge keys
    """

    # fetch the compartment concat result as the basis for merge groups
    for record in records.values():
        if pathlib.Path(record[0]["destination_path"]).stem.lower() not in [
            name.lower() for name in metadata
        ]:
            first_result = record
            break

    # gather the workflow result for basis if it's not yet returned
    basis = (
        first_result.result()
        if isinstance(first_result, PrefectFuture)
        else first_result
    )

    # read only the table's merge_columns
    merge_column_rows = parquet.read_table(
        source=basis[0]["destination_path"], columns=merge_columns_compartments
    ).to_pylist()

    # build and return the chunked merge column rows
    return [
        merge_column_rows[i : i + merge_chunk_size]
        for i in range(0, len(merge_column_rows), merge_chunk_size)
    ]


@task
def merge_record_chunk(
    records: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    compartments: Union[List[str], Tuple[str, ...]],
    metadata: Union[List[str], Tuple[str, ...]],
    merge_columns_compartments: Union[List[str], Tuple[str, ...]],
    merge_columns_metadata: Union[List[str], Tuple[str, ...]],
    merge_group: List[Dict[str, Any]],
) -> str:
    """
    Merge records based on merge group keys (group of specific merge column values)
    """

    # form filters variable for use in pyarrow.parquet.read_table
    # see the following for more details on the filters argument expectations:
    # https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html
    compartment_filters = [
        [
            # create groups of merge column filters where values always
            # are expected to equal those within the merge_group together
            [merge_column, "=", merge_column_value]
            for merge_column, merge_column_value in chunk.items()
            if merge_column in merge_columns_compartments
        ]
        for chunk in merge_group
    ]
    metadata_filters = [
        [
            # create groups of merge column filters where values always
            # are expected to equal those within the merge_group together
            [merge_column, "=", merge_column_value]
            for merge_column, merge_column_value in chunk.items()
            if merge_column in merge_columns_metadata
        ]
        for chunk in merge_group
    ]

    records_flat = list(itertools.chain(*list(records.values())))

    # begin looping through the other records after the first
    for index, record in enumerate(records_flat):
        # determine whether we're working with an incoming compartment or metadata
        is_compartment = (
            True
            if any(
                [
                    compartment.lower()
                    in AnyPath(record["destination_path"]).stem.lower()
                    for compartment in compartments
                ]
            )
            else False
        )

        if index == 0:
            # spur the result using the first record bas a basis for the loop below
            result = parquet.read_table(
                source=records_flat[0]["destination_path"],
                filters=(compartment_filters if is_compartment else metadata_filters),
            )

        else:
            # join the record to the result
            result = result.join(
                right_table=parquet.read_table(
                    source=record["destination_path"],
                    filters=(
                        compartment_filters if is_compartment else metadata_filters
                    ),
                ),
                # determine the keys to join by using the compartment or metadata names
                keys=(
                    merge_columns_compartments
                    if is_compartment
                    else merge_columns_metadata
                ),
                # use right outer join to expand the dataset as needed
                join_type="full outer",
            )

    result_file_path = (
        # store the result in the parent of the dest_path
        f"{str(pathlib.Path(dest_path).parent)}/"
        # use the dest_path stem in the name
        f"{str(pathlib.Path(dest_path).stem)}-"
        # give the merge chunk result a unique to arbitrarily
        # differentiate from other chunk groups which are mapped
        # and before they are brought together as one dataset
        f"{str(uuid.uuid4().hex)}.parquet"
    )

    # write the result
    parquet.write_table(
        table=result,
        where=result_file_path,
    )

    return result_file_path


@task
def concat_merge_records(
    dest_path: str, merge_records: List[str], records: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Concatenate merge records from parquet-based chunks.
    """

    # remove the unmerged concatted compartments to prepare final dest_path usage
    # (we now have merged results)
    flattened_records = list(itertools.chain(*list(records.values())))
    for record in flattened_records:
        pathlib.Path(record["destination_path"]).unlink(missing_ok=True)

    # remove dir if we have it
    if pathlib.Path(dest_path).is_dir():
        pathlib.Path(dest_path).rmdir()

    # also remove any pre-existing files which may already be at file destination
    pathlib.Path(dest_path).unlink(missing_ok=True)

    # write the concatted result as a parquet file
    parquet.write_table(
        table=pa.concat_tables(
            tables=[parquet.read_table(table_path) for table_path in merge_records]
        ),
        where=dest_path,
    )

    # remove merge chunks as we have the final result
    for table_path in merge_records:
        pathlib.Path(table_path).unlink()

    # return modified records format to indicate the final result
    # and retain the other record data for reference as needed
    return {
        pathlib.Path(dest_path).name: [{"destination_path": dest_path}],
        "sources": flattened_records,
    }


@task
def infer_record_group_common_schema(record_group: List[Dict[str, Any]]):
    """
    Infers a common schema for record group
    """

    # read first file for basis of schema and column order for all others
    common_schema = parquet.read_schema(record_group[0]["destination_path"])

    # infer common basis of schema and column order for all others
    for schema in [
        parquet.read_schema(record["destination_path"]) for record in record_group
    ]:

        # account for completely equal schema
        if schema.equals(common_schema):
            continue

        # gather field names from schema
        schema_field_names = [item.name for item in schema]

        # reversed enumeration because removing indexes ascendingly changes schema field order
        for index, field in reversed(list(enumerate(common_schema))):

            # check whether field name is contained within writer basis, remove if not
            # note: because this only checks for naming, we defer to initially detected type
            if field.name not in schema_field_names:

                common_schema = common_schema.remove(index)

            # check if we have an integer to float challenge and enable later casting
            elif pa.types.is_integer(field.type) and pa.types.is_floating(
                schema.field(field.name).type
            ):
                common_schema = common_schema.set(index, field.with_type(pa.float64()))

    if len(list(common_schema.names)) == 0:
        raise SchemaException(
            (
                "No common schema basis to perform concatenation for record group."
                " All columns mismatch one another within the group."
            )
        )

    # return a python-native list of tuples with column names and str types
    return list(
        zip(
            common_schema.names,
            [str(schema_type) for schema_type in common_schema.types],
        )
    )


@flow
def to_parquet(  # pylint: disable=too-many-arguments
    source_path: str,
    dest_path: str,
    source_datatype: Optional[str],
    compartments: Union[List[str], Tuple[str, ...]],
    metadata: Union[List[str], Tuple[str, ...]],
    concat: bool,
    merge: bool,
    merge_columns_compartments: Optional[Union[List[str], Tuple[str, ...]]],
    merge_columns_metadata: Optional[Union[List[str], Tuple[str, ...]]],
    merge_chunk_size: Optional[int],
    infer_common_schema: bool,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Export data to parquet.

    Args:
      source_path: str:
        str reference to read source files from.
        Note: may be local or remote object-storage location using convention "s3://..." or similar.
      dest_path: str:
        Path to write files to.
        Note: this may only be a local path.
      source_datatype: Optional[str]: (Default value = None)
        Source datatype to focus on during conversion.
      compartments: Union[List[str], Tuple[str, ...]]: (Default value = None)
        Compartment names to use for conversion.
      concat: bool:
        Whether to concatenate similar files together.
      merge: bool:
        Whether to merge the compartment data together into one dataset
      merge_columns: Optional[Union[List[str], Tuple[str, ...]]],
        Column names which appear in all compartments to use when performing merge
      merge_chunk_size: Optional[int],
        Size of merge chunks which is used to limit data size during merge ops
      infer_common_schema: bool:  (Default value = True)
        Whether to infer a common schema when concatenating records.

    Returns:
      Dict[str, List[Dict[str, Any]]]:
        Grouped records which include metadata about destination filepath
        where parquet file was written.
    """
    # gather records to be processed
    records = gather_records(
        source_path=source_path,
        source_datatype=source_datatype,
        targets=list(compartments) + list(metadata),
        **kwargs,
    )

    if not isinstance(records, Dict):
        records = records.result()

    results = {}
    # for each group of records, map writing parquet per file
    for record_group_name, record_group in records.items():

        # read files
        record_group = read_file.map(record=record_group)

        # rename cols to include compartment or meta names
        renamed_record_group = prepend_column_name.map(
            record=record_group,
            record_group_name=unmapped(record_group_name),
            merge_columns=unmapped(
                list(merge_columns_compartments)
                if merge_columns_compartments is not None
                else [] + list(merge_columns_metadata)
                if merge_columns_metadata is not None
                else []
            ),
        )

        # map for writing parquet files with list of files via records
        results[record_group_name] = write_parquet.map(
            record=renamed_record_group,
            dest_path=unmapped(dest_path),
            # if the record group has more than one record, we will need a unique name
            # arg set to true or false based on evaluation of len(record_group)
            unique_name=unmapped(len(renamed_record_group) >= 2),
        )

        if concat and infer_common_schema:
            common_schema = infer_record_group_common_schema(
                record_group=results[record_group_name]
            )

        # if concat or merge, concat the record groups
        # note: merge implies a concat, but concat does not imply a merge
        if concat or merge:
            # build a new concatenated record group
            results[record_group_name] = concat_record_group.submit(
                record_group=results[record_group_name],
                dest_path=dest_path,
                common_schema=common_schema,
            )

    # conditional section for merging
    # note: merge implies a concat, but concat does not imply a merge
    if merge:
        # map merged results based on the merge groups gathered above
        # note: after mapping we end up with a list of strings (task returns str)
        merge_records_result = merge_record_chunk.map(
            # gather the result of concatted records prior to
            # merge group merging as each mapped task run will need
            # full concat results
            records=unmapped(
                {
                    key: value.result() if isinstance(value, PrefectFuture) else value
                    for key, value in results.items()
                }
            ),
            dest_path=unmapped(dest_path),
            compartments=unmapped(compartments),
            metadata=unmapped(metadata),
            merge_columns_compartments=unmapped(merge_columns_compartments),
            merge_columns_metadata=unmapped(merge_columns_metadata),
            # get merging chunks by merge columns
            merge_group=get_merge_chunks(
                records=results,
                merge_columns_compartments=merge_columns_compartments,
                merge_chunk_size=merge_chunk_size,
                metadata=metadata,
            ),
        )

        # concat our merge chunks together as one cohesive dataset
        # return results in common format which includes metadata
        # for lineage and debugging
        results = concat_merge_records(
            dest_path=dest_path,
            merge_records=(
                merge_records_result.result()
                if isinstance(merge_records_result, PrefectFuture)
                else merge_records_result
            ),
            records=results,
        )

    return {
        key: value.result()
        if isinstance(value, PrefectFuture)
        else [
            inner_result.result()
            if isinstance(inner_result, PrefectFuture)
            else inner_result
            for inner_result in value
        ]
        if not isinstance(value, Dict)
        else value
        for key, value in results.items()
    }


def convert(  # pylint: disable=too-many-arguments
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    compartments: Union[List[str], Tuple[str, ...]] = DEFAULT_NAMES_COMPARTMENTS,
    metadata: Union[List[str], Tuple[str, ...]] = DEFAULT_NAMES_METADATA,
    concat: bool = True,
    merge: bool = True,
    merge_columns_compartments: Optional[
        Union[List[str], Tuple[str, ...]]
    ] = DEFAULT_MERGE_COLUMNS_COMPARTMENTS,
    merge_columns_metadata: Optional[
        Union[List[str], Tuple[str, ...]]
    ] = DEFAULT_MERGE_COLUMNS_METADATA,
    merge_chunk_size: Optional[int] = DEFAULT_MERGE_CHUNK_SIZE,
    infer_common_schema: bool = True,
    task_runner: BaseTaskRunner = ConcurrentTaskRunner,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Convert file-based data from various sources to Pycytominer-compatible standards.

    Note: source paths may be local or remote object-storage location
    using convention "s3://..." or similar.

    Args:
      source_path: str:
        str reference to read source files from.
        Note: may be local or remote object-storage location using convention "s3://..." or similar.
      dest_path: str:
        Path to write files to.
        Note: this may only be a local path.
      dest_datatype: Literal["parquet"]:
        Destination datatype to write to.
      source_datatype: Optional[str]:  (Default value = None)
        Source datatype to focus on during conversion.
      compartments: Union[List[str], Tuple[str, str, str, str]]:
        (Default value = DEFAULT_COMPARTMENTS)
        Compartment names to use for conversion.
      concat: bool:  (Default value = True)
        Whether to concatenate similar files together.
      merge: bool:  (Default value = True)
        Whether to merge the compartment data together into one dataset
      merge_columns: Optional[Union[List[str], Tuple[str, ...]]]
        (Default value = DEFAULT_MERGE_COLUMNS)
        Column names which appear in all compartments to use when performing merge
      merge_chunk_size: Optional[int] (Default value = DEFAULT_MERGE_CHUNK_SIZE)
        Size of merge chunks which is used to limit data size during merge ops
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

        # using a local path
        convert(
            source_path="./tests/data/cellprofiler/csv_single",
            source_datatype="csv",
            dest_path=".",
            dest_datatype="parquet",
        )

        # using an s3-compatible path with no signature for client
        convert(
            source_path="s3://s3path",
            source_datatype="csv",
            dest_path=".",
            dest_datatype="parquet",
            concat=True,
            no_sign_request=True,
        )
    """

    # send records to be written to parquet if selected
    if dest_datatype == "parquet":
        output = to_parquet.with_options(task_runner=task_runner)(
            source_path=source_path,
            dest_path=dest_path,
            source_datatype=source_datatype,
            compartments=compartments,
            metadata=metadata,
            concat=concat,
            merge=merge,
            merge_columns_compartments=merge_columns_compartments,
            merge_columns_metadata=merge_columns_metadata,
            merge_chunk_size=merge_chunk_size,
            infer_common_schema=infer_common_schema,
            **kwargs,
        )

    return output

"""
pycytominer-transform: convert - transforming data for use with pyctyominer.
"""

import itertools
import os
import pathlib
import uuid
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

import duckdb
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
from pycytominer_transform.presets import config
from pycytominer_transform.utils import column_sort


@task
def build_path(
    path: Union[str, pathlib.Path, AnyPath], **kwargs
) -> Union[pathlib.Path, Any]:
    """
    Build a path client or return local path.

    Args:
        path: Union[pathlib.Path, Any]:
            Path to seek filepaths within.
        **kwargs: Any
            keyword arguments to be used with 
            Cloudpathlib.CloudPath.client

    Returns:
        Union[pathlib.Path, Any]
            A local pathlib.Path or Cloudpathlib.AnyPath type path.
    """

    processed_path = AnyPath(path)

    # set the client for a CloudPath
    if isinstance(processed_path, CloudPath):
        processed_path.client = processed_path.client.__class__(**kwargs)

    return processed_path


@task
def get_source_filepaths(
    path: Union[pathlib.Path, AnyPath],
    targets: List[str],
    source_datatype: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gather dataset of filepaths from a provided directory path.

    Args:
        path: Union[pathlib.Path, Any]:
            Path to seek filepaths within.
        targets: List[str]:
            Compartment and metadata names to seek within the provided path.
        source_datatype: Optional[str]  (Default value = None)
            Data type for source data files.
            

    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the compartments.
    """

    if source_datatype == "sqlite" or (path.is_file() and path.suffix == ".sqlite"):
        return {
            f"{table_name}.sqlite": [{"table_name": table_name, "source_path": path}]
            for table_name in duckdb.connect()
            .execute(
                """
                /* install and load sqlite plugin for duckdb */
                INSTALL sqlite_scanner;
                LOAD sqlite_scanner;
                /* perform query on sqlite_master table for metadata on tables */
                SELECT name as table_name
                from sqlite_scan(?, 'sqlite_master')
                where type='table'
                """,
                parameters=[str(path)],
            )
            .arrow()["table_name"]
            .to_pylist()
            if any(target.lower() in table_name.lower() for target in targets)
        }

    # gathers files from provided path using compartments as a filter
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
        raise NoInputDataException(f"No input data to process at path: {str(path)}")

    grouped_records = {}

    # group files together by similar filename for later data operations
    for unique_source in set(source["source_path"].name for source in records):
        grouped_records[unique_source.capitalize()] = [
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
        targets: Optional[List[str]]:  (Default value = None)
            The source file names to target within the provided path.

    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the compartments.
    """

    source_path = build_path(path=source_path, **kwargs)

    # gather filepaths which will be used as the basis for this work
    records = get_source_filepaths(
        path=source_path, source_datatype=source_datatype, targets=targets
    )

    # infer or validate the source datatype based on source filepaths
    source_datatype = infer_source_datatype(
        records=records, source_datatype=source_datatype
    )

    # filter source filepaths to inferred or source datatype
    return filter_source_filepaths(records=records, source_datatype=source_datatype)


@task
def read_data(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Read data from record.

    Args:
        record: Dict[str, Any]:
            Data containing filepath to csv file

    Returns:
        record: Dict[str, Any]
            Updated record (Dict[str, Any]) with source data in-memory
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
            input_file=record["source_path"], parse_options=parse_options,
        )

    if AnyPath(record["source_path"]).suffix == ".sqlite":  # pylint: disable=no-member

        record["table"] = (
            duckdb.connect()
            .execute(
                """
                /* install and load sqlite plugin for duckdb */
                INSTALL sqlite_scanner;
                LOAD sqlite_scanner;
                /* perform query on sqlite_master table for metadata on tables */
                SELECT * from sqlite_scan(?, ?)
                """,
                parameters=[str(record["source_path"]), str(record["table_name"])],
            )
            .arrow()
        )

    return record


@task
def prepend_column_name(
    record: Dict[str, Any],
    record_group_name: str,
    identifying_columns: Union[List[str], Tuple[str, ...]],
    metadata: Union[List[str], Tuple[str, ...]],
    targets: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Rename columns using the record group name, avoiding identifying columns.

    Args:
        record: Dict[str, Any]:
            Individual data source record which includes meta about source
            as well as Arrow table with data.
        record_group_name: str:
            Name of data source record group (for common compartments, etc).
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be 
            ignored with regards to renaming.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        targets: Optional[List[str]] = None:
            List of source data names which are used as compartments

    Returns:
        Dict[str, Any]
            Updated record which includes the updated table column names
    """

    record_group_name_stem = str(pathlib.Path(record_group_name).stem)

    if targets is not None:
        record_group_name_stem = [
            target.capitalize()
            for target in targets
            if target.lower() in record_group_name_stem.lower()
        ][0]

    record["table"] = record["table"].rename_columns(
        [
            f"{record_group_name_stem}_{column_name}"
            if column_name not in identifying_columns
            and not column_name.startswith(record_group_name_stem)
            else f"Metadata_{record_group_name_stem}_{column_name}"
            if (
                not column_name.startswith("Metadata_")
                and column_name in identifying_columns
                and not any(item.capitalize() in column_name for item in metadata)
                and not "ObjectNumber" in column_name
            )
            else f"Metadata_{column_name}"
            if (
                not column_name.startswith("Metadata_")
                and column_name in identifying_columns
            )
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

    For a reference to data concatenation within Arrow see the following:
    https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html

    Args:
        record_group: List[Dict[str, Any]]:
            Data structure containing grouped data for concatenation.
        dest_path: Optional[str] (Default value = None)
            Optional destination path for concatenated records.
        common_schema: List[Tuple[str, str]] (Default value = None)
            Common schema to use for concatenation amongst arrow tables
            which may have slightly different but compatible schema.

    Returns:
        List[Dict[str, Any]]
            Updated dictionary containing concatenated records.
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
        (f"{dest_path}/" f"{record_group[0]['source_path'].stem}" ".parquet")
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
    stub_name = str(record["source_path"].stem)
    if "table_name" in record.keys():
        stub_name = f"{record['table_name']}"
    destination_path = pathlib.Path(f"{dest_path}/{stub_name}.parquet")

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
def get_join_chunks(
    records: Dict[str, List[Dict[str, Any]]],
    metadata: Union[List[str], Tuple[str, ...]],
    chunk_columns: Union[List[str], Tuple[str, ...]],
    chunk_size: int,
) -> List[List[Dict[str, Any]]]:
    """
    Build groups of join keys for later join operations

    Args:
        records: Dict[List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        chunk_columns: Union[List[str], Tuple[str, ...]]:
            Column names which appear in all compartments to use when performing join
        chunk_size: int:
            Size of join chunks which is used to limit data size during join ops

    Returns:
        List[List[Dict[str, Any]]]]:
            A list of lists with at most chunk size length that contain join keys
    """

    # fetch the compartment concat result as the basis for join groups
    for key, record in records.items():
        if any(name.lower() in pathlib.Path(key).stem.lower() for name in metadata):
            first_result = record
            break

    # gather the workflow result for basis if it's not yet returned
    basis = (
        first_result.result()
        if isinstance(first_result, PrefectFuture)
        else first_result
    )

    # read only the table's chunk_columns
    join_column_rows = parquet.read_table(
        source=basis[0]["destination_path"], columns=chunk_columns
    ).to_pylist()

    # build and return the chunked join column rows
    return [
        join_column_rows[i : i + chunk_size]
        for i in range(0, len(join_column_rows), chunk_size)
    ]


@task
def join_record_chunk(
    records: Dict[str, List[Dict[str, Any]]],
    dest_path: str,
    joins: str,
    join_group: List[Dict[str, Any]],
    drop_null: bool,
) -> str:
    """
    Join records based on join group keys (group of specific join column values)

    Args:
        records: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        dest_path: str:
            Destination path to write file-based content.
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join
            operations using the join_group keys as a reference.
        join_group: List[Dict[str, Any]]:
            Group of joinable keys to be used as "chunked" filter
            of overall dataset.
        drop_null: bool:
            Whether to drop rows with null values within the resulting
            joined data.


    Returns:
        str
            Path to joined file which is created as a result of this function.
    """

    # replace with real location of sources for join sql
    for key, val in records.items():
        if pathlib.Path(key).stem.lower() in joins.lower():
            joins = joins.replace(
                str(pathlib.Path(val[0]["destination_path"]).name.lower()),
                str(val[0]["destination_path"]),
            )

    # update the join groups to include unique values per table
    updated_join_group = []
    for key in records.keys():
        updated_join_group.extend(
            [
                {
                    f"{str(pathlib.Path(key).stem)}.{join_key}": val
                    for join_key, val in chunk.items()
                }
                for chunk in join_group
            ]
        )

    # form where clause for sql joins to filter the results
    joins += (
        "WHERE ("
        + ") OR (".join(
            [
                " AND ".join(
                    [
                        # create groups of join column filters where values always
                        # are expected to equal those within the join_group together
                        f"{join_column} = {join_column_value}"
                        if not isinstance(join_column_value, str)
                        # account for string values
                        else (f"{join_column} = " f"'{join_column_value}'")
                        for join_column, join_column_value in chunk.items()
                    ]
                )
                for chunk in updated_join_group
            ]
        )
        + ")"
    )

    # perform compartment joins
    result = duckdb.connect().execute(joins).arrow()

    if drop_null:
        result = result.drop_null()

    # account for duplicate column names from joins
    cols = []
    # reversed order column check as col removals will change index order
    for i, colname in reversed(list(enumerate(result.column_names))):
        if colname not in cols:
            cols.append(colname)
        else:
            result = result.remove_column(i)

    # inner sorted alphabetizes any columns which may not be part of custom_sort
    # outer sort provides pycytominer-specific column sort order
    result = result.select(sorted(sorted(result.column_names), key=column_sort))

    result_file_path = (
        # store the result in the parent of the dest_path
        f"{str(pathlib.Path(dest_path).parent)}/"
        # use the dest_path stem in the name
        f"{str(pathlib.Path(dest_path).stem)}-"
        # give the join chunk result a unique to arbitrarily
        # differentiate from other chunk groups which are mapped
        # and before they are brought together as one dataset
        f"{str(uuid.uuid4().hex)}.parquet"
    )

    # write the result
    parquet.write_table(
        table=result, where=result_file_path,
    )

    return result_file_path


@task
def concat_join_records(
    records: Dict[str, List[Dict[str, Any]]], dest_path: str, join_records: List[str],
) -> str:
    """
    Concatenate join records from parquet-based chunks.

    For a reference to data concatenation within Arrow see the following:
    https://arrow.apache.org/docs/python/generated/pyarrow.concat_tables.html

    Args:
        records: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
            Includes the metadata concerning location of actual data.
        dest_path: str:
            Destination path to write file-based content.
        join_records: List[str]:
            List of local filepath destination for join record chunks
            which will be concatenated.

    Returns:
        str
            Path to concatenated file which is created as a result of this function.
    """

    # remove the unjoined concatted compartments to prepare final dest_path usage
    # (we now have joined results)
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
            tables=[parquet.read_table(table_path) for table_path in join_records]
        ),
        where=dest_path,
    )

    # remove join chunks as we have the final result
    for table_path in join_records:
        pathlib.Path(table_path).unlink()

    # return modified records format to indicate the final result
    # and retain the other record data for reference as needed
    return dest_path


@task
def infer_record_group_common_schema(
    record_group: List[Dict[str, Any]]
) -> List[Tuple[str, str]]:
    """
    Infers a common schema for group of parquet files which may have 
    similar but slightly different schema or data. Intended to assist with
    data concatenation and other operations.

    Args:
        record_group: List[Dict[str, Any]]:
            Group of one or more data records which includes metadata about
            path to parquet data.
    
    Returns:
        List[Tuple[str, str]]
            A list of tuples which includes column name and PyArrow datatype.
            This data will later be used as the basis for forming a PyArrow schema.
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

            # check if we have a nulltype and non-nulltype conflict, deferring to non-nulltype
            elif pa.types.is_null(field.type) and not pa.types.is_null(
                schema.field(field.name).type
            ):
                common_schema = common_schema.set(
                    index, field.with_type(schema.field(field.name).type)
                )

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
def to_parquet(  # pylint: disable=too-many-arguments, too-many-locals
    source_path: str,
    dest_path: str,
    source_datatype: Optional[str],
    compartments: Union[List[str], Tuple[str, ...]],
    metadata: Union[List[str], Tuple[str, ...]],
    identifying_columns: Union[List[str], Tuple[str, ...]],
    concat: bool,
    join: bool,
    joins: Optional[str],
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
    chunk_size: Optional[int],
    infer_common_schema: bool,
    drop_null: bool,
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
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
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be 
            ignored with regards to renaming.
        concat: bool:
            Whether to concatenate similar files together.
        join: bool:
            Whether to join the compartment data together into one dataset
        joins: str:
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
            Column names which appear in all compartments to use when performing join
        chunk_size: Optional[int],
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool:  (Default value = True)
            Whether to infer a common schema when concatenating records.
        drop_null: bool:
            Whether to drop null results.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]:
            Grouped records which include metadata about destination filepath
            where parquet file was written or a string filepath for the joined
            result.
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

        # read data from record groups
        record_group = read_data.map(record=record_group)

        # rename cols to include compartment or meta names
        renamed_record_group = prepend_column_name.map(
            record=record_group,
            targets=unmapped(list(compartments) + list(metadata)),
            record_group_name=unmapped(record_group_name),
            identifying_columns=unmapped(identifying_columns),
            metadata=unmapped(metadata),
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

        # if concat or join, concat the record groups
        # note: join implies a concat, but concat does not imply a join
        if concat or join:
            # build a new concatenated record group
            results[record_group_name] = concat_record_group.submit(
                record_group=results[record_group_name],
                dest_path=dest_path,
                common_schema=common_schema,
            )

    # conditional section for merging
    # note: join implies a concat, but concat does not imply a join
    if join:

        # map joined results based on the join groups gathered above
        # note: after mapping we end up with a list of strings (task returns str)
        join_records_result = join_record_chunk.map(
            # gather the result of concatted records prior to
            # join group merging as each mapped task run will need
            # full concat results
            records=unmapped(
                {
                    key: value.result() if isinstance(value, PrefectFuture) else value
                    for key, value in results.items()
                }
            ),
            dest_path=unmapped(dest_path),
            joins=unmapped(joins),
            # get merging chunks by join columns
            join_group=get_join_chunks(
                records=results,
                chunk_columns=chunk_columns,
                chunk_size=chunk_size,
                metadata=metadata,
            ),
            drop_null=unmapped(drop_null),
        )

        # concat our join chunks together as one cohesive dataset
        # return results in common format which includes metadata
        # for lineage and debugging
        results = concat_join_records(
            dest_path=dest_path,
            join_records=(
                join_records_result.result()
                if isinstance(join_records_result, PrefectFuture)
                else join_records_result
            ),
            records=results,
        )

    return (
        {
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
        if isinstance(results, dict)
        else results
    )


def convert(  # pylint: disable=too-many-arguments,too-many-locals
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    compartments: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_COMPARTMENTS"]
    ),
    metadata: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_METADATA"]
    ),
    identifying_columns: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_IDENTIFYING_COLUMNS"]
    ),
    concat: bool = True,
    join: bool = True,
    joins: Optional[str] = cast(str, config["cellprofiler_csv"]["CONFIG_JOINS"]),
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_CHUNK_COLUMNS"]
    ),
    chunk_size: Optional[int] = cast(
        int, config["cellprofiler_csv"]["CONFIG_CHUNK_SIZE"]
    ),
    infer_common_schema: bool = True,
    drop_null: bool = True,
    preset: Optional[str] = None,
    task_runner: BaseTaskRunner = ConcurrentTaskRunner,
    log_level: str = "ERROR",
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
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
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        identifying_columns: Union[List[str], Tuple[str, ...]]:
            Column names which are used as ID's and as a result need to be 
            ignored with regards to renaming.
        concat: bool:  (Default value = True)
            Whether to concatenate similar files together.
        join: bool:  (Default value = True)
            Whether to join the compartment data together into one dataset
        joins: str: (Default value = presets.config["cellprofiler_csv"]["CONFIG_JOINS"]):
            DuckDB-compatible SQL which will be used to perform the join operations.
        chunk_columns: Optional[Union[List[str], Tuple[str, ...]]]
            (Default value = DEFAULT_CHUNK_COLUMNS)
            Column names which appear in all compartments to use when performing join
        chunk_size: Optional[int] (Default value = DEFAULT_CHUNK_SIZE)
            Size of join chunks which is used to limit data size during join ops
        infer_common_schema: bool: (Default value = True)
            Whether to infer a common schema when concatenating records.
        drop_null: bool (Default value = True)
            Whether to drop nan/null values from results
        preset: str (Default value = None)
            an optional group of presets to use based on common configurations
        task_runner: BaseTaskRunner: (Default value = ConcurrentTaskRunner)
            Prefect task runner to use with flows.
        log_level: str: (Default value = "ERROR"):
            Log level for Prefect flow and task operations.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]
            Grouped records which include metadata about destination filepath
            where parquet file was written or str of joined result filepath.

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

    # log level overrides for prefect
    os.environ["PREFECT_LOGGING_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_ROOT_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_HANDLERS_CONSOLE_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_HANDLERS_CONSOLE_FLOW_RUNS_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_HANDLERS_CONSOLE_TASK_RUNS_LEVEL"] = log_level
    os.environ["PREFECT_LOGGING_SERVER_LEVEL"] = log_level

    # optionally load preset configuration for arguments
    if preset is not None:
        compartments = cast(list, config[preset]["CONFIG_NAMES_COMPARTMENTS"])
        metadata = cast(list, config[preset]["CONFIG_NAMES_METADATA"])
        identifying_columns = cast(list, config[preset]["CONFIG_IDENTIFYING_COLUMNS"])
        joins = cast(str, config[preset]["CONFIG_JOINS"])
        chunk_columns = cast(list, config[preset]["CONFIG_CHUNK_COLUMNS"])
        chunk_size = cast(int, config[preset]["CONFIG_CHUNK_SIZE"])

    # send records to be written to parquet if selected
    if dest_datatype == "parquet":
        output = to_parquet.with_options(task_runner=task_runner)(
            source_path=source_path,
            dest_path=dest_path,
            source_datatype=source_datatype,
            compartments=compartments,
            metadata=metadata,
            identifying_columns=identifying_columns,
            concat=concat,
            join=join,
            joins=joins,
            chunk_columns=chunk_columns,
            chunk_size=chunk_size,
            infer_common_schema=infer_common_schema,
            drop_null=drop_null,
            **kwargs,
        )

    return output

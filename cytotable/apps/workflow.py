import logging
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)
from parsl.app.app import join_app, python_app


@join_app
def _to_parquet(  # pylint: disable=too-many-arguments, too-many-locals
    source_path: str,
    dest_path: str,
    source_datatype: Optional[str],
    metadata: Union[List[str], Tuple[str, ...]],
    compartments: Union[List[str], Tuple[str, ...]],
    identifying_columns: Union[List[str], Tuple[str, ...]],
    concat: bool,
    join: bool,
    joins: Optional[str],
    chunk_columns: Optional[Union[List[str], Tuple[str, ...]]],
    chunk_size: Optional[int],
    infer_common_schema: bool,
    drop_null: bool,
    add_tablenumber: bool,
    data_type_cast_map: Optional[Dict[str, str]],
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
    """
    Export data to parquet.

    Args:
        source_path: str:
            str reference to read source files from.
            Note: may be local or remote object-storage
            location using convention "s3://..." or similar.
        dest_path: str:
            Path to write files to.
            Note: this may only be a local path.
        source_datatype: Optional[str]: (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, ...]]: (Default value = None)
            Compartment names to use for conversion.
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
            Whether to infer a common schema when concatenating sources.
        drop_null: bool:
            Whether to drop null results.
        add_tablenumber: bool
            Whether to attempt to add TableNumber to resulting data
        data_type_cast_map: Dict[str, str]
            A dictionary mapping data type groups to specific types.
            Roughly includes to Arrow data types language from:
            https://arrow.apache.org/docs/python/api/datatypes.html
        **kwargs: Any:
            Keyword args used for gathering source data, primarily relevant for
            Cloudpathlib cloud-based client configuration.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]:
            Grouped sources which include metadata about destination filepath
            where parquet file was written or a string filepath for the joined
            result.
    """

    import pathlib

    from cytotable.apps.combine import (
        _concat_join_sources,
        _concat_source_group,
        _get_join_chunks,
        _infer_source_group_common_schema,
        _join_source_chunk,
    )
    from cytotable.apps.modify import _cast_data_types, _prepend_column_name
    from cytotable.apps.source import (
        _gather_tablenumber,
        _get_table_chunk_offsets,
        _source_chunk_to_parquet,
    )
    from cytotable.apps.workflow import _gather_paths, _return_future

    # gather sources to be processed
    sources = _gather_paths(
        source_path=source_path,
        source_datatype=source_datatype,
        targets=list(metadata) + list(compartments),
        **kwargs,
    ).result()

    # if we already have a file in dest_path, remove it
    if pathlib.Path(dest_path).is_file():
        pathlib.Path(dest_path).unlink()

    # prepare offsets for chunked data export from source tables
    offsets_prepared = {
        source_group_name: [
            dict(
                source,
                **{
                    "offsets": _get_table_chunk_offsets(
                        source=source,
                        chunk_size=chunk_size,
                    ).result()
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in sources.items()
    }

    # if offsets is none and we haven't halted, remove the file as there
    # were input formatting errors which will create challenges downstream
    invalid_files_dropped = {
        source_group_name: [
            # ensure we have offsets
            source
            for source in source_group_vals
            if source["offsets"] is not None
        ]
        for source_group_name, source_group_vals in offsets_prepared.items()
        # ensure we have source_groups with at least one source table
        if len(source_group_vals) > 0
    }

    # add tablenumber details (providing a placeholder if add_tablenumber == False)
    tablenumber_prepared = {
        source_group_name: [
            dict(
                source,
                **{
                    "tablenumber": _gather_tablenumber(
                        source=source,
                        source_group_name=source_group_name,
                    ).result()
                    # only add the tablenumber if parameter tells us so
                    if add_tablenumber
                    else None
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in invalid_files_dropped.items()
    }

    results = {
        source_group_name: [
            dict(
                source,
                **{
                    "table": [
                        # perform column renaming and create potential return result
                        _prepend_column_name(
                            table_path=_cast_data_types(
                                # perform chunked data export to parquet using offsets
                                table_path=_source_chunk_to_parquet(
                                    source_group_name=source_group_name,
                                    source=source,
                                    chunk_size=chunk_size,
                                    offset=offset,
                                    dest_path=dest_path,
                                ),
                                data_type_cast_map=data_type_cast_map,
                            ),
                            source_group_name=source_group_name,
                            identifying_columns=identifying_columns,
                            metadata=metadata,
                            compartments=compartments,
                        ).result()
                        for offset in source["offsets"]
                    ]
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in tablenumber_prepared.items()
    }

    # if we're concatting or joining and need to infer the common schema
    if (concat or join) and infer_common_schema:
        # create a common schema for concatenation work
        common_schema_determined = {
            source_group_name: {
                "sources": source_group_vals,
                "common_schema": _infer_source_group_common_schema(
                    source_group=source_group_vals
                ),
            }
            for source_group_name, source_group_vals in results.items()
        }

    # if concat or join, concat the source groups
    # note: join implies a concat, but concat does not imply a join
    # We concat to join in order to create a common schema for join work
    # performed after concatenation.
    if concat or join:
        # create a potential return result for concatenation output
        results = {
            source_group_name: _concat_source_group(
                source_group_name=source_group_name,
                source_group=source_group_vals["sources"],
                dest_path=dest_path,
                common_schema=source_group_vals["common_schema"],
            ).result()
            for source_group_name, source_group_vals in common_schema_determined.items()
        }

    # conditional section for merging
    # note: join implies a concat, but concat does not imply a join
    if join:
        # map joined results based on the join groups gathered above
        # note: after mapping we end up with a list of strings (task returns str)
        join_sources_result = [
            _cast_data_types(
                # recast to avoid duckdb type inference
                table_path=_join_source_chunk(
                    # gather the result of concatted sources prior to
                    # join group merging as each mapped task run will need
                    # full concat results
                    sources=results,
                    dest_path=dest_path,
                    joins=joins,
                    # get merging chunks by join columns
                    join_group=join_group,
                    drop_null=drop_null,
                ),
                data_type_cast_map=data_type_cast_map,
            ).result()
            # create join group for querying the concatenated
            # data in order to perform memory-safe joining
            # per user chunk size specification.
            for join_group in _get_join_chunks(
                sources=results,
                chunk_columns=chunk_columns,
                chunk_size=chunk_size,
                metadata=metadata,
            ).result()
        ]

        # concat our join chunks together as one cohesive dataset
        # return results in common format which includes metadata
        # for lineage and debugging
        results = _concat_join_sources(
            dest_path=dest_path,
            join_sources=join_sources_result,
            sources=results,
        ).result()

    # wrap the final result as a future and return
    return _return_future(results)


@join_app
def _gather_paths(
    source_path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Flow for gathering data sources for conversion

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

    from cytotable.apps.path import (
        _build_path,
        _filter_filepaths,
        _get_filepaths,
        _infer_path_datatype,
    )

    source_path = _build_path(path=source_path, **kwargs)

    # gather filepaths which will be used as the basis for this work
    sources = _get_filepaths(path=source_path, targets=targets)

    # infer or validate the source datatype based on source filepaths
    source_datatype = _infer_path_datatype(
        sources=sources, source_datatype=source_datatype
    )

    # filter source filepaths to inferred or source datatype
    return _filter_filepaths(sources=sources, source_datatype=source_datatype)


@python_app
def _return_future(input: Any) -> Any:
    """
    This is a simple wrapper python_app to allow
    the return of join_app-compliant output (must be a Parsl future)

    Args:
        input: Any
            Any input which will be used within the context of a
            Parsl join_app future return.

    Returns:
        Any
            Returns the input as provided wrapped within the context
            of a python_app for the purpose of a join_app.
    """

    return input

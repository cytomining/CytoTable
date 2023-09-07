"""
CytoTable: sources - tasks and flows related to
source data and metadata for performing conversion work.
"""

import pathlib
from typing import Any, Dict, List, Optional, Union

from cloudpathlib import AnyPath
from parsl.app.app import join_app, python_app


@python_app
def _build_path(
    path: Union[str, pathlib.Path, AnyPath], **kwargs
) -> Union[pathlib.Path, AnyPath]:
    """
    Build a path client or return local path.

    Args:
        path: Union[pathlib.Path, Any]:
            Path to seek filepaths within.
        **kwargs: Any
            keyword arguments to be used with
            Cloudpathlib.CloudPath.client .

    Returns:
        Union[pathlib.Path, Any]
            A local pathlib.Path or Cloudpathlib.AnyPath type path.
    """

    from cloudpathlib import CloudPath

    from cytotable.utils import _expand_path

    # form a path using cloudpathlib AnyPath, stripping certain characters
    processed_path = _expand_path(str(path).strip("'\" "))

    # set the client for a CloudPath
    if isinstance(processed_path, CloudPath):
        processed_path.client = processed_path.client.__class__(**kwargs)

    return processed_path


@python_app
def _get_source_filepaths(
    path: Union[pathlib.Path, AnyPath],
    targets: List[str],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Gather dataset of filepaths from a provided directory path.

    Args:
        path: Union[pathlib.Path, Any]:
            Either a directory path to seek filepaths within or a path directly to a file.
        targets: List[str]:
            Compartment and metadata names to seek within the provided path.

    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the compartments.
    """

    import pathlib

    from cloudpathlib import AnyPath

    from cytotable.exceptions import NoInputDataException
    from cytotable.utils import _cache_cloudpath_to_local, _duckdb_reader

    # gathers files from provided path using compartments + metadata as a filter
    sources = [
        # build source_paths for all files
        # note: builds local cache for sqlite files from cloud
        {"source_path": _cache_cloudpath_to_local(subpath)}
        # loop for navigating single file or subpaths
        for subpath in (
            (path,)
            # used if the source path is a single file
            if AnyPath(path).is_file()
            # iterates through a source directory
            else (x for x in AnyPath(path).glob("**/*") if AnyPath(x).is_file())
        )
        # ensure the subpaths meet certain specifications
        if (
            targets is None
            # checks for name of the file from targets (compartment + metadata names)
            or str(subpath.stem).lower() in [target.lower() for target in targets]
            # checks for sqlite extension (which may include compartment + metadata names)
            or subpath.suffix.lower() == ".sqlite"
        )
    ]

    # expand sources to include sqlite tables similarly to files (one entry per table)
    expanded_sources = []
    with _duckdb_reader() as ddb_reader:
        for element in sources:
            # check that the path is of sqlite type
            if element["source_path"].suffix.lower() == ".sqlite":
                # creates individual entries for each table
                expanded_sources += [
                    {
                        "source_path": AnyPath(
                            f"{element['source_path']}/{table_name}.sqlite"
                        ),
                        "table_name": table_name,
                    }
                    # perform a query to find the table names from the sqlite file
                    for table_name in ddb_reader.execute(
                        """
                        /* perform query on sqlite_master table for metadata on tables */
                        SELECT name as table_name
                        from sqlite_scan(?, 'sqlite_master')
                        where type='table'
                        """,
                        parameters=[str(element["source_path"])],
                    )
                    .arrow()["table_name"]
                    .to_pylist()
                    # make sure the table names match with compartment + metadata names
                    if any(target.lower() in table_name.lower() for target in targets)
                ]
            else:
                # if we don't have sqlite source, append the existing element
                expanded_sources.append(element)

    # reset sources to expanded_sources
    sources = expanded_sources

    # if we collected no files above, raise exception
    if len(sources) < 1:
        raise NoInputDataException(f"No input data to process at path: {str(path)}")

    # group files together by similar filename for later data operations
    grouped_sources = {}
    for unique_source in set(source["source_path"].name for source in sources):
        grouped_sources[unique_source.capitalize()] = [
            # case for files besides sqlite
            source if source["source_path"].suffix.lower() != ".sqlite"
            # if we have sqlite entries, update the source_path to the parent
            # (the parent table database file) as grouped key name will now
            # encapsulate the table name details.
            else {
                "source_path": source["source_path"].parent,
                "table_name": source["table_name"],
            }
            for source in sources
            # focus only on entries which include the unique_source name
            if source["source_path"].name == unique_source
        ]

    return grouped_sources


@python_app
def _infer_source_datatype(
    sources: Dict[str, List[Dict[str, Any]]], source_datatype: Optional[str] = None
) -> str:
    """
    Infers and optionally validates datatype (extension) of files.

    Args:
        sources: Dict[str, List[Dict[str, Any]]]:
            Grouped datasets of files which will be used by other functions.
        source_datatype: Optional[str]:  (Default value = None)
            Optional source datatype to validate within the context of
            detected datatypes.

    Returns:
        str
            A string of the datatype detected or validated source_datatype.
    """

    from cytotable.exceptions import DatatypeException

    # gather file extension suffixes
    suffixes = list(set((group.split(".")[-1]).lower() for group in sources))

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


@python_app
def _filter_source_filepaths(
    sources: Dict[str, List[Dict[str, Any]]], source_datatype: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Filter source filepaths based on provided source_datatype.

    Args:
        sources: Dict[str, List[Dict[str, Any]]]
            Grouped datasets of files which will be used by other functions.
        source_datatype: str
            Source datatype to use for filtering the dataset.

    Returns:
        Dict[str, List[Dict[str, Any]]]
            Data structure which groups related files based on the datatype.
    """

    import pathlib

    from cloudpathlib import AnyPath, CloudPath

    return {
        filegroup: [
            file
            for file in files
            # ensure the filesize is greater than 0
            if file["source_path"].stat().st_size > 0
            # ensure the datatype matches the source datatype
            and file["source_path"].suffix == f".{source_datatype}"
        ]
        for filegroup, files in sources.items()
    }


@join_app
def _gather_sources(
    source_path: str,
    source_datatype: Optional[str] = None,
    targets: Optional[List[str]] = None,
    **kwargs,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Flow for gathering data sources for conversion.

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

    from cytotable.sources import (
        _build_path,
        _filter_source_filepaths,
        _get_source_filepaths,
        _infer_source_datatype,
    )

    source_path = _build_path(path=source_path, **kwargs)

    # gather filepaths which will be used as the basis for this work
    sources = _get_source_filepaths(path=source_path, targets=targets)

    # infer or validate the source datatype based on source filepaths
    source_datatype = _infer_source_datatype(
        sources=sources, source_datatype=source_datatype
    )

    # filter source filepaths to inferred or source datatype
    return _filter_source_filepaths(sources=sources, source_datatype=source_datatype)

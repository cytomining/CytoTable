"""
CytoTable: convert - transforming data for use with pyctyominer.
"""


import logging
from typing import Any, Dict, List, Literal, Optional, Tuple, Union, cast

import parsl

from cytotable.apps.workflow import _to_parquet
from cytotable.presets import config
from cytotable.utils import _default_parsl_config

logger = logging.getLogger(__name__)


def convert(  # pylint: disable=too-many-arguments,too-many-locals
    source_path: str,
    dest_path: str,
    dest_datatype: Literal["parquet"],
    source_datatype: Optional[str] = None,
    metadata: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_METADATA"]
    ),
    compartments: Union[List[str], Tuple[str, ...]] = cast(
        list, config["cellprofiler_csv"]["CONFIG_NAMES_COMPARTMENTS"]
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
    parsl_config: Optional[parsl.Config] = None,
    **kwargs,
) -> Union[Dict[str, List[Dict[str, Any]]], str]:
    """
    Convert file-based data from various sources to Pycytominer-compatible standards.

    Note: source paths may be local or remote object-storage location
    using convention "s3://..." or similar.

    Args:
        source_path: str:
            str reference to read source files from.
            Note: may be local or remote object-storage location
            using convention "s3://..." or similar.
        dest_path: str:
            Path to write files to.
            Note: this may only be a local path.
        dest_datatype: Literal["parquet"]:
            Destination datatype to write to.
        source_datatype: Optional[str]:  (Default value = None)
            Source datatype to focus on during conversion.
        metadata: Union[List[str], Tuple[str, ...]]:
            Metadata names to use for conversion.
        compartments: Union[List[str], Tuple[str, str, str, str]]:
            (Default value = DEFAULT_COMPARTMENTS)
            Compartment names to use for conversion.
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
            Whether to infer a common schema when concatenating sources.
        drop_null: bool (Default value = True)
            Whether to drop nan/null values from results
        preset: str (Default value = None)
            an optional group of presets to use based on common configurations
        parsl_config: Optional[parsl.Config] (Default value = None)
            Optional Parsl configuration to use for running CytoTable operations.

    Returns:
        Union[Dict[str, List[Dict[str, Any]]], str]
            Grouped sources which include metadata about destination filepath
            where parquet file was written or str of joined result filepath.

    Example:

        .. code-block:: python

            from cytotable import convert

            # using a local path with cellprofiler csv presets
            convert(
                source_path="./tests/data/cellprofiler/csv_single",
                source_datatype="csv",
                dest_path=".",
                dest_datatype="parquet",
                preset="cellprofiler_csv",
            )

            # using an s3-compatible path with no signature for client
            # and cellprofiler csv presets
            convert(
                source_path="s3://s3path",
                source_datatype="csv",
                dest_path=".",
                dest_datatype="parquet",
                concat=True,
                preset="cellprofiler_csv",
                no_sign_request=True,
            )

            # using local path with cellprofiler sqlite presets
            convert(
                source_path="example.sqlite",
                dest_path="example.parquet",
                dest_datatype="parquet",
                preset="cellprofiler_sqlite",
            )
    """

    # attempt to load parsl configuration
    try:
        # if we don't have a parsl configuration provided, load the default
        if parsl_config is None:
            parsl.load(_default_parsl_config())
        else:
            # else we attempt to load the given parsl configuration
            parsl.load(parsl_config)
    except RuntimeError as runtime_exc:
        # catch cases where parsl has already been loaded and defer to
        # previously loaded configuration with a warning
        if str(runtime_exc) == "Config has already been loaded":
            logger.warning(str(runtime_exc))

            # if we're supplying a new config, attempt to clear current config
            # and use the new configuration instead. Otherwise, use existing.
            if parsl_config is not None:
                # clears the existing parsl configuration
                parsl.clear()
                # then load the supplied configuration
                parsl.load(parsl_config)

        # for other potential runtime errors besides config already being loaded
        else:
            raise

    # optionally load preset configuration for arguments
    # note: defer to overrides from parameters whose values
    # are not None (allows intermixing of presets and overrides)
    if preset is not None:
        metadata = (
            cast(list, config[preset]["CONFIG_NAMES_METADATA"])
            if metadata is None or preset != "cellprofiler_csv"
            else metadata
        )
        compartments = (
            cast(list, config[preset]["CONFIG_NAMES_COMPARTMENTS"])
            if compartments is None or preset != "cellprofiler_csv"
            else compartments
        )
        identifying_columns = (
            cast(list, config[preset]["CONFIG_IDENTIFYING_COLUMNS"])
            if identifying_columns is None or preset != "cellprofiler_csv"
            else identifying_columns
        )
        joins = (
            cast(str, config[preset]["CONFIG_JOINS"])
            if joins is None or preset != "cellprofiler_csv"
            else joins
        )
        chunk_columns = (
            cast(list, config[preset]["CONFIG_CHUNK_COLUMNS"])
            if chunk_columns is None or preset != "cellprofiler_csv"
            else chunk_columns
        )
        chunk_size = (
            cast(int, config[preset]["CONFIG_CHUNK_SIZE"])
            if chunk_size is None or preset != "cellprofiler_csv"
            else chunk_size
        )

    # send sources to be written to parquet if selected
    if dest_datatype == "parquet":
        output = _to_parquet(
            source_path=source_path,
            dest_path=dest_path,
            source_datatype=source_datatype,
            metadata=metadata,
            compartments=compartments,
            identifying_columns=identifying_columns,
            concat=concat,
            join=join,
            joins=joins,
            chunk_columns=chunk_columns,
            chunk_size=chunk_size,
            infer_common_schema=infer_common_schema,
            drop_null=drop_null,
            **kwargs,
        ).result()

    return output

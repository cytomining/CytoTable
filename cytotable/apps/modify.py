"""
cytotable.apps.modify : work related to modifying table data.
"""
import logging
from typing import List, Tuple, Union

from parsl.app.app import python_app

logger = logging.getLogger(__name__)


@python_app
def _prepend_column_name(
    table_path: str,
    source_group_name: str,
    identifying_columns: List[str],
    metadata: Union[List[str], Tuple[str, ...]],
    compartments: List[str],
) -> str:
    """
    Rename columns using the source group name, avoiding identifying columns.

    Notes:
    * A source_group_name represents a filename referenced as part of what
    is specified within targets.

    Args:
        table_path: str:
            Path to a parquet file which will be modified.
        source_group_name: str:
            Name of data source source group (for common compartments, etc).
        identifying_columns: List[str]:
            Column names which are used as ID's and as a result need to be
            treated differently when renaming.
        metadata: Union[List[str], Tuple[str, ...]]:
            List of source data names which are used as metadata
        compartments: List[str]:
            List of source data names which are used as compartments

    Returns:
        str
            Path to the modified file
    """

    import pathlib

    import pyarrow.parquet as parquet

    targets = tuple(metadata) + tuple(compartments)

    table = parquet.read_table(source=table_path)

    # stem of source group name
    # for example:
    #   targets: ['cytoplasm']
    #   source_group_name: 'Per_Cytoplasm.sqlite'
    #   source_group_name_stem: 'Cytoplasm'
    source_group_name_stem = targets[
        # return first result from generator below as index to targets
        next(
            i
            for i, val in enumerate(targets)
            # compare if value from targets in source_group_name stem
            if val.lower() in str(pathlib.Path(source_group_name).stem).lower()
        )
        # capitalize the result
    ].capitalize()

    # capture updated column names as new variable
    updated_column_names = []

    for column_name in table.column_names:
        # if-condition for prepending source_group_name_stem to column name
        # where colname is not in identifying_columns parameter values
        # and where the column is not already prepended with source_group_name_stem
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'AreaShape_Area'
        #   updated_column_name: 'Cells_AreaShape_Area'
        if column_name not in identifying_columns and not column_name.startswith(
            source_group_name_stem.capitalize()
        ):
            updated_column_names.append(f"{source_group_name_stem}_{column_name}")
        # if-condition for prepending 'Metadata_' to column name
        # where colname is in identifying_columns parameter values
        # and where the column is already prepended with source_group_name_stem
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'Cells_Number_Object_Number'
        #   updated_column_name: 'Metadata_Cells_Number_Object_Number'
        elif column_name in identifying_columns and column_name.startswith(
            source_group_name_stem.capitalize()
        ):
            updated_column_names.append(f"Metadata_{column_name}")
        # if-condition for prepending 'Metadata' and source_group_name_stem to column name
        # where colname is in identifying_columns parameter values
        # and where the colname does not already start with 'Metadata_'
        # and colname not in metadata list
        # and colname does not include 'ObjectNumber'
        # for example:
        #   source_group_name_stem: 'Cells'
        #   column_name: 'Parent_Nuclei'
        #   updated_column_name: 'Metadata_Cells_Parent_Nuclei'
        elif (
            column_name in identifying_columns
            and not column_name.startswith("Metadata_")
            and not any(item.capitalize() in column_name for item in metadata)
            and not "ObjectNumber" in column_name
        ):
            updated_column_names.append(
                f"Metadata_{source_group_name_stem}_{column_name}"
            )
        # if-condition for prepending 'Metadata' to column name
        # where colname doesn't already start with 'Metadata_'
        # and colname is in identifying_columns parameter values
        # for example:
        #   column_name: 'ObjectNumber'
        #   updated_column_name: 'Metadata_ObjectNumber'
        elif (
            not column_name.startswith("Metadata_")
            and column_name in identifying_columns
        ):
            updated_column_names.append(f"Metadata_{column_name}")
        # else we add the existing colname to the updated list as-is
        else:
            updated_column_names.append(column_name)

    # perform table column name updates
    parquet.write_table(
        table=table.rename_columns(updated_column_names), where=table_path
    )

    return table_path


@python_app
def _cast_data_types(
    table_path: str, data_type_cast_map: Optional[Dict[str, str]] = None
) -> str:
    """
    Cast data types per what is received in cast_map.

    Example:
    - table_path: parquet file
    - data_type_cast_map: {"float": "float32"}

    The above passed through this function will rewrite the parquet file
    with float32 columns where any float-like column are encountered.

    Args:
        table_path: str:
            Path to a parquet file which will be modified.
        data_type_cast_map: Dict[str, str]
            A dictionary mapping data type groups to specific types.
            Roughly includes to Arrow data types language from:
            https://arrow.apache.org/docs/python/api/datatypes.html

    Returns:
        str
            Path to the modified file
    """

    import pyarrow as pa
    import pyarrow.parquet as parquet

    if data_type_cast_map is None:
        return table_path

    parquet.write_table(
        # build a new table which casts the data types
        # as per the specification below
        # reference arrow type information for more details:
        # https://arrow.apache.org/docs/python/api/datatypes.html
        table=parquet.read_table(source=table_path).cast(
            # build a new schema
            pa.schema(
                [
                    # for casting to new float type
                    field.with_type(pa.type_for_alias(data_type_cast_map["float"]))
                    if "float" in data_type_cast_map.keys()
                    and pa.types.is_floating(field.type)
                    # for casting to new int type
                    else field.with_type(
                        pa.type_for_alias(data_type_cast_map["integer"])
                    )
                    if "integer" in data_type_cast_map.keys()
                    and pa.types.is_integer(field.type)
                    # for casting to new string type
                    else field.with_type(
                        pa.type_for_alias(data_type_cast_map["string"])
                    )
                    if "string" in data_type_cast_map.keys()
                    and (
                        # we check for both large_string and string here
                        # as there is no "any string" type checking built-in
                        pa.types.is_string(field.type)
                        or pa.types.is_large_string(field.type)
                    )
                    # else we retain the existing data field type
                    else field
                    for field in parquet.read_schema(where=table_path)
                ]
            )
        ),
        # rewrite to the same location
        where=table_path,
    )

    return table_path

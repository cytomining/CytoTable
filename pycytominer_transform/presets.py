"""
Presets for common pycytominer-transform configurations.
"""

config = {
    "cellprofiler_csv": {
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "ImageNumber",
            "ObjectNumber",
            "Metadata_Well",
            "Parent_Cells",
            "Parent_Nuclei",
        ),
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # chunking columns to use along with chunk size for join operations
        "CONFIG_CHUNK_COLUMNS": ("Metadata_ImageNumber",),
        # compartment and metadata joins performed in this order with dict keys which
        # roughly align with pyarrow.Table.join arguments, where they will eventually be used
        # note: first join takes place arbitrarily and requires compartment or metadata
        # name specification. For all but the first join we use "result" as a reference
        # to the previously joined data, building upon each join sequentially in this list.
        "CONFIG_JOINS": (
            # join cells into cytoplasm compartment data
            {
                "left": "cytoplasm",
                "left_columns": None,
                "left_join_columns": [
                    "Metadata_ImageNumber",
                    "Metadata_Cytoplasm_Parent_Cells",
                ],
                "left_suffix": "_cytoplasm",
                "right": "cells",
                "right_columns": None,
                "right_join_columns": ["Metadata_ImageNumber", "Metadata_ObjectNumber"],
                "right_suffix": "_cells",
                "how": "full outer",
            },
            # join nuclei into cytoplasm and cell compartment data
            {
                "left": "result",
                "left_columns": None,
                "left_join_columns": [
                    "Metadata_ImageNumber",
                    "Metadata_Cytoplasm_Parent_Nuclei",
                ],
                "left_suffix": "_cytoplasm",
                "right": "nuclei",
                "right_columns": None,
                "right_join_columns": ["Metadata_ImageNumber", "Metadata_ObjectNumber"],
                "right_suffix": "_nuclei",
                "how": "full outer",
            },
            # join image data into cytosplasm, cell, and nuclei data
            {
                "left": "result",
                "left_columns": None,
                "left_join_columns": ["Metadata_ImageNumber"],
                "left_suffix": None,
                "right": "image",
                "right_columns": ["Metadata_ImageNumber", "Metadata_Well"],
                "right_join_columns": ["Metadata_ImageNumber"],
                "right_suffix": None,
                "how": "left outer",
            },
        ),
    },
    "cellprofiler_sqlite": {
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "ImageNumber",
            "ObjectNumber",
            "Metadata_Well",
            "Parent_Cells",
            "Parent_Nuclei",
        ),
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # chunking columns to use along with chunk size for join operations
        "CONFIG_CHUNK_COLUMNS": ("Metadata_ImageNumber",),
        # compartment and metadata joins performed in this order with dict keys which
        # roughly align with pyarrow.Table.join arguments, where they will eventually be used
        # note: first join takes place arbitrarily and requires compartment or metadata
        # name specification. For all but the first join we use "result" as a reference
        # to the previously joined data, building upon each join sequentially in this list.
        "CONFIG_JOINS": (
            # join cells into cytoplasm compartment data
            {
                "left": "per_cytoplasm",
                "left_columns": None,
                "left_join_columns": [
                    "Metadata_ImageNumber",
                    "Cytoplasm_Parent_Cells",
                ],
                "left_suffix": "_cytoplasm",
                "right": "per_cells",
                "right_columns": None,
                "right_join_columns": [
                    "Metadata_ImageNumber",
                    "Cells_Number_Object_Number",
                ],
                "right_suffix": "_cells",
                "how": "full outer",
            },
            # join nuclei into cytoplasm and cell compartment data
            {
                "left": "result",
                "left_columns": None,
                "left_join_columns": [
                    "Metadata_ImageNumber",
                    "Cytoplasm_Parent_OrigNuclei",
                ],
                "left_suffix": "_cytoplasm",
                "right": "per_nuclei",
                "right_columns": None,
                "right_join_columns": [
                    "Metadata_ImageNumber",
                    "Nuclei_Number_Object_Number",
                ],
                "right_suffix": "_nuclei",
                "how": "full outer",
            },
            # join image data into cytosplasm, cell, and nuclei data
            {
                "left": "result",
                "left_columns": None,
                "left_join_columns": ["Metadata_ImageNumber"],
                "left_suffix": None,
                "right": "per_image",
                "right_columns": ["Metadata_ImageNumber", "Image_Metadata_Well"],
                "right_join_columns": ["Metadata_ImageNumber"],
                "right_suffix": None,
                "how": "left outer",
            },
        ),
    },
}

"""
Presets for common CytoTable configurations.
"""

config = {
    "cellprofiler_csv": {
        # version specifications using related references
        "CONFIG_SOURCE_VERSION": {
            "cellprofiler": "v4.0.0",
        },
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
            "Metadata_Plate",
            "Parent_Cells",
            "Parent_Nuclei",
        ),
        # pagination keys for use with this data
        # of the rough format "table" -> "column".
        # note: page keys are expected to be numeric (int, float)
        "CONFIG_PAGE_KEYS": {
            "image": "ImageNumber",
            "cells": "ObjectNumber",
            "nuclei": "ObjectNumber",
            "cytoplasm": "ObjectNumber",
            "join": "Cytoplasm_Number_Object_Number",
        },
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            SELECT
                image.Metadata_ImageNumber,
                cytoplasm.* EXCLUDE (Metadata_ImageNumber),
                cells.* EXCLUDE (Metadata_ImageNumber, Metadata_ObjectNumber),
                nuclei.* EXCLUDE (Metadata_ImageNumber, Metadata_ObjectNumber)
            FROM
                read_parquet('cytoplasm.parquet') AS cytoplasm
            LEFT JOIN read_parquet('cells.parquet') AS cells USING (Metadata_ImageNumber)
            LEFT JOIN read_parquet('nuclei.parquet') AS nuclei USING (Metadata_ImageNumber)
            LEFT JOIN read_parquet('image.parquet') AS image USING (Metadata_ImageNumber)
            WHERE
                cells.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
                AND nuclei.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
            """,
    },
    "cellprofiler_sqlite": {
        # version specifications using related references
        "CONFIG_SOURCE_VERSION": {
            "cellprofiler": "v4.2.4",
        },
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "ImageNumber",
            "Metadata_Well",
            "Parent_Cells",
            "Parent_Nuclei",
        ),
        # pagination keys for use with this data
        # of the rough format "table" -> "column".
        # note: page keys are expected to be numeric (int, float)
        "CONFIG_PAGE_KEYS": {
            "image": "ImageNumber",
            "cells": "Cells_Number_Object_Number",
            "nuclei": "Nuclei_Number_Object_Number",
            "cytoplasm": "Cytoplasm_Number_Object_Number",
            "join": "Cytoplasm_Number_Object_Number",
        },
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            SELECT
                per_image.Metadata_ImageNumber,
                per_image.Image_Metadata_Well,
                per_image.Image_Metadata_Plate,
                per_cytoplasm.* EXCLUDE (Metadata_ImageNumber),
                per_cells.* EXCLUDE (Metadata_ImageNumber),
                per_nuclei.* EXCLUDE (Metadata_ImageNumber)
            FROM
                read_parquet('per_cytoplasm.parquet') AS per_cytoplasm
            LEFT JOIN read_parquet('per_cells.parquet') AS per_cells USING (Metadata_ImageNumber)
            LEFT JOIN read_parquet('per_nuclei.parquet') AS per_nuclei USING (Metadata_ImageNumber)
            LEFT JOIN read_parquet('per_image.parquet') AS per_image USING (Metadata_ImageNumber)
            WHERE
                per_cells.Cells_Number_Object_Number = per_cytoplasm.Cytoplasm_Parent_Cells
                AND per_nuclei.Nuclei_Number_Object_Number = per_cytoplasm.Cytoplasm_Parent_Nuclei
            """,
    },
    "cellprofiler_sqlite_cpg0016_jump": {
        # version specifications using related references
        "CONFIG_SOURCE_VERSION": {
            "cellprofiler": "v4.0.0",
        },
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
            "Metadata_Plate",
            "Parent_Cells",
            "Parent_Nuclei",
        ),
        # pagination keys for use with this data
        # of the rough format "table" -> "column".
        # note: page keys are expected to be numeric (int, float)
        "CONFIG_PAGE_KEYS": {
            "image": "ImageNumber",
            "cells": "ObjectNumber",
            "nuclei": "ObjectNumber",
            "cytoplasm": "ObjectNumber",
            "join": "Cytoplasm_Number_Object_Number",
        },
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            SELECT
                image.Image_TableNumber,
                image.Metadata_ImageNumber,
                image.Metadata_Plate,
                image.Metadata_Well,
                image.Image_Metadata_Site,
                image.Image_Metadata_Row,
                cytoplasm.* EXCLUDE (Metadata_ImageNumber),
                cells.* EXCLUDE (Metadata_ImageNumber),
                nuclei.* EXCLUDE (Metadata_ImageNumber)
            FROM
                read_parquet('cytoplasm.parquet') AS cytoplasm
            LEFT JOIN read_parquet('cells.parquet') AS cells ON
                cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND cells.Metadata_ObjectNumber = cytoplasm.Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('nuclei.parquet') AS nuclei ON
                nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND nuclei.Metadata_ObjectNumber = cytoplasm.Cytoplasm_Parent_Nuclei
            LEFT JOIN read_parquet('image.parquet') AS image ON
                image.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
            """,
    },
    "cellprofiler_sqlite_pycytominer": {
        # version specifications using related references
        "CONFIG_SOURCE_VERSION": {
            "cellprofiler": "v4.2.4",
            "pycytominer": "c90438fd7c11ad8b1689c21db16dab1a5280de6c",
        },
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "ImageNumber",
            "Metadata_Well",
            "Parent_Cells",
            "Parent_Nuclei",
            "Cytoplasm_Parent_Cells",
            "Cytoplasm_Parent_Nuclei",
            "Cells_Number_Object_Number",
            "Nuclei_Number_Object_Number",
        ),
        # pagination keys for use with this data
        # of the rough format "table" -> "column".
        # note: page keys are expected to be numeric (int, float)
        "CONFIG_PAGE_KEYS": {
            "image": "ImageNumber",
            "cells": "Cells_Number_Object_Number",
            "nuclei": "Nuclei_Number_Object_Number",
            "cytoplasm": "Cytoplasm_Number_Object_Number",
            "join": "Cytoplasm_Number_Object_Number",
        },
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            SELECT
                per_image.Metadata_ImageNumber,
                per_image.Image_Metadata_Well,
                per_image.Image_Metadata_Plate,
                per_cytoplasm.* EXCLUDE (Metadata_ImageNumber),
                per_cells.* EXCLUDE (Metadata_ImageNumber),
                per_nuclei.* EXCLUDE (Metadata_ImageNumber)
            FROM
                read_parquet('per_cytoplasm.parquet') AS per_cytoplasm
            LEFT JOIN read_parquet('per_cells.parquet') AS per_cells USING (Metadata_ImageNumber)
            LEFT JOIN read_parquet('per_nuclei.parquet') AS per_nuclei USING (Metadata_ImageNumber)
            LEFT JOIN read_parquet('per_image.parquet') AS per_image USING (Metadata_ImageNumber)
            WHERE
                per_cells.Metadata_Cells_Number_Object_Number = per_cytoplasm.Metadata_Cytoplasm_Parent_Cells
                AND per_nuclei.Metadata_Nuclei_Number_Object_Number = per_cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
            """,
    },
    "cell-health-cellprofiler-to-cytominer-database": {
        # version specifications using related references
        "CONFIG_SOURCE_VERSION": {
            "cell-health-dataset": "v5",
            "cellprofiler": "v2.X",
            "cytominer-database": "5aa00f58e4a31bbbd2a3779c87e7a3620b0030db",
        },
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": ("cells", "nuclei", "cytoplasm"),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": ("image",),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "TableNumber",
            "ImageNumber",
            "Metadata_Well",
            "Parent_Cells",
            "Parent_Nuclei",
            "Cytoplasm_Parent_Cells",
            "Cytoplasm_Parent_Nuclei",
            "Cells_ObjectNumber",
            "Nuclei_ObjectNumber",
        ),
        # pagination keys for use with this data
        # of the rough format "table" -> "column".
        # note: page keys are expected to be numeric (int, float)
        "CONFIG_PAGE_KEYS": {
            "image": "ImageNumber",
            "cells": "ObjectNumber",
            "nuclei": "ObjectNumber",
            "cytoplasm": "ObjectNumber",
            "join": "Cytoplasm_Number_Object_Number",
        },
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            SELECT
                image.Metadata_TableNumber,
                image.Metadata_ImageNumber,
                image.Image_Metadata_Well,
                image.Image_Metadata_Plate,
                cytoplasm.* EXCLUDE (Metadata_TableNumber, Metadata_ImageNumber),
                cells.* EXCLUDE (Metadata_TableNumber, Metadata_ImageNumber),
                nuclei.* EXCLUDE (Metadata_TableNumber, Metadata_ImageNumber)
            FROM
                read_parquet('cytoplasm.parquet') AS cytoplasm
            LEFT JOIN read_parquet('cells.parquet') AS cells USING (Metadata_TableNumber, Metadata_ImageNumber)
            LEFT JOIN read_parquet('nuclei.parquet') AS nuclei USING (Metadata_TableNumber, Metadata_ImageNumber)
            LEFT JOIN read_parquet('image.parquet') AS image USING (Metadata_TableNumber, Metadata_ImageNumber)
            WHERE
                cells.Cells_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
                AND nuclei.Nuclei_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
        """,
    },
    "in-carta": {
        # version specifications using related references
        "CONFIG_SOURCE_VERSION": {
            "in-carta": "v1.17.0412545",
        },
        # names of source table compartments (for ex. cells.csv, etc.)
        "CONFIG_NAMES_COMPARTMENTS": tuple(),
        # names of source table metadata (for ex. image.csv, etc.)
        "CONFIG_NAMES_METADATA": tuple(),
        # column names in any compartment or metadata tables which contain
        # unique names to avoid renaming
        "CONFIG_IDENTIFYING_COLUMNS": (
            "OBJECT ID",
            "Row",
            "Column",
            "FOV",
            "WELL LABEL",
            "Z",
            "T",
        ),
        # pagination keys for use with this data
        # of the rough format "table" -> "column".
        # note: page keys are expected to be numeric (int, float)
        "CONFIG_PAGE_KEYS": {
            "test": '"OBJECT ID"',
        },
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": "",
    },
}
"""
Configuration presets for CytoTable
"""

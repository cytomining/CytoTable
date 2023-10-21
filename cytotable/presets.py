"""
Presets for common pycytominer-transform configurations.
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
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            WITH Image_Filtered AS (
                SELECT
                    /* seeks columns by name, avoiding failure if some do not exist */
                    COLUMNS('^Metadata_ImageNumber$|^Image_Metadata_Well$|^Image_Metadata_Plate$')
                FROM
                    read_parquet('image.parquet')
                )
            SELECT
                *
            FROM
                Image_Filtered AS image
            LEFT JOIN read_parquet('cytoplasm.parquet') AS cytoplasm ON
                cytoplasm.Metadata_ImageNumber = image.Metadata_ImageNumber
            LEFT JOIN read_parquet('cells.parquet') AS cells ON
                cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND cells.Metadata_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('nuclei.parquet') AS nuclei ON
                nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
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
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            WITH Per_Image_Filtered AS (
                SELECT
                    Metadata_ImageNumber,
                    Image_Metadata_Well,
                    Image_Metadata_Plate
                FROM
                    read_parquet('per_image.parquet')
                )
            SELECT
                *
            FROM
                Per_Image_Filtered AS per_image
            LEFT JOIN read_parquet('per_cytoplasm.parquet') AS per_cytoplasm ON
                per_cytoplasm.Metadata_ImageNumber = per_image.Metadata_ImageNumber
            LEFT JOIN read_parquet('per_cells.parquet') AS per_cells ON
                per_cells.Metadata_ImageNumber = per_cytoplasm.Metadata_ImageNumber
                AND per_cells.Cells_Number_Object_Number = per_cytoplasm.Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('per_nuclei.parquet') AS per_nuclei ON
                per_nuclei.Metadata_ImageNumber = per_cytoplasm.Metadata_ImageNumber
                AND per_nuclei.Nuclei_Number_Object_Number = per_cytoplasm.Cytoplasm_Parent_Nuclei
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
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            WITH Per_Image_Filtered AS (
                SELECT
                    Metadata_ImageNumber,
                    Image_Metadata_Well,
                    Image_Metadata_Plate
                FROM
                    read_parquet('per_image.parquet')
                )
            SELECT
                *
            FROM
                Per_Image_Filtered AS per_image
            LEFT JOIN read_parquet('per_cytoplasm.parquet') AS per_cytoplasm ON
                per_cytoplasm.Metadata_ImageNumber = per_image.Metadata_ImageNumber
            LEFT JOIN read_parquet('per_cells.parquet') AS per_cells ON
                per_cells.Metadata_ImageNumber = per_cytoplasm.Metadata_ImageNumber
                AND per_cells.Metadata_Cells_Number_Object_Number = per_cytoplasm.Metadata_Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('per_nuclei.parquet') AS per_nuclei ON
                per_nuclei.Metadata_ImageNumber = per_cytoplasm.Metadata_ImageNumber
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
        # chunk size to use for join operations to help with possible performance issues
        # note: this number is an estimate and is may need changes contingent on data
        # and system used by this library.
        "CONFIG_CHUNK_SIZE": 1000,
        # compartment and metadata joins performed using DuckDB SQL
        # and modified at runtime as needed
        "CONFIG_JOINS": """
            WITH Image_Filtered AS (
                SELECT
                    Metadata_TableNumber,
                    Metadata_ImageNumber,
                    Image_Metadata_Well,
                    Image_Metadata_Plate
                FROM
                    read_parquet('image.parquet')
                )
            SELECT
                *
            FROM
                Image_Filtered AS image
            LEFT JOIN read_parquet('cytoplasm.parquet') AS cytoplasm ON
                cytoplasm.Metadata_TableNumber = image.Metadata_TableNumber
                AND cytoplasm.Metadata_ImageNumber = image.Metadata_ImageNumber
            LEFT JOIN read_parquet('cells.parquet') AS cells ON
                cells.Metadata_TableNumber = cytoplasm.Metadata_TableNumber
                AND cells.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND cells.Cells_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Cells
            LEFT JOIN read_parquet('nuclei.parquet') AS nuclei ON
                nuclei.Metadata_TableNumber = cytoplasm.Metadata_TableNumber
                AND nuclei.Metadata_ImageNumber = cytoplasm.Metadata_ImageNumber
                AND nuclei.Nuclei_ObjectNumber = cytoplasm.Metadata_Cytoplasm_Parent_Nuclei
        """,
    },
}
"""
Configuration presets for pycytominer-transform
"""

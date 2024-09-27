"""
CytoTable: constants - storing various constants to be used throughout cytotable.
"""

import multiprocessing
import os
from typing import cast

from cytotable.utils import _get_cytotable_version

# read max threads from environment if necessary
# max threads will be used with default Parsl config and Duckdb
MAX_THREADS = (
    multiprocessing.cpu_count()
    if "CYTOTABLE_MAX_THREADS" not in os.environ
    else int(cast(int, os.environ.get("CYTOTABLE_MAX_THREADS")))
)

# enables overriding default memory mapping behavior with pyarrow memory mapping
CYTOTABLE_ARROW_USE_MEMORY_MAPPING = (
    os.environ.get("CYTOTABLE_ARROW_USE_MEMORY_MAPPING", "1") == "1"
)

DDB_DATA_TYPE_SYNONYMS = {
    "real": ["float32", "float4", "float"],
    "double": ["float64", "float8", "numeric", "decimal"],
    "integer": ["int32", "int4", "int", "signed"],
    "bigint": ["int64", "int8", "long"],
}

# A reference dictionary for SQLite affinity and storage class types
# See more here: https://www.sqlite.org/datatype3.html#affinity_name_examples
SQLITE_AFFINITY_DATA_TYPE_SYNONYMS = {
    "integer": [
        "int",
        "integer",
        "tinyint",
        "smallint",
        "mediumint",
        "bigint",
        "unsigned big int",
        "int2",
        "int8",
    ],
    "text": [
        "character",
        "varchar",
        "varying character",
        "nchar",
        "native character",
        "nvarchar",
        "text",
        "clob",
    ],
    "blob": ["blob"],
    "real": [
        "real",
        "double",
        "double precision",
        "float",
    ],
    "numeric": [
        "numeric",
        "decimal",
        "boolean",
        "date",
        "datetime",
    ],
}

CYTOTABLE_DEFAULT_PARQUET_METADATA = {
    "data-producer": "https://github.com/cytomining/CytoTable",
    "data-producer-version": str(_get_cytotable_version()),
}

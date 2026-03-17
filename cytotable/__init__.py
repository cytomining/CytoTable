"""
__init__.py for cytotable
"""

# note: version data is maintained by poetry-dynamic-versioning (do not edit)
__version__ = "0.0.0"

# filter warnings about pkg_resources deprecation
# note: these stem from cloudpathlib google cloud
# dependencies.
import warnings

warnings.filterwarnings(
    "ignore",
    message=(".*pkg_resources is deprecated as an API.*"),
    category=UserWarning,
    module="google_crc32c.__config__",
)

from .convert import convert
from .exceptions import (
    CytoTableException,
    DatatypeException,
    NoInputDataException,
    SchemaException,
)
from .iceberg import (
    describe_iceberg_warehouse,
    list_iceberg_tables,
    read_iceberg_table,
    write_iceberg_warehouse,
)
from .presets import config

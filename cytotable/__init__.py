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
    message=(
        "pkg_resources is deprecated as an API."
        "See https://setuptools.pypa.io/en/latest/pkg_resources.html."
    ),
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
from .presets import config

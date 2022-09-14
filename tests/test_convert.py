import os
from pycytominer_transform import convert


def test_convert():
    output = convert(
        path=f"{os.path.dirname(__file__)}/data/",
        source_datatype="csv",
        dest_datatype="parquet",
    )

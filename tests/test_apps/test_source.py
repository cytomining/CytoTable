"""
Tests for cytotable.apps.source
"""

# pylint: disable=unused-argument

import itertools
from typing import Any, Dict, List

from cytotable.apps.source import _gather_tablenumber


def test_gather_tablenumber(
    load_parsl: None, example_local_sources: Dict[str, List[Dict[str, Any]]]
):
    """
    Tests _gather_tablenumber
    """

    tablenumber_prepared = {
        source_group_name: [
            dict(
                source,
                **{
                    "tablenumber": _gather_tablenumber(  # pylint: disable=no-member
                        source=source,
                        source_group_name=source_group_name,
                    ).result()
                },
            )
            for source in source_group_vals
        ]
        for source_group_name, source_group_vals in example_local_sources.items()
    }

    # compare to see that we have a tablenumber key for each element and also
    # that we received the checksum values for the related tables
    assert [
        elem["tablenumber"]
        for elem in list(itertools.chain(*list(tablenumber_prepared.values())))
    ] == [782642759, 2915137387, 2213917770, 744364272, 3277408204]

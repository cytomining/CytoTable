"""
Testing CytoTable utility functions found within util.py
"""

import pytest

from cytotable.utils import _generate_pagesets, _natural_sort


def test_generate_pageset():  # pylint: disable=too-many-statements
    """
    Test the generate_pageset function with various scenarios.
    """

    # Test case with a single element
    keys = [1]
    chunk_size = 3
    expected = [(1, 1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case when chunk size is larger than the list
    keys = [1, 2, 3]
    chunk_size = 10
    expected = [(1, 3)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with all elements being the same
    keys = [1, 1, 1, 1, 1]
    chunk_size = 2
    expected = [(1, 1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with one duplicate of chunk size and others
    keys = [1, 1, 1, 2, 3, 4]
    chunk_size = 3
    expected = [(1, 1), (2, 4)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with a chunk size of one
    keys = [1, 2, 3, 4, 5]
    chunk_size = 1
    expected = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with no duplicates
    keys = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    chunk_size = 3
    expected = [(1, 3), (4, 6), (7, 9), (10, 10)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with non-continuous keys
    keys = [1, 3, 5, 7, 9, 12, 14]
    chunk_size = 2
    expected = [(1, 3), (5, 7), (9, 12), (14, 14)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Test case with inconsistent duplicates
    keys = [1, 1, 3, 4, 5, 5, 8, 8, 8]
    chunk_size = 3
    expected = [(1, 3), (4, 5), (8, 8)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Bigger test case with inconsistent duplicates
    keys = [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 12, 12, 12]
    chunk_size = 3
    expected = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10), (12, 12)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with no duplicates
    keys = [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.1]
    chunk_size = 3
    expected = [(1.1, 3.3), (4.4, 6.6), (7.7, 9.9), (10.1, 10.1)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with non-continuous float keys
    keys = [1.1, 3.3, 5.5, 7.7, 9.9, 12.12, 14.14]
    chunk_size = 2
    expected = [(1.1, 3.3), (5.5, 7.7), (9.9, 12.12), (14.14, 14.14)]
    assert _generate_pagesets(keys, chunk_size) == expected

    # Float test case with inconsistent duplicates
    keys = [1.1, 1.1, 3.3, 4.4, 5.5, 5.5, 8.8, 8.8, 8.8]
    chunk_size = 3
    expected = [(1.1, 3.3), (4.4, 5.5), (8.8, 8.8)]
    assert _generate_pagesets(keys, chunk_size) == expected


@pytest.mark.parametrize(
    "input_list, expected",
    [
        ([], []),
        (["a1"], ["a1"]),
        (["a1", "a10", "a2", "a3"], ["a1", "a2", "a3", "a10"]),
        (["1", "10", "2", "11", "21", "20"], ["1", "2", "10", "11", "20", "21"]),
        (["b1", "a1", "b2", "a2"], ["a1", "a2", "b1", "b2"]),
        (["apple1", "Apple10", "apple2"], ["Apple10", "apple1", "apple2"]),
        (["a1", "A1", "a10", "A10"], ["A1", "A10", "a1", "a10"]),
        (
            ["a-1", "a-10", "b-2", "B-1", "b-3", "a-2", "A-3"],
            ["A-3", "B-1", "a-1", "a-2", "a-10", "b-2", "b-3"],
        ),
    ],
)
def test_natural_sort(input_list, expected):
    """
    Tests for _natural_sort
    """
    assert _natural_sort(input_list) == expected

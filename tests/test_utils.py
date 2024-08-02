"""
Testing CytoTable utility functions found within util.py
"""

import pytest
from cytotable.utils import _generate_pagesets
from cytotable.exceptions import CytoTableException


def test_generate_pageset():
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

    # Test case with an empty list
    keys = []
    chunk_size = 3
    expected = []
    with pytest.raises(CytoTableException):
        _generate_pagesets(keys, chunk_size)

"""
Provide hierarchy of exceptions for CytoTable
"""


class CytoTableException(Exception):
    """
    Root exception for custom hierarchy of exceptions
    with CytoTable.
    """


class NoInputDataException(CytoTableException):
    """
    Exception for no input data.
    """


class DatatypeException(CytoTableException):
    """
    Exception for datatype challenges.
    """


class SchemaException(CytoTableException):
    """
    Exception for schema challenges.
    """

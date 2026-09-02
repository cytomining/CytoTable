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


class SQLiteReadOnlyException(CytoTableException):
    """
    Exception for when a SQLite source cannot be opened due to it
    being in WAL journal mode without write access to its directory
    (SQLite requires the ability to create '-wal'/'-shm' companion
    files even for read-only queries against a WAL-mode database).
    """

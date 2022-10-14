"""
Provide hierarchy of exceptions for pycytominer-transform
"""

# a root exception for custom hierarchy of exceptions
class CytominerException(Exception):
    pass


# exception for no input data
class NoInputDataException(CytominerException):
    pass


# exception for datatype challenges
class DatatypeException(CytominerException):
    pass


# exception for schema challenges
class SchemaException(CytominerException):
    pass

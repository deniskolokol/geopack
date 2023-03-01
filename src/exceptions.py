# -*- coding: utf-8 -*-

"""Project wide exceptions."""

class MalformedValueError(Exception):
    """Raised when value is wrongly formatted."""
    pass


class UnsupportedValueError(Exception):
    """Raised when value is not that expected."""
    pass


class FieldError(Exception):
    """Analog to KeyError, but for fields in a DB."""
    pass


class WrongValueError(MalformedValueError):
    """Raised when value of the field is not that expected."""
    pass


class MissingDataError(Exception):
    """Raised when data cannot be obtained for a field."""
    pass


class DuplicateValueError(Exception):
    """Duplicate fields error."""
    pass


class RequestFailedError(Exception):
    """Undefined request failure."""
    pass


class MissingAuthError(Exception):
    """Raised when the request missing authentication data."""
    pass


class ConnectionInterruptedError(Exception):
    """Raised when the request missing authentication data."""
    pass

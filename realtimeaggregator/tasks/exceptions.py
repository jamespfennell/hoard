"""Provides exceptions for the Realtime Aggregator software."""


class RAException(Exception):
    """Base exception class for the Realtime Aggregator software."""
    pass


class UnreadableRemoteSettingsFileError(RAException):
    """Exception to raise if the remote settings file is not readable."""
    pass


class InvalidRemoteSettingsFileError(RAException):
    """Exception to raise if the remote settings file is not valid."""
    pass


class TaskAlreadyRanError(RAException):
    pass

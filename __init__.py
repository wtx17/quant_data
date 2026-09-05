"""Public API for the quant data access package."""

from .client import DataClient
from ._version import __version__ as __version__
from .exceptions import (
    AuditWriteError,
    BackendConnectionError,
    DatasetNotFoundError,
    DatasetRegistrationError,
    DuplicateObservationError,
    FieldNotFoundError,
    InvalidQueryError,
    QuantDataError,
    RemoteQueryError,
    SchemaMismatchError,
)
from .models import ClickHouseConfig, TushareConfig

__all__ = [
    "AuditWriteError",
    "BackendConnectionError",
    "ClickHouseConfig",
    "DataClient",
    "DatasetNotFoundError",
    "DatasetRegistrationError",
    "DuplicateObservationError",
    "FieldNotFoundError",
    "InvalidQueryError",
    "QuantDataError",
    "RemoteQueryError",
    "SchemaMismatchError",
    "TushareConfig",
    "__version__",
]

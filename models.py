"""Runtime data models shared by the client and dataset factories."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

import pandas as pd
import pyarrow as pa

Panels = dict[str, pd.DataFrame]


@dataclass(frozen=True, slots=True)
class Query:
    """Represent a normalized dataset panel request.

    Parameters
    ----------
    dataset
        Registered dataset name included in validation error messages.
    fields
        Non-key value columns to return, in caller order.
    start, end
        Inclusive normalized time bounds interpreted with the dataset's
        query timezone.
    instruments
        Requested instruments in caller order, ``None`` for all instruments,
        or an empty tuple for a guaranteed empty result.
    adjusted
        Resolved price-adjustment flag for this query.
    """

    dataset: str
    fields: tuple[str, ...]
    start: datetime | None = None
    end: datetime | None = None
    instruments: tuple[str, ...] | None = None
    adjusted: bool = False


@dataclass(frozen=True, slots=True)
class Dataset:
    """Hold one registered dataset and its bound panel execution path.

    This is the only runtime dataset record. Everything the generic query
    validation and result finalization need is stored here once; source and
    domain specifics live inside the bound functions.

    Parameters
    ----------
    schema
        Complete Arrow schema available to callers.
    time_column
        Output panel index key name.
    instrument_column
        Output panel column key name.
    read_panel
        Execute the query and return one wide panel per requested field.
        The function may enrich the passed audit record with source facts.
    fingerprint
        Return a fresh sanitized provenance dictionary for each query audit.
    query_timezone
        IANA timezone used to localize or convert query bounds, or ``None``
        when bounds are never localized (generic Parquet).
    frequency
        Optional sampling-frequency metadata.
    version
        Optional dataset version metadata.
    requires_range
        Whether both ``start`` and ``end`` are required.
    instrument_suffixes
        Required instrument identifier suffixes, or ``None`` when any
        identifier form is accepted.
    adjustment
        Optional multiplicative price-adjustment policy.

    Notes
    -----
    ``read_panel`` and ``fingerprint`` capture sessions and connection names,
    never a bare network client, so replacing a connection configuration is
    picked up by already-registered datasets. Query-level state is written to
    the passed :class:`Query`/:class:`QueryAudit` only.
    """

    schema: pa.Schema
    time_column: str
    instrument_column: str
    read_panel: Callable[[Query, QueryAudit], Panels]
    fingerprint: Callable[[], dict[str, object]]
    query_timezone: str | None = None
    frequency: str | None = None
    version: str | None = None
    requires_range: bool = False
    instrument_suffixes: tuple[str, ...] | None = None
    adjustment: PriceAdjustment | None = None


@dataclass(frozen=True, slots=True)
class ClickHouseConfig:
    """Configure one lazily opened ClickHouse connection.

    Parameters
    ----------
    host
        ClickHouse server hostname.
    port
        HTTP or HTTPS service port.
    username
        Optional login name.
    password
        Optional password value. The field is excluded from ``repr`` output.
    password_env
        Environment variable read on first connection when ``password`` is
        not supplied.
    secure
        Whether to use TLS.
    connect_timeout
        Connection timeout in seconds.
    query_timeout
        Send/receive timeout in seconds.

    Notes
    -----
    Creating this configuration does not connect to ClickHouse or read the
    password environment variable.
    """

    host: str
    port: int = 8123
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    password_env: str | None = None
    secure: bool = False
    connect_timeout: int = 10
    query_timeout: int = 300


@dataclass(frozen=True, slots=True)
class TushareConfig:
    """Configure credentials for a Tushare Pro connection.

    Parameters
    ----------
    token
        Optional token value. The field is excluded from ``repr`` output.
    token_env
        Environment variable read when the client is first initialized and
        ``token`` is not supplied.
    """

    token: str | None = field(default=None, repr=False)
    token_env: str | None = "TUSHARE_TOKEN"


@dataclass(frozen=True, slots=True)
class PriceAdjustment:
    """Describe multiplicative price adjustment for selected fields.

    Parameters
    ----------
    factor_column
        Column containing the row-level adjustment multiplier.
    fields
        Price columns eligible for multiplication by the factor.
    default
        Whether adjustment is enabled when the caller passes ``adjusted=None``.
    """

    factor_column: str
    fields: tuple[str, ...]
    default: bool = True


@dataclass(frozen=True, slots=True)
class FileFingerprint:
    """Capture file identity used in reproducibility metadata.

    Parameters
    ----------
    path
        Absolute file path.
    size
        File size in bytes.
    mtime_ns
        Modification time in nanoseconds.
    """

    path: str
    size: int
    mtime_ns: int


@dataclass(slots=True)
class QueryAudit:
    """Store the durable audit state for one query.

    Parameters
    ----------
    query_id
        UUID associated with the result and audit file.
    dataset
        Registered dataset name.
    fields
        Requested non-key fields.
    parameters
        Sanitized query parameters.
    started_at
        UTC start timestamp in ISO 8601 form.
    framework_version
        Package version that executed the query.
    operation
        Always ``"panel"``.

    Notes
    -----
    Remaining attributes are populated as the query progresses and are
    serialized by :class:`quant_data.audit.AuditWriter`.
    """

    query_id: str
    dataset: str
    fields: list[str]
    parameters: dict[str, Any]
    started_at: str
    framework_version: str
    operation: str = "panel"
    source: dict[str, Any] = field(default_factory=dict)
    frequency: str | None = None
    dataset_version: str | None = None
    adjusted: bool = False
    calendar_aligned: bool = False
    status: str = "running"
    duration_ms: float | None = None
    result_shapes: dict[str, list[int]] = field(default_factory=dict)
    error: dict[str, str] | None = None

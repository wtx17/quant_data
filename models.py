"""Data models shared by the client and backends."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow as pa


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Describe a local Parquet dataset.

    Parameters
    ----------
    name
        Stable name used to register and query the dataset.
    paths
        Parquet files, directories, or glob patterns. Directories are searched
        recursively and all matching files form one logical table.
    time_column
        Column used as the panel index and time-range filter.
    instrument_column
        Column used as the panel columns and instrument filter.
    frequency
        Optional human-readable sampling frequency stored in query metadata.
    timezone
        Optional IANA timezone recorded for the dataset. Local Parquet values
        are not localized during query parsing.
    version
        Optional dataset version stored in query metadata and audit records.
    backend
        Backend identifier. The built-in local implementation requires
        ``"parquet"``.

    Notes
    -----
    Every matched file must contain both key columns. Schemas are merged with
    permissive Arrow promotion when the dataset is registered.
    """

    name: str
    paths: Sequence[str | Path]
    time_column: str = "time"
    instrument_column: str = "ts_code"
    frequency: str | None = None
    timezone: str | None = None
    version: str | None = None
    backend: str = "parquet"


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
class ClickHouseDatasetSpec:
    """Describe a ClickHouse table exposed as a dataset.

    Parameters
    ----------
    name
        Stable registration name.
    connection
        Name previously passed to
        :meth:`quant_data.DataClient.add_clickhouse_connection`.
    table
        ClickHouse table in ``database.table`` form.
    time_column
        Column used for time filtering and panel rows.
        With ``"date_time"`` and source columns ``date`` / ``time_int``, SQL
        synthesizes a millisecond timestamp in Asia/Shanghai. ``date`` is Date,
        Date32 or a YYYYMMDD integer; ``time_int`` is milliseconds since midnight.
        A physical ``date_time`` column is not required and is ignored if present.
    instrument_column
        Column used for instrument filtering and panel columns.
    partition_column
        Optional date partition column. When set, queries require both time
        bounds and push a partition-range predicate to ClickHouse.
    order_columns
        Columns used for deterministic server-side ordering.
    frequency
        Optional sampling frequency stored in result metadata.
    timezone
        IANA timezone used to localize or convert query bounds.
    version
        Optional dataset version stored in result metadata.
    require_time_range
        Explicitly require both ``start`` and ``end``. ``None`` derives the
        requirement from ``partition_column``.

    Notes
    -----
    ``backend`` is fixed to ``"clickhouse"``. Built-in Minghu tables use a
    local schema catalog, so registration stays offline; custom tables are
    described remotely during registration.
    """

    name: str
    connection: str
    table: str
    time_column: str
    instrument_column: str = "code"
    partition_column: str | None = None
    order_columns: tuple[str, ...] = ()
    frequency: str | None = None
    timezone: str | None = "Asia/Shanghai"
    version: str | None = None
    require_time_range: bool | None = None
    backend: str = field(default="clickhouse", init=False)


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
class TushareDatasetSpec:
    """Describe a logical catalog-backed Tushare dataset.

    Parameters
    ----------
    name
        Stable registration name.
    connection
        Name previously passed to
        :meth:`quant_data.DataClient.add_tushare_connection`.
    dataset
        Optional logical catalog name. When omitted, ``name`` is used. Supply
        this only when registering an alias or a fixed-parameter view.
    fixed_params
        Constant API parameters added to every request. Backend-managed
        parameters such as fields, dates, and instruments are
        reserved.
    timezone
        IANA timezone used to interpret query bounds.
    version
        Optional dataset version stored in result metadata.
    disclosure_lag
        Number of trading sessions between the snapped disclosure date and
        first availability in a point-in-time panel.
    calendar_exchange
        Tushare exchange code used to request the trading calendar.
    fetch_buffer_days
        Calendar days fetched before ``start`` so earlier disclosures can be
        carried into the requested panel.
    fetch_margin_days
        Calendar days fetched after ``end`` to make disclosure-lag alignment
        possible near the right boundary.

    Notes
    -----
    ``backend`` is fixed to ``"tushare"``. Keys, frequencies, table behavior,
    panel behavior, and remote routes come exclusively from the logical
    catalog. Disclosure datasets automatically produce point-in-time panels;
    no panel-mode registration is required.
    """

    name: str
    connection: str
    dataset: str | None = None
    fixed_params: Mapping[str, object] = field(default_factory=dict)
    timezone: str | None = "Asia/Shanghai"
    version: str | None = None
    disclosure_lag: int = 0
    calendar_exchange: str = "SSE"
    fetch_buffer_days: int = 180
    fetch_margin_days: int = 31
    backend: str = field(default="tushare", init=False)


@dataclass(frozen=True, slots=True)
class TushareParquetDatasetSpec:
    """Describe a local Parquet snapshot with Tushare dataset semantics.

    Parameters
    ----------
    name
        Stable registration name.
    data_dir
        Root directory containing one manifest-backed subdirectory per
        logical Tushare dataset.
    calendar_connection
        Tushare connection used only to fetch ``trade_cal`` for panel queries.
    dataset
        Optional logical catalog name. When omitted, ``name`` is used. Supply
        this when registering an alias or a fixed-parameter view.
    fixed_params
        Constant source parameters represented by stored archive columns.
        Parameters that cannot be reconstructed from the snapshot are rejected.
    timezone
        IANA timezone used to interpret query bounds.
    version
        Optional dataset version stored in result metadata.
    disclosure_lag
        Trading sessions between the snapped disclosure date and availability.
    calendar_exchange
        Tushare exchange code used for the remote trading calendar.
    fetch_buffer_days
        Calendar days read before a PIT panel start for carry-in state.
    fetch_margin_days
        Calendar days requested after a PIT panel end for disclosure alignment.

    Notes
    -----
    ``backend`` remains ``"parquet"``. Table data never calls a Tushare data
    API; only panel queries use the configured connection for ``trade_cal``.
    Dataset keys, identity columns, table ordering, and panel behavior come
    from the same logical catalog as :class:`TushareDatasetSpec`.
    """

    name: str
    data_dir: str | Path
    calendar_connection: str
    dataset: str | None = None
    fixed_params: Mapping[str, object] = field(default_factory=dict)
    timezone: str | None = "Asia/Shanghai"
    version: str | None = None
    disclosure_lag: int = 0
    calendar_exchange: str = "SSE"
    fetch_buffer_days: int = 180
    fetch_margin_days: int = 31
    backend: str = field(default="parquet", init=False)


@dataclass(frozen=True, slots=True)
class BuiltInDatasetSpec:
    """Describe a bundled logical dataset and its auxiliary ClickHouse connection.

    ``name`` is the registration alias; ``dataset`` selects the built-in semantics.
    Currently only ``membership_events`` is supported. Parquet supplies its
    events; ``connection`` supplies the market and calendar via stock_base.daily.
    """

    name: str = "membership_events"
    dataset: str = "membership_events"
    connection: str = "minghu"
    timezone: str = "Asia/Shanghai"
    version: str | None = None
    backend: str = field(default="parquet", init=False)


DatasetDefinition = (
    BuiltInDatasetSpec
    | DatasetSpec
    | ClickHouseDatasetSpec
    | TushareDatasetSpec
    | TushareParquetDatasetSpec
)


@dataclass(frozen=True, slots=True)
class DatasetContract:
    """Prepared panel contract derived by the backend during registration.

    ``source_time_column`` identifies source observations (the report period
    for disclosures); ``panel_time_column`` names the output index.
    Frequency is optional metadata, not a sampling restriction.
    """

    source_time_column: str
    instrument_column: str
    panel_time_column: str
    panel_frequency: str | None = None
    timezone: str | None = None
    version: str | None = None

    panel_requires_time_range: bool = False


@dataclass(frozen=True, slots=True)
class RegisteredDataset:
    """Hold a validated dataset and backend-specific prepared state.

    Parameters
    ----------
    spec
        Normalized dataset definition.
    schema
        Complete Arrow schema available to callers.
    source
        Backend-owned source descriptor.
    contract
        Prepared method-specific keys, frequencies, range requirements, and
        panel capability.
    adjustment
        Optional price-adjustment policy.

    Notes
    -----
    Backend implementations create this object in ``prepare`` and receive it
    again for scans and fingerprints.
    """

    spec: DatasetDefinition
    schema: pa.Schema
    source: Any
    contract: DatasetContract
    adjustment: PriceAdjustment | None = None


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
class DataQuery:
    """Represent a normalized backend scan request.

    Parameters
    ----------
    fields
        Non-key columns to project.
    start, end
        Inclusive normalized time bounds.
    instruments
        Requested instruments in caller order, ``None`` for all instruments,
        or an empty tuple for a guaranteed empty result.
    """

    fields: tuple[str, ...]
    start: datetime | None = None
    end: datetime | None = None
    instruments: tuple[str, ...] | None = None


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

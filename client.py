"""High-level data registration and query API."""

from __future__ import annotations

import time
import uuid
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, cast
from zoneinfo import ZoneInfo

import pandas as pd

from ._version import __version__
from .audit import AuditWriter
from .backends.clickhouse import ClickHouseSession
from . import datasets as dataset_factories
from .exceptions import (
    DatasetNotFoundError,
    FieldNotFoundError,
    InvalidQueryError,
)
from .models import ClickHouseConfig, Dataset, Query, QueryAudit
from ._universes import load_universe


class DataClient:
    """Register datasets and execute source-independent data queries.

    Parameters
    ----------
    audit_dir
        Directory that receives one durable JSON audit record per query.
    clickhouse_client_factory
        Optional factory used to create ClickHouse clients. This is primarily
        useful for dependency injection and offline tests.

    Notes
    -----
    A client starts with a ClickHouse session but no datasets.
    Remote connections are cached and released by :meth:`close`. Use the
    client as a context manager to close them automatically.

    Examples
    --------
    Register a local dataset and request one panel::

        from quant_data import DataClient

        with DataClient() as data:
            data.register_parquet("daily", ["data/*.parquet"])
            close = data.get_panel("daily", ["close"])["close"]
    """

    def __init__(
        self,
        audit_dir: str | Path = ".quant_data/audit",
        *,
        clickhouse_client_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._datasets: dict[str, Dataset] = {}
        self._clickhouse = ClickHouseSession(clickhouse_client_factory)
        self._audit = AuditWriter(audit_dir)

    def add_clickhouse_connection(self, name: str, config: ClickHouseConfig) -> None:
        """Add or replace a named ClickHouse connection profile.

        Parameters
        ----------
        name
            Identifier referenced by ClickHouse dataset registrations.
        config
            Host, credentials, TLS, and timeout settings.

        Raises
        ------
        DatasetRegistrationError
            If the name, host, port, or timeout settings are invalid.

        Notes
        -----
        No connection is opened until a query or a custom-table schema lookup
        requires it. Replacing an already opened profile closes its client.
        """

        self._clickhouse.add_connection(name, config)

    def register_parquet(
        self,
        name: str,
        paths: Sequence[str | Path],
        *,
        time_column: str = "time",
        instrument_column: str = "ts_code",
        frequency: str | None = None,
        timezone: str | None = None,
        version: str | None = None,
    ) -> None:
        """Register local Parquet files as one dataset.

        Parameters
        ----------
        name
            Stable name used to register and query the dataset.
        paths
            Parquet files, directories, or glob patterns. Directories are
            searched recursively and all matching files form one logical
            table.
        time_column
            Column used as the panel index and time-range filter.
        instrument_column
            Column used as the panel columns and instrument filter.
        frequency
            Optional sampling-frequency metadata stored in query metadata.
        timezone
            Optional IANA timezone recorded for the dataset. Local Parquet
            values are not localized during query parsing.
        version
            Optional dataset version stored in query metadata and audit
            records.

        Raises
        ------
        DatasetRegistrationError
            If the name, key columns, paths, or timezone are invalid.

        Notes
        -----
        Every matched file must contain both key columns. Schemas are merged
        with permissive Arrow promotion when the dataset is registered.
        Registering a name again replaces the prior dataset; a failed
        registration keeps the old one.
        """

        self._datasets[name] = dataset_factories.parquet_dataset(
            name,
            paths,
            time_column=time_column,
            instrument_column=instrument_column,
            frequency=frequency,
            timezone=timezone,
            version=version,
        )

    def register_clickhouse(
        self,
        name: str,
        *,
        connection: str,
        table: str,
        time_column: str,
        instrument_column: str = "code",
        partition_column: str | None = None,
        order_columns: tuple[str, ...] = (),
        frequency: str | None = None,
        timezone: str | None = "Asia/Shanghai",
        version: str | None = None,
        require_time_range: bool | None = None,
    ) -> None:
        """Register one ClickHouse table as a dataset.

        Parameters
        ----------
        name
            Stable registration name.
        connection
            Name previously passed to :meth:`add_clickhouse_connection`.
        table
            ClickHouse table in ``database.table`` form.
        time_column
            Column used for time filtering and panel rows. With
            ``"date_time"`` and source columns ``date`` / ``time_int``, SQL
            synthesizes a millisecond timestamp in Asia/Shanghai. ``date`` is
            Date, Date32 or a YYYYMMDD integer; ``time_int`` is milliseconds
            since midnight. A physical ``date_time`` column is not required
            and is ignored if present.
        instrument_column
            Column used for instrument filtering and panel columns.
        partition_column
            Optional date partition column. When set, queries require both
            time bounds and push a partition-range predicate to ClickHouse.
        order_columns
            Columns used for deterministic server-side ordering.
        frequency
            Optional sampling frequency stored in result metadata.
        timezone
            IANA timezone used to localize or convert query bounds.
        version
            Optional dataset version stored in result metadata.
        require_time_range
            Explicitly require both ``start`` and ``end``. ``None`` derives
            the requirement from ``partition_column``.

        Raises
        ------
        DatasetRegistrationError
            If the definition, profile, identifiers, or configured columns
            are invalid.
        RemoteQueryError
            If a custom table cannot be described remotely during
            registration.

        Notes
        -----
        Built-in Minghu tables use a local schema catalog, so registration
        stays offline; custom tables are described remotely during
        registration.
        """

        self._datasets[name] = dataset_factories.clickhouse_dataset(
            self._clickhouse,
            name,
            connection=connection,
            table=table,
            time_column=time_column,
            instrument_column=instrument_column,
            partition_column=partition_column,
            order_columns=order_columns,
            frequency=frequency,
            timezone=timezone,
            version=version,
            require_time_range=require_time_range,
        )

    def register_tushare(
        self,
        name: str,
        *,
        data_dir: str | Path,
        dataset: str | None = None,
        calendar_connection: str = "minghu",
        fixed_params: dict[str, object] | None = None,
        timezone: str | None = "Asia/Shanghai",
        version: str | None = None,
        disclosure_lag: int = 0,
        fetch_buffer_days: int = 180,
        fetch_margin_days: int = 31,
    ) -> None:
        """Register a manifest-backed local Tushare archive.

        ``dataset`` selects the logical dataset when ``name`` is an alias.
        ``calendar_connection`` names a ClickHouse connection used for PIT and
        industry calendars from stock_base.daily. daily_basic needs no connection.
        Fixed parameters filter archive columns; disclosure lag and history/calendar
        windows retain their existing meaning. No Tushare API or token is used.
        """
        self._datasets[name] = dataset_factories.tushare_dataset(
            self._clickhouse,
            name,
            data_dir=data_dir,
            dataset=dataset,
            calendar_connection=calendar_connection,
            fixed_params=fixed_params,
            timezone=timezone,
            version=version,
            disclosure_lag=disclosure_lag,
            fetch_buffer_days=fetch_buffer_days,
            fetch_margin_days=fetch_margin_days,
        )

    def register_builtin(
        self,
        name: str = "membership_events",
        *,
        dataset: str = "membership_events",
        connection: str = "minghu",
        timezone: str = "Asia/Shanghai",
        version: str | None = None,
    ) -> None:
        """Register a bundled logical dataset and its auxiliary connection.

        Parameters
        ----------
        name
            Registration alias.
        dataset
            Bundled dataset selector; currently only ``membership_events``
            is supported. Parquet supplies its events; ``connection``
            supplies the market and calendar via ``stock_base.daily``.
        connection
            Named ClickHouse connection profile.
        timezone
            IANA timezone used to interpret query bounds.
        version
            Optional dataset version stored in result metadata.

        Raises
        ------
        DatasetRegistrationError
            If the dataset selection or connection is invalid.
        """

        self._datasets[name] = dataset_factories.builtin_dataset(
            self._clickhouse,
            name,
            dataset=dataset,
            connection=connection,
            timezone=timezone,
            version=version,
        )

    def get_panel(
        self,
        dataset: str,
        fields: Sequence[str],
        start: Any | None = None,
        end: Any | None = None,
        instruments: Sequence[str] | None = None,
        adjusted: bool | None = None,
        *,
        universe: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Query fields as ``time × instrument`` Pandas panels.

        Parameters
        ----------
        dataset
            Registered dataset name.
        fields
            Non-key columns to return. Names must be non-empty and unique.
        start, end
            Optional inclusive time bounds. Values accepted by
            :class:`pandas.Timestamp` are supported.
        instruments
            Instrument identifiers in desired output-column order. ``None``
            requests all available instruments; an empty sequence requests an
            empty panel.
        adjusted
            ``True`` forces configured price adjustment, ``False`` requests
            raw values, and ``None`` uses the dataset default.
        universe
            Optional historical built-in stock-pool name: one of ``"hs300"``,
            ``"zz500"``, and ``"zz1000"``. Names are case-insensitive and
            surrounding whitespace is ignored. This parameter requires both
            ``start`` and ``end`` and is mutually exclusive with
            ``instruments``.

        Returns
        -------
        dict[str, pandas.DataFrame]
            One panel per requested field, preserving field order. Every panel
            carries query metadata in ``DataFrame.attrs``.

        Raises
        ------
        DatasetNotFoundError
            If ``dataset`` has not been registered.
        FieldNotFoundError
            If a requested field is absent from the registered schema.
        InvalidQueryError
            If parameters are invalid.
        DuplicateObservationError
            If an ordinary panel contains duplicate time/instrument pairs.
        SchemaMismatchError
            If result key columns are missing or contain nulls.
        AuditWriteError
            If the required audit record cannot be persisted.

        Notes
        -----
        Query bounds are closed. Requested instruments without observations
        remain as all-missing columns. A named universe selects the union of
        all membership states effective within the requested date range.
        Disclosed Tushare datasets always align announcements to a trading
        calendar, apply their availability lag, and carry whole-row
        point-in-time state.
        """

        return self._execute(
            dataset,
            fields,
            start,
            end,
            instruments,
            adjusted,
            universe=universe,
        )

    def close(self) -> None:
        """Close cached sessions and release their resources.

        Notes
        -----
        Local Parquet scans hold no persistent connection. Calling this
        method more than once is safe.
        """

        self._clickhouse.close()

    def __enter__(self) -> DataClient:
        """Return this client when entering a context manager."""

        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Close session resources when leaving a context manager."""

        self.close()

    def _execute(
        self,
        dataset: str,
        fields: Sequence[str],
        start: Any | None,
        end: Any | None,
        instruments: Sequence[str] | None,
        adjusted: bool | None,
        universe: str | None,
    ) -> dict[str, pd.DataFrame]:
        query_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        started_clock = time.perf_counter()
        record = QueryAudit(
            query_id=query_id,
            dataset=dataset,
            fields=_safe_list(fields),
            parameters={
                "start": DataClient._audit_value(start),
                "end": DataClient._audit_value(end),
                "instruments": (
                    instruments
                    if isinstance(instruments, str)
                    else _safe_list(instruments)
                    if instruments is not None
                    else None
                ),
                "universe": (
                    universe if isinstance(universe, (str, type(None))) else repr(universe)
                ),
                "adjusted": adjusted,
            },
            started_at=started_at.isoformat(),
            framework_version=__version__,
        )

        try:
            entry = self._datasets.get(dataset)
            if entry is None:
                raise DatasetNotFoundError(f"Dataset {dataset!r} is not registered")
            record.frequency = entry.frequency
            record.dataset_version = entry.version
            record.source = entry.fingerprint()

            if universe is not None and instruments is not None:
                raise InvalidQueryError("instruments and universe are mutually exclusive")
            query = self._normalize_query(entry, dataset, fields, start, end, instruments)
            if universe is not None:
                if query.start is None or query.end is None:
                    raise InvalidQueryError("universe queries require both start and end")
                panel = load_universe(universe)
                selected_instruments = panel.select(query.start.date(), query.end.date())
                query = replace(query, instruments=selected_instruments)
                record.parameters["instruments"] = list(selected_instruments)
                record.parameters["universe"] = {
                    "name": panel.name,
                    "first_change_date": panel.first_change_date.isoformat(),
                    "last_change_date": panel.last_change_date.isoformat(),
                    "count": len(selected_instruments),
                    "sha256": panel.sha256,
                }

            apply_adjustment = self._resolve_adjustment(entry, dataset, adjusted)
            record.adjusted = apply_adjustment
            record.parameters["adjusted"] = apply_adjustment
            query = replace(query, adjusted=apply_adjustment)

            panels = entry.read_panel(query, record)
            self._finish_panels(panels, dataset, entry, record)
            record.status = "success"
        except Exception as exc:
            record.status = "failed"
            record.error = {"type": type(exc).__name__, "message": str(exc)}
            record.duration_ms = (time.perf_counter() - started_clock) * 1000
            self._audit.write(record)
            raise

        record.duration_ms = (time.perf_counter() - started_clock) * 1000
        self._audit.write(record)
        return panels

    @staticmethod
    def _finish_panels(
        panels: dict[str, pd.DataFrame],
        dataset_name: str,
        dataset: Dataset,
        record: QueryAudit,
    ) -> None:
        attrs = {
            "query_id": record.query_id,
            "dataset": dataset_name,
            "frequency": dataset.frequency,
            "version": dataset.version,
            "parameters": record.parameters,
            "adjusted": record.adjusted,
            "calendar_aligned": record.calendar_aligned,
        }
        for panel in panels.values():
            panel.attrs.update(attrs)
        record.result_shapes = {
            field: [int(panel.shape[0]), int(panel.shape[1])] for field, panel in panels.items()
        }

    @staticmethod
    def _resolve_adjustment(dataset: Dataset, dataset_name: str, adjusted: bool | None) -> bool:
        if adjusted is not None and not isinstance(adjusted, bool):
            raise InvalidQueryError("adjusted must be True, False, or None")
        if adjusted is None:
            return dataset.adjustment.default if dataset.adjustment else False
        if adjusted and dataset.adjustment is None:
            raise InvalidQueryError(
                f"Dataset {dataset_name!r} does not define a price adjustment factor"
            )
        return adjusted

    @staticmethod
    def _normalize_query(
        dataset: Dataset,
        dataset_name: str,
        fields: Sequence[str],
        start: Any | None,
        end: Any | None,
        instruments: Sequence[str] | None,
    ) -> Query:
        requested_fields = tuple(fields)
        if not requested_fields:
            raise InvalidQueryError("At least one field is required")
        if not all(isinstance(field, str) and field for field in requested_fields):
            raise InvalidQueryError("Field names must be non-empty strings")
        if len(set(requested_fields)) != len(requested_fields):
            raise InvalidQueryError("Fields cannot contain duplicates")
        keys = {dataset.time_column, dataset.instrument_column}
        invalid_keys = keys.intersection(requested_fields)
        if invalid_keys:
            raise InvalidQueryError(f"Key columns cannot be requested as fields: {invalid_keys}")
        missing = set(requested_fields).difference(dataset.schema.names)
        if missing:
            raise FieldNotFoundError(f"Fields not found in dataset: {sorted(missing)}")

        parsed_start = DataClient._parse_time(start, "start", dataset.query_timezone)
        parsed_end = DataClient._parse_time(end, "end", dataset.query_timezone)
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise InvalidQueryError("start must be earlier than or equal to end")
        if dataset.requires_range and (parsed_start is None or parsed_end is None):
            raise InvalidQueryError(
                f"Dataset {dataset_name!r} panel query requires both start and end"
            )

        requested_instruments: tuple[str, ...] | None = None
        if instruments is not None:
            if isinstance(instruments, str):
                raise InvalidQueryError(
                    "instruments must be a sequence of identifiers, not a string; "
                    "wrap one identifier in a list or use get_panel(..., universe=...)"
                )
            requested_instruments = tuple(instruments)
            if not all(isinstance(item, str) and item for item in requested_instruments):
                raise InvalidQueryError("Instrument identifiers must be non-empty strings")
            if len(set(requested_instruments)) != len(requested_instruments):
                raise InvalidQueryError("Instruments cannot contain duplicates")
            if dataset.instrument_suffixes is not None:
                missing_suffix = [
                    item
                    for item in requested_instruments
                    if not item.endswith(dataset.instrument_suffixes)
                ]
                if missing_suffix:
                    raise InvalidQueryError(
                        f"Dataset {dataset_name!r} requires instrument identifiers "
                        "with exchange suffixes such as '000001.SZ'"
                    )
        return Query(
            dataset=dataset_name,
            fields=requested_fields,
            start=parsed_start,
            end=parsed_end,
            instruments=requested_instruments,
        )

    @staticmethod
    def _parse_time(value: Any | None, name: str, timezone_name: str | None) -> datetime | None:
        if value is None:
            return None
        try:
            parsed = pd.Timestamp(value)
        except (TypeError, ValueError) as exc:
            raise InvalidQueryError(f"Invalid {name} value: {value!r}") from exc
        if pd.isna(parsed):
            raise InvalidQueryError(f"Invalid {name} value: {value!r}")
        result = cast(datetime, parsed.to_pydatetime())
        if timezone_name:
            zone = ZoneInfo(timezone_name)
            if result.tzinfo is None:
                result = result.replace(tzinfo=zone)
            else:
                result = result.astimezone(zone)
        return result

    @staticmethod
    def _audit_value(value: Any | None) -> str | None:
        if value is None:
            return None
        try:
            return str(pd.Timestamp(value).isoformat())
        except (TypeError, ValueError):
            return repr(value)


def _safe_list(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    try:
        return list(value)
    except TypeError:
        return [repr(value)]

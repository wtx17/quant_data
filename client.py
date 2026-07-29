"""High-level data registration and query API."""

from __future__ import annotations

import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Sequence, cast
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from ._version import __version__
from .audit import AuditWriter
from .backends.base import DataBackend, TushareSemanticBackend
from .backends.clickhouse import ClickHouseBackend
from .backends.parquet import DuckDBParquetBackend
from .backends.tushare import TushareBackend
from .exceptions import (
    DatasetNotFoundError,
    DatasetRegistrationError,
    FieldNotFoundError,
    InvalidQueryError,
    SchemaMismatchError,
)
from .models import (
    ClickHouseConfig,
    ClickHouseDatasetSpec,
    DataQuery,
    DatasetDefinition,
    DatasetSpec,
    QueryAudit,
    RegisteredDataset,
    TushareConfig,
    TushareDatasetSpec,
    TushareParquetDatasetSpec,
)
from .transforms import build_daily_panels, build_panels
from ._universes import load_universe

QueryMode = Literal["panel", "table"]
_MINGHU_CODE_SUFFIXES = (".SZ", ".SH", ".BJ")


class DataClient:
    """Register datasets and execute backend-independent data queries.

    Parameters
    ----------
    audit_dir
        Directory that receives one durable JSON audit record per query.
    clickhouse_client_factory
        Optional factory used to create ClickHouse clients. This is primarily
        useful for dependency injection and offline tests.
    tushare_client_factory
        Optional factory used to create Tushare clients. This is primarily
        useful for dependency injection and offline tests.

    Notes
    -----
    A client starts with Parquet, ClickHouse, and Tushare backends but no
    datasets. Remote connections are cached and released by :meth:`close`.
    Use the client as a context manager to close them automatically.

    Examples
    --------
    Register a local dataset and request one panel::

        from quant_data import DataClient, DatasetSpec

        with DataClient() as data:
            data.register(DatasetSpec("daily", ["data/*.parquet"]))
            close = data.get_panel("daily", ["close"])["close"]
    """

    def __init__(
        self,
        audit_dir: str | Path = ".quant_data/audit",
        *,
        clickhouse_client_factory: Callable[..., Any] | None = None,
        tushare_client_factory: Callable[..., Any] | None = None,
    ) -> None:
        self._datasets: dict[str, RegisteredDataset] = {}
        self._clickhouse = ClickHouseBackend(clickhouse_client_factory)
        self._tushare = TushareBackend(tushare_client_factory)
        self._parquet = DuckDBParquetBackend(self._tushare)
        self._backends: dict[str, DataBackend] = {
            "parquet": self._parquet,
            "clickhouse": self._clickhouse,
            "tushare": self._tushare,
        }
        self._audit = AuditWriter(audit_dir)

    def add_clickhouse_connection(self, name: str, config: ClickHouseConfig) -> None:
        """Add or replace a named ClickHouse connection profile.

        Parameters
        ----------
        name
            Identifier referenced by :class:`ClickHouseDatasetSpec` objects.
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

    def add_tushare_connection(self, name: str, config: TushareConfig) -> None:
        """Add or replace a named Tushare connection profile.

        Parameters
        ----------
        name
            Identifier referenced by :class:`TushareDatasetSpec` objects.
        config
            Token or token-environment configuration.

        Raises
        ------
        DatasetRegistrationError
            If the name or token configuration is invalid.

        Notes
        -----
        Replacing an already initialized profile closes its client.
        """

        self._tushare.add_connection(name, config)

    def register(self, spec: DatasetDefinition) -> None:
        """Validate and register a dataset definition.

        Parameters
        ----------
        spec
            Parquet, ClickHouse, or Tushare dataset specification.

        Raises
        ------
        DatasetRegistrationError
            If the specification, connection, paths, schema, or backend is
            invalid.
        SchemaMismatchError
            If storage schemas cannot be reconciled.
        BackendConnectionError
            If registration requires remote schema discovery and the backend
            cannot be reached.
        RemoteQueryError
            If remote schema discovery fails.

        Notes
        -----
        Registering a name again replaces the prior prepared dataset. Built-in
        Minghu tables use an offline catalog; custom ClickHouse tables may run
        ``DESCRIBE TABLE`` during registration.
        """

        self._validate_spec(spec)
        backend = self._backends.get(spec.backend)
        if backend is None:
            raise DatasetRegistrationError(f"Unsupported backend: {spec.backend!r}")
        self._datasets[spec.name] = backend.prepare(spec)

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
            Optional built-in stock-pool name: one of ``"hs300"``, ``"sz50"``,
            ``"zz500"``, and ``"zz1000"``. Names are case-insensitive and
            surrounding whitespace is ignored. This parameter is mutually
            exclusive with ``instruments``.

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
            If parameters are invalid or the dataset is event-shaped and
            cannot be pivoted.
        DuplicateObservationError
            If an ordinary panel contains duplicate time/instrument pairs.
        SchemaMismatchError
            If result key columns are missing or contain nulls.
        AuditWriteError
            If the required audit record cannot be persisted.

        Notes
        -----
        Query bounds are closed. Requested instruments without observations
        remain as all-missing columns. Built-in universes are fixed snapshots,
        not historical point-in-time membership. Disclosed Tushare datasets
        always align announcements to a trading calendar, apply their
        availability lag, and carry whole-row point-in-time state.
        """

        result = self._execute(
            "panel",
            dataset,
            fields,
            start,
            end,
            instruments,
            limit=None,
            adjusted=adjusted,
            universe=universe,
        )
        return cast(dict[str, pd.DataFrame], result)

    def get_table(
        self,
        dataset: str,
        fields: Sequence[str],
        start: Any | None = None,
        end: Any | None = None,
        instruments: Sequence[str] | None = None,
        limit: int | None = None,
        adjusted: bool | None = None,
    ) -> pa.Table:
        """Query fields as a normalized Arrow long table.

        Parameters
        ----------
        dataset
            Registered dataset name.
        fields
            Non-key columns to return. Time and instrument keys are added
            automatically.
        start, end
            Optional inclusive time bounds.
        instruments
            Optional instrument filter. ``None`` requests all instruments and
            an empty sequence returns a typed empty table.
        limit
            Optional positive maximum number of rows.
        adjusted
            ``True`` forces configured price adjustment, ``False`` requests
            raw values, and ``None`` uses the dataset default.

        Returns
        -------
        pyarrow.Table
            Long table with time and instrument keys first. Query identifiers,
            dataset name, parameters, and adjustment state are stored in the
            Arrow schema metadata.

        Raises
        ------
        DatasetNotFoundError
            If ``dataset`` has not been registered.
        FieldNotFoundError
            If a requested field is absent from the registered schema.
        InvalidQueryError
            If fields, bounds, instruments, adjustment, or limit are invalid.
        SchemaMismatchError
            If the backend result is inconsistent with the registered schema.
        AuditWriteError
            If the required audit record cannot be persisted.

        Notes
        -----
        Event and revision tables may contain repeated time/instrument pairs
        and should be queried with this method. Tushare disclosure tables are
        filtered by report period and retain all announcement/revision rows;
        membership tables retain their effective-dated intervals.
        """

        result = self._execute(
            "table",
            dataset,
            fields,
            start,
            end,
            instruments,
            limit,
            adjusted,
            universe=None,
        )
        return cast(pa.Table, result)

    def close(self) -> None:
        """Close cached backend clients and release their resources.

        Notes
        -----
        The built-in Parquet backend has no persistent connection. Calling
        this method more than once is safe for the built-in backends.
        """

        for backend in self._backends.values():
            backend.close()

    def __enter__(self) -> DataClient:
        """Return this client when entering a context manager."""

        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        """Close backend resources when leaving a context manager."""

        self.close()

    def _execute(
        self,
        mode: QueryMode,
        dataset: str,
        fields: Sequence[str],
        start: Any | None,
        end: Any | None,
        instruments: Sequence[str] | None,
        limit: int | None,
        adjusted: bool | None,
        universe: str | None,
    ) -> dict[str, pd.DataFrame] | pa.Table:
        query_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        started_clock = time.perf_counter()
        audited_instruments: list[str] | str | None = (
            instruments
            if isinstance(instruments, str)
            else list(instruments)
            if instruments is not None
            else None
        )
        record = QueryAudit(
            query_id=query_id,
            dataset=dataset,
            fields=list(fields),
            parameters={
                "start": self._audit_value(start),
                "end": self._audit_value(end),
                "instruments": audited_instruments,
                "universe": (
                    universe if isinstance(universe, (str, type(None))) else repr(universe)
                ),
                "limit": limit,
                "adjusted": adjusted,
            },
            started_at=started_at.isoformat(),
            framework_version=__version__,
            operation=mode,
        )

        try:
            registered = self._datasets.get(dataset)
            if registered is None:
                raise DatasetNotFoundError(f"Dataset {dataset!r} is not registered")
            spec = registered.spec
            contract = registered.contract
            backend = self._backends[spec.backend]
            record.frequency = (
                contract.panel_frequency if mode == "panel" else contract.table_frequency
            )
            record.dataset_version = contract.version
            record.source = backend.fingerprint(registered)

            if mode == "panel" and not contract.panel_compatible:
                raise InvalidQueryError(
                    f"Dataset {dataset!r} is event data and cannot be pivoted; use get_table"
                )
            resolved_instruments = instruments
            if mode == "panel" and universe is not None:
                if instruments is not None:
                    raise InvalidQueryError("instruments and universe are mutually exclusive")
                snapshot = load_universe(universe)
                resolved_instruments = snapshot.instruments
                record.parameters["instruments"] = list(snapshot.instruments)
                record.parameters["universe"] = {
                    "name": snapshot.name,
                    "snapshot_date": snapshot.snapshot_date.isoformat(),
                    "count": len(snapshot.instruments),
                    "sha256": snapshot.sha256,
                }
            query = self._prepare_query(
                mode,
                registered,
                fields,
                start,
                end,
                resolved_instruments,
                limit,
            )
            semantic_backend = (
                cast(TushareSemanticBackend, backend)
                if isinstance(spec, (TushareDatasetSpec, TushareParquetDatasetSpec))
                else None
            )
            if isinstance(spec, TushareParquetDatasetSpec):
                if semantic_backend is None:
                    raise SchemaMismatchError("Tushare Parquet semantic backend is unavailable")
                query = semantic_backend.normalize_snapshot_query(registered, query, mode)
                record.parameters["effective_start"] = self._audit_value(query.start)
                record.parameters["effective_end"] = self._audit_value(query.end)
            if isinstance(spec, TushareDatasetSpec):
                data_api = self._tushare.route_name(registered, query)
                record.parameters["data_api"] = data_api
                record.source["selected_api"] = data_api
                calendar_api = record.source.get("calendar_api")
                if isinstance(calendar_api, str):
                    record.parameters["calendar_api"] = calendar_api
            apply_adjustment = self._resolve_adjustment(registered, adjusted)
            record.adjusted = apply_adjustment
            record.parameters["adjusted"] = apply_adjustment
            scan_query = self._with_adjustment_factor(registered, query, apply_adjustment)
            tushare_panel_kind = (
                semantic_backend.panel_kind(registered)
                if mode == "panel" and semantic_backend is not None
                else None
            )
            if tushare_panel_kind == "disclosure":
                if semantic_backend is None:
                    raise SchemaMismatchError("Disclosure panel backend is unavailable")
                result = self._build_tushare_disclosure_panels(
                    dataset,
                    registered,
                    scan_query,
                    record,
                    semantic_backend,
                )
            elif tushare_panel_kind == "membership":
                if semantic_backend is None:
                    raise SchemaMismatchError("Membership panel backend is unavailable")
                table = semantic_backend.scan_membership_panel(registered, scan_query)
                record.calendar_aligned = True
                record.parameters["calendar_api"] = "trade_cal"
                record.source["calendar_api"] = "trade_cal"
                result = self._build_panels(
                    table,
                    dataset,
                    registered,
                    query,
                    record,
                    apply_adjustment=False,
                )
            else:
                if scan_query.instruments == ():
                    table = self._empty_table(registered, scan_query.fields, mode)
                else:
                    table = backend.scan(registered, scan_query)
                time_column, instrument_column = self._mode_keys(registered, mode)
                self._validate_table_keys(
                    table,
                    registered,
                    time_column=time_column,
                    instrument_column=instrument_column,
                )
                if apply_adjustment:
                    table = self._adjust_prices(table, registered)

                if mode == "table":
                    table = table.select(self._table_columns(registered, query.fields))
                    result = self._attach_table_metadata(
                        table, query_id, dataset, record.parameters
                    )
                    record.result_shapes = {"table": [result.num_rows, result.num_columns]}
                else:
                    table = table.select([time_column, instrument_column, *query.fields])
                    result = self._build_panels(
                        table,
                        dataset,
                        registered,
                        query,
                        record,
                        apply_adjustment,
                    )
            record.status = "success"
        except Exception as exc:
            record.status = "failed"
            record.error = {"type": type(exc).__name__, "message": str(exc)}
            record.duration_ms = (time.perf_counter() - started_clock) * 1000
            self._audit.write(record)
            raise

        record.duration_ms = (time.perf_counter() - started_clock) * 1000
        self._audit.write(record)
        return result

    def _build_tushare_disclosure_panels(
        self,
        dataset_name: str,
        dataset: RegisteredDataset,
        query: DataQuery,
        record: QueryAudit,
        backend: TushareSemanticBackend,
    ) -> dict[str, pd.DataFrame]:
        spec = dataset.spec
        if not isinstance(spec, (TushareDatasetSpec, TushareParquetDatasetSpec)):
            raise SchemaMismatchError("PIT panels require a Tushare dataset")
        if query.start is None or query.end is None:
            raise InvalidQueryError(
                f"Dataset {dataset_name!r} PIT panel requires both start and end"
            )

        table = backend.scan_disclosure_events(dataset, query)
        contract = dataset.contract
        self._validate_table_keys(
            table,
            dataset,
            time_column=contract.table_time_column,
            instrument_column=contract.instrument_column,
        )
        disclosure_column, period_column, revision_order = backend.pit_panel_semantics(dataset)
        calendar = backend.trade_calendar(dataset, query)
        panels = build_daily_panels(
            table,
            dataset_name=dataset_name,
            disclosure_column=disclosure_column,
            instrument_column=contract.instrument_column,
            period_column=period_column,
            fields=query.fields,
            instruments=query.instruments,
            calendar=calendar,
            panel_start=pd.Timestamp(query.start.date()),
            panel_end=pd.Timestamp(query.end.date()),
            disclosure_lag=spec.disclosure_lag,
            revision_order=revision_order,
            index_name=contract.panel_time_column or "trade_date",
        )
        record.calendar_aligned = True
        record.parameters["disclosure_lag"] = spec.disclosure_lag
        record.parameters["calendar_api"] = "trade_cal"
        record.source["calendar_api"] = "trade_cal"
        self._finish_panels(panels, dataset_name, dataset, record, apply_adjustment=False)
        return panels

    def _build_panels(
        self,
        table: pa.Table,
        dataset_name: str,
        dataset: RegisteredDataset,
        query: DataQuery,
        record: QueryAudit,
        apply_adjustment: bool,
    ) -> dict[str, pd.DataFrame]:
        time_column, instrument_column = self._mode_keys(dataset, "panel")
        self._validate_table_keys(
            table,
            dataset,
            time_column=time_column,
            instrument_column=instrument_column,
        )
        panels = build_panels(
            table,
            dataset_name=dataset_name,
            time_column=time_column,
            instrument_column=instrument_column,
            fields=query.fields,
            instruments=query.instruments,
        )
        self._finish_panels(panels, dataset_name, dataset, record, apply_adjustment)
        return panels

    @staticmethod
    def _finish_panels(
        panels: dict[str, pd.DataFrame],
        dataset_name: str,
        dataset: RegisteredDataset,
        record: QueryAudit,
        apply_adjustment: bool,
    ) -> None:
        attrs = {
            "query_id": record.query_id,
            "dataset": dataset_name,
            "frequency": dataset.contract.panel_frequency,
            "version": dataset.contract.version,
            "parameters": record.parameters,
            "adjusted": apply_adjustment,
            "calendar_aligned": record.calendar_aligned,
        }
        for panel in panels.values():
            panel.attrs.update(attrs)
        record.result_shapes = {
            field: [int(panel.shape[0]), int(panel.shape[1])] for field, panel in panels.items()
        }

    @staticmethod
    def _resolve_adjustment(dataset: RegisteredDataset, adjusted: bool | None) -> bool:
        if adjusted is not None and not isinstance(adjusted, bool):
            raise InvalidQueryError("adjusted must be True, False, or None")
        if adjusted is None:
            return dataset.adjustment.default if dataset.adjustment else False
        if adjusted and dataset.adjustment is None:
            raise InvalidQueryError(
                f"Dataset {dataset.spec.name!r} does not define a price adjustment factor"
            )
        return adjusted

    @staticmethod
    def _with_adjustment_factor(
        dataset: RegisteredDataset,
        query: DataQuery,
        apply_adjustment: bool,
    ) -> DataQuery:
        adjustment = dataset.adjustment
        if not apply_adjustment or adjustment is None:
            return query
        if not set(query.fields).intersection(adjustment.fields):
            return query
        if adjustment.factor_column in query.fields:
            return query
        return replace(query, fields=(*query.fields, adjustment.factor_column))

    @staticmethod
    def _adjust_prices(table: pa.Table, dataset: RegisteredDataset) -> pa.Table:
        adjustment = dataset.adjustment
        if adjustment is None or adjustment.factor_column not in table.column_names:
            return table
        factor = table[adjustment.factor_column]
        for field in adjustment.fields:
            if field not in table.column_names:
                continue
            index = table.schema.get_field_index(field)
            adjusted_values = pc.multiply(table[field], factor)
            table = table.set_column(index, field, adjusted_values)
        return table

    @staticmethod
    def _returns_suffixed_clickhouse_codes(dataset: RegisteredDataset) -> bool:
        spec = dataset.spec
        return (
            isinstance(spec, ClickHouseDatasetSpec)
            and dataset.contract.instrument_column == "code"
            and "exg" in dataset.schema.names
        )

    @staticmethod
    def _is_suffixed_instrument(value: str) -> bool:
        return value.endswith(_MINGHU_CODE_SUFFIXES)

    @staticmethod
    def _validate_spec(spec: DatasetDefinition) -> None:
        if not spec.name.strip():
            raise DatasetRegistrationError("Dataset name cannot be empty")
        if isinstance(spec, (DatasetSpec, ClickHouseDatasetSpec)):
            if not spec.time_column or not spec.instrument_column:
                raise DatasetRegistrationError("Key column names cannot be empty")
            if spec.time_column == spec.instrument_column:
                raise DatasetRegistrationError("Time and instrument columns must be different")
        if isinstance(spec, DatasetSpec):
            if not spec.paths:
                raise DatasetRegistrationError("Dataset paths cannot be empty")
        if isinstance(spec, (TushareDatasetSpec, TushareParquetDatasetSpec)):
            if spec.dataset is not None and not spec.dataset.strip():
                raise DatasetRegistrationError("Tushare dataset cannot be empty")
            if isinstance(spec, TushareParquetDatasetSpec):
                if not str(spec.data_dir).strip():
                    raise DatasetRegistrationError("Tushare Parquet data_dir cannot be empty")
                if not spec.calendar_connection.strip():
                    raise DatasetRegistrationError("Tushare calendar_connection cannot be empty")
            if not spec.calendar_exchange.strip():
                raise DatasetRegistrationError("Tushare calendar_exchange cannot be empty")
            if (
                isinstance(spec.disclosure_lag, bool)
                or not isinstance(spec.disclosure_lag, int)
                or spec.disclosure_lag < 0
            ):
                raise DatasetRegistrationError("disclosure_lag must be non-negative")
            if (
                isinstance(spec.fetch_buffer_days, bool)
                or not isinstance(spec.fetch_buffer_days, int)
                or spec.fetch_buffer_days < 0
            ):
                raise DatasetRegistrationError("fetch_buffer_days must be non-negative")
            if (
                isinstance(spec.fetch_margin_days, bool)
                or not isinstance(spec.fetch_margin_days, int)
                or spec.fetch_margin_days < 0
            ):
                raise DatasetRegistrationError("fetch_margin_days must be non-negative")
        if spec.timezone:
            try:
                ZoneInfo(spec.timezone)
            except Exception as exc:
                raise DatasetRegistrationError(f"Invalid timezone: {spec.timezone!r}") from exc

    @staticmethod
    def _prepare_query(
        mode: QueryMode,
        dataset: RegisteredDataset,
        fields: Sequence[str],
        start: Any | None,
        end: Any | None,
        instruments: Sequence[str] | None,
        limit: int | None,
    ) -> DataQuery:
        requested_fields = tuple(fields)
        if not requested_fields:
            raise InvalidQueryError("At least one field is required")
        if not all(isinstance(field, str) and field for field in requested_fields):
            raise InvalidQueryError("Field names must be non-empty strings")
        if len(set(requested_fields)) != len(requested_fields):
            raise InvalidQueryError("Fields cannot contain duplicates")
        time_column, instrument_column = DataClient._mode_keys(dataset, mode)
        keys = {time_column, instrument_column}
        invalid_keys = keys.intersection(requested_fields)
        if invalid_keys:
            raise InvalidQueryError(f"Key columns cannot be requested as fields: {invalid_keys}")
        missing = set(requested_fields).difference(dataset.schema.names)
        if missing:
            raise FieldNotFoundError(f"Fields not found in dataset: {sorted(missing)}")

        query_timezone = (
            None if isinstance(dataset.spec, DatasetSpec) else dataset.contract.timezone
        )
        parsed_start = DataClient._parse_time(start, "start", query_timezone)
        parsed_end = DataClient._parse_time(end, "end", query_timezone)
        if parsed_start is not None and parsed_end is not None and parsed_start > parsed_end:
            raise InvalidQueryError("start must be earlier than or equal to end")
        requires_range = (
            dataset.contract.panel_requires_time_range
            if mode == "panel"
            else dataset.contract.table_requires_time_range
        )
        if requires_range and (parsed_start is None or parsed_end is None):
            raise InvalidQueryError(
                f"Dataset {dataset.spec.name!r} {mode} query requires both start and end"
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
            if DataClient._returns_suffixed_clickhouse_codes(dataset):
                missing_suffix = [
                    item
                    for item in requested_instruments
                    if not DataClient._is_suffixed_instrument(item)
                ]
                if missing_suffix:
                    raise InvalidQueryError(
                        f"Dataset {dataset.spec.name!r} requires instrument identifiers "
                        "with exchange suffixes such as '000001.SZ'"
                    )
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
        ):
            raise InvalidQueryError("limit must be a positive integer")
        return DataQuery(requested_fields, parsed_start, parsed_end, requested_instruments, limit)

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
    def _empty_table(
        dataset: RegisteredDataset,
        fields: tuple[str, ...],
        mode: QueryMode,
    ) -> pa.Table:
        time_column, instrument_column = DataClient._mode_keys(dataset, mode)
        columns = (
            DataClient._table_columns(dataset, fields)
            if mode == "table"
            else (time_column, instrument_column, *fields)
        )
        arrays: dict[str, pa.Array] = {}
        for column in columns:
            data_type = (
                dataset.schema.field(column).type if column in dataset.schema.names else pa.date32()
            )
            arrays[column] = pa.array([], type=data_type)
        return pa.table(arrays)

    @staticmethod
    def _validate_table_keys(
        table: pa.Table,
        dataset: RegisteredDataset,
        *,
        time_column: str,
        instrument_column: str,
    ) -> None:
        for column in (time_column, instrument_column):
            if column not in table.column_names:
                raise SchemaMismatchError(f"Query result is missing key column {column!r}")
            if pc.any(pc.is_null(table[column])).as_py():
                raise SchemaMismatchError(
                    f"Dataset {dataset.spec.name!r} contains null values in key column {column!r}"
                )

    @staticmethod
    def _mode_keys(dataset: RegisteredDataset, mode: QueryMode) -> tuple[str, str]:
        contract = dataset.contract
        time_column = contract.panel_time_column if mode == "panel" else contract.table_time_column
        if time_column is None:
            raise InvalidQueryError(f"Dataset {dataset.spec.name!r} does not support panel queries")
        return time_column, contract.instrument_column

    @staticmethod
    def _table_columns(dataset: RegisteredDataset, fields: tuple[str, ...]) -> tuple[str, ...]:
        contract = dataset.contract
        result: list[str] = []
        for column in (
            contract.table_time_column,
            contract.instrument_column,
            *contract.table_identity_columns,
            *fields,
        ):
            if column not in result:
                result.append(column)
        return tuple(result)

    @staticmethod
    def _attach_table_metadata(
        table: pa.Table,
        query_id: str,
        dataset: str,
        parameters: dict[str, Any],
    ) -> pa.Table:
        metadata = dict(table.schema.metadata or {})
        metadata.update(
            {
                b"quant_data.query_id": query_id.encode(),
                b"quant_data.dataset": dataset.encode(),
                b"quant_data.parameters": str(parameters).encode(),
                b"quant_data.adjusted": str(bool(parameters.get("adjusted"))).lower().encode(),
            }
        )
        return table.replace_schema_metadata(metadata)

    @staticmethod
    def _audit_value(value: Any | None) -> str | None:
        if value is None:
            return None
        try:
            return str(pd.Timestamp(value).isoformat())
        except (TypeError, ValueError):
            return repr(value)

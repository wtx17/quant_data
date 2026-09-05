"""Tushare backend implemented with the Tushare Pro API."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Callable, Mapping, cast

import pandas as pd
import pyarrow as pa

from ..exceptions import (
    BackendConnectionError,
    DatasetRegistrationError,
    InvalidQueryError,
    RemoteQueryError,
    SchemaMismatchError,
)
from ..models import (
    DataQuery,
    DatasetContract,
    DatasetDefinition,
    RegisteredDataset,
    TushareConfig,
    TushareDatasetSpec,
    TushareParquetDatasetSpec,
)
from .tushare_catalog import (
    DateRangeQuery,
    DisclosureSemantics,
    MembershipQuery,
    MembershipSemantics,
    TradeDateQuery,
    TushareApiRoute,
    TUSHARE_DATASETS,
    TushareDatasetCatalog,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class TushareSource:
    """Store prepared Tushare source metadata.

    Parameters
    ----------
    connection
        Named Tushare connection profile.
    dataset
        Logical catalog dataset name.
    schema_hash
        Stable hash of the catalog schema.
    fixed_params
        Sanitized constant API parameters from the dataset specification.
    """

    connection: str
    dataset: str
    schema_hash: str
    fixed_params: Mapping[str, object]


class TushareBackend:
    """Query catalog-backed Tushare APIs and normalize them to Arrow.

    Parameters
    ----------
    client_factory
        Optional callable receiving ``token=...`` and returning a Tushare-like
        client. It supports deterministic tests without the Tushare package or
        network access.

    Notes
    -----
    The catalog defines schemas, deterministic API routes, table identity
    columns, panel compatibility, disclosure state rules, and membership
    intervals.
    Trading calendars are cached per connection, exchange, year, and month.
    """

    def __init__(self, client_factory: Callable[..., Any] | None = None) -> None:
        self._configs: dict[str, TushareConfig] = {}
        self._clients: dict[str, Any] = {}
        self._client_factory = client_factory
        self._calendar_cache: dict[tuple[str, str, int, int], list[date]] = {}

    def add_connection(self, name: str, config: TushareConfig) -> None:
        """Add or replace a validated Tushare connection profile.

        Parameters
        ----------
        name
            Identifier used by dataset specifications.
        config
            Direct token or token-environment configuration.

        Raises
        ------
        DatasetRegistrationError
            If the name or token configuration is invalid.

        Notes
        -----
        The token environment variable is not read here. Replacing an
        initialized profile closes its cached client.
        """

        if not name or not _IDENTIFIER.fullmatch(name):
            raise DatasetRegistrationError(f"Invalid Tushare connection name: {name!r}")
        if config.token is not None and not config.token:
            raise DatasetRegistrationError("Tushare token cannot be empty")
        if config.token_env is not None and not config.token_env:
            raise DatasetRegistrationError("Tushare token environment variable cannot be empty")
        if config.token is None and config.token_env is None:
            raise DatasetRegistrationError("Tushare token or token_env must be configured")
        existing = self._clients.pop(name, None)
        if existing is not None:
            self._close_client(existing)
        self._configs[name] = config

    def has_connection(self, name: str) -> bool:
        """Return whether a named Tushare profile has been configured."""

        return name in self._configs

    def fetch_calendar(
        self,
        connection: str,
        exchange: str,
        start: datetime,
        end: datetime,
    ) -> list[date]:
        """Fetch and cache open trading sessions for another backend.

        Parameters
        ----------
        connection
            Configured Tushare connection profile.
        exchange
            Exchange code forwarded to ``trade_cal``.
        start, end
            Inclusive calendar bounds.

        Returns
        -------
        list[datetime.date]
            Sorted open sessions within the requested bounds.
        """

        if connection not in self._configs:
            raise DatasetRegistrationError(f"Tushare connection {connection!r} is not configured")
        return self._fetch_calendar(connection, exchange, start, end)

    def normalize_snapshot_query(
        self,
        dataset: RegisteredDataset,
        query: DataQuery,
    ) -> DataQuery:
        """Return a remote query unchanged.

        Remote Tushare datasets are not bounded by a local archive. This
        method completes the shared semantic-backend protocol used by the
        client and the local Parquet implementation.
        """

        del dataset
        return query

    def prepare(self, definition: DatasetDefinition) -> RegisteredDataset:
        """Prepare a logical catalog-backed Tushare dataset without connecting.

        Parameters
        ----------
        definition
            Tushare dataset specification referencing a configured profile and
            logical catalog name.

        Returns
        -------
        RegisteredDataset
            Normalized specification, catalog schema, and source metadata.

        Raises
        ------
        DatasetRegistrationError
            If the definition, catalog name, profile, columns, or fixed parameters
            are invalid.

        Notes
        -----
        Preparation is fully offline. Tokens and clients are resolved lazily on
        the first query.
        """

        if not isinstance(definition, TushareDatasetSpec):
            raise DatasetRegistrationError("Tushare backend requires TushareDatasetSpec")
        logical_name = definition.dataset or definition.name
        catalog = self._catalog(logical_name)
        self._validate_definition(definition, catalog)
        if definition.connection not in self._configs:
            raise DatasetRegistrationError(
                f"Tushare connection {definition.connection!r} is not configured"
            )
        normalized = json.dumps(
            [(field.name, str(field.type)) for field in catalog.schema],
            separators=(",", ":"),
        )
        source = TushareSource(
            definition.connection,
            catalog.name,
            hashlib.sha256(normalized.encode()).hexdigest(),
            dict(definition.fixed_params),
        )
        return RegisteredDataset(
            spec=definition,
            schema=catalog.schema,
            source=source,
            contract=self._contract(definition, catalog),
        )

    def scan(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Fetch a normalized, lossless Tushare long table.

        Parameters
        ----------
        dataset
            Prepared Tushare dataset.
        query
            Normalized fields, closed time range, stock universe.

        Returns
        -------
        pyarrow.Table
            Typed, ordered long table retaining every returned source row.

        Raises
        ------
        InvalidQueryError
            If the API requires instruments or time bounds that are missing.
        RemoteQueryError
            If a Tushare or calendar call fails or returns an invalid object.
        SchemaMismatchError
            If returned columns, values, or prepared state conflict with the
            catalog.

        Notes
        -----
        Observation datasets use this scan. Disclosure and membership panels
        use their dedicated semantic scans.
        """

        spec, source, catalog = self._state(dataset)
        selected = (
            dataset.contract.panel_time_column,
            dataset.contract.instrument_column,
            *query.fields,
        )
        if query.instruments == ():
            return self._empty_arrow(catalog.schema, selected)
        route = self._select_route(catalog, query.instruments)
        client = self._client(source.connection)
        remote_fields = self._remote_columns(selected, catalog)
        trade_dates: tuple[date, ...] | None = None
        if isinstance(route.query_shape, TradeDateQuery):
            if query.start is None or query.end is None:
                raise InvalidQueryError(f"Dataset {spec.name!r} requires both start and end")
            trade_dates = tuple(
                self.fetch_calendar(
                    source.connection,
                    spec.calendar_exchange,
                    query.start,
                    query.end,
                )
            )
        frames = self._fetch_route_frames(
            client,
            source.fixed_params,
            route,
            query,
            remote_fields,
            trade_dates=trade_dates,
        )
        frame = self._normalize_remote_frames(frames, catalog, remote_fields, route)
        if isinstance(route.query_shape, TradeDateQuery):
            frame = self._filter_instruments(
                frame,
                catalog.instrument_column,
                query.instruments,
            )
        semantics = catalog.semantics
        frame = self._filter_time(frame, dataset.contract.panel_time_column, query)
        frame = self._sort_by(frame, semantics.source_order)
        return self._frame_to_arrow(frame, catalog.schema, selected)

    def scan_disclosure_events(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Fetch disclosure events required by a point-in-time panel.

        Parameters
        ----------
        dataset
            Prepared logical disclosure dataset.
        query
            Panel request with both time bounds.

        Returns
        -------
        pyarrow.Table
            Disclosure, instrument, period, and requested field columns ordered
            by disclosure chronology.

        Raises
        ------
        InvalidQueryError
            If bounds, instruments, or disclosure-range parameters are not
            available for the selected API.
        RemoteQueryError
            If a remote request fails.
        SchemaMismatchError
            If the response conflicts with the catalog schema.

        Notes
        -----
        The fetch starts ``fetch_buffer_days`` before the requested panel to
        carry previously disclosed values into its left boundary. All revisions
        are retained for the point-in-time state machine.
        """

        spec, source, catalog = self._state(dataset)
        if query.start is None or query.end is None:
            raise InvalidQueryError(
                f"Dataset {spec.name!r} point-in-time panel requires both start and end"
            )
        semantics = catalog.semantics
        if not isinstance(semantics, DisclosureSemantics):
            raise InvalidQueryError(f"Tushare dataset {catalog.name!r} is not disclosure data")
        route = self._select_route(catalog, query.instruments)
        if not isinstance(route.query_shape, DateRangeQuery):
            raise InvalidQueryError(
                f"Tushare api {route.api_name!r} cannot serve a point-in-time panel"
            )
        client = self._client(source.connection)
        selected = self._unique_columns(
            (
                semantics.disclosure_column,
                catalog.instrument_column,
                semantics.period_column,
                *semantics.identity_columns,
                *query.fields,
            )
        )
        remote_fields = self._remote_columns(selected, catalog)
        fetch_start = query.start - timedelta(days=spec.fetch_buffer_days)
        fetch_query = DataQuery(
            query.fields,
            fetch_start,
            query.end,
            query.instruments,
        )
        frames = self._fetch_disclosure_route_frames(
            client, source.fixed_params, route, fetch_query, remote_fields
        )
        frame = self._normalize_remote_frames(frames, catalog, remote_fields, route)
        frame = self._filter_time(frame, semantics.disclosure_column, fetch_query)
        frame = self._sort_by(
            frame,
            self._unique_columns(
                (
                    semantics.disclosure_column,
                    catalog.instrument_column,
                    semantics.period_column,
                    *semantics.revision_order,
                )
            ),
        )
        return self._frame_to_arrow(frame, catalog.schema, selected)

    def trade_calendar(self, dataset: RegisteredDataset, query: DataQuery) -> list[date]:
        """Return the buffered trading calendar for a PIT panel.

        Parameters
        ----------
        dataset
            Prepared Tushare dataset.
        query
            Panel query with both time bounds.

        Returns
        -------
        list[datetime.date]
            Open sessions from the buffered start through the margin-adjusted
            end date.

        Raises
        ------
        InvalidQueryError
            If either time bound is missing.
        RemoteQueryError
            If ``trade_cal`` fails or omits its date column.
        """

        spec, source, _ = self._state(dataset)
        if query.start is None or query.end is None:
            raise InvalidQueryError(f"Dataset {spec.name!r} panel requires both start and end")
        start = query.start - timedelta(days=spec.fetch_buffer_days)
        end = query.end + timedelta(days=spec.fetch_margin_days)
        return self._fetch_calendar(source.connection, spec.calendar_exchange, start, end)

    def pit_panel_semantics(self, dataset: RegisteredDataset) -> tuple[str, str, tuple[str, ...]]:
        """Return disclosure, report-period, and revision precedence columns.

        Parameters
        ----------
        dataset
            Prepared Tushare dataset.

        Returns
        -------
        tuple[str, str, tuple[str, ...]]
            Disclosure column, period column, and ordered revision columns.
        """

        _, _, catalog = self._state(dataset)
        semantics = catalog.semantics
        if not isinstance(semantics, DisclosureSemantics):
            raise SchemaMismatchError(f"Tushare dataset {catalog.name!r} is not disclosure data")
        return (
            semantics.disclosure_column,
            semantics.period_column,
            semantics.revision_order,
        )

    def fingerprint(self, dataset: RegisteredDataset) -> dict[str, object]:
        """Return sanitized API and schema provenance.

        Parameters
        ----------
        dataset
            Prepared Tushare dataset.

        Returns
        -------
        dict[str, object]
            JSON-serializable connection name, API, schema hash, and stringified
            fixed parameters. Tokens are excluded.
        """

        _, source, catalog = self._state(dataset)
        result: dict[str, object] = {
            "backend": "tushare",
            "connection": source.connection,
            "dataset": source.dataset,
            "available_apis": [route.api_name for route in catalog.routes],
            "schema_hash": source.schema_hash,
            "fixed_params": {str(key): str(value) for key, value in source.fixed_params.items()},
        }
        if any(isinstance(route.query_shape, TradeDateQuery) for route in catalog.routes):
            result["calendar_api"] = "trade_cal"
        return result

    def route_name(self, dataset: RegisteredDataset, query: DataQuery) -> str | None:
        """Return the deterministic data API selected for audit metadata."""

        _, _, catalog = self._state(dataset)
        if query.instruments == ():
            return None
        return self._select_route(catalog, query.instruments).api_name

    def panel_kind(self, dataset: RegisteredDataset) -> str:
        """Return the catalog's panel construction kind."""

        _, _, catalog = self._state(dataset)
        if isinstance(catalog.semantics, DisclosureSemantics):
            return "disclosure"
        if isinstance(catalog.semantics, MembershipSemantics):
            return "membership"
        return "observation"

    def scan_membership_panel(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Expand raw membership intervals over the requested trading calendar."""

        spec, source, catalog = self._state(dataset)
        semantics = catalog.semantics
        if not isinstance(semantics, MembershipSemantics):
            raise SchemaMismatchError(f"Tushare dataset {catalog.name!r} is not membership data")
        if query.start is None or query.end is None:
            raise InvalidQueryError(
                f"Dataset {spec.name!r} membership panel requires both start and end"
            )
        selected_raw = self._unique_columns(
            (
                semantics.interval_start_column,
                catalog.instrument_column,
                semantics.interval_end_column,
                *semantics.identity_columns,
                *query.fields,
            )
        )
        route = self._select_route(catalog, query.instruments)
        remote_fields = self._remote_columns(selected_raw, catalog)
        if query.instruments == ():
            frames: list[pd.DataFrame] = []
        else:
            frames = self._fetch_route_frames(
                self._client(source.connection),
                source.fixed_params,
                route,
                query,
                remote_fields,
            )
        raw = self._normalize_remote_frames(frames, catalog, remote_fields, route)
        raw = self._filter_membership_overlap(raw, semantics, query)
        calendar = self._fetch_calendar(
            source.connection,
            spec.calendar_exchange,
            query.start,
            query.end,
        )
        selected_panel = self._unique_columns(
            (
                semantics.panel_time_column,
                catalog.instrument_column,
                *query.fields,
            )
        )
        expanded = self._expand_membership_panel(
            raw,
            semantics,
            catalog.instrument_column,
            query,
            calendar,
            selected_panel,
        )
        return self._membership_frame_to_arrow(
            expanded,
            catalog.schema,
            semantics.panel_time_column,
            selected_panel,
        )

    def close(self) -> None:
        """Close cached Tushare clients and clear calendar entries."""

        for client in self._clients.values():
            self._close_client(client)
        self._clients.clear()
        self._calendar_cache.clear()

    def _client(self, name: str) -> Any:
        existing = self._clients.get(name)
        if existing is not None:
            return existing
        config = self._configs.get(name)
        if config is None:
            raise DatasetRegistrationError(f"Tushare connection {name!r} is not configured")
        token = config.token
        if token is None and config.token_env:
            token = os.environ.get(config.token_env)
            if token is None:
                raise BackendConnectionError(
                    f"Tushare token environment variable {config.token_env!r} is not set"
                )
        if token is None:
            raise BackendConnectionError("Tushare token is not configured")
        factory = self._client_factory
        if factory is None:
            try:
                import tushare as ts
            except ImportError as exc:
                raise BackendConnectionError(
                    "Tushare support is not installed; install the tushare package"
                ) from exc
            try:
                ts_module = cast(Any, ts)
                ts_module.set_token(token)
                client = ts_module.pro_api()
                client._DataApi__http_url = "https://tx.xiaodefa.top/"
            except Exception as exc:
                raise BackendConnectionError(f"Unable to initialize Tushare client: {exc}") from exc
        else:
            try:
                client = factory(token=token)
            except Exception as exc:
                raise BackendConnectionError(f"Unable to initialize Tushare client: {exc}") from exc
        self._clients[name] = client
        return client

    @staticmethod
    def _catalog(dataset_name: str) -> TushareDatasetCatalog:
        catalog = TUSHARE_DATASETS.get(dataset_name)
        if catalog is None:
            supported = ", ".join(sorted(TUSHARE_DATASETS))
            raise DatasetRegistrationError(
                f"Unsupported Tushare dataset {dataset_name!r}; supported datasets: {supported}"
            )
        return catalog

    @staticmethod
    def _contract(
        definition: TushareDatasetSpec | TushareParquetDatasetSpec,
        catalog: TushareDatasetCatalog,
    ) -> DatasetContract:
        semantics = catalog.semantics
        if isinstance(semantics, DisclosureSemantics):
            return DatasetContract(
                source_time_column=semantics.period_column,
                instrument_column=catalog.instrument_column,
                panel_time_column=semantics.panel_time_column,
                panel_frequency=semantics.panel_frequency,
                timezone=definition.timezone,
                version=definition.version,
                panel_requires_time_range=True,
            )
        return DatasetContract(
            source_time_column=semantics.source_time_column,
            instrument_column=catalog.instrument_column,
            panel_time_column=semantics.panel_time_column,
            panel_frequency=semantics.panel_frequency,
            timezone=definition.timezone,
            version=definition.version,
            panel_requires_time_range=True,
        )

    @staticmethod
    def _validate_definition(
        definition: TushareDatasetSpec | TushareParquetDatasetSpec,
        catalog: TushareDatasetCatalog,
    ) -> None:
        if not isinstance(definition.fixed_params, Mapping):
            raise DatasetRegistrationError("Tushare fixed_params must be a mapping")
        invalid_param_keys = [
            key for key in definition.fixed_params if not isinstance(key, str) or not key
        ]
        if invalid_param_keys:
            raise DatasetRegistrationError("Tushare fixed_params keys must be non-empty strings")
        schema_names = set(catalog.schema.names)
        semantics = catalog.semantics
        required = {catalog.instrument_column}
        if isinstance(semantics, DisclosureSemantics):
            required.update(
                {
                    semantics.period_column,
                    semantics.disclosure_column,
                    *semantics.identity_columns,
                    *semantics.revision_order,
                    *semantics.source_order,
                }
            )
        elif isinstance(semantics, MembershipSemantics):
            required.update(
                {
                    semantics.source_time_column,
                    semantics.interval_start_column,
                    semantics.interval_end_column,
                    *semantics.identity_columns,
                    *semantics.source_order,
                }
            )
        else:
            required.update(
                {
                    semantics.source_time_column,
                    *semantics.identity_columns,
                    *semantics.source_order,
                }
            )
        missing = required.difference(schema_names)
        if missing:
            raise DatasetRegistrationError(
                f"Tushare dataset {catalog.name!r} is missing configured columns: {sorted(missing)}"
            )
        reserved = {"fields"}
        for route in catalog.routes:
            reserved.add(route.instrument_param)
            query_shape = route.query_shape
            if isinstance(query_shape, DateRangeQuery):
                reserved.update({query_shape.start_param, query_shape.end_param})
            elif isinstance(query_shape, TradeDateQuery):
                reserved.add(query_shape.date_param)
            # Membership status is user-fixable; otherwise fetch current and history.
        conflicts = reserved.intersection(definition.fixed_params)
        if conflicts:
            raise DatasetRegistrationError(
                f"Tushare fixed_params cannot define backend-managed parameters: "
                f"{sorted(conflicts)}"
            )
        if not isinstance(semantics, DisclosureSemantics):
            if definition.disclosure_lag != 0:
                raise DatasetRegistrationError(
                    "disclosure_lag is only valid for disclosure datasets"
                )
            if definition.fetch_buffer_days != 180 or definition.fetch_margin_days != 31:
                raise DatasetRegistrationError(
                    "fetch_buffer_days and fetch_margin_days are only configurable "
                    "for disclosure datasets"
                )

    def _state(
        self, dataset: RegisteredDataset
    ) -> tuple[TushareDatasetSpec, TushareSource, TushareDatasetCatalog]:
        spec = dataset.spec
        source = dataset.source
        if not isinstance(spec, TushareDatasetSpec) or not isinstance(source, TushareSource):
            raise SchemaMismatchError("Invalid Tushare registered dataset")
        return spec, source, self._catalog(source.dataset)

    @staticmethod
    def _select_route(
        catalog: TushareDatasetCatalog,
        instruments: tuple[str, ...] | None,
    ) -> TushareApiRoute:
        allowed = {"whole_market", "both"} if instruments is None else {"instrument_only", "both"}
        for route in catalog.routes:
            if route.universe in allowed:
                return route
        universe = "whole market" if instruments is None else "instrument list"
        raise InvalidQueryError(
            f"Tushare dataset {catalog.name!r} has no route for {universe} queries"
        )

    @staticmethod
    def _unique_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
        result: list[str] = []
        for column in columns:
            if column not in result:
                result.append(column)
        return tuple(result)

    @staticmethod
    def _remote_columns(
        selected: tuple[str, ...],
        catalog: TushareDatasetCatalog,
    ) -> tuple[str, ...]:
        columns = list(selected)
        semantics = catalog.semantics
        if isinstance(semantics, DisclosureSemantics):
            internal = (*semantics.revision_order, *semantics.source_order)
        else:
            internal = semantics.source_order
        for column in internal:
            if column not in columns:
                columns.append(column)
        return tuple(columns)

    def _fetch_route_frames(
        self,
        client: Any,
        fixed_params: Mapping[str, object],
        route: TushareApiRoute,
        query: DataQuery,
        fields: tuple[str, ...],
        *,
        trade_dates: tuple[date, ...] | None = None,
    ) -> list[pd.DataFrame]:
        """Fetch observation or membership rows for panel construction."""

        if query.instruments == ():
            return []
        instruments: tuple[str | None, ...] = (
            query.instruments if query.instruments is not None else (None,)
        )
        shape = route.query_shape
        statuses: tuple[str | None, ...] = (None,)
        dates: tuple[date | None, ...] = (None,)
        if isinstance(shape, TradeDateQuery):
            if trade_dates is None:
                raise SchemaMismatchError(
                    f"Tushare api {route.api_name!r} requires resolved trading dates"
                )
            instruments = (None,)
            dates = trade_dates
        elif isinstance(shape, MembershipQuery):
            statuses = (None,) if shape.status_param in fixed_params else tuple(shape.status_values)

        frames: list[pd.DataFrame] = []
        for instrument in instruments:
            for status in statuses:
                for trade_date in dates:
                    params = self._route_params(
                        fixed_params,
                        route,
                        fields,
                        membership_status=status,
                        trade_date=trade_date,
                    )
                    if instrument is not None:
                        params[route.instrument_param] = instrument
                    frame = self._call_api(client, route.api_name, params)
                    if isinstance(shape, TradeDateQuery) and len(frame) >= shape.max_rows:
                        rendered_date = (
                            trade_date.isoformat() if trade_date is not None else "unknown"
                        )
                        raise RemoteQueryError(
                            f"Tushare api {route.api_name!r} returned "
                            f"{len(frame)} rows for {rendered_date}; "
                            f"the result may be truncated at the "
                            f"{shape.max_rows}-row API limit"
                        )
                    frames.append(frame)
        return frames

    def _fetch_disclosure_route_frames(
        self,
        client: Any,
        fixed_params: Mapping[str, object],
        route: TushareApiRoute,
        query: DataQuery,
        fields: tuple[str, ...],
    ) -> list[pd.DataFrame]:
        """Fetch disclosure events through the route chosen for the universe."""

        shape = route.query_shape
        if not isinstance(shape, DateRangeQuery):
            raise InvalidQueryError(f"Tushare api {route.api_name!r} has no disclosure query")
        if query.instruments == ():
            return []
        instruments: tuple[str | None, ...] = (
            query.instruments if query.instruments is not None else (None,)
        )
        frames: list[pd.DataFrame] = []
        for instrument in instruments:
            params = dict(fixed_params)
            params["fields"] = ",".join(fields)
            if query.start is not None:
                params[shape.start_param] = query.start.strftime("%Y%m%d")
            if query.end is not None:
                params[shape.end_param] = query.end.strftime("%Y%m%d")
            if instrument is not None:
                params[route.instrument_param] = instrument
            frames.append(self._call_api(client, route.api_name, params))
        return frames

    @staticmethod
    def _route_params(
        fixed_params: Mapping[str, object],
        route: TushareApiRoute,
        fields: tuple[str, ...],
        *,
        membership_status: str | None,
        trade_date: date | None,
    ) -> dict[str, object]:
        params = dict(fixed_params)
        params["fields"] = ",".join(fields)
        shape = route.query_shape
        if isinstance(shape, TradeDateQuery) and trade_date is not None:
            params[shape.date_param] = trade_date.strftime("%Y%m%d")
        elif isinstance(shape, MembershipQuery) and membership_status is not None:
            params[shape.status_param] = membership_status
        return params

    @staticmethod
    def _call_api(client: Any, api_name: str, params: dict[str, object]) -> pd.DataFrame:
        try:
            method = getattr(client, api_name, None)
            if callable(method):
                result = method(**params)
            elif callable(getattr(client, "query", None)):
                result = client.query(api_name, **params)
            else:
                raise AttributeError(f"Tushare client does not expose {api_name!r}")
        except Exception as exc:
            raise RemoteQueryError(f"Tushare query failed for api {api_name!r}: {exc}") from exc
        if not isinstance(result, pd.DataFrame):
            raise RemoteQueryError(
                f"Tushare api {api_name!r} returned {type(result).__name__}, expected DataFrame"
            )
        return result

    def _fetch_calendar(
        self,
        connection: str,
        exchange: str,
        start: datetime,
        end: datetime,
    ) -> list[date]:
        trading: list[date] = []
        year, month = start.year, start.month
        end_year, end_month = end.year, end.month
        while (year, month) <= (end_year, end_month):
            key = (connection, exchange, year, month)
            cached = self._calendar_cache.get(key)
            if cached is None:
                cached = self._fetch_calendar_month(connection, exchange, year, month)
                self._calendar_cache[key] = cached
            trading.extend(cached)
            month += 1
            if month > 12:
                year += 1
                month = 1
        start_date = start.date()
        end_date = end.date()
        return sorted(day for day in trading if start_date <= day <= end_date)

    def _fetch_calendar_month(
        self, connection: str, exchange: str, year: int, month: int
    ) -> list[date]:
        client = self._client(connection)
        first = date(year, month, 1)
        if month == 12:
            last = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            last = date(year, month + 1, 1) - timedelta(days=1)
        params: dict[str, object] = {
            "exchange": exchange,
            "start_date": first.strftime("%Y%m%d"),
            "end_date": last.strftime("%Y%m%d"),
            "is_open": "1",
        }
        frame = self._call_api(client, "trade_cal", params)
        if "cal_date" not in frame.columns:
            raise RemoteQueryError("Tushare trade_cal result is missing the 'cal_date' column")
        days = [
            datetime.strptime(str(value), "%Y%m%d").date() for value in frame["cal_date"].tolist()
        ]
        days.sort()
        return days

    def _normalize_remote_frames(
        self,
        frames: list[pd.DataFrame],
        catalog: TushareDatasetCatalog,
        columns: tuple[str, ...],
        route: TushareApiRoute,
    ) -> pd.DataFrame:
        normalized: list[pd.DataFrame] = []
        for current in frames:
            if current.empty:
                continue
            missing = set(columns).difference(current.columns)
            if missing:
                raise SchemaMismatchError(
                    f"Tushare api {route.api_name!r} result is missing columns: {sorted(missing)}"
                )
            selected = current.loc[:, list(columns)].copy()
            normalized.append(self._coerce_frame(selected, catalog.schema))
        if normalized:
            return pd.concat(normalized, ignore_index=True)
        empty = pd.DataFrame(columns=columns)
        return self._coerce_frame(empty, catalog.schema)

    @staticmethod
    def _expand_membership_panel(
        frame: pd.DataFrame,
        semantics: MembershipSemantics,
        instrument_column: str,
        query: DataQuery,
        calendar: list[date],
        columns: tuple[str, ...],
    ) -> pd.DataFrame:
        if query.start is None or query.end is None:
            raise InvalidQueryError("Membership panels require both start and end")
        if frame.empty or not calendar:
            return pd.DataFrame(columns=columns)

        panel_start = query.start.date()
        panel_end = query.end.date()
        sessions = [day for day in calendar if panel_start <= day <= panel_end]
        blocks: list[pd.DataFrame] = []
        for _, row in frame.iterrows():
            raw_start = row[semantics.interval_start_column]
            if pd.isna(raw_start):
                continue
            raw_end = row[semantics.interval_end_column]
            interval_start = max(cast(date, raw_start), panel_start)
            interval_end = panel_end if pd.isna(raw_end) else min(cast(date, raw_end), panel_end)
            active = [day for day in sessions if interval_start <= day <= interval_end]
            if not active:
                continue
            block = pd.DataFrame({semantics.panel_time_column: active})
            for column in frame.columns:
                block[column] = row[column]
            blocks.append(block)
        if not blocks:
            return pd.DataFrame(columns=columns)

        expanded = pd.concat(blocks, ignore_index=True)
        precedence = [
            column
            for column in (semantics.interval_start_column, "is_new")
            if column in expanded.columns
        ]
        sort_columns = [
            semantics.panel_time_column,
            instrument_column,
            *precedence,
        ]
        expanded = expanded.sort_values(sort_columns, kind="mergesort", na_position="first")
        keys = [semantics.panel_time_column, instrument_column]
        winners: list[pd.Series] = []
        for _, group in expanded.groupby(keys, sort=False, dropna=False):
            if precedence:
                latest = group.iloc[-1]
                tied = group
                for column in precedence:
                    value = latest[column]
                    tied = tied.loc[
                        tied[column].isna() if pd.isna(value) else tied[column].eq(value)
                    ]
            else:
                tied = group
            comparable = [column for column in columns if column not in keys]
            if len(tied.loc[:, comparable].drop_duplicates()) > 1:
                day, instrument = group.iloc[-1][keys].tolist()
                raise SchemaMismatchError(
                    "Conflicting membership rows have identical precedence for "
                    f"{instrument!r} on {day!r}"
                )
            winners.append(tied.iloc[-1])
        result = pd.DataFrame(winners)
        return result.loc[:, list(columns)].sort_values(keys, kind="mergesort")

    @staticmethod
    def _empty_arrow(schema: pa.Schema, selected: tuple[str, ...]) -> pa.Table:
        return pa.table(
            {column: pa.array([], type=schema.field(column).type) for column in selected}
        )

    @staticmethod
    def _membership_frame_to_arrow(
        frame: pd.DataFrame,
        schema: pa.Schema,
        panel_time_column: str,
        selected: tuple[str, ...],
    ) -> pa.Table:
        fields = [
            pa.field(column, pa.date32()) if column == panel_time_column else schema.field(column)
            for column in selected
        ]
        selected_schema = pa.schema(fields)
        if frame.empty:
            return pa.table(
                {field.name: pa.array([], type=field.type) for field in selected_schema}
            )
        try:
            return pa.Table.from_pandas(
                frame.loc[:, list(selected)],
                schema=selected_schema,
                preserve_index=False,
            )
        except (pa.ArrowException, ValueError, TypeError) as exc:
            raise SchemaMismatchError(
                f"Unable to convert Tushare membership panel to Arrow: {exc}"
            ) from exc

    @staticmethod
    def _coerce_frame(frame: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
        for field in schema:
            if field.name not in frame.columns:
                continue
            if pa.types.is_date32(field.type):
                frame[field.name] = TushareBackend._coerce_yyyymmdd(frame[field.name], field.name)
            elif pa.types.is_string(field.type):
                frame[field.name] = frame[field.name].astype("string")
            elif pa.types.is_integer(field.type):
                frame[field.name] = pd.to_numeric(frame[field.name], errors="coerce").astype(
                    "Int64"
                )
            elif pa.types.is_floating(field.type):
                frame[field.name] = pd.to_numeric(frame[field.name], errors="coerce")
        return frame

    @staticmethod
    def _coerce_yyyymmdd(series: pd.Series, name: str) -> pd.Series:
        result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
        mask = series.notna() & (series.astype("string") != "")
        if mask.any():
            parsed = pd.to_datetime(
                series.loc[mask].astype("string"), format="%Y%m%d", errors="coerce"
            )
            if parsed.isna().any():
                bad = series.loc[mask][parsed.isna()].head(5).to_list()
                raise SchemaMismatchError(
                    f"Tushare column {name!r} contains invalid YYYYMMDD values: {bad}"
                )
            result.loc[mask] = parsed
        return result.dt.date

    @staticmethod
    def _filter_time(
        frame: pd.DataFrame,
        time_column: str,
        query: DataQuery,
    ) -> pd.DataFrame:
        if frame.empty:
            return frame
        values = pd.to_datetime(frame[time_column])
        if query.start is not None:
            start = pd.Timestamp(query.start.date())
            frame = frame.loc[values.notna() & (values >= start)]
            values = values.loc[frame.index]
        if query.end is not None:
            end = pd.Timestamp(query.end.date())
            frame = frame.loc[values.notna() & (values <= end)]
        return frame

    @staticmethod
    def _filter_instruments(
        frame: pd.DataFrame,
        instrument_column: str,
        instruments: tuple[str, ...] | None,
    ) -> pd.DataFrame:
        if frame.empty or instruments is None:
            return frame
        return frame.loc[frame[instrument_column].isin(instruments)]

    @staticmethod
    def _filter_membership_overlap(
        frame: pd.DataFrame,
        semantics: MembershipSemantics,
        query: DataQuery,
    ) -> pd.DataFrame:
        """Keep intervals that overlap the closed query range."""

        if frame.empty:
            return frame
        if query.start is not None:
            start = pd.Timestamp(query.start.date())
            interval_ends = pd.to_datetime(frame[semantics.interval_end_column])
            frame = frame.loc[interval_ends.isna() | (interval_ends >= start)]
        if query.end is not None:
            end = pd.Timestamp(query.end.date())
            interval_starts = pd.to_datetime(frame[semantics.interval_start_column])
            frame = frame.loc[interval_starts.notna() & (interval_starts <= end)]
        return frame

    @staticmethod
    def _sort_by(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
        if frame.empty:
            return frame
        available = [column for column in columns if column in frame.columns]
        if not available:
            return frame
        return frame.sort_values(available, kind="mergesort", na_position="last")

    @staticmethod
    def _frame_to_arrow(
        frame: pd.DataFrame,
        schema: pa.Schema,
        selected: tuple[str, ...],
    ) -> pa.Table:
        selected_schema = pa.schema([schema.field(column) for column in selected])
        if frame.empty:
            return pa.table(
                {field.name: pa.array([], type=field.type) for field in selected_schema}
            )
        try:
            return pa.Table.from_pandas(
                frame.loc[:, list(selected)],
                schema=selected_schema,
                preserve_index=False,
            )
        except (pa.ArrowException, ValueError, TypeError) as exc:
            raise SchemaMismatchError(f"Unable to convert Tushare result to Arrow: {exc}") from exc

    @staticmethod
    def _close_client(client: Any) -> None:
        close = getattr(client, "close", None)
        if callable(close):
            close()

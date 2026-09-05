"""Tushare Pro session, remote API readers, and trading-calendar cache."""

from __future__ import annotations

import hashlib
import json
import os
import re
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
from ..models import Query
from .tushare_catalog import TushareApiRoute, TushareDatasetCatalog
from .tushare_common import (
    coerce_frame,
    empty_arrow,
    filter_instruments,
    filter_time,
    frame_to_arrow,
    remote_columns,
    select_route,
    sort_by,
    unique_columns,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Actual transport endpoint used by the project's default Tushare client.
_TUSHARE_HTTP_URL = "https://tx.xiaodefa.top/"


class TushareSession:
    """Manage named Tushare connections, clients, and calendar caches.

    Parameters
    ----------
    client_factory
        Optional callable receiving ``token=...`` and returning a
        Tushare-like client. It supports deterministic tests without the
        Tushare package or network access.

    Notes
    -----
    Connections are configured up front but clients are created lazily on
    the first request. Tokens come from the direct value or the configured
    environment variable. Trading calendars are cached per connection,
    exchange, year, and month.
    """

    def __init__(self, client_factory: Callable[..., Any] | None = None) -> None:
        self._configs: dict[str, Any] = {}
        self._clients: dict[str, Any] = {}
        self._client_factory = client_factory
        self._calendar_cache: dict[tuple[str, str, int, int], list[date]] = {}

    def add_connection(self, name: str, config: Any) -> None:
        """Add or replace a validated Tushare connection profile.

        Parameters
        ----------
        name
            Identifier used by registered datasets.
        config
            Direct token or token-environment configuration.

        Raises
        ------
        DatasetRegistrationError
            If the name or token configuration is invalid.

        Notes
        -----
        The token environment variable is not read here. Replacing an
        initialized profile closes its cached client. The connection's
        cached calendars are kept until :meth:`close`.
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

    def client(self, name: str) -> Any:
        """Return the cached client for a profile, creating it on first use.

        Raises
        ------
        BackendConnectionError
            If the token or the Tushare package is unavailable or the
            client cannot be initialized.
        """

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
                client._DataApi__http_url = _TUSHARE_HTTP_URL
            except Exception as exc:
                raise BackendConnectionError(f"Unable to initialize Tushare client: {exc}") from exc
        else:
            try:
                client = factory(token=token)
            except Exception as exc:
                raise BackendConnectionError(f"Unable to initialize Tushare client: {exc}") from exc
        self._clients[name] = client
        return client

    def fetch_calendar(
        self,
        connection: str,
        exchange: str,
        start: datetime,
        end: datetime,
    ) -> list[date]:
        """Fetch and cache open trading sessions for a closed range.

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

    def close(self) -> None:
        """Close cached Tushare clients and clear calendar entries."""

        for client in self._clients.values():
            self._close_client(client)
        self._clients.clear()
        self._calendar_cache.clear()

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
        client = self.client(connection)
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
        frame = call_api(client, "trade_cal", params)
        if "cal_date" not in frame.columns:
            raise RemoteQueryError("Tushare trade_cal result is missing the 'cal_date' column")
        days = [
            datetime.strptime(str(value), "%Y%m%d").date() for value in frame["cal_date"].tolist()
        ]
        days.sort()
        return days

    @staticmethod
    def _close_client(client: Any) -> None:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def call_api(client: Any, api_name: str, params: dict[str, object]) -> pd.DataFrame:
    """Call one Tushare API and require a DataFrame response."""

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


def fetch_trade_date_frames(
    client: Any,
    route: TushareApiRoute,
    fixed_params: Mapping[str, object],
    fields: tuple[str, ...],
    trade_dates: tuple[date, ...],
) -> list[pd.DataFrame]:
    """Fetch one whole-market response per open trading date.

    Raises
    ------
    RemoteQueryError
        If a single-date response reaches the route's row limit and may be
        truncated.
    """

    frames: list[pd.DataFrame] = []
    for trade_date in trade_dates:
        params = dict(fixed_params)
        params["fields"] = ",".join(fields)
        params[route.date_param] = trade_date.strftime("%Y%m%d")
        frame = call_api(client, route.api_name, params)
        if route.max_rows is not None and len(frame) >= route.max_rows:
            raise RemoteQueryError(
                f"Tushare api {route.api_name!r} returned "
                f"{len(frame)} rows for {trade_date.isoformat()}; "
                f"the result may be truncated at the "
                f"{route.max_rows}-row API limit"
            )
        frames.append(frame)
    return frames


def fetch_disclosure_frames(
    client: Any,
    route: TushareApiRoute,
    fixed_params: Mapping[str, object],
    query: Query,
    fields: tuple[str, ...],
) -> list[pd.DataFrame]:
    """Fetch disclosure events through the route chosen for the universe."""

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
            params[route.start_param] = query.start.strftime("%Y%m%d")
        if query.end is not None:
            params[route.end_param] = query.end.strftime("%Y%m%d")
        if instrument is not None:
            params[route.instrument_param] = instrument
        frames.append(call_api(client, route.api_name, params))
    return frames


def fetch_membership_frames(
    client: Any,
    route: TushareApiRoute,
    fixed_params: Mapping[str, object],
    query: Query,
    fields: tuple[str, ...],
) -> list[pd.DataFrame]:
    """Fetch current and historical membership rows per instrument.

    Notes
    -----
    A membership status fixed on the dataset sends exactly one request;
    otherwise both status values are fetched.
    """

    if query.instruments == ():
        return []
    instruments: tuple[str | None, ...] = (
        query.instruments if query.instruments is not None else (None,)
    )
    statuses: tuple[str | None, ...] = (
        (None,) if route.status_param in fixed_params else tuple(route.status_values)
    )
    frames: list[pd.DataFrame] = []
    for instrument in instruments:
        for status in statuses:
            params = dict(fixed_params)
            params["fields"] = ",".join(fields)
            if status is not None:
                params[route.status_param] = status
            if instrument is not None:
                params[route.instrument_param] = instrument
            frames.append(call_api(client, route.api_name, params))
    return frames


def normalize_remote_frames(
    frames: list[pd.DataFrame],
    catalog: TushareDatasetCatalog,
    columns: tuple[str, ...],
    route: TushareApiRoute,
) -> pd.DataFrame:
    """Select, type, and concatenate non-empty remote responses."""

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
        normalized.append(coerce_frame(selected, catalog.schema))
    if normalized:
        return pd.concat(normalized, ignore_index=True)
    return coerce_frame(pd.DataFrame(columns=columns), catalog.schema)


def scan_remote_observations(
    session: TushareSession,
    connection: str,
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
    dataset_name: str,
    time_column: str,
    instrument_column: str,
    calendar_exchange: str,
    fields: tuple[str, ...],
    query: Query,
) -> pa.Table:
    """Fetch and normalize one remote observation long table.

    Raises
    ------
    InvalidQueryError
        If the route fetches per trading date and bounds are missing.
    RemoteQueryError
        If a remote request fails.
    SchemaMismatchError
        If responses conflict with the catalog schema.
    """

    selected = (time_column, instrument_column, *fields)
    if query.instruments == ():
        return empty_arrow(catalog.schema, selected)
    route = select_route(catalog, query.instruments)
    if route.request != "trade_date":
        raise SchemaMismatchError(
            f"Tushare api {route.api_name!r} requires resolved trading dates"
        )
    if query.start is None or query.end is None:
        raise InvalidQueryError(f"Dataset {dataset_name!r} requires both start and end")
    client = session.client(connection)
    remote_fields = remote_columns(selected, catalog)
    trade_dates = tuple(
        session.fetch_calendar(connection, calendar_exchange, query.start, query.end)
    )
    frames = fetch_trade_date_frames(client, route, fixed_params, remote_fields, trade_dates)
    frame = normalize_remote_frames(frames, catalog, remote_fields, route)
    frame = filter_instruments(frame, instrument_column, query.instruments)
    frame = filter_time(frame, time_column, query.start, query.end)
    frame = sort_by(frame, catalog.semantics.source_order)
    return frame_to_arrow(frame, catalog.schema, selected)


def fetch_remote_disclosure_events(
    session: TushareSession,
    connection: str,
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
    query: Query,
    disclosure_column: str,
    instrument_column: str,
    period_column: str,
    identity_columns: tuple[str, ...],
    revision_order: tuple[str, ...],
    fields: tuple[str, ...],
) -> pa.Table:
    """Fetch disclosure events required by a point-in-time panel.

    Notes
    -----
    The fetch starts ``fetch_buffer_days`` before the requested panel to
    carry previously disclosed values into its left boundary. All revisions
    are retained for the point-in-time state machine.
    """

    route = select_route(catalog, query.instruments)
    if route.request != "date_range":
        raise InvalidQueryError(
            f"Tushare api {route.api_name!r} cannot serve a point-in-time panel"
        )
    selected_order = (disclosure_column, instrument_column, period_column, *revision_order)
    selected = unique_columns(
        (disclosure_column, instrument_column, period_column, *identity_columns, *fields)
    )
    remote_fields = remote_columns(selected, catalog)
    client = session.client(connection)
    frames = fetch_disclosure_frames(client, route, fixed_params, query, remote_fields)
    frame = normalize_remote_frames(frames, catalog, remote_fields, route)
    frame = filter_time(frame, disclosure_column, query.start, query.end)
    frame = sort_by(frame, unique_columns(selected_order))
    return frame_to_arrow(frame, catalog.schema, selected)


def fetch_remote_intervals(
    session: TushareSession,
    connection: str,
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
    query: Query,
    interval_start_column: str,
    interval_end_column: str,
    identity_columns: tuple[str, ...],
    fields: tuple[str, ...],
) -> pd.DataFrame:
    """Fetch raw remote membership intervals overlapping the query range."""

    selected_raw = unique_columns(
        (interval_start_column, catalog.instrument_column, interval_end_column, *identity_columns, *fields)
    )
    route = select_route(catalog, query.instruments)
    remote_fields = remote_columns(selected_raw, catalog)
    if query.instruments == ():
        frames: list[pd.DataFrame] = []
    else:
        frames = fetch_membership_frames(
            session.client(connection),
            route,
            fixed_params,
            query,
            remote_fields,
        )
    return normalize_remote_frames(frames, catalog, remote_fields, route)


def remote_tushare_fingerprint(
    connection: str,
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
) -> dict[str, object]:
    """Return sanitized remote API and schema provenance."""

    normalized = json.dumps(
        [(field.name, str(field.type)) for field in catalog.schema],
        separators=(",", ":"),
    )
    result: dict[str, object] = {
        "backend": "tushare",
        "connection": connection,
        "dataset": catalog.name,
        "available_apis": [route.api_name for route in catalog.routes],
        "schema_hash": hashlib.sha256(normalized.encode()).hexdigest(),
        "fixed_params": {str(key): str(value) for key, value in fixed_params.items()},
    }
    if any(route.request == "trade_date" for route in catalog.routes):
        result["calendar_api"] = "trade_cal"
    return result

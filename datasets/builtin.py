"""Bundled cross-source membership dataset registration."""

from __future__ import annotations

import pandas as pd

from ..backends import clickhouse as clickhouse_backend
from ..backends import parquet as parquet_backend
from ..exceptions import DatasetRegistrationError, InvalidQueryError
from ..models import Dataset, Panels, Query, QueryAudit
from ..transforms.membership import build_membership_panel
from .validation import _validate_name, _validate_timezone


def builtin_dataset(
    session: clickhouse_backend.ClickHouseSession,
    name: str = "membership_events",
    *,
    dataset: str = "membership_events",
    connection: str = "minghu",
    timezone: str = "Asia/Shanghai",
    version: str | None = None,
) -> Dataset:
    """Register a bundled dataset composed with an auxiliary ClickHouse connection.

    Currently only ``membership_events`` is supported: the packaged event
    Parquet supplies index transitions and ``stock_base.daily`` supplies
    trading dates and the listed-instrument universe.

    Raises
    ------
    DatasetRegistrationError
        If the dataset selection or connection is invalid.
    """

    _validate_name(name)
    if dataset != "membership_events":
        raise DatasetRegistrationError(f"Unknown built-in dataset: {dataset!r}")
    _validate_timezone(timezone)
    market_source = clickhouse_backend.prepare_clickhouse_table(
        session,
        connection=connection,
        table="stock_base.daily",
        time_column="date",
        instrument_column="code",
    )
    events_path = parquet_backend.MEMBERSHIP_EVENTS_PATH

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        if query.start is None or query.end is None:
            raise InvalidQueryError("Membership requires both start and end")
        record.source["market"] = clickhouse_backend.clickhouse_fingerprint(session, market_source)
        market = clickhouse_backend.scan_clickhouse(
            session,
            market_source,
            name,
            (),
            Query(
                dataset=name,
                fields=(),
                start=(pd.Timestamp(query.start) - pd.DateOffset(months=1)).to_pydatetime(),
                end=query.end,
            ),
        ).to_pandas()
        calendar = pd.DatetimeIndex(pd.to_datetime(market["date"]).unique()).sort_values()
        calendar = calendar[
            (calendar >= pd.Timestamp(query.start.date()))
            & (calendar <= pd.Timestamp(query.end.date()))
        ]
        calendar.name = "date"
        available = set(market["code"])
        codes = list(query.instruments) if query.instruments is not None else sorted(available)
        missing = sorted(set(codes) - available)
        if missing:
            raise InvalidQueryError(
                f"Instruments absent from stock_base.daily in query range including "
                f"one-month lookback: {missing}"
            )
        table = parquet_backend.read_membership_events(events_path)
        panel = build_membership_panel(table, calendar, codes)
        panel.attrs["events_sha256"] = record.source["events_sha256"]
        record.calendar_aligned = True
        return {"membership": panel}

    return Dataset(
        schema=parquet_backend.BUILTIN_MEMBERSHIP_SCHEMA,
        time_column="date",
        instrument_column="code",
        read_panel=read_panel,
        fingerprint=lambda: parquet_backend.membership_events_fingerprint(events_path),
        query_timezone=timezone,
        frequency="1d",
        version=version,
        requires_range=True,
        instrument_suffixes=None,
        adjustment=None,
    )

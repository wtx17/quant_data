"""Local Tushare archives with ClickHouse trading dates for event-shaped data."""

from collections.abc import Mapping
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pandas as pd

from ..backends.clickhouse import (
    ClickHouseSession,
    clickhouse_fingerprint,
    prepare_clickhouse_table,
    read_trade_calendar,
)
from ..backends.tushare_archive import (
    archive_fingerprint,
    effective_local_fixed_params,
    filter_time,
    frame_to_arrow,
    load_archive,
    membership_frame_to_arrow,
    read_archive_frame,
    sort_by,
    validate_local_fixed_params,
    validate_snapshot_bounds,
)
from ..backends.tushare_catalog import catalog_for
from ..exceptions import DatasetRegistrationError
from ..models import Dataset, Panels, Query, QueryAudit
from ..transforms import build_daily_panels, build_panels
from ..transforms.intervals import expand_intervals, filter_overlapping_intervals
from .validation import (
    _validate_disclosure_windows,
    _validate_mapping_keys,
    _validate_name,
    _validate_timezone,
)


def tushare_dataset(
    session: ClickHouseSession,
    name: str,
    *,
    data_dir: str | Path,
    dataset: str | None = None,
    calendar_connection: str = "minghu",
    fixed_params: Mapping[str, object] | None = None,
    timezone: str | None = "Asia/Shanghai",
    version: str | None = None,
    disclosure_lag: int = 0,
    fetch_buffer_days: int = 180,
    fetch_margin_days: int = 31,
) -> Dataset:
    """Validate a local archive and bind its ordinary, PIT or interval panel reader.

    daily_basic is fully local. Disclosure and industry panels use distinct
    stock_base.daily dates from the named ClickHouse connection; credentials
    remain lazy, and no stock filter can restrict the trading calendar.
    """
    _validate_name(name)
    _validate_timezone(timezone)
    _validate_disclosure_windows(disclosure_lag, fetch_buffer_days, fetch_margin_days)
    if not str(data_dir).strip():
        raise DatasetRegistrationError("Tushare data_dir cannot be empty")
    if dataset is not None and not dataset.strip():
        raise DatasetRegistrationError("Tushare dataset cannot be empty")
    catalog = catalog_for(dataset or name)
    kind = catalog["kind"]
    if kind != "disclosure" and (
        disclosure_lag != 0 or fetch_buffer_days != 180 or fetch_margin_days != 31
    ):
        raise DatasetRegistrationError(
            "Disclosure lag and fetch windows only apply to disclosure datasets"
        )
    params = dict(fixed_params) if fixed_params is not None else {}
    _validate_mapping_keys(params)
    validate_local_fixed_params(catalog, params)
    archive = load_archive(
        data_dir, catalog["name"], catalog, effective_local_fixed_params(catalog, params)
    )
    market = (
        prepare_clickhouse_table(
            session,
            connection=calendar_connection,
            table="stock_base.daily",
            time_column="date",
            instrument_column="code",
        )
        if kind != "observation"
        else None
    )
    panel_time = catalog["panel_time_column"]

    def fingerprint() -> dict[str, object]:
        source = archive_fingerprint(archive)
        if market is not None:
            source["calendar"] = clickhouse_fingerprint(session, market)
        return source

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        # The public query validator enforces a closed range before entering here.
        assert query.start is not None and query.end is not None
        validate_snapshot_bounds(
            archive,
            name,
            query.start,
            query.end,
            fetch_buffer_days=fetch_buffer_days,
            is_disclosure=kind == "disclosure",
        )
        record.parameters["effective_start"] = query.start.isoformat()
        record.parameters["effective_end"] = query.end.isoformat()
        source_time = catalog["source_time_column"]
        selected = tuple(
            dict.fromkeys((source_time, "ts_code", *catalog["identity_columns"], *query.fields))
        )
        columns = tuple(
            dict.fromkeys((*selected, *catalog["source_order"], *catalog.get("revision_order", ())))
        )
        fetch_query = query
        order = catalog["source_order"]
        date_column = source_time
        if kind == "disclosure":
            date_column = catalog["disclosure_column"]
            fetch_query = replace(query, start=query.start - timedelta(days=fetch_buffer_days))
            order = tuple(
                dict.fromkeys((date_column, "ts_code", source_time, *catalog["revision_order"]))
            )
        frame = read_archive_frame(
            archive,
            name,
            catalog,
            fetch_query,
            columns,
            date_column=None if kind == "membership" else date_column,
            membership=kind == "membership",
            order_columns=order,
        )
        if kind != "membership":
            frame = sort_by(
                filter_time(frame, date_column, fetch_query.start, fetch_query.end), order
            )
        if kind == "observation":
            table = frame_to_arrow(frame, catalog["schema"], (panel_time, "ts_code", *query.fields))
            return build_panels(
                table,
                dataset_name=name,
                time_column=panel_time,
                instrument_column="ts_code",
                fields=query.fields,
                instruments=query.instruments,
            )
        assert market is not None
        calendar_start = fetch_query.start if kind == "disclosure" else query.start
        assert calendar_start is not None
        calendar_end = (
            query.end + timedelta(days=fetch_margin_days) if kind == "disclosure" else query.end
        )
        record.parameters["calendar_table"] = market.table
        calendar = read_trade_calendar(session, market, calendar_start, calendar_end)
        if kind == "disclosure":
            table = frame_to_arrow(frame, catalog["schema"], columns)
            panels = build_daily_panels(
                table,
                dataset_name=name,
                disclosure_column=catalog["disclosure_column"],
                instrument_column="ts_code",
                period_column=source_time,
                fields=query.fields,
                instruments=query.instruments,
                calendar=calendar,
                panel_start=pd.Timestamp(query.start.date()),
                panel_end=pd.Timestamp(query.end.date()),
                disclosure_lag=disclosure_lag,
                revision_order=catalog["revision_order"],
                index_name=panel_time,
            )
            record.parameters["disclosure_lag"] = disclosure_lag
        else:
            frame = filter_overlapping_intervals(
                frame,
                start_column=source_time,
                end_column=catalog["interval_end_column"],
                start=query.start.date(),
                end=query.end.date(),
            )
            panel_columns = tuple(dict.fromkeys((panel_time, "ts_code", *query.fields)))
            expanded = expand_intervals(
                frame,
                start_column=source_time,
                end_column=catalog["interval_end_column"],
                panel_time_column=panel_time,
                instrument_column="ts_code",
                precedence_columns=(source_time, "is_new"),
                panel_start=query.start.date(),
                panel_end=query.end.date(),
                calendar=calendar,
                columns=panel_columns,
            )
            table = membership_frame_to_arrow(
                expanded, catalog["schema"], panel_time, panel_columns
            )
            panels = build_panels(
                table,
                dataset_name=name,
                time_column=panel_time,
                instrument_column="ts_code",
                fields=query.fields,
                instruments=query.instruments,
            )
        record.calendar_aligned = True
        return panels

    return Dataset(
        schema=catalog["schema"],
        time_column=panel_time,
        instrument_column="ts_code",
        read_panel=read_panel,
        fingerprint=fingerprint,
        query_timezone=timezone,
        frequency="d",
        version=version,
        requires_range=True,
    )

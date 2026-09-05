"""Dataset registration factories and bound panel handlers.

Each factory validates one registration completely and returns a
:class:`quant_data.models.Dataset` whose ``read_panel`` closure already
binds the resolved source, semantics, and reader functions. The factories
are organized by ordinary observations, Tushare remote/local semantics, and
the bundled cross-source membership dataset.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from .backends import clickhouse as clickhouse_backend
from .backends import parquet as parquet_backend
from .backends import tushare as tushare_backend
from .backends.tushare_archive import (
    TushareArchive,
    archive_fingerprint,
    effective_local_fixed_params,
    load_archive,
    read_archive_frame,
    validate_local_fixed_params,
    validate_snapshot_bounds,
)
from .backends.tushare_catalog import (
    DisclosureSemantics,
    MembershipSemantics,
    TushareDatasetCatalog,
    catalog_for,
)
from .backends.tushare_common import (
    filter_time,
    frame_to_arrow,
    membership_frame_to_arrow,
    remote_columns,
    select_route,
    sort_by,
    unique_columns,
)
from .exceptions import DatasetRegistrationError, InvalidQueryError, SchemaMismatchError
from .models import Dataset, Panels, PriceAdjustment, Query, QueryAudit
from .transforms import build_daily_panels, build_panels
from .transforms.intervals import expand_intervals, filter_overlapping_intervals
from .transforms.membership import build_membership_panel

ReadPanel = Callable[[Query, QueryAudit], Panels]
TableReader = Callable[[tuple[str, ...], Query], pa.Table]

_MINGHU_CODE_SUFFIXES = (".SZ", ".SH", ".BJ")


# ---------------------------------------------------------------------------
# Shared ordinary-observation execution
# ---------------------------------------------------------------------------


def _empty_table(schema: pa.Schema, columns: tuple[str, ...]) -> pa.Table:
    arrays: dict[str, pa.Array] = {}
    for column in columns:
        data_type = schema.field(column).type if column in schema.names else pa.date32()
        arrays[column] = pa.array([], type=data_type)
    return pa.table(arrays)


def _adjust_prices(table: pa.Table, adjustment: PriceAdjustment) -> pa.Table:
    factor = table[adjustment.factor_column]
    for field in adjustment.fields:
        if field not in table.column_names:
            continue
        index = table.schema.get_field_index(field)
        adjusted_values = pc.multiply(table[field], factor)
        table = table.set_column(index, field, adjusted_values)
    return table


def observation_read_panel(
    dataset_name: str,
    reader: TableReader,
    schema: pa.Schema,
    time_column: str,
    instrument_column: str,
    adjustment: PriceAdjustment | None,
    *,
    before_read: Callable[[Query, QueryAudit], None] | None = None,
) -> ReadPanel:
    """Build one ordinary-observation ``read_panel`` closure.

    The bound reader returns the projected long table; this closure handles
    the adjustment-factor projection, empty-instrument short circuit, price
    multiplication, and the ordinary pivot.
    """

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        if before_read is not None:
            before_read(query, record)
        scan_fields = query.fields
        if (
            query.adjusted
            and adjustment is not None
            and set(query.fields).intersection(adjustment.fields)
            and adjustment.factor_column not in query.fields
        ):
            scan_fields = (*query.fields, adjustment.factor_column)
        if query.instruments == ():
            table = _empty_table(schema, (time_column, instrument_column, *scan_fields))
        else:
            table = reader(scan_fields, query)
        for column in (time_column, instrument_column):
            if column not in table.column_names:
                raise SchemaMismatchError(f"Query result is missing key column {column!r}")
        if query.adjusted and adjustment is not None:
            table = _adjust_prices(table, adjustment)
        table = table.select([time_column, instrument_column, *query.fields])
        return build_panels(
            table,
            dataset_name=dataset_name,
            time_column=time_column,
            instrument_column=instrument_column,
            fields=query.fields,
            instruments=query.instruments,
        )

    return read_panel


# ---------------------------------------------------------------------------
# Generic Parquet registration
# ---------------------------------------------------------------------------


def parquet_dataset(
    name: str,
    paths: Sequence[str | Path],
    *,
    time_column: str = "time",
    instrument_column: str = "ts_code",
    frequency: str | None = None,
    timezone: str | None = None,
    version: str | None = None,
) -> Dataset:
    """Register local Parquet files as one ordinary-observation dataset.

    Raises
    ------
    DatasetRegistrationError
        If the name, key columns, paths, or timezone are invalid.
    SchemaMismatchError
        If the file schemas cannot be unified.
    """

    _validate_name(name)
    _validate_key_columns(time_column, instrument_column)
    _validate_timezone(timezone)
    files = parquet_backend.resolve_parquet_paths(paths)
    schema = parquet_backend.inspect_parquet_schema(files)
    parquet_backend.validate_parquet_keys(files, time_column, instrument_column)

    def reader(fields: tuple[str, ...], query: Query) -> pa.Table:
        return parquet_backend.scan_parquet(
            files, name, time_column, instrument_column, fields, query
        )

    return Dataset(
        schema=schema,
        time_column=time_column,
        instrument_column=instrument_column,
        read_panel=observation_read_panel(
            name, reader, schema, time_column, instrument_column, None
        ),
        fingerprint=lambda: parquet_backend.parquet_fingerprint(files),
        query_timezone=None,
        frequency=frequency,
        version=version,
        requires_range=False,
        instrument_suffixes=None,
        adjustment=None,
    )


# ---------------------------------------------------------------------------
# ClickHouse registration
# ---------------------------------------------------------------------------


def clickhouse_dataset(
    session: clickhouse_backend.ClickHouseSession,
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
) -> Dataset:
    """Register one ClickHouse table as an ordinary-observation dataset.

    Raises
    ------
    DatasetRegistrationError
        If the definition, profile, identifiers, or configured columns are
        invalid.
    RemoteQueryError
        If a custom table cannot be described remotely.
    SchemaMismatchError
        If a ClickHouse type cannot be mapped to Arrow.
    """

    _validate_name(name)
    _validate_key_columns(time_column, instrument_column)
    _validate_timezone(timezone)
    source = clickhouse_backend.prepare_clickhouse_table(
        session,
        connection=connection,
        table=table,
        time_column=time_column,
        instrument_column=instrument_column,
        partition_column=partition_column,
        order_columns=order_columns,
    )
    schema = clickhouse_backend.build_arrow_schema(source.public_column_types(), table)
    adjustment = None
    minghu = clickhouse_backend.minghu_daily_adjustment(source)
    if minghu is not None:
        factor_column, price_fields = minghu
        adjustment = PriceAdjustment(factor_column=factor_column, fields=price_fields)

    def reader(fields: tuple[str, ...], query: Query) -> pa.Table:
        return clickhouse_backend.scan_clickhouse(session, source, name, fields, query)

    return Dataset(
        schema=schema,
        time_column=time_column,
        instrument_column=instrument_column,
        read_panel=observation_read_panel(
            name, reader, schema, time_column, instrument_column, adjustment
        ),
        fingerprint=lambda: clickhouse_backend.clickhouse_fingerprint(session, source),
        query_timezone="Asia/Shanghai" if source.time_expression else timezone,
        frequency=frequency,
        version=version,
        requires_range=bool(
            require_time_range is True
            or (require_time_range is None and partition_column is not None)
        ),
        instrument_suffixes=_MINGHU_CODE_SUFFIXES if source.adds_code_suffix else None,
        adjustment=adjustment,
    )


# ---------------------------------------------------------------------------
# Tushare registration (remote API or local Parquet snapshot)
# ---------------------------------------------------------------------------


def tushare_dataset(
    session: tushare_backend.TushareSession,
    name: str,
    *,
    dataset: str | None = None,
    connection: str | None = None,
    data_dir: str | Path | None = None,
    calendar_connection: str | None = None,
    fixed_params: Mapping[str, object] | None = None,
    timezone: str | None = "Asia/Shanghai",
    version: str | None = None,
    disclosure_lag: int = 0,
    calendar_exchange: str = "SSE",
    fetch_buffer_days: int = 180,
    fetch_margin_days: int = 31,
) -> Dataset:
    """Register one logical Tushare dataset from the remote API or a local snapshot.

    Providing ``data_dir`` selects the local snapshot source, whose trading
    calendar comes from ``calendar_connection``; otherwise the remote API is
    used through ``connection``. The two source parameters are mutually
    exclusive.

    Raises
    ------
    DatasetRegistrationError
        If the definition, catalog name, connection, archive, or fixed
        parameters are invalid.
    SchemaMismatchError
        If the archive conflicts with the catalog.
    """

    params = dict(fixed_params) if fixed_params is not None else {}
    _validate_name(name)
    if dataset is not None and not dataset.strip():
        raise DatasetRegistrationError("Tushare dataset cannot be empty")
    if not calendar_exchange.strip():
        raise DatasetRegistrationError("Tushare calendar_exchange cannot be empty")
    _validate_disclosure_windows(disclosure_lag, fetch_buffer_days, fetch_margin_days)
    _validate_timezone(timezone)
    catalog = catalog_for(dataset or name)
    _validate_tushare_definition(catalog, params, disclosure_lag, fetch_buffer_days, fetch_margin_days)

    if data_dir is not None:
        if connection is not None:
            raise DatasetRegistrationError(
                "Tushare connection and data_dir are mutually exclusive; use "
                "calendar_connection for the local trading calendar"
            )
        if calendar_connection is None:
            raise DatasetRegistrationError(
                "Tushare local registration requires calendar_connection"
            )
        if not str(data_dir).strip():
            raise DatasetRegistrationError("Tushare Parquet data_dir cannot be empty")
        if not calendar_connection.strip():
            raise DatasetRegistrationError("Tushare calendar_connection cannot be empty")
        return _local_tushare_dataset(
            session,
            name,
            catalog,
            data_dir=data_dir,
            calendar_connection=calendar_connection,
            fixed_params=params,
            timezone=timezone,
            version=version,
            disclosure_lag=disclosure_lag,
            calendar_exchange=calendar_exchange,
            fetch_buffer_days=fetch_buffer_days,
            fetch_margin_days=fetch_margin_days,
        )
    if connection is None:
        raise DatasetRegistrationError("Remote Tushare registration requires connection")
    return _remote_tushare_dataset(
        session,
        name,
        catalog,
        connection=connection,
        fixed_params=params,
        timezone=timezone,
        version=version,
        disclosure_lag=disclosure_lag,
        calendar_exchange=calendar_exchange,
        fetch_buffer_days=fetch_buffer_days,
        fetch_margin_days=fetch_margin_days,
    )


def _remote_tushare_dataset(
    session: tushare_backend.TushareSession,
    name: str,
    catalog: TushareDatasetCatalog,
    *,
    connection: str,
    fixed_params: dict[str, object],
    timezone: str | None,
    version: str | None,
    disclosure_lag: int,
    calendar_exchange: str,
    fetch_buffer_days: int,
    fetch_margin_days: int,
) -> Dataset:
    if not session.has_connection(connection):
        raise DatasetRegistrationError(f"Tushare connection {connection!r} is not configured")
    semantics = catalog.semantics
    read_panel: ReadPanel
    if isinstance(semantics, DisclosureSemantics):
        read_panel = _remote_pit_read_panel(
            session,
            name,
            catalog,
            connection=connection,
            fixed_params=fixed_params,
            calendar_exchange=calendar_exchange,
            semantics=semantics,
            disclosure_lag=disclosure_lag,
            fetch_buffer_days=fetch_buffer_days,
            fetch_margin_days=fetch_margin_days,
        )
    elif isinstance(semantics, MembershipSemantics):
        read_panel = _remote_interval_read_panel(
            session,
            name,
            catalog,
            connection=connection,
            fixed_params=fixed_params,
            calendar_exchange=calendar_exchange,
            semantics=semantics,
        )
    else:

        def reader(fields: tuple[str, ...], query: Query) -> pa.Table:
            return tushare_backend.scan_remote_observations(
                session,
                connection,
                catalog,
                fixed_params,
                name,
                semantics.panel_time_column,
                catalog.instrument_column,
                calendar_exchange,
                fields,
                query,
            )

        read_panel = observation_read_panel(
            name,
            reader,
            catalog.schema,
            semantics.panel_time_column,
            catalog.instrument_column,
            None,
            before_read=lambda query, record: _audit_remote_route(catalog, query, record),
        )
    return Dataset(
        schema=catalog.schema,
        time_column=semantics.panel_time_column,
        instrument_column=catalog.instrument_column,
        read_panel=read_panel,
        fingerprint=lambda: tushare_backend.remote_tushare_fingerprint(
            connection, catalog, fixed_params
        ),
        query_timezone=timezone,
        frequency=semantics.panel_frequency,
        version=version,
        requires_range=True,
        instrument_suffixes=None,
        adjustment=None,
    )


def _local_tushare_dataset(
    session: tushare_backend.TushareSession,
    name: str,
    catalog: TushareDatasetCatalog,
    *,
    data_dir: str | Path,
    calendar_connection: str,
    fixed_params: dict[str, object],
    timezone: str | None,
    version: str | None,
    disclosure_lag: int,
    calendar_exchange: str,
    fetch_buffer_days: int,
    fetch_margin_days: int,
) -> Dataset:
    validate_local_fixed_params(catalog, fixed_params)
    if not session.has_connection(calendar_connection):
        raise DatasetRegistrationError(
            f"Tushare calendar connection {calendar_connection!r} is not configured"
        )
    archive = load_archive(
        data_dir, catalog.name, catalog, effective_local_fixed_params(catalog, fixed_params)
    )
    semantics = catalog.semantics
    read_panel: ReadPanel
    if isinstance(semantics, DisclosureSemantics):
        read_panel = _local_pit_read_panel(
            session,
            name,
            catalog,
            archive=archive,
            calendar_connection=calendar_connection,
            calendar_exchange=calendar_exchange,
            semantics=semantics,
            disclosure_lag=disclosure_lag,
            fetch_buffer_days=fetch_buffer_days,
            fetch_margin_days=fetch_margin_days,
        )
    elif isinstance(semantics, MembershipSemantics):
        read_panel = _local_interval_read_panel(
            session,
            name,
            catalog,
            archive=archive,
            calendar_connection=calendar_connection,
            calendar_exchange=calendar_exchange,
            semantics=semantics,
        )
    else:

        def reader(fields: tuple[str, ...], query: Query) -> pa.Table:
            selected = (semantics.panel_time_column, catalog.instrument_column, *fields)
            remote_fields = remote_columns(selected, catalog)
            frame = read_archive_frame(
                archive,
                name,
                catalog,
                query,
                remote_fields,
                date_column=semantics.panel_time_column,
                order_columns=semantics.source_order,
            )
            frame = filter_time(
                frame, semantics.panel_time_column, query.start, query.end
            )
            frame = sort_by(frame, semantics.source_order)
            return frame_to_arrow(frame, catalog.schema, selected)

        read_panel = observation_read_panel(
            name,
            reader,
            catalog.schema,
            semantics.panel_time_column,
            catalog.instrument_column,
            None,
            before_read=lambda query, record: _validate_local_snapshot(
                archive, name, query, record, fetch_buffer_days, False
            ),
        )
    return Dataset(
        schema=catalog.schema,
        time_column=semantics.panel_time_column,
        instrument_column=catalog.instrument_column,
        read_panel=read_panel,
        fingerprint=lambda: archive_fingerprint(archive, calendar_connection),
        query_timezone=timezone,
        frequency=semantics.panel_frequency,
        version=version,
        requires_range=True,
        instrument_suffixes=None,
        adjustment=None,
    )


def _audit_remote_route(
    catalog: TushareDatasetCatalog,
    query: Query,
    record: QueryAudit,
) -> None:
    """Record the deterministic data API chosen for this query."""

    api_name = (
        None if query.instruments == () else select_route(catalog, query.instruments).api_name
    )
    record.parameters["data_api"] = api_name
    record.source["selected_api"] = api_name
    calendar_api = record.source.get("calendar_api")
    if isinstance(calendar_api, str):
        record.parameters["calendar_api"] = calendar_api


def _audit_time_value(value: object) -> str | None:
    if value is None:
        return None
    try:
        return str(pd.Timestamp(value).isoformat())
    except (TypeError, ValueError):
        return repr(value)


def _required_bounds(name: str, query: Query, reason: str) -> tuple[datetime, datetime]:
    """Return the closed query bounds required by date-aligned panel kinds."""

    if query.start is None or query.end is None:
        raise InvalidQueryError(f"Dataset {name!r} {reason} requires both start and end")
    return query.start, query.end


def _validate_local_snapshot(
    archive: TushareArchive,
    name: str,
    query: Query,
    record: QueryAudit,
    fetch_buffer_days: int,
    is_disclosure: bool,
) -> None:
    """Validate snapshot bounds and record the effective query boundaries."""

    validate_snapshot_bounds(
        archive,
        name,
        query.start,
        query.end,
        fetch_buffer_days=fetch_buffer_days,
        is_disclosure=is_disclosure,
    )
    record.parameters["effective_start"] = _audit_time_value(query.start)
    record.parameters["effective_end"] = _audit_time_value(query.end)


def _remote_pit_read_panel(
    session: tushare_backend.TushareSession,
    name: str,
    catalog: TushareDatasetCatalog,
    *,
    connection: str,
    fixed_params: dict[str, object],
    calendar_exchange: str,
    semantics: DisclosureSemantics,
    disclosure_lag: int,
    fetch_buffer_days: int,
    fetch_margin_days: int,
) -> ReadPanel:
    instrument_column = catalog.instrument_column

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        panel_start_dt, panel_end_dt = _required_bounds(name, query, "point-in-time panel")
        _audit_remote_route(catalog, query, record)
        fetch_query = replace(
            query, start=panel_start_dt - timedelta(days=fetch_buffer_days)
        )
        table = tushare_backend.fetch_remote_disclosure_events(
            session,
            connection,
            catalog,
            fixed_params,
            fetch_query,
            semantics.disclosure_column,
            instrument_column,
            semantics.period_column,
            semantics.identity_columns,
            semantics.revision_order,
            query.fields,
        )
        calendar = session.fetch_calendar(
            connection,
            calendar_exchange,
            panel_start_dt - timedelta(days=fetch_buffer_days),
            panel_end_dt + timedelta(days=fetch_margin_days),
        )
        panels = build_daily_panels(
            table,
            dataset_name=name,
            disclosure_column=semantics.disclosure_column,
            instrument_column=instrument_column,
            period_column=semantics.period_column,
            fields=query.fields,
            instruments=query.instruments,
            calendar=calendar,
            panel_start=pd.Timestamp(panel_start_dt.date()),
            panel_end=pd.Timestamp(panel_end_dt.date()),
            disclosure_lag=disclosure_lag,
            revision_order=semantics.revision_order,
            index_name=semantics.panel_time_column,
        )
        record.calendar_aligned = True
        record.parameters["disclosure_lag"] = disclosure_lag
        record.parameters["calendar_api"] = "trade_cal"
        record.source["calendar_api"] = "trade_cal"
        return panels

    return read_panel


def _local_pit_read_panel(
    session: tushare_backend.TushareSession,
    name: str,
    catalog: TushareDatasetCatalog,
    *,
    archive: TushareArchive,
    calendar_connection: str,
    calendar_exchange: str,
    semantics: DisclosureSemantics,
    disclosure_lag: int,
    fetch_buffer_days: int,
    fetch_margin_days: int,
) -> ReadPanel:
    instrument_column = catalog.instrument_column
    order = unique_columns(
        (
            semantics.disclosure_column,
            instrument_column,
            semantics.period_column,
            *semantics.revision_order,
        )
    )

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        panel_start_dt, panel_end_dt = _required_bounds(name, query, "point-in-time panel")
        _validate_local_snapshot(archive, name, query, record, fetch_buffer_days, True)
        selected = unique_columns(
            (
                semantics.disclosure_column,
                instrument_column,
                semantics.period_column,
                *semantics.identity_columns,
                *query.fields,
            )
        )
        remote_fields = remote_columns(selected, catalog)
        fetch_query = replace(
            query, start=panel_start_dt - timedelta(days=fetch_buffer_days)
        )
        frame = read_archive_frame(
            archive,
            name,
            catalog,
            fetch_query,
            remote_fields,
            date_column=semantics.disclosure_column,
            order_columns=order,
        )
        frame = filter_time(
            frame, semantics.disclosure_column, fetch_query.start, fetch_query.end
        )
        frame = sort_by(frame, order)
        table = frame_to_arrow(frame, catalog.schema, selected)
        calendar = session.fetch_calendar(
            calendar_connection,
            calendar_exchange,
            panel_start_dt - timedelta(days=fetch_buffer_days),
            panel_end_dt + timedelta(days=fetch_margin_days),
        )
        panels = build_daily_panels(
            table,
            dataset_name=name,
            disclosure_column=semantics.disclosure_column,
            instrument_column=instrument_column,
            period_column=semantics.period_column,
            fields=query.fields,
            instruments=query.instruments,
            calendar=calendar,
            panel_start=pd.Timestamp(panel_start_dt.date()),
            panel_end=pd.Timestamp(panel_end_dt.date()),
            disclosure_lag=disclosure_lag,
            revision_order=semantics.revision_order,
            index_name=semantics.panel_time_column,
        )
        record.calendar_aligned = True
        record.parameters["disclosure_lag"] = disclosure_lag
        record.parameters["calendar_api"] = "trade_cal"
        record.source["calendar_api"] = "trade_cal"
        return panels

    return read_panel


def _remote_interval_read_panel(
    session: tushare_backend.TushareSession,
    name: str,
    catalog: TushareDatasetCatalog,
    *,
    connection: str,
    fixed_params: dict[str, object],
    calendar_exchange: str,
    semantics: MembershipSemantics,
) -> ReadPanel:
    instrument_column = catalog.instrument_column
    precedence = (semantics.interval_start_column, "is_new")

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        panel_start_dt, panel_end_dt = _required_bounds(name, query, "membership panel")
        _audit_remote_route(catalog, query, record)
        raw = tushare_backend.fetch_remote_intervals(
            session,
            connection,
            catalog,
            fixed_params,
            query,
            semantics.interval_start_column,
            semantics.interval_end_column,
            semantics.identity_columns,
            query.fields,
        )
        raw = filter_overlapping_intervals(
            raw,
            start_column=semantics.interval_start_column,
            end_column=semantics.interval_end_column,
            start=panel_start_dt.date(),
            end=panel_end_dt.date(),
        )
        calendar = session.fetch_calendar(
            connection, calendar_exchange, panel_start_dt, panel_end_dt
        )
        selected_panel = unique_columns(
            (semantics.panel_time_column, instrument_column, *query.fields)
        )
        expanded = expand_intervals(
            raw,
            start_column=semantics.interval_start_column,
            end_column=semantics.interval_end_column,
            panel_time_column=semantics.panel_time_column,
            instrument_column=instrument_column,
            precedence_columns=precedence,
            panel_start=panel_start_dt.date(),
            panel_end=panel_end_dt.date(),
            calendar=calendar,
            columns=selected_panel,
        )
        table = membership_frame_to_arrow(
            expanded, catalog.schema, semantics.panel_time_column, selected_panel
        )
        record.calendar_aligned = True
        record.parameters["calendar_api"] = "trade_cal"
        record.source["calendar_api"] = "trade_cal"
        return build_panels(
            table,
            dataset_name=name,
            time_column=semantics.panel_time_column,
            instrument_column=instrument_column,
            fields=query.fields,
            instruments=query.instruments,
        )

    return read_panel


def _local_interval_read_panel(
    session: tushare_backend.TushareSession,
    name: str,
    catalog: TushareDatasetCatalog,
    *,
    archive: TushareArchive,
    calendar_connection: str,
    calendar_exchange: str,
    semantics: MembershipSemantics,
) -> ReadPanel:
    instrument_column = catalog.instrument_column
    precedence = (semantics.interval_start_column, "is_new")

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        panel_start_dt, panel_end_dt = _required_bounds(name, query, "membership panel")
        _validate_local_snapshot(archive, name, query, record, 0, False)
        selected_raw = unique_columns(
            (
                semantics.interval_start_column,
                instrument_column,
                semantics.interval_end_column,
                *semantics.identity_columns,
                *query.fields,
            )
        )
        remote_fields = remote_columns(selected_raw, catalog)
        frame = read_archive_frame(
            archive,
            name,
            catalog,
            query,
            remote_fields,
            membership=semantics,
            order_columns=semantics.source_order,
        )
        frame = filter_overlapping_intervals(
            frame,
            start_column=semantics.interval_start_column,
            end_column=semantics.interval_end_column,
            start=panel_start_dt.date(),
            end=panel_end_dt.date(),
        )
        calendar = session.fetch_calendar(
            calendar_connection, calendar_exchange, panel_start_dt, panel_end_dt
        )
        selected_panel = unique_columns(
            (semantics.panel_time_column, instrument_column, *query.fields)
        )
        expanded = expand_intervals(
            frame,
            start_column=semantics.interval_start_column,
            end_column=semantics.interval_end_column,
            panel_time_column=semantics.panel_time_column,
            instrument_column=instrument_column,
            precedence_columns=precedence,
            panel_start=panel_start_dt.date(),
            panel_end=panel_end_dt.date(),
            calendar=calendar,
            columns=selected_panel,
        )
        table = membership_frame_to_arrow(
            expanded, catalog.schema, semantics.panel_time_column, selected_panel
        )
        record.calendar_aligned = True
        record.parameters["calendar_api"] = "trade_cal"
        record.source["calendar_api"] = "trade_cal"
        return build_panels(
            table,
            dataset_name=name,
            time_column=semantics.panel_time_column,
            instrument_column=instrument_column,
            fields=query.fields,
            instruments=query.instruments,
        )

    return read_panel


# ---------------------------------------------------------------------------
# Bundled cross-source membership registration
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Shared registration validation
# ---------------------------------------------------------------------------


def _validate_name(name: str) -> None:
    if not name.strip():
        raise DatasetRegistrationError("Dataset name cannot be empty")


def _validate_key_columns(time_column: str, instrument_column: str) -> None:
    if not time_column or not instrument_column:
        raise DatasetRegistrationError("Key column names cannot be empty")
    if time_column == instrument_column:
        raise DatasetRegistrationError("Time and instrument columns must be different")


def _validate_timezone(timezone: str | None) -> None:
    if timezone:
        try:
            ZoneInfo(timezone)
        except Exception as exc:
            raise DatasetRegistrationError(f"Invalid timezone: {timezone!r}") from exc


def _validate_disclosure_windows(
    disclosure_lag: int, fetch_buffer_days: int, fetch_margin_days: int
) -> None:
    for label, value in (
        ("disclosure_lag", disclosure_lag),
        ("fetch_buffer_days", fetch_buffer_days),
        ("fetch_margin_days", fetch_margin_days),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise DatasetRegistrationError(f"{label} must be non-negative")


def _validate_tushare_definition(
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
    disclosure_lag: int,
    fetch_buffer_days: int,
    fetch_margin_days: int,
) -> None:
    """Validate catalog column wiring and fixed parameters (shared by both sources)."""

    if not isinstance(fixed_params, Mapping):
        raise DatasetRegistrationError("Tushare fixed_params must be a mapping")
    invalid_param_keys = [
        key for key in fixed_params if not isinstance(key, str) or not key
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
        if route.request == "date_range":
            reserved.update({route.start_param, route.end_param})
        elif route.request == "trade_date":
            reserved.add(route.date_param)
        # Membership status is user-fixable; otherwise fetch current and history.
    conflicts = reserved.intersection(fixed_params)
    if conflicts:
        raise DatasetRegistrationError(
            f"Tushare fixed_params cannot define backend-managed parameters: "
            f"{sorted(conflicts)}"
        )
    if not isinstance(semantics, DisclosureSemantics):
        if disclosure_lag != 0:
            raise DatasetRegistrationError(
                "disclosure_lag is only valid for disclosure datasets"
            )
        if fetch_buffer_days != 180 or fetch_margin_days != 31:
            raise DatasetRegistrationError(
                "fetch_buffer_days and fetch_margin_days are only configurable "
                "for disclosure datasets"
            )


__all__ = [
    "builtin_dataset",
    "clickhouse_dataset",
    "observation_read_panel",
    "parquet_dataset",
    "tushare_dataset",
]

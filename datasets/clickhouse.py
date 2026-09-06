"""ClickHouse dataset registration."""

from __future__ import annotations

import pyarrow as pa

from ..backends import clickhouse as clickhouse_backend
from ..models import Dataset, PriceAdjustment, Query
from .observation import observation_read_panel
from .validation import _validate_key_columns, _validate_name, _validate_timezone

_MINGHU_CODE_SUFFIXES = (".SZ", ".SH", ".BJ")


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

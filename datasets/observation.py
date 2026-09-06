"""Shared execution tail for ordinary-observation datasets."""

from __future__ import annotations

from collections.abc import Callable

import pyarrow as pa
import pyarrow.compute as pc

from ..exceptions import SchemaMismatchError
from ..models import Panels, PriceAdjustment, Query, QueryAudit
from ..transforms import build_panels

ReadPanel = Callable[[Query, QueryAudit], Panels]
TableReader = Callable[[tuple[str, ...], Query], pa.Table]


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
) -> ReadPanel:
    """Build one ordinary-observation ``read_panel`` closure.

    The bound reader returns the projected long table; this closure handles
    the adjustment-factor projection, empty-instrument short circuit, price
    multiplication, and the ordinary pivot. The factor column is fetched and
    applied only when the query requests at least one adjustable price field.
    """

    def read_panel(query: Query, record: QueryAudit) -> Panels:
        price_adjustment = (
            adjustment
            if query.adjusted
            and adjustment is not None
            and set(query.fields).intersection(adjustment.fields)
            else None
        )
        scan_fields = query.fields
        if price_adjustment is not None and price_adjustment.factor_column not in query.fields:
            scan_fields = (*query.fields, price_adjustment.factor_column)
        if query.instruments == ():
            table = _empty_table(schema, (time_column, instrument_column, *scan_fields))
        else:
            table = reader(scan_fields, query)
        for column in (time_column, instrument_column):
            if column not in table.column_names:
                raise SchemaMismatchError(f"Query result is missing key column {column!r}")
        if price_adjustment is not None:
            table = _adjust_prices(table, price_adjustment)
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

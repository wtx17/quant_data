"""Column and frame normalization shared by Tushare remote and archive readers."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pyarrow as pa

from ..exceptions import InvalidQueryError, SchemaMismatchError
from .tushare_catalog import (
    DisclosureSemantics,
    TushareApiRoute,
    TushareDatasetCatalog,
)


def select_route(
    catalog: TushareDatasetCatalog,
    instruments: tuple[str, ...] | None,
) -> TushareApiRoute:
    """Return the deterministic route serving the requested universe."""

    allowed = {"whole_market", "both"} if instruments is None else {"instruments", "both"}
    for route in catalog.routes:
        if route.scope in allowed:
            return route
    universe = "whole market" if instruments is None else "instrument list"
    raise InvalidQueryError(
        f"Tushare dataset {catalog.name!r} has no route for {universe} queries"
    )


def unique_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    """Return the input columns in order without duplicates."""

    result: list[str] = []
    for column in columns:
        if column not in result:
            result.append(column)
    return tuple(result)


def remote_columns(
    selected: tuple[str, ...],
    catalog: TushareDatasetCatalog,
) -> tuple[str, ...]:
    """Add the internal ordering columns a scan must retain."""

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


def coerce_frame(frame: pd.DataFrame, schema: pa.Schema) -> pd.DataFrame:
    """Coerce string-typed remote values to the catalog schema types."""

    for field in schema:
        if field.name not in frame.columns:
            continue
        if pa.types.is_date32(field.type):
            frame[field.name] = coerce_yyyymmdd(frame[field.name], field.name)
        elif pa.types.is_string(field.type):
            frame[field.name] = frame[field.name].astype("string")
        elif pa.types.is_integer(field.type):
            frame[field.name] = pd.to_numeric(frame[field.name], errors="coerce").astype(
                "Int64"
            )
        elif pa.types.is_floating(field.type):
            frame[field.name] = pd.to_numeric(frame[field.name], errors="coerce")
    return frame


def coerce_yyyymmdd(series: pd.Series, name: str) -> pd.Series:
    """Parse a YYYYMMDD string column into dates, rejecting invalid values."""

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


def filter_time(
    frame: pd.DataFrame,
    time_column: str,
    start: datetime | None,
    end: datetime | None,
) -> pd.DataFrame:
    """Keep rows whose date-granular time value lies within closed bounds."""

    if frame.empty:
        return frame
    values = pd.to_datetime(frame[time_column])
    if start is not None:
        start_bound = pd.Timestamp(start.date())
        frame = frame.loc[values.notna() & (values >= start_bound)]
        values = values.loc[frame.index]
    if end is not None:
        end_bound = pd.Timestamp(end.date())
        frame = frame.loc[values.notna() & (values <= end_bound)]
    return frame


def filter_instruments(
    frame: pd.DataFrame,
    instrument_column: str,
    instruments: tuple[str, ...] | None,
) -> pd.DataFrame:
    """Keep rows for the requested instruments."""

    if frame.empty or instruments is None:
        return frame
    return frame.loc[frame[instrument_column].isin(instruments)]


def sort_by(frame: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    """Sort a frame by the columns present, with null values last."""

    if frame.empty:
        return frame
    available = [column for column in columns if column in frame.columns]
    if not available:
        return frame
    return frame.sort_values(available, kind="mergesort", na_position="last")


def frame_to_arrow(
    frame: pd.DataFrame,
    schema: pa.Schema,
    selected: tuple[str, ...],
) -> pa.Table:
    """Convert a normalized frame to a typed Arrow table."""

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


def membership_frame_to_arrow(
    frame: pd.DataFrame,
    schema: pa.Schema,
    panel_time_column: str,
    selected: tuple[str, ...],
) -> pa.Table:
    """Convert an expanded membership frame with a date32 panel index."""

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


def empty_arrow(schema: pa.Schema, selected: tuple[str, ...]) -> pa.Table:
    """Return a typed empty Arrow table for the selected columns."""

    return pa.table(
        {column: pa.array([], type=schema.field(column).type) for column in selected}
    )


def fixed_param_value(value: object) -> object:
    """Render a fixed parameter value the way archive columns store it."""

    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return value

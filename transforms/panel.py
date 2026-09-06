"""Long-table validation and panel construction."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import pandas as pd
import polars as pl
import pyarrow as pa

from ..exceptions import DuplicateObservationError, SchemaMismatchError


def build_panels(
    table: pa.Table,
    *,
    dataset_name: str,
    time_column: str,
    instrument_column: str,
    fields: Sequence[str],
    instruments: Sequence[str] | None,
) -> dict[str, pd.DataFrame]:
    """Pivot a unique Arrow long table into one panel per field.

    Parameters
    ----------
    table
        Arrow table containing key columns and requested value fields.
    dataset_name
        Name included in validation error messages.
    time_column
        Row-index key.
    instrument_column
        Output-column key.
    fields
        Value columns to pivot in caller order.
    instruments
        Requested output-column order, or ``None`` to sort observed identifiers.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Field-keyed panels with a named DatetimeIndex and instrument columns.

    Raises
    ------
    SchemaMismatchError
        If a key column contains null values.
    DuplicateObservationError
        If more than one row has the same time/instrument key.

    Notes
    -----
    Missing requested instruments are added as all-null columns. An empty input
    returns typed-empty-shaped Pandas objects with the requested column order.
    """

    frame = cast(pl.DataFrame, pl.from_arrow(table))
    if frame.height:
        null_keys = frame.filter(
            pl.col(time_column).is_null() | pl.col(instrument_column).is_null()
        )
        if null_keys.height:
            raise SchemaMismatchError(
                f"Dataset {dataset_name!r} contains {null_keys.height} rows with null key values"
            )

        duplicates = (
            frame.group_by([time_column, instrument_column]).len().filter(pl.col("len") > 1)
        )
        if duplicates.height:
            sample = duplicates.head(5).to_dicts()
            raise DuplicateObservationError(
                f"Dataset {dataset_name!r} has {duplicates.height} duplicate key pairs; "
                f"sample={sample}"
            )

    if instruments is None:
        column_order = sorted(frame[instrument_column].unique().to_list()) if frame.height else []
    else:
        column_order = list(instruments)

    panels: dict[str, pd.DataFrame] = {}
    for field in fields:
        if frame.height:
            wide = frame.pivot(
                on=instrument_column,
                index=time_column,
                values=field,
                aggregate_function=None,
            ).sort(time_column)
            for instrument in column_order:
                if instrument not in wide.columns:
                    wide = wide.with_columns(pl.lit(None).alias(instrument))
            wide = wide.select([time_column, *column_order])
            panel = wide.to_pandas(use_pyarrow_extension_array=True).set_index(time_column)
            panel.index = pd.DatetimeIndex(wide[time_column].to_pandas(), name=time_column)
        else:
            time_type = table.schema.field(time_column).type
            panel = pd.DataFrame(
                columns=column_order,
                index=pd.DatetimeIndex(
                    [],
                    name=time_column,
                    tz=time_type.tz if pa.types.is_timestamp(time_type) else None,
                ),
            )
        panel.columns.name = instrument_column
        panels[field] = panel
    return panels

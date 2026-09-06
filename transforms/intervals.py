"""Pure expansion of effective-dated intervals onto a trading calendar."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import cast

import pandas as pd

from ..exceptions import SchemaMismatchError


def filter_overlapping_intervals(
    frame: pd.DataFrame,
    *,
    start_column: str,
    end_column: str,
    start: date | None,
    end: date | None,
) -> pd.DataFrame:
    """Keep intervals that overlap the closed query range.

    An open-ended interval (null end) never expires; a null start never
    becomes active and is dropped by the end bound.
    """

    if frame.empty:
        return frame
    if start is not None:
        start_bound = pd.Timestamp(start)
        interval_ends = pd.to_datetime(frame[end_column])
        frame = frame.loc[interval_ends.isna() | (interval_ends >= start_bound)]
    if end is not None:
        end_bound = pd.Timestamp(end)
        interval_starts = pd.to_datetime(frame[start_column])
        frame = frame.loc[interval_starts.notna() & (interval_starts <= end_bound)]
    return frame


def expand_intervals(
    frame: pd.DataFrame,
    *,
    start_column: str,
    end_column: str,
    panel_time_column: str,
    instrument_column: str,
    precedence_columns: tuple[str, ...],
    panel_start: date,
    panel_end: date,
    calendar: Sequence[date],
    columns: tuple[str, ...],
) -> pd.DataFrame:
    """Expand interval rows over trading sessions and resolve overlaps.

    Parameters
    ----------
    frame
        Normalized interval rows overlapping ``[panel_start, panel_end]``.
    start_column, end_column
        Interval endpoint columns. A null end extends to ``panel_end``; a
        null start row is skipped.
    panel_time_column
        Output column receiving the expanded trading date.
    instrument_column
        Interval identity column used to group competing rows.
    precedence_columns
        Lexicographic tie-break columns applied in order; the greatest
        present column value wins. Only columns present in ``frame`` apply.
    panel_start, panel_end
        Inclusive output boundaries.
    calendar
        Trading sessions covering the panel range.
    columns
        Output column order of the returned frame.

    Returns
    -------
    pandas.DataFrame
        One winning row per trading session and instrument, restricted to
        ``columns``.

    Raises
    ------
    SchemaMismatchError
        If equally ranked intervals disagree on the output values.
    """

    if frame.empty or not calendar:
        return pd.DataFrame(columns=columns)

    sessions = [day for day in calendar if panel_start <= day <= panel_end]
    blocks: list[pd.DataFrame] = []
    for _, row in frame.iterrows():
        raw_start = row[start_column]
        if pd.isna(raw_start):
            continue
        raw_end = row[end_column]
        interval_start = max(cast(date, raw_start), panel_start)
        interval_end = panel_end if pd.isna(raw_end) else min(cast(date, raw_end), panel_end)
        active = [day for day in sessions if interval_start <= day <= interval_end]
        if not active:
            continue
        block = pd.DataFrame({panel_time_column: active})
        for column in frame.columns:
            block[column] = row[column]
        blocks.append(block)
    if not blocks:
        return pd.DataFrame(columns=columns)

    expanded = pd.concat(blocks, ignore_index=True)
    precedence = [column for column in precedence_columns if column in expanded.columns]
    sort_columns = [
        panel_time_column,
        instrument_column,
        *precedence,
    ]
    expanded = expanded.sort_values(sort_columns, kind="mergesort", na_position="first")
    keys = [panel_time_column, instrument_column]
    winners: list[pd.Series] = []
    for _, group in expanded.groupby(keys, sort=False, dropna=False):
        if precedence:
            latest = group.iloc[-1]
            tied = group
            for column in precedence:
                value = latest[column]
                tied = tied.loc[tied[column].isna() if pd.isna(value) else tied[column].eq(value)]
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

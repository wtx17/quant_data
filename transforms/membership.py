"""Pure expansion of index membership events onto a trading calendar."""

from __future__ import annotations

from typing import Sequence

import pandas as pd
import pyarrow as pa

from ..exceptions import SchemaMismatchError


def build_membership_panel(
    table: pa.Table,
    calendar: pd.DatetimeIndex,
    instruments: Sequence[str],
) -> pd.DataFrame:
    events = table.to_pandas()
    codes = list(instruments)
    fields = ["hs300", "zz500", "zz1000"]
    if list(events.columns) != ["change_date", "code", *fields] or events.isna().any().any():
        raise SchemaMismatchError("Invalid membership event schema or null values")
    if (
        events.duplicated(["change_date", "code"]).any()
        or not events[fields].isin([-1, 0, 1]).all().all()
    ):
        raise SchemaMismatchError("Invalid membership events: duplicate keys or deltas")
    events = events.sort_values(["code", "change_date"])
    states = events.groupby("code")[fields].cumsum()
    if not states.isin([0, 1]).all().all() or (states.sum(axis=1) > 1).any():
        raise SchemaMismatchError("Invalid membership states or overlapping indices")
    events["membership"] = states.dot([1, 2, 3]).astype("int8")
    panel = pd.DataFrame(0, index=calendar, columns=pd.Index(codes, name="code"), dtype="int8")
    for code, rows in events[events["code"].isin(codes)].groupby("code"):
        series = rows.set_index("change_date")["membership"]
        panel[code] = series.reindex(calendar, method="ffill").fillna(0).astype("int8")
    return panel

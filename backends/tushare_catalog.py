"""Static local Tushare tables: schema, time semantics and archive filters."""

from typing import Literal, NotRequired, TypedDict

import pyarrow as pa

from ..exceptions import DatasetRegistrationError
from .tushare_schemas import TUSHARE_SCHEMAS


class TushareTable(TypedDict):
    """Type annotations for the flat catalog entries; no runtime wrapper."""

    name: str
    schema: pa.Schema
    kind: Literal["observation", "disclosure", "membership"]
    source_time_column: str
    panel_time_column: str
    identity_columns: tuple[str, ...]
    source_order: tuple[str, ...]
    filter_columns: dict[str, str]
    default_filters: dict[str, object]
    disclosure_column: NotRequired[str]
    revision_order: NotRequired[tuple[str, ...]]
    interval_end_column: NotRequired[str]


def _financial(
    name: str,
    disclosure: str,
    identity: tuple[str, ...],
    revisions: tuple[str, ...],
    filters: tuple[str, ...],
) -> TushareTable:
    return {
        "name": name,
        "schema": TUSHARE_SCHEMAS[name],
        "kind": "disclosure",
        "source_time_column": "end_date",
        "panel_time_column": "trade_date",
        "disclosure_column": disclosure,
        "identity_columns": identity,
        "revision_order": revisions,
        "source_order": tuple(dict.fromkeys(("end_date", "ts_code", disclosure, *identity))),
        "filter_columns": {key: "end_date" if key == "enddate" else key for key in filters},
        "default_filters": {"report_type": "1"}
        if name in {"income", "balancesheet", "cashflow"}
        else {},
    }


_STATEMENT_IDENTITY = (
    "ann_date",
    "f_ann_date",
    "report_type",
    "comp_type",
    "end_type",
    "update_flag",
)
_STATEMENT_REVISIONS = ("f_ann_date", "ann_date", "update_flag")

TUSHARE_DATASETS: dict[str, TushareTable] = {
    "daily_basic": {
        "name": "daily_basic",
        "schema": TUSHARE_SCHEMAS["daily_basic"],
        "kind": "observation",
        "source_time_column": "trade_date",
        "panel_time_column": "trade_date",
        "identity_columns": (),
        "source_order": ("trade_date", "ts_code"),
        "filter_columns": {},
        "default_filters": {},
    },
    "income": _financial(
        "income",
        "f_ann_date",
        _STATEMENT_IDENTITY,
        _STATEMENT_REVISIONS,
        ("ann_date", "f_ann_date", "report_type", "comp_type"),
    ),
    "balancesheet": _financial(
        "balancesheet",
        "f_ann_date",
        _STATEMENT_IDENTITY,
        _STATEMENT_REVISIONS,
        ("ann_date", "report_type", "comp_type"),
    ),
    "cashflow": _financial(
        "cashflow",
        "f_ann_date",
        _STATEMENT_IDENTITY,
        _STATEMENT_REVISIONS,
        ("ann_date", "f_ann_date", "report_type", "comp_type"),
    ),
    "fina_indicator": _financial(
        "fina_indicator",
        "ann_date",
        ("ann_date", "update_flag"),
        ("ann_date", "update_flag"),
        ("ann_date",),
    ),
    "express": _financial(
        "express", "ann_date", ("ann_date", "is_audit"), ("ann_date",), ("ann_date",)
    ),
    "forecast": _financial(
        "forecast",
        "ann_date",
        ("ann_date", "first_ann_date", "type"),
        ("ann_date", "first_ann_date"),
        ("ann_date", "type"),
    ),
    "stk_holdernumber": _financial(
        "stk_holdernumber", "ann_date", ("ann_date",), ("ann_date",), ("ann_date", "enddate")
    ),
}
for _name in ("ci_index_member", "index_member_all"):
    TUSHARE_DATASETS[_name] = {
        "name": _name,
        "schema": TUSHARE_SCHEMAS["industry_member"],
        "kind": "membership",
        "source_time_column": "in_date",
        "interval_end_column": "out_date",
        "panel_time_column": "date",
        "identity_columns": ("l1_code", "l2_code", "l3_code", "out_date", "is_new"),
        "source_order": ("in_date", "ts_code", "out_date", "l3_code", "is_new"),
        "filter_columns": {key: key for key in ("l1_code", "l2_code", "l3_code", "is_new")},
        "default_filters": {},
    }


def catalog_for(name: str) -> TushareTable:
    try:
        return TUSHARE_DATASETS[name]
    except KeyError as exc:
        raise DatasetRegistrationError(
            f"Unsupported Tushare dataset {name!r}; supported datasets: {', '.join(sorted(TUSHARE_DATASETS))}"
        ) from exc

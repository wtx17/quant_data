"""Built-in ClickHouse schemas for the project-supported Minghu tables."""

from __future__ import annotations


MINGHU_TABLE_COLUMN_TYPES: dict[str, tuple[tuple[str, str], ...]] = {
    "stock_base.daily": (
        ("code", "String"),
        ("date", "Date"),
        ("exg", "UInt8"),
        ("open", "Nullable(Float64)"),
        ("high", "Nullable(Float64)"),
        ("low", "Nullable(Float64)"),
        ("close", "Nullable(Float64)"),
        ("pclose", "Nullable(Float64)"),
        ("change", "Nullable(Float64)"),
        ("pct_chg", "Nullable(Float64)"),
        ("volume", "Nullable(Int64)"),
        ("amount", "Nullable(Float64)"),
        ("hfq", "Nullable(Float64)"),
        ("ztprice", "Nullable(Float64)"),
        ("dtprice", "Nullable(Float64)"),
        ("omax_op", "Nullable(Float64)"),
        ("omin_op", "Nullable(Float64)"),
    ),
    "index_base.daily": (
        ("code", "String"),
        ("date", "Date"),
        ("exg", "UInt8"),
        ("open", "Nullable(Float64)"),
        ("high", "Nullable(Float64)"),
        ("low", "Nullable(Float64)"),
        ("close", "Nullable(Float64)"),
        ("pclose", "Nullable(Float64)"),
        ("volume", "Nullable(Int64)"),
        ("amount", "Nullable(Float64)"),
    ),
    "stock_base.m1": (
        ("code", "String"),
        ("date_time", "DateTime('Asia/Shanghai')"),
        ("exg", "UInt8"),
        ("time_int", "Int32"),
        ("open", "Nullable(Float64)"),
        ("close", "Nullable(Float64)"),
        ("high", "Nullable(Float64)"),
        ("low", "Nullable(Float64)"),
        ("volume", "Nullable(Float64)"),
        ("amount", "Nullable(Float64)"),
        ("date", "Date"),
    ),
}

from __future__ import annotations

from pathlib import Path

import pytest

from quant_data.initialize import (
    TUSHARE_DATASET_NAMES,
    clickhouse_registrations,
    initialize_data_client,
    registered_dataset_names,
)


def test_clickhouse_registrations_only_include_panel_datasets() -> None:
    registrations = clickhouse_registrations("research")
    assert [item.name for item in registrations] == [
        "minghu_daily",
        "minghu_index_daily",
        "minghu_m1",
        "zb_cj_flow_min",
    ]

    by_name = {item.name: item for item in registrations}
    index_daily = by_name["minghu_index_daily"]
    assert index_daily.connection == "research"
    assert index_daily.table == "index_base.daily"
    assert index_daily.time_column == "date"
    assert index_daily.frequency == "1d"
    assert index_daily.partition_column is None


def test_registered_dataset_names_match_default_registrations() -> None:
    names = registered_dataset_names()
    assert names == tuple(
        name
        for name in (
            *(item.name for item in clickhouse_registrations()),
            "membership_events",
            *TUSHARE_DATASET_NAMES,
        )
    )


def test_tushare_names_contain_one_entry_per_logical_dataset() -> None:
    assert TUSHARE_DATASET_NAMES == (
        "daily_basic",
        "income",
        "balancesheet",
        "cashflow",
        "fina_indicator",
        "express",
        "forecast",
        "stk_holdernumber",
        "ci_index_member",
        "index_member_all",
    )
    assert not any(name.endswith(("_vip", "_pit")) for name in TUSHARE_DATASET_NAMES)


def test_default_initialization_is_offline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("QUANT_DATA_TUSHARE_TOKEN", raising=False)
    client = initialize_data_client(audit_dir=tmp_path / "audit")
    try:
        assert registered_dataset_names()
    finally:
        client.close()

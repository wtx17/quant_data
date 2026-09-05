from __future__ import annotations

from pathlib import Path

import pytest

from quant_data import BuiltInDatasetSpec
from quant_data.initialize import (
    clickhouse_dataset_specs,
    initialize_data_client,
    registered_dataset_names,
    tushare_dataset_specs,
)


def test_clickhouse_dataset_specs_only_include_panel_datasets() -> None:
    specs = clickhouse_dataset_specs("research")
    assert [spec.name for spec in specs] == [
        "minghu_daily",
        "minghu_index_daily",
        "minghu_m1",
    ]

    by_name = {spec.name: spec for spec in specs}
    index_daily = by_name["minghu_index_daily"]
    assert index_daily.connection == "research"
    assert index_daily.table == "index_base.daily"
    assert index_daily.time_column == "date"
    assert index_daily.frequency == "1d"
    assert index_daily.partition_column is None


def test_registered_dataset_names_match_supported_specs() -> None:
    names = registered_dataset_names()
    assert names == tuple(
        spec.name
        for spec in (*clickhouse_dataset_specs(), BuiltInDatasetSpec(), *tushare_dataset_specs())
    )


def test_tushare_specs_contain_one_entry_per_logical_dataset() -> None:
    specs = tushare_dataset_specs("research")
    assert [spec.name for spec in specs] == [
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
    ]
    assert all(spec.connection == "research" for spec in specs)
    assert all(spec.dataset is None for spec in specs)
    assert not any(spec.name.endswith(("_vip", "_pit")) for spec in specs)


def test_default_initialization_is_offline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("QUANT_DATA_TUSHARE_TOKEN", raising=False)
    client = initialize_data_client(audit_dir=tmp_path / "audit")
    try:
        assert registered_dataset_names()
    finally:
        client.close()

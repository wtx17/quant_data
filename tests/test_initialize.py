from __future__ import annotations

from pathlib import Path

import pytest

from quant_data import FieldNotFoundError
from quant_data.initialize import (
    CLICKHOUSE_PANEL_DEFS,
    TUSHARE_DATASET_NAMES,
    initialize_data_client,
    registered_dataset_names,
)


def test_default_clickhouse_definitions_list_standard_panels() -> None:
    assert [(definition["name"], definition["table"]) for definition in CLICKHOUSE_PANEL_DEFS] == [
        ("minghu_daily", "stock_base.daily"),
        ("minghu_index_daily", "index_base.daily"),
        ("minghu_m1", "stock_base.m1"),
        ("zb_cj_flow_min", "zhangruiqi.zb_cj_flow_min"),
    ]
    minute = next(d for d in CLICKHOUSE_PANEL_DEFS if d["name"] == "minghu_m1")
    assert minute["partition_column"] == "date"
    assert minute["order_columns"] == ("date_time", "code")
    assert minute["frequency"] == "1min"
    daily = next(d for d in CLICKHOUSE_PANEL_DEFS if d["name"] == "minghu_daily")
    assert daily["partition_column"] is None
    assert daily["time_column"] == "date"


def test_initialize_registers_clickhouse_datasets_without_a_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Field validation precedes any connection, so registration is observable offline."""

    monkeypatch.setenv("QUANT_DATA_CLICKHOUSE_HOST", "offline-invalid.example")
    client = initialize_data_client(
        audit_dir=tmp_path / "audit",
        register_tushare=False,
    )
    try:
        for definition in CLICKHOUSE_PANEL_DEFS:
            with pytest.raises(FieldNotFoundError):
                client.get_panel(
                    definition["name"],
                    ["no_such_field"],
                    start="2026-03-02",
                    end="2026-03-03",
                )
        with pytest.raises(FieldNotFoundError):
            client.get_panel(
                "membership_events",
                ["no_such_field"],
                start="2026-03-02",
                end="2026-03-03",
            )
    finally:
        client.close()


def test_registered_dataset_names_match_default_registrations() -> None:
    names = registered_dataset_names()
    assert names == tuple(
        name
        for name in (
            *(str(definition["name"]) for definition in CLICKHOUSE_PANEL_DEFS),
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


def test_default_initialization_requires_local_archive(tmp_path, monkeypatch):
    from quant_data import DatasetRegistrationError

    monkeypatch.delenv("QUANT_DATA_TUSHARE_DATA_DIR", raising=False)
    with pytest.raises(DatasetRegistrationError, match="tushare_data_dir"):
        initialize_data_client(audit_dir=tmp_path / "audit")

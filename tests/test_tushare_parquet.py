from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from quant_data import (
    BackendConnectionError,
    DataClient,
    DatasetRegistrationError,
    InvalidQueryError,
)
from quant_data.backends.tushare_catalog import TUSHARE_DATASETS
from quant_data.initialize import TUSHARE_DATASET_NAMES, initialize_data_client


from tushare_fixtures import (  # noqa: F401
    CalendarClient,
    CalendarFactory,
    make_client,
    write_archive,
    write_daily_basic_archive,
)


def register_local(
    client: DataClient,
    root: Path,
    dataset: str,
    **kwargs: Any,
) -> None:
    client.register_tushare(
        dataset,
        data_dir=root,
        calendar_connection="calendar",
        **kwargs,
    )


def test_local_daily_basic_reads_date_partitions_without_api_calls(
    tmp_path: Path,
) -> None:
    root = tmp_path / "archive"
    write_daily_basic_archive(
        root,
        {
            "20240101": [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20240101",
                    "close": 9.0,
                    "turnover_rate": 1.0,
                    "limit_status": 0,
                }
            ],
            "20240102": [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20240102",
                    "close": 10.0,
                    "turnover_rate": 1.1,
                    "limit_status": 1,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240102",
                    "close": 20.0,
                    "turnover_rate": 2.1,
                    "limit_status": 4,
                },
            ],
            "20240103": [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20240103",
                    "close": 11.0,
                    "turnover_rate": 1.2,
                    "limit_status": 0,
                }
            ],
        },
    )
    client, calendar, factory = make_client(tmp_path)
    register_local(client, root, "daily_basic")

    table = client.get_panel(
        "daily_basic",
        ["close", "limit_status"],
        start="2024-01-02",
        end="2024-01-03",
        instruments=["600000.SH"],
    )
    panel = client.get_panel(
        "daily_basic",
        ["turnover_rate"],
        start="2024-01-02",
        end="2024-01-03",
        instruments=["000001.SZ", "600000.SH"],
    )["turnover_rate"]

    assert list(table["close"].index) == [pd.Timestamp(2024, 1, 2), pd.Timestamp(2024, 1, 3)]
    assert table["close"]["600000.SH"].tolist() == pytest.approx([10.0, 11.0])
    assert table["limit_status"]["600000.SH"].tolist() == [1, 0]
    assert list(panel.index) == [pd.Timestamp(2024, 1, 2), pd.Timestamp(2024, 1, 3)]
    assert list(panel.columns) == ["000001.SZ", "600000.SH"]
    assert panel.loc[pd.Timestamp(2024, 1, 2), "000001.SZ"] == pytest.approx(2.1)
    assert pd.isna(panel.loc[pd.Timestamp(2024, 1, 3), "000001.SZ"])
    assert panel.loc[pd.Timestamp(2024, 1, 3), "600000.SH"] == pytest.approx(1.2)
    assert calendar.calls == []
    assert factory.calls == 0

    audits = [json.loads(path.read_text()) for path in (tmp_path / "audit").rglob("*.json")]
    assert {audit["source"]["backend"] for audit in audits} == {"parquet"}
    assert all("selected_api" not in audit["source"] for audit in audits)
    assert all("calendar_api" not in audit["source"] for audit in audits)


def test_local_pit_panel_only_fetches_trade_calendar(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(
        root,
        "income",
        [
            {
                "ts_code": "600000.SH",
                "ann_date": "20240426",
                "f_ann_date": "20240426",
                "end_date": "20240331",
                "report_type": "1",
                "comp_type": "1",
                "end_type": "1",
                "update_flag": "0",
                "total_revenue": 7.0,
            },
            {
                "ts_code": "600000.SH",
                "ann_date": "20240426",
                "f_ann_date": "20240426",
                "end_date": "20240331",
                "report_type": "2",
                "comp_type": "1",
                "end_type": "1",
                "update_flag": "0",
                "total_revenue": 70.0,
            },
        ],
    )
    client, calendar, factory = make_client(tmp_path)
    register_local(
        client,
        root,
        "income",
        disclosure_lag=0,
        fetch_buffer_days=30,
        fetch_margin_days=5,
    )

    panel = client.get_panel(
        "income",
        ["total_revenue"],
        start="2024-04-25",
        end="2024-04-30",
        instruments=["600000.SH"],
    )["total_revenue"]

    assert pd.isna(panel.loc[pd.Timestamp("2024-04-25"), "600000.SH"])
    assert panel.loc[pd.Timestamp("2024-04-26"), "600000.SH"] == pytest.approx(7.0)
    assert panel.loc[pd.Timestamp("2024-04-30"), "600000.SH"] == pytest.approx(7.0)
    assert factory.calls == 1
    assert calendar.calls
    assert len(calendar.calls) == 1
    assert calendar.calls[0][0].startswith("SELECT DISTINCT `date`")


def test_local_statement_defaults_to_tushare_report_type_one_and_allows_override(
    tmp_path: Path,
) -> None:
    root = tmp_path / "archive"
    write_archive(
        root,
        "income",
        [
            {
                "ts_code": "300180.SZ",
                "ann_date": "20230425",
                "f_ann_date": "20231009",
                "end_date": "20221231",
                "report_type": "1",
                "comp_type": "1",
                "end_type": "4",
                "update_flag": "1",
                "total_revenue": 423.0,
            },
            {
                "ts_code": "300180.SZ",
                "ann_date": "20230425",
                "f_ann_date": "20231009",
                "end_date": "20221231",
                "report_type": "2",
                "comp_type": "1",
                "end_type": "4",
                "update_flag": "1",
                "total_revenue": 91.0,
            },
        ],
        range_start="20220101",
    )
    default_client, _, _ = make_client(tmp_path / "default")
    register_local(default_client, root, "income", fetch_buffer_days=30)

    default_panel = default_client.get_panel(
        "income",
        ["total_revenue"],
        start="2023-10-09",
        end="2023-10-10",
    )["total_revenue"]

    assert default_panel.loc[pd.Timestamp("2023-10-09"), "300180.SZ"] == pytest.approx(423.0)
    default_audit = json.loads(next((tmp_path / "default" / "audit").rglob("*.json")).read_text())
    assert default_audit["source"]["fixed_params"]["report_type"] == "1"

    override_client, _, _ = make_client(tmp_path / "override")
    override_client.register_tushare(
        "income_single_quarter",
        dataset="income",
        data_dir=root,
        calendar_connection="calendar",
        fixed_params={"report_type": "2"},
    )
    override_table = override_client.get_panel(
        "income_single_quarter",
        ["total_revenue"],
        start="2023-10-09",
        end="2023-10-10",
    )

    assert override_table["total_revenue"].loc[pd.Timestamp("2023-10-09"), "300180.SZ"] == 91.0


def test_local_membership_panel_match_interval_semantics(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(
        root,
        "ci_index_member",
        [
            {
                "l1_code": "OLD",
                "l1_name": "old",
                "l2_code": "L2",
                "l2_name": "two",
                "l3_code": "L3",
                "l3_name": "three",
                "ts_code": "600000.SH",
                "name": "PF",
                "in_date": "20200101",
                "out_date": "20240103",
                "is_new": "N",
            },
            {
                "l1_code": "NEW",
                "l1_name": "new",
                "l2_code": "L2",
                "l2_name": "two",
                "l3_code": "L3",
                "l3_name": "three",
                "ts_code": "600000.SH",
                "name": "PF",
                "in_date": "20240104",
                "out_date": None,
                "is_new": "Y",
            },
        ],
    )
    client, calendar, _ = make_client(tmp_path)
    register_local(client, root, "ci_index_member")

    panel = client.get_panel(
        "ci_index_member",
        ["l1_name"],
        start="2024-01-02",
        end="2024-01-08",
        instruments=["600000.SH"],
    )["l1_name"]

    assert panel.loc[pd.Timestamp(2024, 1, 2), "600000.SH"] == "old"
    assert panel.loc[pd.Timestamp(2024, 1, 4), "600000.SH"] == "new"
    assert len(calendar.calls) == 1
    assert calendar.calls[0][0].startswith("SELECT DISTINCT `date`")


def test_local_fixed_params_map_or_fail_at_registration(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(root, "cashflow", [])
    client, _, _ = make_client(tmp_path)
    with pytest.raises(DatasetRegistrationError, match="is_calc"):
        register_local(
            client,
            root,
            "cashflow",
            fixed_params={"is_calc": 1},
        )


def test_local_snapshot_rejects_explicit_and_pit_buffer_overflow(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(root, "income", [])
    client, _, _ = make_client(tmp_path)
    register_local(client, root, "income", fetch_buffer_days=30)

    with pytest.raises(InvalidQueryError, match="starts at"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2023-12-31",
            end="2024-03-31",
        )
    with pytest.raises(InvalidQueryError, match="carry-in buffer"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-01-15",
            end="2024-01-31",
        )


def test_manifest_metadata_mismatch_fails_registration(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    manifest_path = write_archive(root, "income", [])
    manifest = json.loads(manifest_path.read_text())
    manifest["partitions"]["all"]["bytes"] += 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    client, _, _ = make_client(tmp_path)

    with pytest.raises(DatasetRegistrationError, match="size differs"):
        register_local(client, root, "income")


def test_local_initialization_registers_standard_names_without_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    assert list(TUSHARE_DATASET_NAMES) == list(TUSHARE_DATASETS)
    for name in TUSHARE_DATASET_NAMES:
        if name == "daily_basic":
            write_daily_basic_archive(root, {"20240701": []})
        else:
            write_archive(root, name, [])
    monkeypatch.delenv("MISSING_LOCAL_CALENDAR_PASSWORD", raising=False)
    monkeypatch.delenv("QUANT_DATA_CLICKHOUSE_PASSWORD", raising=False)

    client = initialize_data_client(
        audit_dir=tmp_path / "audit",
        register_clickhouse=False,
        tushare_data_dir=root,
        clickhouse_connection="calendar",
        clickhouse_password_env="MISSING_LOCAL_CALENDAR_PASSWORD",
    )

    daily = client.get_panel(
        "daily_basic",
        ["close"],
        start="2024-07-01",
        end="2024-07-01",
    )
    assert daily["close"].empty
    with pytest.raises(BackendConnectionError, match="MISSING_LOCAL_CALENDAR_PASSWORD"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-07-01",
            end="2024-07-05",
        )
    client.close()


def test_local_pit_preserves_revisions_and_requested_identity_fields(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(
        root,
        "income",
        [
            {
                "ts_code": "600000.SH",
                "ann_date": announcement,
                "f_ann_date": announcement,
                "end_date": "20240331",
                "report_type": "1",
                "update_flag": revision,
                "total_revenue": value,
            }
            for announcement, revision, value in [("20240422", "0", 10.0), ("20240424", "1", 11.0)]
        ],
    )
    client, calendar, _ = make_client(tmp_path)
    register_local(client, root, "income", fetch_buffer_days=30)
    panels = client.get_panel(
        "income",
        ["total_revenue", "ann_date", "update_flag", "end_date"],
        start="2024-04-22",
        end="2024-04-25",
        instruments=["600000.SH"],
    )
    assert panels["total_revenue"].loc[pd.Timestamp("2024-04-22"), "600000.SH"] == 10.0
    assert panels["total_revenue"].loc[pd.Timestamp("2024-04-24"), "600000.SH"] == 11.0
    assert panels["update_flag"].loc[pd.Timestamp("2024-04-24"), "600000.SH"] == "1"
    assert pd.Timestamp(
        panels["ann_date"].loc[pd.Timestamp("2024-04-24"), "600000.SH"]
    ) == pd.Timestamp("2024-04-24")
    assert len(calendar.calls) == 1
    assert calendar.calls[0][0].startswith("SELECT DISTINCT `date`")

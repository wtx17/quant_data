"""Point-in-time behavior over local archives and ClickHouse trading dates."""

from __future__ import annotations
import json
from pathlib import Path
from typing import Any
import pandas as pd
import pytest
from quant_data import DataClient, InvalidQueryError, SchemaMismatchError
from tushare_fixtures import make_client as archive_client, write_archive


def income_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = [
        "ts_code",
        "ann_date",
        "f_ann_date",
        "end_date",
        "report_type",
        "comp_type",
        "end_type",
        "update_flag",
        "total_revenue",
    ]
    normalized: list[dict[str, Any]] = []
    for row in rows:
        current = {
            "report_type": "1",
            "comp_type": "1",
            "end_type": "1",
            "update_flag": 0,
            **row,
        }
        normalized.append(current)
    return pd.DataFrame(normalized, columns=columns)


def indicator_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    columns = ["ts_code", "ann_date", "end_date", "update_flag", "roe"]
    normalized = [{"update_flag": 0, **row} for row in rows]
    return pd.DataFrame(normalized, columns=columns)


def make_client(tmp_path, data):
    for dataset, frame in data.items():
        write_archive(
            tmp_path / "archive", dataset, frame.to_dict("records"), range_start="20230101"
        )
    return archive_client(tmp_path)


def register_income(
    client: DataClient,
    *,
    name: str = "income",
    disclosure_lag: int = 0,
) -> None:
    client.register_tushare(
        name,
        dataset=None if name == "income" else "income",
        data_dir=client._audit.root.parent / "archive",
        calendar_connection="calendar",
        disclosure_lag=disclosure_lag,
        fetch_buffer_days=60,
        fetch_margin_days=15,
    )


def test_panel_defaults_to_zero_disclosure_lag(tmp_path: Path) -> None:
    data = {
        "income": income_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240426",
                    "f_ann_date": "20240426",
                    "end_date": "20240331",
                    "total_revenue": 7.0,
                }
            ]
        )
    }
    client, _, _ = make_client(tmp_path, data)
    register_income(client)

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


def test_panel_snaps_weekend_then_applies_trading_session_lag(tmp_path: Path) -> None:
    data = {
        "income": income_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240427",
                    "f_ann_date": "20240427",
                    "end_date": "20240331",
                    "total_revenue": 9.0,
                }
            ]
        )
    }
    client, _, _ = make_client(tmp_path, data)
    register_income(client, disclosure_lag=1)

    panel = client.get_panel(
        "income",
        ["total_revenue"],
        start="2024-04-26",
        end="2024-05-01",
        instruments=["600000.SH"],
    )["total_revenue"]

    assert pd.isna(panel.loc[pd.Timestamp("2024-04-29"), "600000.SH"])
    assert panel.loc[pd.Timestamp("2024-04-30"), "600000.SH"] == pytest.approx(9.0)


def test_late_old_period_revision_does_not_displace_new_period(tmp_path: Path) -> None:
    data = {
        "income": income_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240410",
                    "f_ann_date": "20240410",
                    "end_date": "20231231",
                    "total_revenue": 1.0,
                },
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240422",
                    "f_ann_date": "20240422",
                    "end_date": "20240331",
                    "total_revenue": 2.0,
                },
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240430",
                    "f_ann_date": "20240430",
                    "end_date": "20231231",
                    "update_flag": 1,
                    "total_revenue": 99.0,
                },
            ]
        )
    }
    client, _, _ = make_client(tmp_path, data)
    register_income(client)

    panel = client.get_panel(
        "income",
        ["total_revenue"],
        start="2024-04-19",
        end="2024-05-02",
        instruments=["600000.SH"],
    )["total_revenue"]

    assert panel.loc[pd.Timestamp("2024-04-22"), "600000.SH"] == pytest.approx(2.0)
    assert panel.loc[pd.Timestamp("2024-04-30"), "600000.SH"] == pytest.approx(2.0)


def test_active_period_revision_updates_state(tmp_path: Path) -> None:
    data = {
        "income": income_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240422",
                    "f_ann_date": "20240422",
                    "end_date": "20240331",
                    "total_revenue": 2.0,
                },
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240430",
                    "f_ann_date": "20240430",
                    "end_date": "20240331",
                    "update_flag": 1,
                    "total_revenue": 3.0,
                },
            ]
        )
    }
    client, _, _ = make_client(tmp_path, data)
    register_income(client)

    panel = client.get_panel(
        "income",
        ["total_revenue"],
        start="2024-04-22",
        end="2024-05-01",
        instruments=["600000.SH"],
    )["total_revenue"]

    assert panel.loc[pd.Timestamp("2024-04-29"), "600000.SH"] == pytest.approx(2.0)
    assert panel.loc[pd.Timestamp("2024-04-30"), "600000.SH"] == pytest.approx(3.0)


def test_new_report_explicit_null_is_not_field_level_filled(tmp_path: Path) -> None:
    data = {
        "income": income_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240410",
                    "f_ann_date": "20240410",
                    "end_date": "20231231",
                    "total_revenue": 5.0,
                },
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240422",
                    "f_ann_date": "20240422",
                    "end_date": "20240331",
                    "total_revenue": None,
                },
            ]
        )
    }
    client, _, _ = make_client(tmp_path, data)
    register_income(client)

    panel = client.get_panel(
        "income",
        ["total_revenue"],
        start="2024-04-19",
        end="2024-04-24",
        instruments=["600000.SH"],
    )["total_revenue"]

    assert panel.loc[pd.Timestamp("2024-04-19"), "600000.SH"] == pytest.approx(5.0)
    assert pd.isna(panel.loc[pd.Timestamp("2024-04-22"), "600000.SH"])
    assert pd.isna(panel.loc[pd.Timestamp("2024-04-24"), "600000.SH"])


def test_conflicting_equally_ranked_revisions_are_rejected(tmp_path: Path) -> None:
    row = {
        "ts_code": "600000.SH",
        "ann_date": "20240422",
        "f_ann_date": "20240422",
        "end_date": "20240331",
    }
    data = {
        "income": income_rows(
            [
                {**row, "total_revenue": 2.0},
                {**row, "total_revenue": 3.0},
            ]
        )
    }
    client, _, _ = make_client(tmp_path, data)
    register_income(client)

    with pytest.raises(SchemaMismatchError, match="conflicting equally ranked"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-04-22",
            end="2024-04-24",
            instruments=["600000.SH"],
        )


def test_whole_market_panel_uses_local_archive(tmp_path: Path) -> None:
    data = {
        "income": income_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240422",
                    "f_ann_date": "20240422",
                    "end_date": "20240331",
                    "total_revenue": 2.0,
                },
                {
                    "ts_code": "000004.SZ",
                    "ann_date": "20240422",
                    "f_ann_date": "20240422",
                    "end_date": "20240331",
                    "total_revenue": 3.0,
                },
            ]
        )
    }
    client, fake, _ = make_client(tmp_path, data)
    register_income(client)

    panel = client.get_panel(
        "income",
        ["total_revenue"],
        start="2024-04-22",
        end="2024-04-24",
        instruments=None,
    )["total_revenue"]

    assert list(panel.columns) == ["000004.SZ", "600000.SH"]
    assert panel.loc[pd.Timestamp("2024-04-22"), "000004.SZ"] == pytest.approx(3.0)
    assert len(fake.calls) == 1
    assert fake.calls[0][0].startswith("SELECT DISTINCT `date`")
    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["calendar_aligned"] is True
    assert audit["source"]["format"] == "tushare-archive"
    assert audit["source"]["calendar"]["table"] == "stock_base.daily"


def test_fina_indicator_catalog_uses_ann_date(tmp_path: Path) -> None:
    data = {
        "fina_indicator": indicator_rows(
            [
                {
                    "ts_code": "600000.SH",
                    "ann_date": "20240426",
                    "end_date": "20240331",
                    "roe": 10.5,
                }
            ]
        )
    }
    client, fake, _ = make_client(tmp_path, data)
    client.register_tushare(
        "fina_indicator",
        data_dir=client._audit.root.parent / "archive",
        calendar_connection="calendar",
    )

    panel = client.get_panel(
        "fina_indicator",
        ["roe"],
        start="2024-04-25",
        end="2024-04-29",
        instruments=["600000.SH"],
    )["roe"]

    assert panel.loc[pd.Timestamp("2024-04-26"), "600000.SH"] == pytest.approx(10.5)
    assert len(fake.calls) == 1


def test_disclosure_panel_requires_closed_range(tmp_path: Path) -> None:
    client, _, _ = make_client(tmp_path, {"income": income_rows([])})
    register_income(client)

    with pytest.raises(InvalidQueryError, match="requires both start and end"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-04-25",
            instruments=["600000.SH"],
        )


@pytest.mark.parametrize(
    ("dataset", "field"),
    [
        ("income", "total_revenue"),
        ("balancesheet", "total_assets"),
        ("cashflow", "n_cashflow_act"),
        ("fina_indicator", "roe"),
        ("express", "revenue"),
        ("forecast", "p_change_min"),
        ("stk_holdernumber", "holder_num"),
    ],
)
def test_all_disclosure_datasets_preserve_pit_revisions(
    tmp_path: Path, dataset: str, field: str
) -> None:
    from quant_data.backends.tushare_catalog import TUSHARE_DATASETS

    schema = TUSHARE_DATASETS[dataset]["schema"]
    rows = []
    for announcement, value in [("20240422", 10.0), ("20240424", 11.0)]:
        row = dict.fromkeys(schema.names)
        row.update(ts_code="600000.SH", ann_date=announcement, end_date="20240331")
        if "f_ann_date" in row:
            row["f_ann_date"] = announcement
        row[field] = value
        if "report_type" in row:
            row["report_type"] = "1"
        rows.append(row)
    client, fake, _ = make_client(tmp_path, {dataset: pd.DataFrame(rows)})
    client.register_tushare(
        dataset,
        data_dir=client._audit.root.parent / "archive",
        calendar_connection="calendar",
        fetch_buffer_days=30,
    )

    panel = client.get_panel(
        dataset,
        [field],
        start="2024-04-22",
        end="2024-04-25",
        instruments=["600000.SH", "MISSING.SZ"],
    )[field]

    assert panel.loc[pd.Timestamp("2024-04-22"), "600000.SH"] == 10.0
    assert panel.loc[pd.Timestamp("2024-04-24"), "600000.SH"] == 11.0
    assert panel["MISSING.SZ"].isna().all()
    assert len(fake.calls) == 1
    assert fake.calls[0][1]["start"].isoformat() == "2024-03-23"

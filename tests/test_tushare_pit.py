"""Logical routing and point-in-time semantics for the Tushare backend."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from quant_data import (
    BackendConnectionError,
    DataClient,
    InvalidQueryError,
    RemoteQueryError,
    SchemaMismatchError,
    TushareConfig,
    TushareDatasetSpec,
)


def weekdays(start: date, end: date) -> list[date]:
    result: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


CALENDAR = weekdays(date(2023, 12, 1), date(2024, 6, 30))


class FakeTushareClient:
    def __init__(
        self,
        data: dict[str, pd.DataFrame],
        *,
        fail_apis: set[str] | None = None,
    ) -> None:
        self.data = data
        self.fail_apis = fail_apis or set()
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        self.calls.append((api_name, dict(params)))
        if api_name in self.fail_apis:
            raise RuntimeError(f"forced failure: {api_name}")
        if api_name == "trade_cal":
            start = _parse_date(params["start_date"])
            end = _parse_date(params["end_date"])
            days = [day for day in CALENDAR if start <= day <= end]
            return pd.DataFrame({"cal_date": [day.strftime("%Y%m%d") for day in days]})

        frame = self.data[api_name].copy()
        instrument = params.get("ts_code")
        if instrument is not None:
            frame = frame.loc[frame["ts_code"] == instrument]
        start = params.get("start_date")
        end = params.get("end_date")
        disclosure_column = "f_ann_date" if "f_ann_date" in frame else "ann_date"
        if start is not None:
            frame = frame.loc[frame[disclosure_column].astype(str) >= str(start)]
        if end is not None:
            frame = frame.loc[frame[disclosure_column].astype(str) <= str(end)]
        fields = params.get("fields")
        if fields:
            requested = str(fields).split(",")
            frame = frame.loc[:, [column for column in requested if column in frame]]
        return frame.reset_index(drop=True)


class FakeFactory:
    def __init__(self, client: FakeTushareClient) -> None:
        self.client = client
        self.calls = 0

    def __call__(self, **kwargs: Any) -> FakeTushareClient:
        self.calls += 1
        return self.client


def _parse_date(value: object) -> date:
    return datetime.strptime(str(value), "%Y%m%d").date()


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


def make_client(
    tmp_path: Path,
    data: dict[str, pd.DataFrame],
    *,
    fail_apis: set[str] | None = None,
) -> tuple[DataClient, FakeTushareClient, FakeFactory]:
    fake = FakeTushareClient(data, fail_apis=fail_apis)
    factory = FakeFactory(fake)
    client = DataClient(tmp_path / "audit", tushare_client_factory=factory)
    client.add_tushare_connection("ts", TushareConfig(token="x"))
    return client, fake, factory


def register_income(
    client: DataClient,
    *,
    name: str = "income",
    disclosure_lag: int = 0,
) -> None:
    client.register(
        TushareDatasetSpec(
            name=name,
            dataset=None if name == "income" else "income",
            connection="ts",
            disclosure_lag=disclosure_lag,
            fetch_buffer_days=60,
            fetch_margin_days=15,
        )
    )


def test_registration_is_offline_and_token_is_resolved_on_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MISSING_TUSHARE_TOKEN", raising=False)
    client = DataClient(tmp_path / "audit")
    client.add_tushare_connection(
        "ts", TushareConfig(token=None, token_env="MISSING_TUSHARE_TOKEN")
    )
    client.register(TushareDatasetSpec(name="income", connection="ts"))

    with pytest.raises(BackendConnectionError, match="MISSING_TUSHARE_TOKEN"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-03-31",
            end="2024-03-31",
            instruments=["600000.SH"],
        )


def test_route_failure_does_not_fallback(tmp_path: Path) -> None:
    data = {
        "income": income_rows([]),
        "income_vip": income_rows([]),
    }
    client, fake, _ = make_client(tmp_path, data, fail_apis={"income"})
    register_income(client)

    with pytest.raises(RemoteQueryError, match="income"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-03-31",
            end="2024-03-31",
            instruments=["600000.SH"],
        )
    assert [api for api, _ in fake.calls] == ["income"]


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


def test_whole_market_panel_uses_vip_route(tmp_path: Path) -> None:
    data = {
        "income_vip": income_rows(
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
    data_calls = [(api, params) for api, params in fake.calls if api != "trade_cal"]
    assert [api for api, _ in data_calls] == ["income_vip"]
    assert "ts_code" not in data_calls[0][1]
    audit_path = next((tmp_path / "audit").rglob("*.json"))
    audit = json.loads(audit_path.read_text())
    assert audit["calendar_aligned"] is True
    assert audit["source"]["selected_api"] == "income_vip"
    assert audit["source"]["calendar_api"] == "trade_cal"


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
    client.register(TushareDatasetSpec(name="fina_indicator", connection="ts"))

    panel = client.get_panel(
        "fina_indicator",
        ["roe"],
        start="2024-04-25",
        end="2024-04-29",
        instruments=["600000.SH"],
    )["roe"]

    assert panel.loc[pd.Timestamp("2024-04-26"), "600000.SH"] == pytest.approx(10.5)
    params = next(params for api, params in fake.calls if api == "fina_indicator")
    assert "ann_date" in str(params["fields"]).split(",")
    assert "f_ann_date" not in str(params["fields"]).split(",")


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

    schema = TUSHARE_DATASETS[dataset].schema
    rows = []
    for announcement, value in [("20240422", 10.0), ("20240424", 11.0)]:
        row = dict.fromkeys(schema.names)
        row.update(ts_code="600000.SH", ann_date=announcement, end_date="20240331")
        if "f_ann_date" in row:
            row["f_ann_date"] = announcement
        row[field] = value
        rows.append(row)
    client, fake, _ = make_client(tmp_path, {dataset: pd.DataFrame(rows)})
    client.register(TushareDatasetSpec(name=dataset, connection="ts", fetch_buffer_days=30))

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
    calls = [params for api, params in fake.calls if api == dataset]
    assert len(calls) == 2
    assert all("period" not in params for params in calls)
    assert all(params["start_date"] == "20240323" for params in calls)

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from quant_data import (
    DataClient,
    InvalidQueryError,
    RemoteQueryError,
    TushareConfig,
    TushareDatasetSpec,
)


class FakeDailyBasicClient:
    def __init__(
        self,
        rows: pd.DataFrame,
        sessions: list[date],
    ) -> None:
        self.rows = rows
        self.sessions = sessions
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        self.calls.append((api_name, dict(params)))
        if api_name == "trade_cal":
            start = _parse_date(params["start_date"])
            end = _parse_date(params["end_date"])
            days = [day for day in self.sessions if start <= day <= end]
            return pd.DataFrame({"cal_date": [day.strftime("%Y%m%d") for day in days]})
        if api_name != "daily_basic":
            raise AssertionError(f"Unexpected Tushare api: {api_name}")

        trade_date = str(params["trade_date"])
        frame = self.rows.loc[self.rows["trade_date"].astype(str) == trade_date].copy()
        fields = str(params["fields"]).split(",")
        return frame.loc[:, fields].reset_index(drop=True)


class FakeFactory:
    def __init__(self, client: FakeDailyBasicClient) -> None:
        self.client = client

    def __call__(self, **kwargs: Any) -> FakeDailyBasicClient:
        return self.client


def _parse_date(value: object) -> date:
    return datetime.strptime(str(value), "%Y%m%d").date()


def make_client(
    tmp_path: Path,
    rows: pd.DataFrame,
    sessions: list[date],
) -> tuple[DataClient, FakeDailyBasicClient]:
    fake = FakeDailyBasicClient(rows, sessions)
    client = DataClient(
        tmp_path / "audit",
        tushare_client_factory=FakeFactory(fake),
    )
    client.add_tushare_connection("ts", TushareConfig(token="x"))
    client.register(TushareDatasetSpec(name="daily_basic", connection="ts"))
    return client, fake


def test_daily_basic_panel_fetches_each_open_date_and_uses_generic_pivot(
    tmp_path: Path,
) -> None:
    rows = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_date": "20240102",
                "close": 10.0,
                "pe": 5.0,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "close": 20.0,
                "pe": 6.0,
            },
            {
                "ts_code": "300001.SZ",
                "trade_date": "20240102",
                "close": 30.0,
                "pe": 7.0,
            },
            {
                "ts_code": "600000.SH",
                "trade_date": "20240103",
                "close": 11.0,
                "pe": 5.1,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240103",
                "close": 21.0,
                "pe": 6.1,
            },
        ]
    )
    sessions = [date(2024, 1, 2), date(2024, 1, 3)]
    client, fake = make_client(tmp_path, rows, sessions)

    panels = client.get_panel(
        "daily_basic",
        ["close", "pe"],
        start="2024-01-01",
        end="2024-01-03",
        instruments=["000001.SZ", "600000.SH", "MISSING.SZ"],
    )

    close = panels["close"]
    assert list(close.index) == sessions
    assert list(close.columns) == [
        "000001.SZ",
        "600000.SH",
        "MISSING.SZ",
    ]
    assert close.loc[date(2024, 1, 2), "000001.SZ"] == pytest.approx(20.0)
    assert close.loc[date(2024, 1, 3), "600000.SH"] == pytest.approx(11.0)
    assert close["MISSING.SZ"].isna().all()
    assert panels["pe"].loc[date(2024, 1, 3), "000001.SZ"] == pytest.approx(6.1)

    data_calls = [params for api_name, params in fake.calls if api_name == "daily_basic"]
    assert [params["trade_date"] for params in data_calls] == [
        "20240102",
        "20240103",
    ]
    assert all("ts_code" not in params for params in data_calls)
    assert all("start_date" not in params for params in data_calls)
    assert all("end_date" not in params for params in data_calls)
    assert all(
        str(params["fields"]).split(",") == ["trade_date", "ts_code", "close", "pe"]
        for params in data_calls
    )

    audit_path = next((tmp_path / "audit").rglob("*.json"))
    audit = json.loads(audit_path.read_text())
    assert audit["parameters"]["data_api"] == "daily_basic"
    assert audit["parameters"]["calendar_api"] == "trade_cal"
    assert audit["source"]["selected_api"] == "daily_basic"
    assert audit["source"]["calendar_api"] == "trade_cal"


def test_daily_basic_panel_filters_instruments_after_daily_fetch(
    tmp_path: Path,
) -> None:
    rows = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_date": "20240103",
                "turnover_rate": 1.2,
                "limit_status": 1,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240102",
                "turnover_rate": 2.3,
                "limit_status": 4,
            },
            {
                "ts_code": "600000.SH",
                "trade_date": "20240102",
                "turnover_rate": 1.1,
                "limit_status": 0,
            },
        ]
    )
    sessions = [date(2024, 1, 2), date(2024, 1, 3)]
    client, fake = make_client(tmp_path, rows, sessions)

    table = client.get_panel(
        "daily_basic",
        ["turnover_rate", "limit_status"],
        start="2024-01-02",
        end="2024-01-03",
        instruments=["600000.SH"],
    )

    assert list(table["turnover_rate"].index) == sessions
    assert list(table["turnover_rate"].columns) == ["600000.SH"]
    assert table["turnover_rate"]["600000.SH"].tolist() == pytest.approx([1.1, 1.2])
    assert table["limit_status"]["600000.SH"].tolist() == [0, 1]
    assert table["limit_status"]["600000.SH"].dtype == "int64[pyarrow]"

    data_calls = [params for api_name, params in fake.calls if api_name == "daily_basic"]
    assert len(data_calls) == 2
    assert all("ts_code" not in params for params in data_calls)


def test_daily_basic_requires_closed_range_for_panel(
    tmp_path: Path,
) -> None:
    rows = pd.DataFrame(columns=["ts_code", "trade_date", "close"])
    client, fake = make_client(tmp_path, rows, [])

    with pytest.raises(InvalidQueryError, match="requires both start and end"):
        client.get_panel(
            "daily_basic",
            ["close"],
            start="2024-01-02",
        )
    with pytest.raises(InvalidQueryError, match="requires both start and end"):
        client.get_panel(
            "daily_basic",
            ["close"],
            end="2024-01-03",
        )

    assert fake.calls == []


def test_daily_basic_rejects_a_day_at_the_api_row_limit(tmp_path: Path) -> None:
    trade_date = date(2024, 1, 2)
    rows = pd.DataFrame(
        {
            "ts_code": [f"{index:06d}.SZ" for index in range(6000)],
            "trade_date": ["20240102"] * 6000,
            "close": [1.0] * 6000,
        }
    )
    client, _ = make_client(tmp_path, rows, [trade_date])

    with pytest.raises(RemoteQueryError, match="6000-row API limit"):
        client.get_panel(
            "daily_basic",
            ["close"],
            start=trade_date,
            end=trade_date,
        )

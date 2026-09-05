from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from quant_data import DataClient, TushareConfig, TushareDatasetSpec


def weekdays(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


class FakeTushareClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.members = pd.DataFrame(
            [
                {
                    "l1_code": "CI1OLD",
                    "l1_name": "old",
                    "l2_code": "CI2OLD",
                    "l2_name": "old-2",
                    "l3_code": "CI3OLD",
                    "l3_name": "old-3",
                    "ts_code": "600000.SH",
                    "name": "PF",
                    "in_date": "20200101",
                    "out_date": "20240103",
                    "is_new": "N",
                },
                {
                    "l1_code": "CI1NEW",
                    "l1_name": "new",
                    "l2_code": "CI2NEW",
                    "l2_name": "new-2",
                    "l3_code": "CI3NEW",
                    "l3_name": "new-3",
                    "ts_code": "600000.SH",
                    "name": "PF",
                    "in_date": "20240104",
                    "out_date": "",
                    "is_new": "Y",
                },
                {
                    "l1_code": "CI1SZ",
                    "l1_name": "sz",
                    "l2_code": "CI2SZ",
                    "l2_name": "sz-2",
                    "l3_code": "CI3SZ",
                    "l3_name": "sz-3",
                    "ts_code": "000004.SZ",
                    "name": "GH",
                    "in_date": "20240102",
                    "out_date": "",
                    "is_new": "Y",
                },
            ]
        )

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        self.calls.append((api_name, dict(params)))
        if api_name == "trade_cal":
            start = datetime.strptime(str(params["start_date"]), "%Y%m%d").date()
            end = datetime.strptime(str(params["end_date"]), "%Y%m%d").date()
            return pd.DataFrame(
                {"cal_date": [day.strftime("%Y%m%d") for day in weekdays(start, end)]}
            )
        if api_name not in {"ci_index_member", "index_member_all"}:
            raise AssertionError(api_name)
        frame = self.members
        ts_code = params.get("ts_code")
        if ts_code is not None:
            frame = frame[frame["ts_code"] == ts_code]
        is_new = params.get("is_new")
        if is_new is not None:
            frame = frame[frame["is_new"] == is_new]
        fields = params.get("fields")
        if fields:
            frame = frame.loc[:, [column for column in str(fields).split(",") if column]]
        return frame.reset_index(drop=True)


class FakeFactory:
    def __init__(self, client: FakeTushareClient) -> None:
        self.client = client

    def __call__(self, **kwargs: Any) -> FakeTushareClient:
        return self.client


def make_client(tmp_path: Path) -> tuple[DataClient, FakeTushareClient]:
    fake = FakeTushareClient()
    client = DataClient(tmp_path / "audit", tushare_client_factory=FakeFactory(fake))
    client.add_tushare_connection("ts", TushareConfig(token="x"))
    return client, fake


def test_ci_index_member_expands_intervals_to_daily_panel(tmp_path: Path) -> None:
    client, fake = make_client(tmp_path)
    client.register(
        TushareDatasetSpec(
            name="citic_industry",
            connection="ts",
            dataset="ci_index_member",
        )
    )

    panels = client.get_panel(
        "citic_industry",
        fields=["l1_name", "l3_code"],
        start="2024-01-02",
        end="2024-01-08",
        instruments=["600000.SH", "000004.SZ", "MISSING.SH"],
    )

    l1_name = panels["l1_name"]
    assert list(l1_name.index) == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ]
    assert list(l1_name.columns) == ["600000.SH", "000004.SZ", "MISSING.SH"]
    assert l1_name.loc[date(2024, 1, 2), "600000.SH"] == "old"
    assert l1_name.loc[date(2024, 1, 4), "600000.SH"] == "new"
    assert l1_name.loc[date(2024, 1, 2), "000004.SZ"] == "sz"
    assert l1_name["MISSING.SH"].isna().all()

    member_calls = [params for api_name, params in fake.calls if api_name == "ci_index_member"]
    assert len(member_calls) == 6
    assert all("date" not in str(params["fields"]).split(",") for params in member_calls)


def test_ci_index_member_whole_industry_query_does_not_send_ts_code(tmp_path: Path) -> None:
    client, fake = make_client(tmp_path)
    client.register(
        TushareDatasetSpec(
            name="citic_electronics",
            connection="ts",
            dataset="ci_index_member",
            fixed_params={"l2_code": "CI005835.CI"},
        )
    )

    panels = client.get_panel(
        "citic_electronics",
        fields=["l1_name"],
        start="2024-01-02",
        end="2024-01-03",
        instruments=None,
    )

    assert sorted(panels["l1_name"].columns.tolist()) == ["000004.SZ", "600000.SH"]
    member_calls = [params for api_name, params in fake.calls if api_name == "ci_index_member"]
    assert len(member_calls) == 2
    assert all("ts_code" not in params for params in member_calls)
    assert all(params["l2_code"] == "CI005835.CI" for params in member_calls)


def test_index_member_all_expands_intervals_to_daily_panel(tmp_path: Path) -> None:
    client, fake = make_client(tmp_path)
    client.register(
        TushareDatasetSpec(
            name="sw_industry",
            connection="ts",
            dataset="index_member_all",
        )
    )

    panels = client.get_panel(
        "sw_industry",
        fields=["l1_name", "l3_code"],
        start="2024-01-02",
        end="2024-01-08",
        instruments=["600000.SH", "000004.SZ", "MISSING.SH"],
    )

    l1_name = panels["l1_name"]
    assert list(l1_name.index) == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
        date(2024, 1, 5),
        date(2024, 1, 8),
    ]
    assert list(l1_name.columns) == ["600000.SH", "000004.SZ", "MISSING.SH"]
    assert l1_name.loc[date(2024, 1, 2), "600000.SH"] == "old"
    assert l1_name.loc[date(2024, 1, 4), "600000.SH"] == "new"
    assert l1_name.loc[date(2024, 1, 2), "000004.SZ"] == "sz"
    assert l1_name["MISSING.SH"].isna().all()

    member_calls = [params for api_name, params in fake.calls if api_name == "index_member_all"]
    assert len(member_calls) == 6
    assert all("date" not in str(params["fields"]).split(",") for params in member_calls)


def test_index_member_all_whole_industry_query_does_not_send_ts_code(tmp_path: Path) -> None:
    client, fake = make_client(tmp_path)
    client.register(
        TushareDatasetSpec(
            name="sw_gold",
            connection="ts",
            dataset="index_member_all",
            fixed_params={"l3_code": "850531.SI"},
        )
    )

    panels = client.get_panel(
        "sw_gold",
        fields=["l1_name"],
        start="2024-01-02",
        end="2024-01-03",
        instruments=None,
    )

    assert sorted(panels["l1_name"].columns.tolist()) == ["000004.SZ", "600000.SH"]
    member_calls = [params for api_name, params in fake.calls if api_name == "index_member_all"]
    assert len(member_calls) == 2
    assert all("ts_code" not in params for params in member_calls)
    assert all(params["l3_code"] == "850531.SI" for params in member_calls)


def test_fixed_membership_status_uses_one_request(tmp_path: Path) -> None:
    client, fake = make_client(tmp_path)
    client.register(
        TushareDatasetSpec(
            name="current_members",
            dataset="ci_index_member",
            connection="ts",
            fixed_params={"is_new": "Y"},
        )
    )

    table = client.get_panel(
        "current_members",
        fields=["l1_name", "is_new"],
        start="2024-01-02",
        end="2024-01-08",
        instruments=None,
    )

    assert set(table["is_new"].stack().tolist()) == {"Y"}
    member_calls = [params for api, params in fake.calls if api == "ci_index_member"]
    assert len(member_calls) == 1
    assert member_calls[0]["is_new"] == "Y"

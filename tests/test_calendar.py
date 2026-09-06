"""The local event readers use market dates, independent of stock selection."""

import json
from datetime import date, datetime

import pandas as pd
import pyarrow as pa
import pytest

from quant_data import ClickHouseConfig, DataClient, RemoteQueryError, SchemaMismatchError
from quant_data.backends.clickhouse import prepare_clickhouse_table, read_trade_calendar
from tushare_fixtures import CalendarClient, industry_frame, write_archive


@pytest.mark.parametrize("instruments", [None, [], ["600000.SH", "missing"]])
def test_calendar_holiday_and_session_reuse(tmp_path, instruments):
    root = tmp_path / "archive"
    write_archive(
        root, "ci_index_member", industry_frame().to_dict("records"), range_start="20190101"
    )
    calendar = CalendarClient(excluded=[date(2024, 1, 3)])
    factories = []

    def factory(**kwargs):
        factories.append(kwargs)
        return calendar

    with DataClient(tmp_path / "audit", clickhouse_client_factory=factory) as client:
        client.add_clickhouse_connection("calendar", ClickHouseConfig(host="fake"))
        client.register_tushare("ci_index_member", data_dir=root, calendar_connection="calendar")
        for _ in range(2):
            panel = client.get_panel(
                "ci_index_member", ["l1_name"], "2024-01-02", "2024-01-05", instruments
            )["l1_name"]
            assert list(panel.index) == list(
                pd.to_datetime(["2024-01-02", "2024-01-04", "2024-01-05"]).date
                if instruments != []
                else []
            )
        assert len(factories) == 1
        assert len(calendar.calls) == 2
        assert panel.attrs["parameters"]["calendar_table"] == "stock_base.daily"


@pytest.mark.parametrize(
    "result",
    [pa.table({"date": [None]}), pa.table({"wrong": [1]}), pa.table({"date": ["20240102"]})],
)
def test_calendar_rejects_invalid_dates(tmp_path, result):
    class BadCalendar:
        def query_arrow(self, *args, **kwargs):
            return result

        def close(self):
            pass

    with DataClient(
        tmp_path / "audit", clickhouse_client_factory=lambda **kw: BadCalendar()
    ) as client:
        client.add_clickhouse_connection("calendar", ClickHouseConfig(host="fake"))
        source = prepare_clickhouse_table(
            client._clickhouse,
            connection="calendar",
            table="stock_base.daily",
            time_column="date",
            instrument_column="code",
        )
        with pytest.raises(SchemaMismatchError):
            read_trade_calendar(
                client._clickhouse, source, datetime(2024, 1, 2), datetime(2024, 1, 5)
            )


def test_calendar_failure_is_audited_without_remote_fallback(tmp_path):
    root = tmp_path / "archive"
    write_archive(
        root, "ci_index_member", industry_frame().to_dict("records"), range_start="20190101"
    )

    class BrokenCalendar:
        def query_arrow(self, *args, **kwargs):
            raise RuntimeError("private-secret")

        def close(self):
            pass

    with DataClient(
        tmp_path / "audit", clickhouse_client_factory=lambda **kw: BrokenCalendar()
    ) as client:
        client.add_clickhouse_connection("calendar", ClickHouseConfig(host="fake"))
        client.register_tushare("ci_index_member", data_dir=root, calendar_connection="calendar")
        with pytest.raises(RemoteQueryError, match="calendar query failed") as error:
            client.get_panel("ci_index_member", ["l1_name"], "2024-01-02", "2024-01-05")
        assert "private-secret" not in str(error.value)
    records = list((tmp_path / "audit").rglob("*.json"))
    assert len(records) == 1
    text = records[0].read_text()
    assert "private-secret" not in text
    record = json.loads(text)
    assert record["parameters"]["calendar_table"] == "stock_base.daily"
    assert record["source"]["calendar"]["table"] == "stock_base.daily"

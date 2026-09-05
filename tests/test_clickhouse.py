from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pandas as pd
import pytest

from quant_data import (
    BackendConnectionError,
    ClickHouseConfig,
    ClickHouseDatasetSpec,
    DataClient,
    DatasetRegistrationError,
    InvalidQueryError,
)
from quant_data.initialize import clickhouse_dataset_specs


CLICKHOUSE_TYPES = {
    "code": "String",
    "date": "Date",
    "exg": "UInt8",
    "date_time": "DateTime64(3, 'Asia/Shanghai')",
    "close": "Nullable(Float64)",
    "volume": "Nullable(Int64)",
    "hfq": "Nullable(Float64)",
}
SUFFIX_EXPRESSION = (
    "concat(`_q`.`code`, multiIf(`_q`.`exg` = 1, '.SZ', "
    "`_q`.`exg` = 2, '.SH', `_q`.`exg` = 3, '.BJ', ''))"
)


class FakeClickHouseClient:
    def __init__(self, result: pa.Table) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, object], bool | None]] = []
        self.closed = False

    def query_arrow(
        self,
        query: str,
        parameters: dict[str, object] | None = None,
        use_strings: bool | None = None,
    ) -> pa.Table:
        self.calls.append((query, parameters or {}, use_strings))
        if query.startswith("DESCRIBE TABLE"):
            return pa.table(
                {
                    "name": list(CLICKHOUSE_TYPES),
                    "type": list(CLICKHOUSE_TYPES.values()),
                }
            )
        if query.endswith("LIMIT 0"):
            raise RuntimeError("LIMIT 0 must not scan the remote table during registration")
        return self.result

    def close(self) -> None:
        self.closed = True


class FakeFactory:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs: Any) -> FakeClickHouseClient:
        self.calls.append(kwargs)
        return self.client


def make_remote_client(
    tmp_path: Path,
    result: pa.Table,
    *,
    password: str | None = "secret",
    password_env: str | None = None,
) -> tuple[DataClient, FakeClickHouseClient, FakeFactory]:
    fake = FakeClickHouseClient(result)
    factory = FakeFactory(fake)
    client = DataClient(tmp_path / "audit", clickhouse_client_factory=factory)
    client.add_clickhouse_connection(
        "minghu",
        ClickHouseConfig(
            host="chdb.tradegdb.com",
            username="researcher",
            password=password,
            password_env=password_env,
        ),
    )
    return client, fake, factory


def test_daily_panel_and_connection_reuse(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2), date(2026, 3, 2)]),
            "code": ["000002.SZ", "000001.SZ"],
            "close": [11.2, 10.3],
            "hfq": [2.0, 2.0],
        }
    )
    client, fake, factory = make_remote_client(tmp_path, result)
    spec = ClickHouseDatasetSpec(
        name="minghu_daily",
        connection="minghu",
        table="stock_base.daily",
        time_column="date",
        frequency="1d",
    )
    client.register(spec)
    assert fake.calls == []
    assert factory.calls == []
    panel = client.get_panel("minghu_daily", ["close"], start="2026-03-02", end="2026-03-02")[
        "close"
    ]

    assert list(panel.columns) == ["000001.SZ", "000002.SZ"]
    assert panel.loc[date(2026, 3, 2), "000001.SZ"] == pytest.approx(20.6)
    assert panel.loc[date(2026, 3, 2), "000002.SZ"] == pytest.approx(22.4)
    assert panel.attrs["adjusted"] is True
    assert len(factory.calls) == 1
    assert factory.calls[0]["password"] == "secret"
    sql, parameters, _ = fake.calls[-1]
    assert f"{SUFFIX_EXPRESSION} AS `code`" in sql
    assert "SELECT `_q`.`date` AS `date`" in sql
    assert "FROM `stock_base`.`daily` AS `_q`" in sql
    assert "{start:Date}" in sql and parameters["start"] == date(2026, 3, 2)

    client.close()
    assert fake.closed


def test_all_builtin_minghu_specs_register_without_connecting(tmp_path: Path) -> None:
    result = pa.table({"date": [], "code": [], "close": []})
    client, fake, factory = make_remote_client(tmp_path, result)

    for spec in clickhouse_dataset_specs("minghu"):
        client.register(spec)

    assert fake.calls == []
    assert factory.calls == []


def test_daily_rejects_unsuffixed_instruments(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2)]),
            "code": ["000001.SZ"],
            "close": [10.3],
            "hfq": [2.0],
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )

    with pytest.raises(InvalidQueryError, match="requires instrument identifiers"):
        client.get_panel(
            "daily",
            ["close"],
            start="2026-03-02",
            end="2026-03-02",
            instruments=["000001"],
        )

    assert fake.calls == []


def test_daily_accepts_suffixed_instruments(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2)]),
            "code": ["000001.SZ"],
            "close": [10.3],
            "hfq": [2.0],
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )

    table = client.get_panel(
        "daily",
        ["close"],
        start="2026-03-02",
        end="2026-03-02",
        instruments=["000001.SZ"],
    )

    assert list(table["close"].columns) == ["000001.SZ"]
    sql, parameters, _ = fake.calls[-1]
    assert f"{SUFFIX_EXPRESSION} IN {{instruments:Array(String)}}" in sql
    assert "000001.SZ" not in sql
    assert parameters["instruments"] == ["000001.SZ"]


def test_daily_named_universe_is_expanded_as_bound_parameters(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2)]),
            "code": ["600028.SH"],
            "close": [10.3],
            "hfq": [1.0],
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )

    panel = client.get_panel(
        "daily",
        ["close"],
        start="2026-03-02",
        end="2026-03-02",
        adjusted=False,
        universe="HS300",
    )["close"]

    assert len(panel.columns) == 300
    assert panel.columns[0] == "000001.SZ"
    assert panel.loc[date(2026, 3, 2), "600028.SH"] == pytest.approx(10.3)
    sql, parameters, _ = fake.calls[-1]
    assert parameters["instruments"][0] == "000001.SZ"
    assert len(parameters["instruments"]) == 300
    assert "600028.SH" not in sql
    assert f"{SUFFIX_EXPRESSION} IN {{instruments:Array(String)}}" in sql


def test_index_daily_supports_panel_with_suffixes(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2), date(2026, 3, 2)]),
            "code": ["000001.SH", "399001.SZ"],
            "close": [3350.4, 10820.2],
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="minghu_index_daily",
            connection="minghu",
            table="index_base.daily",
            time_column="date",
            frequency="1d",
        )
    )

    with pytest.raises(InvalidQueryError, match="requires instrument identifiers"):
        client.get_panel(
            "minghu_index_daily",
            ["close"],
            start="2026-03-02",
            end="2026-03-02",
            instruments=["000001"],
        )

    instruments = ["399001.SZ", "000001.SH"]
    panel = client.get_panel(
        "minghu_index_daily",
        ["close"],
        start="2026-03-02",
        end="2026-03-02",
        instruments=instruments,
    )["close"]
    assert list(panel.columns) == instruments
    assert panel.loc[date(2026, 3, 2), "000001.SH"] == pytest.approx(3350.4)
    assert panel.attrs["adjusted"] is False

    panel_sql, panel_parameters, _ = fake.calls[-1]
    assert "FROM `index_base`.`daily` AS `_q`" in panel_sql
    assert f"{SUFFIX_EXPRESSION} AS `code`" in panel_sql
    assert f"{SUFFIX_EXPRESSION} IN {{instruments:Array(String)}}" in panel_sql
    assert panel_parameters["instruments"] == instruments


def test_daily_can_return_raw_prices_and_hides_factor(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2)]),
            "code": ["000001.SZ"],
            "close": [10.3],
            "volume": pa.array([100], type=pa.int64()),
            "hfq": [2.0],
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )
    table = client.get_panel("daily", ["close", "volume"], adjusted=False)
    assert list(table) == ["close", "volume"]
    assert list(table["close"].columns) == ["000001.SZ"]
    assert table["close"]["000001.SZ"].tolist() == [10.3]
    assert table["volume"]["000001.SZ"].tolist() == [100]
    assert "`hfq`" not in fake.calls[-1][0]
    assert table["close"].attrs["parameters"]["adjusted"] is False
    assert table["close"].attrs["adjusted"] is False


def test_daily_adjustment_preserves_null_factor(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2)]),
            "code": ["000001.SZ"],
            "close": [10.3],
            "hfq": pa.array([None], type=pa.float64()),
        }
    )
    client, _, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )
    table = client.get_panel("daily", ["close"])
    assert table["close"].isna().all().all()


def test_m1_requires_range_and_pushes_partition_filter(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date_time": pa.array([], type=pa.timestamp("ms", tz="Asia/Shanghai")),
            "code": pa.array([], type=pa.string()),
            "close": pa.array([], type=pa.float64()),
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="minghu_m1",
            connection="minghu",
            table="stock_base.m1",
            time_column="date_time",
            partition_column="date",
            order_columns=("date_time", "code"),
            frequency="1min",
        )
    )
    with pytest.raises(InvalidQueryError, match="requires both start and end"):
        client.get_panel("minghu_m1", ["close"], start="2026-03-02")

    client.get_panel(
        "minghu_m1",
        ["close"],
        start="2026-03-02 09:30",
        end="2026-03-02 09:31",
        instruments=["x'); DROP TABLE stock_base.m1; --.SZ"],
    )
    sql, parameters, _ = fake.calls[-1]
    assert "`_q`.`date` >= {partition_start:Date}" in sql
    assert "LIMIT" not in sql
    assert "DROP TABLE" not in sql
    assert parameters["instruments"] == ["x'); DROP TABLE stock_base.m1; --.SZ"]
    assert parameters["partition_start"] == date(2026, 3, 2)


@pytest.mark.parametrize("date_type", ["Date", "Date32", "UInt32"])
@pytest.mark.parametrize("physical_time", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_minute_synthesized_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    date_type: str,
    physical_time: bool,
    empty: bool,
) -> None:
    columns = {"date": date_type, "time_int": "Int32", "code": "String", "close": "Float64"}
    if physical_time:
        columns["date_time"] = "Int64"
    monkeypatch.setattr(f"{__name__}.CLICKHOUSE_TYPES", columns)
    expected = pd.Timestamp("2026-03-02 09:30:00.123", tz="Asia/Shanghai")
    result = pa.table(
        {
            "date_time": pa.array(
                [] if empty else [expected], type=pa.timestamp("ms", tz="Asia/Shanghai")
            ),
            "code": pa.array([] if empty else ["000001.SZ"], type=pa.string()),
            "close": pa.array([] if empty else [10.3], type=pa.float64()),
        }
    )
    client, fake, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="minute",
            connection="minghu",
            table="custom.minute",
            time_column="date_time",
            partition_column="date",
            order_columns=("date_time", "code"),
        )
    )
    panel = client.get_panel(
        "minute", ["close"], start=expected, end=expected, instruments=["000001.SZ"]
    )["close"]
    assert isinstance(panel.index, pd.DatetimeIndex)
    assert str(panel.index.tz) == "Asia/Shanghai"
    if not empty:
        assert panel.loc[expected, "000001.SZ"] == 10.3
    sql, parameters, _ = fake.calls[-1]
    assert "`_q`.`date_time`" not in sql
    assert "toIntervalMillisecond(`_q`.`time_int`)" in sql
    assert "AS `date_time`" in sql
    assert "start:DateTime64(3, 'Asia/Shanghai')" in sql
    assert ("YYYYMMDDToDate(`_q`.`date`)" in sql) == (date_type == "UInt32")
    assert parameters["partition_start"] == (20260302 if date_type == "UInt32" else expected.date())
    assert parameters["partition_end"] == parameters["partition_start"]
    assert parameters["start"] == "2026-03-02 09:30:00.123000"
    assert parameters["end"] == parameters["start"]
    empty_panel = client.get_panel(
        "minute", ["close"], start=expected, end=expected, instruments=[]
    )["close"]
    assert isinstance(empty_panel.index, pd.DatetimeIndex)
    assert str(empty_panel.index.tz) == "Asia/Shanghai"
    client.close()


def test_remote_audit_is_sanitized(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([], type=pa.date32()),
            "code": pa.array([], type=pa.string()),
            "close": pa.array([], type=pa.float64()),
            "hfq": pa.array([], type=pa.float64()),
        }
    )
    client, _, _ = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )
    client.get_panel("daily", ["close"])
    audit_text = next((tmp_path / "audit").rglob("*.json")).read_text()
    audit = json.loads(audit_text)
    assert audit["source"]["backend"] == "clickhouse"
    assert audit["source"]["table"] == "stock_base.daily"
    assert audit["source"]["schema_source"] == "catalog"
    assert audit["adjusted"] is True
    assert "secret" not in audit_text
    assert "researcher" not in audit_text


def test_missing_password_environment_variable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MISSING_CLICKHOUSE_PASSWORD", raising=False)
    result = pa.table({"date": [], "code": [], "close": []})
    client, _, _ = make_remote_client(
        tmp_path,
        result,
        password=None,
        password_env="MISSING_CLICKHOUSE_PASSWORD",
    )
    client.register(
        ClickHouseDatasetSpec(
            name="daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
        )
    )
    with pytest.raises(BackendConnectionError, match="is not set"):
        client.get_panel("daily", ["close"])


def test_custom_table_falls_back_to_remote_describe(tmp_path: Path) -> None:
    result = pa.table(
        {
            "date": pa.array([date(2026, 3, 2)]),
            "code": ["000001.SZ"],
            "close": [10.3],
        }
    )
    client, fake, factory = make_remote_client(tmp_path, result)
    client.register(
        ClickHouseDatasetSpec(
            name="custom_daily",
            connection="minghu",
            table="custom.daily",
            time_column="date",
        )
    )

    assert [call[0] for call in fake.calls] == ["DESCRIBE TABLE `custom`.`daily`"]
    assert len(factory.calls) == 1
    table = client.get_panel("custom_daily", ["close"])
    assert table["close"]["000001.SZ"].tolist() == [10.3]

    audit_text = next((tmp_path / "audit").rglob("*.json")).read_text()
    audit = json.loads(audit_text)
    assert audit["source"]["schema_source"] == "remote"


def test_catalog_schema_validation_does_not_connect(tmp_path: Path) -> None:
    result = pa.table({"date": [], "code": [], "close": []})
    client, fake, factory = make_remote_client(tmp_path, result)

    with pytest.raises(DatasetRegistrationError, match="missing configured columns"):
        client.register(
            ClickHouseDatasetSpec(
                name="invalid_daily",
                connection="minghu",
                table="stock_base.daily",
                time_column="missing_time",
            )
        )

    assert fake.calls == []
    assert factory.calls == []

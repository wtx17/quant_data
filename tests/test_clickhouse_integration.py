"""Optional smoke tests for the real Minghu ClickHouse service."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import pandas as pd
import pyarrow as pa

from quant_data import ClickHouseConfig, DataClient
from quant_data.backends.clickhouse_catalog import MINGHU_TABLE_COLUMN_TYPES
from quant_data.initialize import clickhouse_registrations

pytestmark = pytest.mark.clickhouse
CODE_SUFFIXES = (".SZ", ".SH", ".BJ")


def require_environment() -> None:
    required = (
        "MINGHU_CLICKHOUSE_HOST",
        "MINGHU_CLICKHOUSE_USERNAME",
        "MINGHU_CLICKHOUSE_PASSWORD",
    )
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        pytest.skip(f"Missing ClickHouse environment variables: {', '.join(missing)}")


def test_minghu_catalog_matches_remote_schema() -> None:
    require_environment()
    from clickhouse_connect import get_client

    client = get_client(
        host=os.environ["MINGHU_CLICKHOUSE_HOST"],
        port=int(os.getenv("MINGHU_CLICKHOUSE_PORT", "8123")),
        username=os.environ["MINGHU_CLICKHOUSE_USERNAME"],
        password=os.environ["MINGHU_CLICKHOUSE_PASSWORD"],
        secure=os.getenv("MINGHU_CLICKHOUSE_SECURE", "").lower() in {"1", "true", "yes", "y", "on"},
    )
    try:
        for table, expected in MINGHU_TABLE_COLUMN_TYPES.items():
            quoted_table = ".".join(f"`{part}`" for part in table.split("."))
            description = client.query_arrow(f"DESCRIBE TABLE {quoted_table}", use_strings=True)
            names = description.column("name").to_pylist()
            types = description.column("type").to_pylist()
            actual = tuple((str(name), str(type_name)) for name, type_name in zip(names, types))
            assert actual == expected, f"Schema drift detected for {table}"
    finally:
        client.close()


def test_minghu_tables_smoke(tmp_path: Path) -> None:
    require_environment()
    query_date = os.getenv("MINGHU_CLICKHOUSE_TEST_DATE", "2026-03-02")
    with DataClient(tmp_path / "audit") as data:
        data.add_clickhouse_connection(
            "minghu",
            ClickHouseConfig(
                host=os.environ["MINGHU_CLICKHOUSE_HOST"],
                port=int(os.getenv("MINGHU_CLICKHOUSE_PORT", "8123")),
                username=os.environ["MINGHU_CLICKHOUSE_USERNAME"],
                password_env="MINGHU_CLICKHOUSE_PASSWORD",
            ),
        )
        data.register_clickhouse(
            "daily",
            connection="minghu",
            table="stock_base.daily",
            time_column="date",
            frequency="1d",
        )
        data.register_clickhouse(
            "minghu_index_daily",
            connection="minghu",
            table="index_base.daily",
            time_column="date",
            frequency="1d",
        )
        data.register_clickhouse(
            "m1",
            connection="minghu",
            table="stock_base.m1",
            time_column="date_time",
            partition_column="date",
            order_columns=("date_time", "code"),
            frequency="1min",
        )
        flow_registration = next(
            item for item in clickhouse_registrations() if item.name == "zb_cj_flow_min"
        )
        data.register_clickhouse(
            flow_registration.name,
            connection=flow_registration.connection,
            table=flow_registration.table,
            time_column=flow_registration.time_column,
            partition_column=flow_registration.partition_column,
            order_columns=flow_registration.order_columns,
            frequency=flow_registration.frequency,
        )
        flow = data.get_panel(
            "zb_cj_flow_min",
            ["cj_all_mn_min", "cj_psell_xl_td_min"],
            start=f"{query_date} 09:30:00",
            end=f"{query_date} 09:31:00",
            instruments=["000001.SZ", "600000.SH", "300750.SZ"],
        )
        for panel in flow.values():
            assert not panel.empty
            assert panel.index.name == "date_time"
            assert list(panel.columns) == ["000001.SZ", "600000.SH", "300750.SZ"]
            assert panel.notna().any().all()
            assert isinstance(panel.index, pd.DatetimeIndex)
            assert str(panel.index.tz) == "Asia/Shanghai"
            assert panel.index.min() >= pd.Timestamp(f"{query_date} 09:30:00", tz="Asia/Shanghai")
            assert panel.index.max() <= pd.Timestamp(f"{query_date} 09:31:00", tz="Asia/Shanghai")
        daily = data.get_panel(
            "daily",
            ["close"],
            start=query_date,
            end=query_date,
            instruments=["000001.SZ"],
        )
        index_daily = data.get_panel(
            "minghu_index_daily",
            ["close", "volume"],
            start=query_date,
            end=query_date,
            instruments=["000001.SH"],
        )
        m1 = data.get_panel(
            "m1",
            ["close", "volume"],
            start=f"{query_date} 09:30:00",
            end=f"{query_date} 09:31:00",
            instruments=["000001.SZ"],
        )

    for panels, index_name, instrument in (
        (daily, "date", "000001.SZ"),
        (index_daily, "date", "000001.SH"),
        (m1, "date_time", "000001.SZ"),
    ):
        assert "close" in panels
        for panel in panels.values():
            assert panel.index.name == index_name
            assert list(panel.columns) == [instrument]
            assert not panel.empty
    for panel in m1.values():
        assert isinstance(panel.index, pd.DatetimeIndex)
        assert str(panel.index.tz) == "Asia/Shanghai"
        assert panel.index.min() >= pd.Timestamp(f"{query_date} 09:30:00", tz="Asia/Shanghai")
        assert panel.index.max() <= pd.Timestamp(f"{query_date} 09:31:00", tz="Asia/Shanghai")


@pytest.mark.parametrize(
    "date_expression,date_type", [("20260302", "UInt32"), ("toDate('2026-03-02')", "Date")]
)
@pytest.mark.parametrize("physical_time", [False, True])
def test_minute_sql_time_synthesis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    date_expression: str,
    date_type: str,
    physical_time: bool,
) -> None:
    """Execute generated SQL against inline rows; no database writes required."""
    require_environment()
    from clickhouse_connect import get_client

    remote = get_client(
        host=os.environ["MINGHU_CLICKHOUSE_HOST"],
        port=int(os.getenv("MINGHU_CLICKHOUSE_PORT", "8123")),
        username=os.environ["MINGHU_CLICKHOUSE_USERNAME"],
        password=os.environ["MINGHU_CLICKHOUSE_PASSWORD"],
        secure=os.getenv("MINGHU_CLICKHOUSE_SECURE", "").lower() in {"1", "true", "yes", "y", "on"},
    )
    query_arrow = remote.query_arrow
    columns = {"date": date_type, "time_int": "Int32", "code": "String", "close": "Float64"}
    physical_sql = ""
    if physical_time:
        columns["date_time"] = "Int64"
        physical_sql = ", toInt64(0) AS date_time"
    rows = (
        f"(SELECT {date_expression} AS date, "
        "arrayJoin([34200122, 34200123, 34200124]) AS time_int, "
        f"'000001.SZ' AS code, 10.3 AS close{physical_sql})"
    )

    def query_inline(sql: str, **kwargs: object) -> pa.Table:
        if sql.startswith("DESCRIBE TABLE"):
            return pa.table({"name": list(columns), "type": list(columns.values())})
        return query_arrow(sql.replace("`custom`.`minute`", rows), **kwargs)

    monkeypatch.setattr(remote, "query_arrow", query_inline)
    with DataClient(tmp_path / "audit", clickhouse_client_factory=lambda **kwargs: remote) as data:
        data.add_clickhouse_connection("test", ClickHouseConfig(host="inline"))
        data.register_clickhouse(
            "minute",
            connection="test",
            table="custom.minute",
            time_column="date_time",
            partition_column="date",
        )
        expected = pd.Timestamp("2026-03-02 09:30:00.123", tz="Asia/Shanghai")
        panel = data.get_panel("minute", ["close"], start=expected, end=expected)["close"]
        assert isinstance(panel.index, pd.DatetimeIndex)
        assert str(panel.index.tz) == "Asia/Shanghai"
        assert panel.index.tolist() == [expected]
        assert panel.loc[expected, "000001.SZ"] == pytest.approx(10.3)


def test_membership_events_real_daily(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three bounded date/code reads with one-month lookback; verify state against independent CSV panels."""
    from quant_data import InvalidQueryError
    from quant_data._universes import load_universe
    from quant_data.initialize import initialize_data_client

    require_environment()
    query_date = os.getenv("MINGHU_CLICKHOUSE_TEST_DATE", "2026-03-02")
    day = pd.Timestamp(query_date).date()
    pools = {
        name: set(load_universe(name).select(day, day)) for name in ("hs300", "zz500", "zz1000")
    }
    reads: list[tuple[int, int]] = []
    market_codes: set[str] = set()
    with initialize_data_client(audit_dir=tmp_path / "audit", register_tushare=False) as data:
        from quant_data.backends import clickhouse as clickhouse_module

        original_scan = clickhouse_module.scan_clickhouse

        def measured_scan(session, source, dataset_name, fields, query):
            assert query.start.date() == (pd.Timestamp(day) - pd.DateOffset(months=1)).date()
            assert query.end.date() == day
            assert query.fields == ()
            table = original_scan(session, source, dataset_name, fields, query)
            assert table.column_names == ["date", "code"]
            reads.append((table.num_rows, table.nbytes))
            market_codes.update(table["code"].to_pylist())
            return table

        monkeypatch.setattr(clickhouse_module, "scan_clickhouse", measured_scan)
        instruments = ["000001.SZ", "600000.SH", "000009.SZ"]
        panel = data.get_panel(
            "membership_events", ["membership"], query_date, query_date, instruments=instruments
        )["membership"]
        assert panel.shape == (1, 3)
        assert panel.columns.tolist() == instruments
        assert panel.index.tolist() == [pd.Timestamp(query_date)]
        for code in instruments:
            expected = sum(i for i, pool in enumerate(pools.values(), 1) if code in pool)
            assert panel.loc[query_date, code] == expected
        assert all(dtype == "int8" for dtype in panel.dtypes)
        missing_members = sorted(pools["hs300"] - market_codes)
        if missing_members:
            with pytest.raises(InvalidQueryError, match="absent from stock_base.daily") as exc:
                data.get_panel(
                    "membership_events", ["membership"], query_date, query_date, universe="hs300"
                )
            assert all(code in str(exc.value) for code in missing_members)
        else:
            universe = data.get_panel(
                "membership_events", ["membership"], query_date, query_date, universe="hs300"
            )["membership"]
            assert universe.shape == (1, 300)
            assert set(universe.columns) == pools["hs300"]
            assert (universe == 1).all().all()
        with pytest.raises(InvalidQueryError, match="absent from stock_base.daily"):
            data.get_panel(
                "membership_events",
                ["membership"],
                query_date,
                query_date,
                instruments=["999999.SZ"],
            )
    assert len(reads) == 3
    print(f"date={query_date}, reads(rows, Arrow bytes)={reads}")
    print(f"membership={panel.iloc[0].to_dict()}, hs300 missing from market={missing_members}")

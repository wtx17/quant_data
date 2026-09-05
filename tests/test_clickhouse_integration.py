"""Optional smoke tests for the real Minghu ClickHouse service."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from quant_data import ClickHouseConfig, ClickHouseDatasetSpec, DataClient
from quant_data.backends.clickhouse_catalog import MINGHU_TABLE_COLUMN_TYPES

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
        data.register(
            ClickHouseDatasetSpec(
                name="daily",
                connection="minghu",
                table="stock_base.daily",
                time_column="date",
                frequency="1d",
            )
        )
        data.register(
            ClickHouseDatasetSpec(
                name="minghu_index_daily",
                connection="minghu",
                table="index_base.daily",
                time_column="date",
                frequency="1d",
            )
        )
        data.register(
            ClickHouseDatasetSpec(
                name="m1",
                connection="minghu",
                table="stock_base.m1",
                time_column="date_time",
                partition_column="date",
                order_columns=("date_time", "code"),
                frequency="1min",
            )
        )
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

"""Deterministic archive files and a read-only ClickHouse calendar for tests."""

from __future__ import annotations
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from quant_data import ClickHouseConfig, DataClient
from quant_data.backends.tushare_catalog import TUSHARE_DATASETS


class CalendarClient:
    def __init__(self, excluded=()):
        self.calls = []
        self.excluded = set(excluded)

    def query_arrow(self, query, parameters=None, **kwargs):
        assert query.startswith("SELECT DISTINCT `date` FROM `stock_base`.`daily`")
        assert "code" not in query and "instruments" not in (parameters or {})
        self.calls.append((query, dict(parameters)))
        current, end = parameters["start"], parameters["end"]
        days = []
        while current <= end:
            if current.weekday() < 5 and current not in self.excluded:
                days.append(current)
            current += timedelta(days=1)
        return pa.table({"date": pa.array(days, type=pa.date32())})

    def close(self):
        pass


class CalendarFactory:
    def __init__(self, client):
        self.client = client
        self.calls = 0

    def __call__(self, **kwargs):
        self.calls += 1
        return self.client


def make_client(tmp_path):
    calendar = CalendarClient()
    factory = CalendarFactory(calendar)
    client = DataClient(tmp_path / "audit", clickhouse_client_factory=factory)
    client.add_clickhouse_connection("calendar", ClickHouseConfig(host="fake"))
    return client, calendar, factory


def _archive_value(value, dtype):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y%m%d")
    return str(value) if pa.types.is_string(dtype) else value


def _archive_schema(dataset: str) -> pa.Schema:
    fields: list[pa.Field] = []
    for field in TUSHARE_DATASETS[dataset]["schema"]:
        data_type = pa.string() if pa.types.is_date32(field.type) else field.type
        fields.append(pa.field(field.name, data_type))
    return pa.schema(fields)


def write_archive(
    root: Path,
    dataset: str,
    rows: list[dict[str, object]],
    *,
    range_start: str = "20240101",
    range_end: str = "20241231",
) -> Path:
    schema = _archive_schema(dataset)
    normalized = [
        {field.name: _archive_value(row.get(field.name), field.type) for field in schema}
        for row in rows
    ]
    table = pa.Table.from_pylist(normalized, schema=schema)
    dataset_dir = root / dataset
    dataset_dir.mkdir(parents=True)
    parquet_path = dataset_dir / "data.parquet"
    pq.write_table(table, parquet_path)
    checksum = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    manifest = {
        "manifest_version": 1,
        "dataset": dataset,
        "schema_hash": f"schema-{dataset}",
        "fields": [{"name": field.name} for field in schema],
        "range_start": range_start,
        "range_end": range_end,
        "updated_at": "2026-07-20T00:00:00+00:00",
        "partitions": {
            "all": {
                "relative_path": f"{dataset}/data.parquet",
                "rows": table.num_rows,
                "bytes": parquet_path.stat().st_size,
                "sha256": checksum,
            }
        },
    }
    manifest_path = dataset_dir / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def write_daily_basic_archive(
    root: Path,
    rows_by_date: dict[str, list[dict[str, object]]],
) -> Path:
    schema = _archive_schema("daily_basic")
    dataset_dir = root / "daily_basic"
    partitions: dict[str, dict[str, object]] = {}
    for trade_date, rows in sorted(rows_by_date.items()):
        normalized = [
            {field.name: _archive_value(row.get(field.name), field.type) for field in schema}
            for row in rows
        ]
        table = pa.Table.from_pylist(normalized, schema=schema)
        relative_path = Path("daily_basic") / "trade_date" / trade_date / "data.parquet"
        parquet_path = root / relative_path
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, parquet_path)
        partitions[trade_date] = {
            "relative_path": relative_path.as_posix(),
            "query": {"trade_date": trade_date},
            "rows": table.num_rows,
            "bytes": parquet_path.stat().st_size,
            "sha256": hashlib.sha256(parquet_path.read_bytes()).hexdigest(),
        }

    dataset_dir.mkdir(parents=True, exist_ok=True)
    partition_dates = sorted(rows_by_date)
    manifest = {
        "manifest_version": 1,
        "dataset": "daily_basic",
        "schema_hash": "schema-daily_basic",
        "fields": [{"name": field.name} for field in schema],
        "range_start": partition_dates[0],
        "range_end": partition_dates[-1],
        "updated_at": "2026-07-20T00:00:00+00:00",
        "partitions": partitions,
    }
    manifest_path = dataset_dir / "_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def industry_frame():
    return pd.DataFrame(
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

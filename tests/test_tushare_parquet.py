from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_data import (
    BackendConnectionError,
    DataClient,
    DatasetRegistrationError,
    InvalidQueryError,
    TushareConfig,
    TushareParquetDatasetSpec,
)
from quant_data.backends.tushare_catalog import TUSHARE_DATASETS
from quant_data.initialize import (
    initialize_data_client,
    tushare_parquet_dataset_specs,
)


class CalendarClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def query(self, api_name: str, **params: Any) -> pd.DataFrame:
        self.calls.append((api_name, dict(params)))
        if api_name != "trade_cal":
            raise AssertionError(f"local archive called data API {api_name!r}")
        start = datetime.strptime(str(params["start_date"]), "%Y%m%d").date()
        end = datetime.strptime(str(params["end_date"]), "%Y%m%d").date()
        days: list[str] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                days.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)
        return pd.DataFrame({"cal_date": days})


class CalendarFactory:
    def __init__(self, client: CalendarClient) -> None:
        self.client = client
        self.calls = 0

    def __call__(self, **kwargs: Any) -> CalendarClient:
        self.calls += 1
        return self.client


def _archive_schema(dataset: str) -> pa.Schema:
    fields: list[pa.Field] = []
    for field in TUSHARE_DATASETS[dataset].schema:
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
    normalized = [{field.name: row.get(field.name) for field in schema} for row in rows]
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
        normalized = [{field.name: row.get(field.name) for field in schema} for row in rows]
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


def make_client(tmp_path: Path) -> tuple[DataClient, CalendarClient, CalendarFactory]:
    calendar = CalendarClient()
    factory = CalendarFactory(calendar)
    client = DataClient(
        tmp_path / "audit",
        tushare_client_factory=factory,
    )
    client.add_tushare_connection("ts", TushareConfig(token="x"))
    return client, calendar, factory


def local_spec(
    root: Path,
    dataset: str,
    **kwargs: Any,
) -> TushareParquetDatasetSpec:
    return TushareParquetDatasetSpec(
        name=dataset,
        data_dir=root,
        calendar_connection="ts",
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
    client.register(local_spec(root, "daily_basic"))

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

    assert list(table["close"].index) == [date(2024, 1, 2), date(2024, 1, 3)]
    assert table["close"]["600000.SH"].tolist() == pytest.approx([10.0, 11.0])
    assert table["limit_status"]["600000.SH"].tolist() == [1, 0]
    assert list(panel.index) == [date(2024, 1, 2), date(2024, 1, 3)]
    assert list(panel.columns) == ["000001.SZ", "600000.SH"]
    assert panel.loc[date(2024, 1, 2), "000001.SZ"] == pytest.approx(2.1)
    assert pd.isna(panel.loc[date(2024, 1, 3), "000001.SZ"])
    assert panel.loc[date(2024, 1, 3), "600000.SH"] == pytest.approx(1.2)
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
    client.register(
        local_spec(
            root,
            "income",
            disclosure_lag=0,
            fetch_buffer_days=30,
            fetch_margin_days=5,
        )
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
    assert {api for api, _ in calendar.calls} == {"trade_cal"}


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
    default_client.register(local_spec(root, "income", fetch_buffer_days=30))

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
    override_client.register(
        TushareParquetDatasetSpec(
            name="income_single_quarter",
            dataset="income",
            data_dir=root,
            calendar_connection="ts",
            fixed_params={"report_type": "2"},
        )
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
    client.register(local_spec(root, "ci_index_member"))

    panel = client.get_panel(
        "ci_index_member",
        ["l1_name"],
        start="2024-01-02",
        end="2024-01-08",
        instruments=["600000.SH"],
    )["l1_name"]

    assert panel.loc[date(2024, 1, 2), "600000.SH"] == "old"
    assert panel.loc[date(2024, 1, 4), "600000.SH"] == "new"
    assert {api for api, _ in calendar.calls} == {"trade_cal"}


def test_local_fixed_params_map_or_fail_at_registration(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(root, "cashflow", [])
    client, _, _ = make_client(tmp_path)
    with pytest.raises(DatasetRegistrationError, match="is_calc"):
        client.register(
            local_spec(
                root,
                "cashflow",
                fixed_params={"is_calc": 1},
            )
        )


def test_local_snapshot_rejects_explicit_and_pit_buffer_overflow(tmp_path: Path) -> None:
    root = tmp_path / "archive"
    write_archive(root, "income", [])
    client, _, _ = make_client(tmp_path)
    client.register(local_spec(root, "income", fetch_buffer_days=30))

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
        client.register(local_spec(root, "income"))


def test_local_initialization_registers_standard_names_without_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    specs = tushare_parquet_dataset_specs(root, calendar_connection="calendar")
    assert [spec.name for spec in specs] == list(TUSHARE_DATASETS)
    for spec in specs:
        if spec.name == "daily_basic":
            write_daily_basic_archive(root, {"20240701": []})
        else:
            write_archive(root, spec.name, [])
    monkeypatch.delenv("MISSING_LOCAL_CALENDAR_TOKEN", raising=False)

    client = initialize_data_client(
        audit_dir=tmp_path / "audit",
        register_clickhouse=False,
        tushare_data_dir=root,
        tushare_connection="calendar",
        tushare_token_env="MISSING_LOCAL_CALENDAR_TOKEN",
    )

    daily = client.get_panel(
        "daily_basic",
        ["close"],
        start="2024-07-01",
        end="2024-07-01",
    )
    assert daily["close"].empty
    with pytest.raises(BackendConnectionError, match="MISSING_LOCAL_CALENDAR_TOKEN"):
        client.get_panel(
            "income",
            ["total_revenue"],
            start="2024-07-01",
            end="2024-07-05",
        )
    client.close()


def test_initialization_can_mix_local_and_remote_tushare_datasets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "archive"
    write_archive(root, "income", [])
    monkeypatch.delenv("MISSING_MIXED_TUSHARE_TOKEN", raising=False)

    with initialize_data_client(
        audit_dir=tmp_path / "audit",
        register_clickhouse=False,
        tushare_data_dir=root,
        tushare_remote_datasets=set(TUSHARE_DATASETS).difference({"income"}),
        tushare_connection="mixed",
        tushare_token_env="MISSING_MIXED_TUSHARE_TOKEN",
    ) as client:
        assert isinstance(client._datasets["income"].spec, TushareParquetDatasetSpec)

        with pytest.raises(BackendConnectionError, match="MISSING_MIXED_TUSHARE_TOKEN"):
            client.get_panel(
                "balancesheet",
                ["total_assets"],
                start="2024-03-31",
                end="2024-03-31",
                instruments=["600000.SH"],
            )


@pytest.mark.parametrize(
    ("data_dir", "remote_datasets", "message"),
    [
        (None, {"income"}, "tushare_data_dir is required"),
        (Path("/unused"), {"unknown"}, "Unsupported Tushare remote datasets"),
        (Path("/unused"), "income", "must be a collection"),
        (Path("/unused"), ["income", 1], "only non-empty strings"),
    ],
)
def test_mixed_initialization_rejects_invalid_remote_selection(
    tmp_path: Path,
    data_dir: Path | None,
    remote_datasets: object,
    message: str,
) -> None:
    with pytest.raises(DatasetRegistrationError, match=message):
        initialize_data_client(
            audit_dir=tmp_path / "audit",
            register_clickhouse=False,
            tushare_data_dir=data_dir,
            tushare_remote_datasets=remote_datasets,  # type: ignore[arg-type]
        )


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
    client.register(local_spec(root, "income", fetch_buffer_days=30))
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
    assert {api for api, _ in calendar.calls} == {"trade_cal"}

"""Run identical panel requests against the pre-refactor and current packages.

Both packages run unchanged, in separate Python module namespaces, against the
same files and independently constructed copies of the same fake services.
Registration syntax and the intentional DatetimeIndex migration are adapted.
No value dtypes, missing values, field order or numeric tolerances are normalized.
UUIDs and elapsed/start times are excluded from equality, along with the explicitly replaced calendar provenance.

Requires the local Git baseline (no network). Run explicitly with
``pytest tests/test_panel_compatibility.py``; source distributions lacking Git
history skip this module instead of downloading or vendoring an old runtime.
"""

from __future__ import annotations

import importlib
import importlib.util
import inspect
import io
import json
import os
import subprocess
import sys
import tarfile
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quant_data
from quant_data.backends.tushare_catalog import TUSHARE_DATASETS
from quant_data.initialize import CLICKHOUSE_PANEL_DEFS

from test_clickhouse import FakeClickHouseClient
from tushare_fixtures import (
    CalendarClient,
    industry_frame,
    write_archive,
    write_daily_basic_archive,
)

BASELINE = "680ab80ed536f959ce6ce563e073dfd87f482cbe"
ROOT = Path(__file__).resolve().parents[1]
SELECTIONS = [None, ["000004.SZ", "600000.SH", "999999.SZ"], []]
DISCLOSURES = [
    ("income", "total_revenue"),
    ("balancesheet", "total_assets"),
    ("cashflow", "n_cashflow_act"),
    ("fina_indicator", "roe"),
    ("express", "revenue"),
    ("forecast", "p_change_min"),
    ("stk_holdernumber", "holder_num"),
]


@pytest.fixture(scope="module")
def baseline():
    probe = subprocess.run(
        ["git", "cat-file", "-e", f"{BASELINE}^{{commit}}"],
        cwd=ROOT,
        capture_output=True,
    )
    if probe.returncode:
        pytest.skip(f"Local Git baseline {BASELINE} is unavailable")
    payload = subprocess.check_output(["git", "archive", BASELINE], cwd=ROOT)
    name = "_quant_data_baseline"
    with TemporaryDirectory(prefix="quant-data-baseline-") as directory:
        source = Path(directory)
        with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
            # Extract regular source/resource files only, with paths checked first.
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                target = (source / member.name).resolve()
                if not target.is_relative_to(source.resolve()):
                    raise ValueError(f"Unsafe archive path: {member.name}")
                stream = archive.extractfile(member)
                assert stream is not None
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(stream.read())
        spec = importlib.util.spec_from_file_location(
            name, source / "__init__.py", submodule_search_locations=[str(source)]
        )
        assert spec is not None and spec.loader is not None
        package = importlib.util.module_from_spec(spec)
        sys.modules[name] = package
        try:
            spec.loader.exec_module(package)
            assert Path(package.__file__).parent == source
            assert Path(quant_data.__file__).resolve().parent == ROOT
            yield package
        finally:
            for module_name in list(sys.modules):
                if module_name == name or module_name.startswith(name + "."):
                    del sys.modules[module_name]


class LegacyCalendar:
    """Only the historical package uses the retired API protocol."""

    def query(self, api_name, **kwargs):
        assert api_name == "trade_cal"
        days = pd.bdate_range(kwargs["start_date"], kwargs["end_date"])
        return pd.DataFrame(
            {"cal_date": days.strftime("%Y%m%d"), "is_open": (days.weekday < 5).astype(int)}
        )


@pytest.fixture
def pair(baseline, tmp_path):
    """Create an old/new client pair; each service gets an independent call log."""
    with ExitStack() as stack:

        def make(kind, fake_factory=None):
            entries = []
            for label, package in (("old", baseline), ("new", quant_data)):
                fake = (
                    (LegacyCalendar() if kind == "tushare" and label == "old" else fake_factory())
                    if fake_factory is not None
                    else None
                )
                kwargs = {}
                if fake is not None:
                    key = (
                        "clickhouse_client_factory"
                        if kind == "clickhouse" or (kind == "tushare" and label == "new")
                        else "tushare_client_factory"
                    )
                    kwargs[key] = lambda _fake=fake, **kw: _fake
                client = stack.enter_context(
                    package.DataClient(tmp_path / label / "audit", **kwargs)
                )
                if kind == "clickhouse" or (kind == "tushare" and label == "new"):
                    client.add_clickhouse_connection("test", package.ClickHouseConfig(host="fake"))
                elif kind == "tushare":
                    client.add_tushare_connection("test", package.TushareConfig(token="fixture"))
                entries.append((package, client, fake))
            return entries

        yield make


def register(entries, kind, name, **options):
    """Translate configuration only; the get_panel implementations are untouched."""
    for package, client, _ in entries:
        if package is quant_data:
            getattr(client, f"register_{kind}")(name, **options)
        else:
            definition = {
                "parquet": "DatasetSpec",
                "clickhouse": "ClickHouseDatasetSpec",
                "builtin": "BuiltInDatasetSpec",
                "tushare": "TushareParquetDatasetSpec"
                if "data_dir" in options
                else "TushareDatasetSpec",
            }[kind]
            client.register(getattr(package, definition)(name=name, **options))


def same_panels(entries, *args, error=None, **kwargs):
    outcomes = []
    audits = []
    for _, client, _ in entries:
        audit_root = client._audit.root
        before = set(audit_root.rglob("*.json"))
        if error is None:
            outcomes.append(client.get_panel(*args, **kwargs))
        else:
            with pytest.raises(Exception) as raised:
                client.get_panel(*args, **kwargs)
            assert type(raised.value).__name__ == error
            outcomes.append(str(raised.value))
        created = set(audit_root.rglob("*.json")) - before
        assert len(created) == 1
        audits.append(json.loads(created.pop().read_text()))

    def calendar_metadata(value):
        if isinstance(value, dict):
            return {
                k: calendar_metadata(v)
                for k, v in value.items()
                if k not in {"calendar_api", "calendar_table", "calendar_connection", "calendar"}
            }
        return value

    def stable_audit(record):
        return {
            key: value
            for key, value in record.items()
            if key not in {"query_id", "started_at", "duration_ms"}
        }

    if isinstance(entries[0][2], LegacyCalendar):
        if audits[1]["calendar_aligned"]:
            assert audits[0]["parameters"]["calendar_api"] == "trade_cal"
            assert audits[1]["parameters"]["calendar_table"] == "stock_base.daily"
            assert audits[1]["source"]["calendar"]["backend"] == "clickhouse"
            assert audits[1]["source"]["calendar"]["table"] == "stock_base.daily"
            assert len(entries[1][2].calls) == 1
        else:
            assert entries[1][2].calls == []
            assert "calendar" not in audits[1]["source"]
    assert audits[0]["query_id"] != audits[1]["query_id"]
    assert calendar_metadata(stable_audit(audits[0])) == calendar_metadata(stable_audit(audits[1]))
    if error is not None:
        assert outcomes[0] == outcomes[1]
    else:
        assert list(outcomes[0]) == list(outcomes[1])
        for field in outcomes[0]:
            old, new = outcomes[0][field], outcomes[1][field]
            assert isinstance(new.index, pd.DatetimeIndex)
            # The public index contract intentionally changed from Arrow dates.
            old.index = pd.DatetimeIndex(old.index).as_unit(new.index.unit)
            # assert_frame_equal covers dtype, null mask, axes, names and ordering.
            pd.testing.assert_frame_equal(
                old, new, check_exact=True, check_index_type=True, check_column_type=True
            )
            attrs = []
            for panel, audit in zip((old, new), audits):
                assert panel.attrs["query_id"] == audit["query_id"]
                attrs.append(
                    {key: value for key, value in panel.attrs.items() if key != "query_id"}
                )
            assert calendar_metadata(attrs[0]) == calendar_metadata(attrs[1])
    # API names, selected fields, scope, calendar windows and call counts agree too.
    if entries[0][2] is not None and not isinstance(entries[0][2], LegacyCalendar):
        assert entries[0][2].calls == entries[1][2].calls


def test_signature(baseline):
    old = inspect.signature(baseline.DataClient.get_panel)
    new = inspect.signature(quant_data.DataClient.get_panel)
    # Multiple named universes are an intentional extension of the baseline API.
    assert old.parameters["universe"].annotation == "str | None"
    assert new.parameters["universe"].annotation == "str | list[str] | None"
    old = old.replace(
        parameters=[
            parameter.replace(annotation=new.parameters["universe"].annotation)
            if parameter.name == "universe"
            else parameter
            for parameter in old.parameters.values()
        ]
    )
    assert str(old) == str(new)


@pytest.mark.parametrize("time_type", ["string", "date", "timestamp", "timezone"])
@pytest.mark.parametrize("instruments", SELECTIONS)
def test_parquet(pair, tmp_path, time_type, instruments):
    times = ["2024-01-02", "2024-01-03"]
    if time_type == "date":
        times = [pd.Timestamp(value).date() for value in times]
    elif time_type in {"timestamp", "timezone"}:
        times = list(pd.to_datetime(times) + pd.Timedelta(milliseconds=123))
        if time_type == "timezone":
            times = [value.tz_localize("Asia/Shanghai") for value in times]
    path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "when": times * 2,
                "code": ["600000.SH"] * 2 + ["000004.SZ"] * 2,
                "value": [1.0, None, 3.0, 4.0],
                "label": ["a", "b", None, "d"],
            }
        ),
        path,
    )
    entries = pair("parquet")
    register(
        entries, "parquet", "custom", paths=[path], time_column="when", instrument_column="code"
    )
    same_panels(entries, "custom", ["label", "value"], times[0], times[1], instruments)


@pytest.mark.parametrize("definition", CLICKHOUSE_PANEL_DEFS, ids=lambda d: d["name"])
@pytest.mark.parametrize("instruments", SELECTIONS)
@pytest.mark.parametrize("adjusted", [None, False])
def test_clickhouse(pair, definition, instruments, adjusted):
    name = definition["name"]
    column = definition["time_column"]
    minute = column == "date_time"
    times = (
        [pd.Timestamp("2026-03-02 09:30:00.123", tz="Asia/Shanghai")] * 2
        if minute
        else [date(2026, 3, 2)] * 2
    )
    fields = (
        ["cj_all_mn_min", "cj_psell_xl_td_min"] if name == "zb_cj_flow_min" else ["close", "volume"]
    )
    result = pa.table(
        {
            column: times,
            "code": ["600000.SH", "000004.SZ"],
            fields[0]: [10.0, None],
            fields[1]: [100, 200],
            "hfq": [2.0, None],
        }
    )
    entries = pair("clickhouse", lambda: FakeClickHouseClient(result))
    register(
        entries,
        "clickhouse",
        name,
        connection="test",
        **{k: v for k, v in definition.items() if k != "name"},
    )
    same_panels(entries, name, fields, times[0], times[1], instruments, adjusted)


@pytest.mark.parametrize("fields", [["volume"], ["close"], ["close", "volume"], ["hfq"]])
@pytest.mark.parametrize("instruments", [None, []])
@pytest.mark.parametrize("adjusted", [None, True, False])
def test_adjustment_projection(pair, fields, instruments, adjusted):
    result = pa.table(
        {
            "date": [date(2026, 3, 2)],
            "code": ["600000.SH"],
            "close": [10.0],
            "volume": [100],
            "hfq": [2.0],
        }
    )
    entries = pair("clickhouse", lambda: FakeClickHouseClient(result))
    register(
        entries,
        "clickhouse",
        "daily",
        connection="test",
        table="stock_base.daily",
        time_column="date",
    )
    same_panels(entries, "daily", fields, instruments=instruments, adjusted=adjusted)


def disclosure_rows(dataset, field):
    events = [
        ("600000.SH", "20240410", "20230930", 1),
        ("600000.SH", "20240420", "20231231", 2),
        ("600000.SH", "20240425", "20231231", 3),
        ("600000.SH", "20240426", "20230930", 99),
        ("600000.SH", "20240427", "20240331", None),
        ("000004.SZ", "20240422", "20231231", 4),
    ]
    rows = []
    for code, announcement, period, value in events:
        values = dict(
            ts_code=code,
            ann_date=announcement,
            f_ann_date=announcement,
            first_ann_date=announcement,
            end_date=period,
            report_type="1",
            comp_type="1",
            end_type="1",
            update_flag=1,
            is_audit=1,
            type="预增",
        )
        values[field] = value
        rows.append(
            {
                column.name: (
                    str(values[column.name])
                    if pa.types.is_string(column.type) and values.get(column.name) is not None
                    else values.get(column.name)
                )
                for column in TUSHARE_DATASETS[dataset]["schema"]
            }
        )
    return rows


@pytest.mark.parametrize("dataset,field", DISCLOSURES)
@pytest.mark.parametrize("lag", [0, 1])
@pytest.mark.parametrize("instruments", SELECTIONS)
def test_disclosures(pair, tmp_path, dataset, field, lag, instruments):
    rows = disclosure_rows(dataset, field)
    options = dict(dataset=dataset, disclosure_lag=lag, fetch_buffer_days=60, fetch_margin_days=15)
    root = tmp_path / "archive"
    write_archive(root, dataset, rows)
    entries = pair("tushare", CalendarClient)
    options.update(data_dir=root, calendar_connection="test")
    register(entries, "tushare", "financial_alias", **options)
    same_panels(
        entries,
        "financial_alias",
        [field, "ann_date", "end_date"],
        "2024-04-19",
        "2024-05-02",
        instruments,
    )


@pytest.mark.parametrize("instruments", SELECTIONS)
def test_daily_basic(pair, tmp_path, instruments):
    rows = [
        {"ts_code": code, "trade_date": day, "close": value, "pe": None}
        for day in ("20240102", "20240103")
        for code, value in (("600000.SH", 10.0), ("000004.SZ", 20.0))
    ]
    root = tmp_path / "archive"
    write_daily_basic_archive(
        root, {day: [r for r in rows if r["trade_date"] == day] for day in ("20240102", "20240103")}
    )
    entries = pair("tushare", CalendarClient)
    options = dict(data_dir=root, calendar_connection="test")
    register(entries, "tushare", "daily_basic", **options)
    same_panels(entries, "daily_basic", ["pe", "close"], "2024-01-02", "2024-01-03", instruments)


@pytest.mark.parametrize("dataset", ["ci_index_member", "index_member_all"])
@pytest.mark.parametrize("fixed", [{}, {"is_new": "Y"}])
@pytest.mark.parametrize("instruments", SELECTIONS)
def test_industry(pair, tmp_path, dataset, fixed, instruments):
    root = tmp_path / "archive"
    write_archive(root, dataset, industry_frame().to_dict("records"), range_start="20190101")
    entries = pair("tushare", CalendarClient)
    options = dict(data_dir=root, calendar_connection="test")
    register(entries, "tushare", dataset, fixed_params=fixed, **options)
    same_panels(
        entries, dataset, ["l1_name", "is_new", "in_date"], "2024-01-02", "2024-01-08", instruments
    )


@pytest.mark.parametrize("universe", ["hs300", " ZZ500 ", "zz1000"])
@pytest.mark.parametrize("kind", ["parquet", "daily_basic", "membership_events"])
def test_universes(pair, baseline, tmp_path, monkeypatch, universe, kind):
    from quant_data._universes import load_universe

    codes = load_universe(universe).select(date(2024, 1, 5), date(2024, 1, 9))
    if kind == "membership_events":
        events = tmp_path / "events.parquet"
        pq.write_table(
            pa.table(
                {
                    "change_date": pd.to_datetime(["2024-01-01"]),
                    "code": [codes[0]],
                    "hs300": [1],
                    "zz500": [0],
                    "zz1000": [0],
                }
            ),
            events,
        )
        for package in (baseline, quant_data):
            backend = importlib.import_module(package.__name__ + ".backends.parquet")
            monkeypatch.setattr(backend, "MEMBERSHIP_EVENTS_PATH", events)
        market = pa.table({"date": [date(2024, 1, 5)] * len(codes), "code": list(codes)})
        entries = pair("clickhouse", lambda: FakeClickHouseClient(market))
        register(entries, "builtin", "membership_events", connection="test")
        fields = ["membership"]
    elif kind == "daily_basic":
        root = tmp_path / "archive"
        write_daily_basic_archive(
            root,
            {
                "20240105": [{"ts_code": codes[0], "trade_date": "20240105", "close": 10.0}],
                "20240109": [],
            },
        )
        entries = pair("tushare", CalendarClient)
        register(entries, "tushare", kind, data_dir=root, calendar_connection="test")
        fields = ["close"]
    else:
        path = tmp_path / "pool.parquet"
        pq.write_table(
            pa.table({"time": [date(2024, 1, 5)], "ts_code": [codes[0]], "close": [10.0]}), path
        )
        entries = pair("parquet")
        register(entries, "parquet", kind, paths=[path])
        fields = ["close"]
    same_panels(entries, kind, fields, "2024-01-05", "2024-01-09", universe=universe)


@pytest.mark.parametrize(
    "options",
    [
        {"fields": []},
        {"fields": ["missing"]},
        {"fields": ["close", "close"]},
        {"fields": ["time"]},
        {"instruments": "600000.SH"},
        {"instruments": ["600000.SH", "600000.SH"]},
        {"start": "invalid"},
        {"start": "2024-02-01", "end": "2024-01-01"},
        {"adjusted": True},
        {"universe": "unknown", "start": "2024-01-01", "end": "2024-02-01"},
        {"universe": "hs300"},
        {"universe": "hs300", "instruments": []},
    ],
)
def test_validation_errors(pair, tmp_path, options):
    path = tmp_path / "invalid.parquet"
    pq.write_table(
        pa.table({"time": [date(2024, 1, 2)], "ts_code": ["600000.SH"], "close": [1.0]}), path
    )
    entries = pair("parquet")
    register(entries, "parquet", "daily", paths=[path])
    error = "FieldNotFoundError" if options.get("fields") == ["missing"] else "InvalidQueryError"
    same_panels(entries, "daily", error=error, **{"fields": ["close"], **options})


@pytest.mark.clickhouse
@pytest.mark.parametrize("definition", CLICKHOUSE_PANEL_DEFS, ids=lambda d: d["name"])
def test_real_clickhouse(pair, definition):
    """Read the same bounded historical query from the actual service twice."""
    required = (
        "MINGHU_CLICKHOUSE_HOST",
        "MINGHU_CLICKHOUSE_USERNAME",
        "MINGHU_CLICKHOUSE_PASSWORD",
    )
    if not all(os.getenv(name) for name in required):
        pytest.skip("Real ClickHouse credentials are not configured")
    entries = pair("clickhouse")
    for package, client, _ in entries:
        client.add_clickhouse_connection(
            "test",
            package.ClickHouseConfig(
                host=os.environ["MINGHU_CLICKHOUSE_HOST"],
                port=int(os.getenv("MINGHU_CLICKHOUSE_PORT", "8123")),
                username=os.environ["MINGHU_CLICKHOUSE_USERNAME"],
                password_env="MINGHU_CLICKHOUSE_PASSWORD",
                secure=os.getenv("MINGHU_CLICKHOUSE_SECURE", "").lower()
                in {"1", "true", "yes", "y", "on"},
            ),
        )
    name = definition["name"]
    register(
        entries,
        "clickhouse",
        name,
        connection="test",
        **{key: value for key, value in definition.items() if key != "name"},
    )
    day = os.getenv("MINGHU_CLICKHOUSE_TEST_DATE", "2026-03-02")
    minute = definition["time_column"] == "date_time"
    fields = (
        ["cj_all_mn_min", "cj_psell_xl_td_min"] if name == "zb_cj_flow_min" else ["close", "volume"]
    )
    instruments = ["000001.SH"] if name == "minghu_index_daily" else ["000001.SZ", "600000.SH"]
    same_panels(
        entries,
        name,
        fields,
        f"{day} 09:30:00" if minute else day,
        f"{day} 09:31:00" if minute else day,
        instruments,
    )

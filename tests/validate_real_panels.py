"""Opt-in read-only old/new verification against live ClickHouse and local archives.

Run in qt: python tests/validate_real_panels.py --archive /path/to/tushare/data
The old package's calendar transport is adapted to real ClickHouse dates only;
its registration, local scans, transforms and get_panel implementation are unchanged.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))
sys.path.insert(0, str(ROOT / "tests"))
import quant_data  # noqa: E402
from quant_data.backends.tushare_catalog import TUSHARE_DATASETS  # noqa: E402
from quant_data.initialize import CLICKHOUSE_PANEL_DEFS  # noqa: E402
from test_panel_compatibility import BASELINE, baseline, register  # noqa: E402


def stable(value):
    if isinstance(value, dict):
        return {
            k: Path(v).name if k == "events_path" else stable(v)
            for k, v in value.items()
            if k
            not in {
                "query_id",
                "started_at",
                "duration_ms",
                "calendar_api",
                "calendar_table",
                "calendar_connection",
                "calendar",
            }
        }
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=ROOT / ".agent/real-panel-validation")
    parser.add_argument("--datasets", nargs="+")
    parser.add_argument("--daily-year", default="2025")
    parser.add_argument("--clean-index", action="store_true")
    parser.add_argument("--instruments-file", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    from clickhouse_connect import get_client

    connection = dict(
        host=os.environ["MINGHU_CLICKHOUSE_HOST"],
        port=int(os.getenv("MINGHU_CLICKHOUSE_PORT", "8123")),
        username=os.environ["MINGHU_CLICKHOUSE_USERNAME"],
        password=os.environ["MINGHU_CLICKHOUSE_PASSWORD"],
        secure=os.getenv("MINGHU_CLICKHOUSE_SECURE", "").lower() in {"1", "true", "yes", "on"},
    )
    report = {
        "baseline": BASELINE,
        "archive": str(args.archive),
        "calendar": "real stock_base.daily (legacy transport adapted)",
        "cases": [],
    }
    with ExitStack() as stack:
        service = get_client(**connection)
        stack.callback(service.close)

        class CalendarTransport:
            def query(self, api_name, **kwargs):
                assert api_name == "trade_cal"
                table = service.query_arrow(
                    "SELECT DISTINCT date FROM stock_base.daily WHERE date >= {start:Date} AND date <= {end:Date} ORDER BY date",
                    parameters={
                        key: datetime.strptime(kwargs[key + "_date"], "%Y%m%d").date()
                        for key in ("start", "end")
                    },
                    use_strings=True,
                )
                days = table["date"].to_pylist()
                return pd.DataFrame(
                    {"cal_date": [d.strftime("%Y%m%d") for d in days], "is_open": [1] * len(days)}
                )

        generator = baseline.__wrapped__()
        old_package = next(generator)
        stack.callback(generator.close)
        entries = []
        for label, package in [("old", old_package), ("new", quant_data)]:
            kwargs = (
                {"tushare_client_factory": lambda **kw: CalendarTransport()}
                if label == "old"
                else {}
            )
            client = stack.enter_context(
                package.DataClient(args.output / label / "audit", **kwargs)
            )
            client.add_clickhouse_connection("live", package.ClickHouseConfig(**connection))
            if label == "old":
                client.add_tushare_connection(
                    "live", package.TushareConfig(token="calendar-transport-only")
                )
            entries.append((package, client, None))
        definitions = {d["name"]: d for d in CLICKHOUSE_PANEL_DEFS}
        names = [*definitions, "membership_events", *TUSHARE_DATASETS]
        for name in names:
            if args.datasets and name not in args.datasets:
                continue
            try:
                if name in definitions:
                    definition = definitions[name]
                    register(
                        entries,
                        "clickhouse",
                        name,
                        connection="live",
                        **{k: v for k, v in definition.items() if k != "name"},
                    )
                    fields = (
                        ["cj_all_mn_min", "cj_psell_xl_td_min"]
                        if name == "zb_cj_flow_min"
                        else ["open", "close", "volume"]
                    )
                elif name == "membership_events":
                    register(entries, "builtin", name, connection="live")
                    fields = ["membership"]
                else:
                    register(
                        entries, "tushare", name, data_dir=args.archive, calendar_connection="live"
                    )
                    catalog = TUSHARE_DATASETS[name]
                    fields = [
                        f
                        for f in catalog["schema"].names
                        if f not in {"ts_code", catalog["panel_time_column"]}
                    ]
            except Exception as exc:
                report["cases"].append(
                    {
                        "dataset": name,
                        "status": "registration_failed",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                print(name, "REGISTRATION FAILED", type(exc).__name__, str(exc), flush=True)
                (args.output / "results.json").write_text(
                    json.dumps(report, ensure_ascii=False, indent=2)
                )
                continue
            minute = name in {"minghu_m1", "zb_cj_flow_min"}
            for scenario in ("short_wide", "long_narrow"):
                start, end = (
                    (
                        ("2026-03-02", "2026-03-03")
                        if scenario == "short_wide"
                        else ("2026-03-02", "2026-03-11")
                    )
                    if minute
                    else (
                        ("2025-04-28", "2025-04-30")
                        if scenario == "short_wide"
                        else ("2025-01-01", "2025-06-30")
                    )
                )
                if not minute:
                    start, end = (
                        start.replace("2025", args.daily_year),
                        end.replace("2025", args.daily_year),
                    )
                if args.clean_index and name == "minghu_index_daily" and scenario == "short_wide":
                    start, end = "2026-03-02", "2026-03-04"
                table = "index_base.daily" if name == "minghu_index_daily" else "stock_base.daily"
                limit = 500 if scenario == "short_wide" else 5
                codes = service.query_arrow(
                    f"SELECT DISTINCT concat(code, multiIf(exg = 1, '.SZ', exg = 2, '.SH', exg = 3, '.BJ', '')) AS instrument FROM {table} WHERE date >= {{start:Date}} AND date <= {{end:Date}} ORDER BY cityHash64(instrument) LIMIT {limit}",
                    parameters={
                        "start": datetime.fromisoformat(start).date(),
                        "end": datetime.fromisoformat(end).date(),
                    },
                    use_strings=True,
                )["instrument"].to_pylist()
                if args.clean_index and name == "minghu_index_daily" and scenario == "long_narrow":
                    codes = ["000902.SH", "000984.SH"]
                if args.instruments_file:
                    codes = json.loads(args.instruments_file.read_text())[:limit]
                case = dict(
                    dataset=name,
                    scenario=scenario,
                    start=start,
                    end=end,
                    instruments=codes,
                    fields=fields,
                    status="running",
                )
                report["cases"].append(case)
                print(
                    name,
                    scenario,
                    start,
                    end,
                    len(codes),
                    "instruments",
                    len(fields),
                    "fields",
                    flush=True,
                )
                try:
                    assert 0 < len(codes) <= 500
                    panels = []
                    audits = []
                    errors = []
                    for label, (_, client, _) in zip(("old", "new"), entries):
                        before = set(client._audit.root.rglob("*.json"))
                        began = time.perf_counter()
                        try:
                            result = client.get_panel(
                                name,
                                fields,
                                start,
                                end + " 23:59:59.999999" if minute else end,
                                codes,
                            )
                            errors.append(None)
                        except Exception as query_error:
                            errors.append(
                                {"type": type(query_error).__name__, "message": str(query_error)}
                            )
                            result = None
                        case[label + "_seconds"] = round(time.perf_counter() - began, 3)
                        panels.append(result)
                        created = set(client._audit.root.rglob("*.json")) - before
                        assert len(created) == 1
                        audits.append(json.loads(created.pop().read_text()))
                    if any(errors):
                        case["query_errors"] = dict(zip(("old", "new"), errors))
                        assert all(errors) and errors[0]["type"] == errors[1]["type"], (
                            "old/new query failure mismatch"
                        )
                        case["status"] = "both_rejected"
                        print("BOTH REJECTED", errors[0]["type"], flush=True)
                        (args.output / "results.json").write_text(
                            json.dumps(report, ensure_ascii=False, indent=2)
                        )
                        continue
                    assert list(panels[0]) == list(panels[1])
                    case["shapes"] = {f: list(p.shape) for f, p in panels[1].items()}
                    case["nonnull_cells"] = sum(
                        int(p.notna().sum().sum()) for p in panels[1].values()
                    )
                    for field in fields:
                        pd.testing.assert_frame_equal(
                            panels[0][field],
                            panels[1][field],
                            check_exact=True,
                            check_index_type=True,
                            check_column_type=True,
                        )
                        assert stable(panels[0][field].attrs) == stable(panels[1][field].attrs), (
                            f"{field}: attrs differ"
                        )
                    assert stable(audits[0]) == stable(audits[1]), "audit differs"
                    case["status"] = "passed"
                    print("PASS", case["nonnull_cells"], "non-null cells", flush=True)
                except Exception as exc:
                    case.update(
                        status="failed", error_type=type(exc).__name__, error=str(exc)[:3000]
                    )
                    print("FAIL", type(exc).__name__, str(exc)[:1000], flush=True)
                (args.output / "results.json").write_text(
                    json.dumps(report, ensure_ascii=False, indent=2)
                )
    failed = [c for c in report["cases"] if c["status"] != "passed"]
    print(f"{len(report['cases']) - len(failed)} passed, {len(failed)} failed", flush=True)
    return bool(failed)


if __name__ == "__main__":
    raise SystemExit(main())

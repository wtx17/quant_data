from datetime import date
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_data import ClickHouseConfig, DataClient, InvalidQueryError
from quant_data.backends import parquet


@pytest.fixture
def setup_membership(tmp_path, monkeypatch):
    path = tmp_path / "events.parquet"
    pq.write_table(
        pa.table(
            {
                "change_date": pd.to_datetime(["2024-01-01", "2024-01-06", "2024-01-09"]),
                "code": ["000001.SZ"] * 3,
                "hs300": [1, -1, 0],
                "zz500": [0, 1, -1],
                "zz1000": [0, 0, 0],
            }
        ),
        path,
    )
    monkeypatch.setattr(parquet, "MEMBERSHIP_EVENTS_PATH", path)
    calls = []

    class Fake:
        def query_arrow(self, query, parameters, **kwargs):
            calls.append((query, parameters))
            return pa.table(
                {
                    "date": [date(2024, 1, d) for d in [5, 8, 9]] + [date(2023, 12, 5)],
                    "code": ["000001.SZ"] * 3 + ["000002.SZ"],
                }
            )

        def close(self):
            pass

    client = DataClient(audit_dir=tmp_path / "audit", clickhouse_client_factory=lambda **kw: Fake())
    client.add_clickhouse_connection("minghu", ClickHouseConfig(host="test"))
    client.register_builtin("membership_events", connection="minghu")
    yield client, calls, tmp_path
    client.close()


def test_state_calendar_order_and_audit(setup_membership):
    client, calls, root = setup_membership
    panel = client.get_panel(
        "membership_events",
        ["membership"],
        "2024-01-05",
        "2024-01-09",
        instruments=["000002.SZ", "000001.SZ"],
    )["membership"]
    assert panel.index.tolist() == list(pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-09"]))
    assert panel.columns.tolist() == ["000002.SZ", "000001.SZ"]
    assert panel.values.tolist() == [[0, 1], [0, 2], [0, 0]]
    assert all(dtype == "int8" for dtype in panel.dtypes)
    assert len(calls) == 1
    assert "stock_base" in calls[0][0]
    assert "instruments" not in calls[0][1]
    audit = json.loads(next((root / "audit").rglob("*.json")).read_text())
    assert audit["calendar_aligned"]
    assert len(audit["source"]["events_sha256"]) == 64


def test_unknown_instrument_fails_and_is_audited(setup_membership):
    client, _, root = setup_membership
    with pytest.raises(InvalidQueryError, match="absent from stock_base.daily"):
        client.get_panel(
            "membership_events",
            ["membership"],
            "2024-01-05",
            "2024-01-09",
            instruments=["999999.SZ"],
        )
    audit = json.loads(next((root / "audit").rglob("*.json")).read_text())
    assert audit["status"] == "failed"


@pytest.mark.parametrize("instruments", [None, []])
def test_all_market_and_empty_selection(setup_membership, instruments):
    client, _, _ = setup_membership
    result = client.get_panel(
        "membership_events", ["membership"], "2024-01-05", "2024-01-09", instruments=instruments
    )["membership"]
    assert result.shape == (3, 2 if instruments is None else 0)


def test_universe_expansion(setup_membership, monkeypatch):
    from quant_data._universes import UniversePanel
    import quant_data.client as client_module

    panel = UniversePanel("hs300", (date(2024, 1, 1),), ("000001.SZ",), ((1,),), "testhash")
    monkeypatch.setattr(client_module, "load_universe", lambda name: panel)
    client, _, _ = setup_membership
    result = client.get_panel(
        "membership_events", ["membership"], "2024-01-05", "2024-01-09", universe="hs300"
    )["membership"]
    assert result["000001.SZ"].tolist() == [1, 2, 0]
    assert result.attrs["parameters"]["universe"]["name"] == "hs300"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"start": "2024-01-01"},
        {"start": "2024-01-01", "end": "2024-01-09", "instruments": "000001.SZ"},
        {"start": "2024-01-01", "end": "2024-01-09", "instruments": [], "universe": "hs300"},
    ],
)
def test_invalid_queries_do_not_access_market(setup_membership, kwargs):
    client, calls, _ = setup_membership
    with pytest.raises(InvalidQueryError):
        client.get_panel("membership_events", ["membership"], **kwargs)
    assert not calls


@pytest.mark.parametrize(
    "start, expected", [("2024-03-31", "2024-02-29"), ("2024-01-05", "2023-12-05")]
)
def test_market_lookback_calendar_month(setup_membership, monkeypatch, start, expected):
    client, _, _ = setup_membership
    from quant_data.backends import clickhouse as clickhouse_module

    queries = []

    def scan(session, source, dataset_name, fields, query):
        queries.append(query)
        return pa.table(
            {
                "date": [pd.Timestamp(expected).date(), pd.Timestamp(start).date()],
                "code": ["000002.SZ", "000001.SZ"],
            }
        )

    monkeypatch.setattr(clickhouse_module, "scan_clickhouse", scan)
    panel = client.get_panel(
        "membership_events", ["membership"], start, start, instruments=["000002.SZ"]
    )["membership"]
    assert queries[0].start.date() == pd.Timestamp(expected).date()
    assert queries[0].end.date() == pd.Timestamp(start).date()
    assert queries[0].fields == ()
    assert queries[0].instruments is None
    assert panel.index.tolist() == [pd.Timestamp(start)]
    assert panel.values.tolist() == [[0]]


def test_builtin_alias_and_event_scan(setup_membership):
    client, _, _ = setup_membership
    client.register_builtin("indices", connection="minghu")
    result = client.get_panel(
        "indices", ["membership"], "2024-01-05", "2024-01-09", instruments=["000001.SZ"]
    )["membership"]
    assert result["000001.SZ"].tolist() == [1, 2, 0]
    assert result.index.tolist() == list(pd.to_datetime(["2024-01-05", "2024-01-08", "2024-01-09"]))


def test_unknown_builtin_is_rejected(setup_membership):
    from quant_data import DatasetRegistrationError

    client, calls, _ = setup_membership
    with pytest.raises(DatasetRegistrationError, match="Unknown built-in dataset"):
        client.register_builtin(dataset="unknown")
    assert not calls

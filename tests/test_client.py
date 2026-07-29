from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_data import (
    DataClient,
    DatasetRegistrationError,
    DatasetSpec,
    DuplicateObservationError,
    FieldNotFoundError,
    InvalidQueryError,
)


def write_table(path: Path, rows: dict[str, list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(rows), path)


@pytest.fixture
def sample_files(tmp_path: Path) -> Path:
    root = tmp_path / "daily"
    write_table(
        root / "part-1.parquet",
        {
            "time": ["2026-01-05", "2026-01-05", "2026-01-06"],
            "ts_code": ["000001.SZ", "000002.SZ", "000001.SZ"],
            "close": [10.32, 11.18, 10.41],
            "volume": [1_200_000, 950_000, 1_300_000],
        },
    )
    write_table(
        root / "part-2.parquet",
        {
            "time": ["2026-01-06"],
            "ts_code": ["000002.SZ"],
            "close": [None],
            "volume": [980_000],
        },
    )
    return root


def make_client(tmp_path: Path, sample_files: Path) -> DataClient:
    client = DataClient(audit_dir=tmp_path / "audit")
    client.register(
        DatasetSpec(
            name="daily",
            paths=[sample_files / "*.parquet"],
            frequency="1d",
            version="v1",
        )
    )
    return client


def test_multi_field_query_filter_order_and_audit(tmp_path: Path, sample_files: Path) -> None:
    client = make_client(tmp_path, sample_files)
    result = client.get_panel(
        "daily",
        ["close", "volume"],
        start="2026-01-05",
        end="2026-01-06",
        instruments=["000002.SZ", "MISSING", "000001.SZ"],
    )

    assert list(result) == ["close", "volume"]
    assert list(result["close"].columns) == ["000002.SZ", "MISSING", "000001.SZ"]
    assert list(result["close"].index) == [
        pd.Timestamp("2026-01-05"),
        pd.Timestamp("2026-01-06"),
    ]
    assert pd.isna(result["close"].loc[pd.Timestamp("2026-01-06"), "000002.SZ"])
    assert result["close"]["MISSING"].isna().all()
    assert result["close"].attrs["dataset"] == "daily"

    audits = list((tmp_path / "audit").rglob("*.json"))
    assert len(audits) == 1
    record = json.loads(audits[0].read_text())
    assert record["status"] == "success"
    assert record["query_id"] == result["close"].attrs["query_id"]
    assert record["source"]["backend"] == "parquet"
    assert len(record["source"]["files"]) == 2


def test_empty_instruments_preserve_requested_fields(tmp_path: Path, sample_files: Path) -> None:
    client = make_client(tmp_path, sample_files)
    result = client.get_panel("daily", ["close"], instruments=[])
    assert list(result) == ["close"]
    assert result["close"].empty
    assert result["close"].index.name == "time"
    assert result["close"].columns.name == "ts_code"


def test_named_universe_expands_in_snapshot_order_and_is_audited(
    tmp_path: Path, sample_files: Path
) -> None:
    client = make_client(tmp_path, sample_files)
    result = client.get_panel("daily", ["close"], universe=" HS300 ")
    panel = result["close"]

    assert len(panel.columns) == 300
    assert panel.columns[0] == "600000.SH"
    assert panel.columns[-1] == "302132.SZ"
    assert panel.loc[pd.Timestamp("2026-01-05"), "000001.SZ"] == 10.32
    assert panel["600000.SH"].isna().all()

    parameters = panel.attrs["parameters"]
    assert parameters["instruments"] == list(panel.columns)
    assert parameters["universe"]["name"] == "hs300"
    assert parameters["universe"]["snapshot_date"] == "2026-07-20"
    assert parameters["universe"]["count"] == 300
    assert len(parameters["universe"]["sha256"]) == 64

    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["parameters"]["instruments"] == list(panel.columns)
    assert audit["parameters"]["universe"] == parameters["universe"]


def test_zz1000_named_universe_uses_its_versioned_snapshot(
    tmp_path: Path, sample_files: Path
) -> None:
    client = make_client(tmp_path, sample_files)
    panel = client.get_panel("daily", ["close"], universe=" ZZ1000 ")["close"]

    assert len(panel.columns) == 1000
    assert panel.columns[0] == "600789.SH"
    assert panel.columns[-1] == "603376.SH"
    assert panel.attrs["parameters"]["universe"]["name"] == "zz1000"
    assert panel.attrs["parameters"]["universe"]["snapshot_date"] == "2026-07-28"
    assert panel.attrs["parameters"]["universe"]["count"] == 1000


def test_panel_rejects_instruments_and_universe_together(
    tmp_path: Path, sample_files: Path
) -> None:
    client = make_client(tmp_path, sample_files)

    with pytest.raises(InvalidQueryError, match="mutually exclusive"):
        client.get_panel(
            "daily",
            ["close"],
            instruments=[],
            universe="hs300",
        )

    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["status"] == "failed"
    assert audit["parameters"]["universe"] == "hs300"


def test_panel_rejects_unknown_universe_and_audits_failure(
    tmp_path: Path, sample_files: Path
) -> None:
    client = make_client(tmp_path, sample_files)

    with pytest.raises(InvalidQueryError, match="supported universes"):
        client.get_panel("daily", ["close"], universe="csi1000")

    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["status"] == "failed"
    assert audit["parameters"]["universe"] == "csi1000"


@pytest.mark.parametrize("method_name", ["get_panel", "get_table"])
def test_query_rejects_bare_string_instruments_and_audits_failure(
    tmp_path: Path,
    sample_files: Path,
    method_name: str,
) -> None:
    client = make_client(tmp_path, sample_files)
    method = getattr(client, method_name)

    with pytest.raises(InvalidQueryError, match="not a string"):
        method("daily", ["close"], instruments="000001.SZ")

    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["status"] == "failed"
    assert audit["parameters"]["instruments"] == "000001.SZ"


def test_get_table_returns_arrow_with_metadata(tmp_path: Path, sample_files: Path) -> None:
    client = make_client(tmp_path, sample_files)
    result = client.get_table(
        "daily", ["close"], start="2026-01-05", instruments=["000001.SZ"], limit=1
    )
    assert result.column_names == ["time", "ts_code", "close"]
    assert result.num_rows == 1
    assert result.schema.metadata[b"quant_data.dataset"] == b"daily"
    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["operation"] == "table"
    assert audit["result_shapes"] == {"table": [1, 3]}


def test_dataset_without_factor_rejects_explicit_adjustment(
    tmp_path: Path, sample_files: Path
) -> None:
    client = make_client(tmp_path, sample_files)
    with pytest.raises(InvalidQueryError, match="does not define a price adjustment factor"):
        client.get_panel("daily", ["close"], adjusted=True)


def test_empty_result_preserves_requested_instruments(tmp_path: Path, sample_files: Path) -> None:
    client = make_client(tmp_path, sample_files)
    result = client.get_panel(
        "daily", ["close"], start="2030-01-01", instruments=["000001.SZ", "NONE"]
    )
    assert list(result["close"].columns) == ["000001.SZ", "NONE"]
    assert result["close"].empty


def test_validation_failure_is_audited(tmp_path: Path, sample_files: Path) -> None:
    client = make_client(tmp_path, sample_files)
    with pytest.raises(FieldNotFoundError):
        client.get_panel("daily", ["unknown"])
    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["status"] == "failed"
    assert audit["error"]["type"] == "FieldNotFoundError"


@pytest.mark.parametrize(
    ("fields", "start", "end", "instruments"),
    [
        ([], None, None, None),
        (["close", "close"], None, None, None),
        (["time"], None, None, None),
        (["close"], "2026-02-01", "2026-01-01", None),
        (["close"], None, None, [""]),
    ],
)
def test_invalid_queries(
    tmp_path: Path,
    sample_files: Path,
    fields: list[str],
    start: str | None,
    end: str | None,
    instruments: list[str] | None,
) -> None:
    client = make_client(tmp_path, sample_files)
    with pytest.raises(InvalidQueryError):
        client.get_panel("daily", fields, start=start, end=end, instruments=instruments)


def test_duplicate_keys_fail(tmp_path: Path) -> None:
    file = tmp_path / "duplicate.parquet"
    write_table(
        file,
        {
            "time": ["2026-01-05", "2026-01-05"],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "close": [1.0, 2.0],
        },
    )
    client = DataClient(tmp_path / "audit")
    client.register(DatasetSpec("duplicate", [file]))
    with pytest.raises(DuplicateObservationError, match="duplicate key pairs"):
        client.get_panel("duplicate", ["close"])


def test_registration_rejects_missing_key_in_any_file(tmp_path: Path) -> None:
    write_table(tmp_path / "good.parquet", {"time": ["2026-01-01"], "ts_code": ["A"], "x": [1]})
    write_table(tmp_path / "bad.parquet", {"time": ["2026-01-02"], "x": [2]})
    client = DataClient(tmp_path / "audit")
    with pytest.raises(DatasetRegistrationError, match="missing key columns"):
        client.register(DatasetSpec("bad", [tmp_path]))


def test_sql_special_characters_are_values_not_sql(tmp_path: Path) -> None:
    file = tmp_path / "special.parquet"
    write_table(
        file,
        {
            "when value": ["2026-01-01"],
            "asset code": ["x'); DROP TABLE source; --"],
            'odd"field': [7],
        },
    )
    client = DataClient(tmp_path / "audit")
    client.register(
        DatasetSpec(
            "special",
            [file],
            time_column="when value",
            instrument_column="asset code",
        )
    )
    result = client.get_panel("special", ['odd"field'], instruments=["x'); DROP TABLE source; --"])
    assert result['odd"field'].iloc[0, 0] == 7

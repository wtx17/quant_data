"""daily_basic needs neither an API nor a ClickHouse calendar."""

from datetime import date
import json
import pytest
from quant_data import DataClient
from tushare_fixtures import write_daily_basic_archive


@pytest.mark.parametrize("instruments", [None, [], ["000001.SZ", "missing"]])
def test_daily_basic_is_fully_local(tmp_path, instruments):
    root = tmp_path / "archive"
    write_daily_basic_archive(
        root,
        {
            "20240102": [
                {"ts_code": "000001.SZ", "trade_date": "20240102", "close": 10.0, "pe": None},
                {"ts_code": "600000.SH", "trade_date": "20240102", "close": 20.0, "pe": 2.0},
            ]
        },
    )
    with DataClient(tmp_path / "audit") as client:
        client.register_tushare("daily_basic", data_dir=root)
        panels = client.get_panel(
            "daily_basic", ["close", "pe"], "2024-01-02", "2024-01-02", instruments
        )
    if instruments == []:
        assert panels["close"].shape == (0, 0)
    else:
        assert panels["close"].loc[date(2024, 1, 2), "000001.SZ"] == 10.0
    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert not audit["calendar_aligned"]
    assert "calendar" not in audit["source"]
    assert "calendar_api" not in audit["parameters"]


def test_local_daily_basic_has_no_api_row_limit(tmp_path):
    root = tmp_path / "archive"
    rows = [
        {"ts_code": f"{i:06}.SZ", "trade_date": "20240102", "close": float(i)} for i in range(6001)
    ]
    write_daily_basic_archive(root, {"20240102": rows})
    with DataClient(tmp_path / "audit") as client:
        client.register_tushare("daily_basic", data_dir=root)
        panel = client.get_panel("daily_basic", ["close"], "2024-01-02", "2024-01-02")["close"]
    assert panel.shape == (1, 6001)

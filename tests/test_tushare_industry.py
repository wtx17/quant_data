"""Industry intervals read only from local archives."""

import json
import pandas as pd
import pytest
from quant_data import SchemaMismatchError
from tushare_fixtures import industry_frame, make_client, write_archive


@pytest.mark.parametrize("dataset", ["ci_index_member", "index_member_all"])
@pytest.mark.parametrize("fixed", [{}, {"is_new": "Y"}])
def test_industry_intervals(tmp_path, dataset, fixed):
    root = tmp_path / "archive"
    write_archive(root, dataset, industry_frame().to_dict("records"), range_start="20190101")
    client, calendar, _ = make_client(tmp_path)
    client.register_tushare(
        "industry",
        data_dir=root,
        dataset=dataset,
        calendar_connection="calendar",
        fixed_params=fixed,
    )
    panels = client.get_panel(
        "industry",
        ["l1_name", "is_new"],
        "2024-01-02",
        "2024-01-08",
        instruments=["000004.SZ", "600000.SH", "unknown"],
    )
    assert list(panels["is_new"].columns) == ["000004.SZ", "600000.SH", "unknown"]
    panel = panels["is_new"]
    assert panel["unknown"].isna().all()
    assert panel.loc[pd.Timestamp(2024, 1, 4), "600000.SH"] == "Y"
    if fixed:
        assert set(panel.stack().dropna()) == {"Y"}
        assert pd.isna(panel.loc[pd.Timestamp(2024, 1, 2), "600000.SH"])
        assert pd.isna(panel.loc[pd.Timestamp(2024, 1, 3), "600000.SH"])
    else:
        assert panel.loc[pd.Timestamp(2024, 1, 3), "600000.SH"] == "N"
    assert len(calendar.calls) == 1
    audit = json.loads(next((tmp_path / "audit").rglob("*.json")).read_text())
    assert audit["source"]["calendar"]["table"] == "stock_base.daily"
    assert "calendar_api" not in audit["parameters"]
    client.close()


def test_industry_conflicting_intervals_fail(tmp_path):
    root = tmp_path / "archive"
    rows = industry_frame().to_dict("records")
    rows.append({**rows[1], "l1_name": "conflict"})
    write_archive(root, "ci_index_member", rows, range_start="20190101")
    client, _, _ = make_client(tmp_path)
    client.register_tushare("ci_index_member", data_dir=root, calendar_connection="calendar")
    with pytest.raises(SchemaMismatchError, match="Conflicting membership"):
        client.get_panel("ci_index_member", ["l1_name"], "2024-01-02", "2024-01-08")
    client.close()

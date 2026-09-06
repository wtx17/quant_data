"""Public time-index contract for ordinary panels across Arrow time types."""

from datetime import date

import pandas as pd
import pyarrow as pa
import pytest

from quant_data.transforms import build_panels


@pytest.mark.parametrize(
    "time_type",
    [pa.date32(), pa.date64(), pa.timestamp("ns"), pa.timestamp("ns", tz="Asia/Shanghai")],
)
@pytest.mark.parametrize("empty", [False, True])
def test_datetime_index_preserves_time_and_timezone(time_type, empty):
    if pa.types.is_timestamp(time_type):
        value = pd.Timestamp("2024-01-02 09:30:00.123456789", tz=time_type.tz)
    else:
        value = date(2024, 1, 2)
    table = pa.table(
        {
            "time": pa.array([] if empty else [value], type=time_type),
            "code": pa.array([] if empty else ["A"], type=pa.string()),
            "value": pa.array([] if empty else [1.0], type=pa.float64()),
        }
    )
    panel = build_panels(
        table,
        dataset_name="test",
        time_column="time",
        instrument_column="code",
        fields=["value"],
        instruments=["A", "MISSING"],
    )["value"]
    assert isinstance(panel.index, pd.DatetimeIndex)
    assert panel.index.name == "time"
    assert list(panel.index) == ([] if empty else [pd.Timestamp(value)])
    expected_tz = time_type.tz if pa.types.is_timestamp(time_type) else None
    assert (str(panel.index.tz) if panel.index.tz is not None else None) == expected_tz
    assert list(panel.columns) == ["A", "MISSING"]

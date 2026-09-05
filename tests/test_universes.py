from __future__ import annotations

import csv
import io
from datetime import date, timedelta

import pytest

from quant_data import InvalidQueryError, SchemaMismatchError
from quant_data._universes import (
    _parse_universe_csv,
    load_universe,
    normalize_universe_name,
)


@pytest.mark.parametrize(
    ("name", "count", "first_date", "last_date"),
    [
        ("hs300", 300, date(2005, 4, 8), date(2026, 6, 15)),
        ("zz500", 500, date(2007, 1, 15), date(2026, 6, 15)),
        ("zz1000", 1000, date(2014, 10, 17), date(2026, 6, 15)),
    ],
)
def test_builtin_universe_panels_are_valid(
    name: str,
    count: int,
    first_date: date,
    last_date: date,
) -> None:
    panel = load_universe(name)

    assert panel.name == name
    assert panel.first_change_date == first_date
    assert panel.last_change_date == last_date
    assert len(panel.change_dates) == len(panel.masks)
    assert len(panel.instruments) > count
    assert len(set(panel.instruments)) == len(panel.instruments)
    assert all(
        len(instrument) == 9
        and instrument[:6].isascii()
        and instrument[:6].isdecimal()
        and instrument[6:] in {".SH", ".SZ", ".BJ"}
        for instrument in panel.instruments
    )
    assert {sum(row) for row in panel.masks} == {count}
    assert len(panel.sha256) == 64


def test_universe_name_is_trimmed_and_case_insensitive() -> None:
    assert normalize_universe_name(" HS300 ") == "hs300"
    assert load_universe(" HS300 ") is load_universe("hs300")


@pytest.mark.parametrize("value", ["", " ", "sz50", "csi1000", 300])
def test_unknown_or_unsupported_universe_name_fails(value: object) -> None:
    with pytest.raises(InvalidQueryError):
        normalize_universe_name(value)


def _active(panel: object, row: int) -> tuple[str, ...]:
    # Keep this helper deliberately small: it mirrors the panel's documented
    # header-order contract without hard-coding a large security list.
    assert hasattr(panel, "instruments") and hasattr(panel, "masks")
    instruments = panel.instruments
    mask = panel.masks[row]
    return tuple(instrument for instrument, included in zip(instruments, mask) if included)


def test_selection_respects_boundaries_and_header_order() -> None:
    panel = load_universe("hs300")
    first, second, last = panel.change_dates[0], panel.change_dates[1], panel.last_change_date

    assert panel.select(first, first) == _active(panel, 0)
    assert panel.select(second, second) == _active(panel, 1)
    expected_union = tuple(
        instrument
        for index, instrument in enumerate(panel.instruments)
        if panel.masks[0][index] or panel.masks[1][index]
    )
    assert panel.select(first, second) == expected_union
    assert panel.select(date(1900, 1, 1), first - timedelta(days=1)) == ()
    assert panel.select(last + timedelta(days=1), date(2100, 1, 1)) == _active(
        panel, len(panel.masks) - 1
    )


def _panel_payload(
    *,
    header: list[str] | None = None,
    rows: list[list[str]] | None = None,
    count: int = 300,
) -> bytes:
    securities = [f"{index:06d}.SZ" for index in range(count)]
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(header or ["change_date", *securities])
    writer.writerows(rows or [["2020-01-01", *("1" for _ in securities)]])
    return output.getvalue().encode()


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (b"\xff", "not valid UTF-8"),
        (b"date,000000.SZ\n2020-01-01,1\n", "first column"),
        (b"change_date\n2020-01-01\n", "at least one security"),
        (b"change_date,000000.SZ\n", "at least one change row"),
        (b"change_date,sh.600000\n2020-01-01,1\n", "invalid instrument code"),
        (
            _panel_payload(header=["change_date", "000000.SZ", "000000.SZ"]),
            "duplicate instrument",
        ),
        (
            _panel_payload(rows=[["2020-01-01", "1"]]),
            "columns",
        ),
        (
            _panel_payload(rows=[["2020-01-01", *(["1"] * 299), "2"]]),
            "invalid mask value",
        ),
        (
            _panel_payload(rows=[["2020-01-01", *(["1"] * 299), "0"]]),
            "expected 300",
        ),
        (
            _panel_payload(
                rows=[
                    ["2020-01-02", *("1" for _ in range(300))],
                    ["2020-01-01", *("1" for _ in range(300))],
                ]
            ),
            "strictly increasing",
        ),
        (
            _panel_payload(rows=[["2020-1-01", *("1" for _ in range(300))]]),
            "invalid change_date",
        ),
    ],
)
def test_malformed_universe_panel_fails(payload: bytes, message: str) -> None:
    with pytest.raises(SchemaMismatchError, match=message):
        _parse_universe_csv("hs300", payload)


def test_utf8_bom_is_accepted() -> None:
    panel = _parse_universe_csv("hs300", b"\xef\xbb\xbf" + _panel_payload())
    assert panel.first_change_date == date(2020, 1, 1)

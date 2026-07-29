from __future__ import annotations

from datetime import date

import pytest

from quant_data import InvalidQueryError, SchemaMismatchError
from quant_data._universes import (
    _parse_universe_csv,
    load_universe,
    normalize_universe_name,
)


@pytest.mark.parametrize(
    ("name", "snapshot_date", "count", "first", "last"),
    [
        ("hs300", date(2026, 7, 20), 300, "600000.SH", "302132.SZ"),
        ("sz50", date(2026, 7, 20), 50, "600028.SH", "688981.SH"),
        ("zz500", date(2026, 7, 20), 500, "600004.SH", "301611.SZ"),
        ("zz1000", date(2026, 7, 28), 1000, "600789.SH", "603376.SH"),
    ],
)
def test_builtin_universe_resources_are_valid(
    name: str,
    snapshot_date: date,
    count: int,
    first: str,
    last: str,
) -> None:
    snapshot = load_universe(name)

    assert snapshot.name == name
    assert snapshot.snapshot_date == snapshot_date
    assert len(snapshot.instruments) == count
    assert len(set(snapshot.instruments)) == count
    assert snapshot.instruments[0] == first
    assert snapshot.instruments[-1] == last
    assert all(
        len(instrument) == 9
        and instrument[:6].isascii()
        and instrument[:6].isdecimal()
        and instrument[6:] in {".SH", ".SZ", ".BJ"}
        for instrument in snapshot.instruments
    )
    assert len(snapshot.sha256) == 64


def test_universe_name_is_trimmed_and_case_insensitive() -> None:
    assert normalize_universe_name(" HS300 ") == "hs300"
    assert load_universe(" HS300 ") is load_universe("hs300")


@pytest.mark.parametrize("value", ["", " ", "csi1000", 300])
def test_unknown_or_invalid_universe_name_fails(value: object) -> None:
    with pytest.raises(InvalidQueryError):
        normalize_universe_name(value)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            b"date,code,code_name\n2026-07-20,sh.600000,name\n",
            "has columns",
        ),
        (
            (
                "updateDate,code,code_name\n2026-07-20,600000.SH,one\n2026-07-21,600001.SH,two\n"
            ).encode(),
            "exactly one updateDate",
        ),
        (
            (
                "updateDate,code,code_name\n2026-07-20,600000.SH,one\n2026-07-20,600000.SH,two\n"
            ).encode(),
            "duplicate instrument",
        ),
        (
            b"updateDate,code,code_name\n2026-07-20,sh.600000,name\n",
            "invalid instrument code",
        ),
        (
            b"updateDate,code,code_name\n2026-07-20,600000.SH,name\n",
            "contains 1 instruments; expected 300",
        ),
    ],
)
def test_malformed_universe_resource_fails(payload: bytes, message: str) -> None:
    with pytest.raises(SchemaMismatchError, match=message):
        _parse_universe_csv("hs300", payload)

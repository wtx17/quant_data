"""Load and validate versioned built-in instrument-universe snapshots."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from importlib.resources import files

from .exceptions import InvalidQueryError, SchemaMismatchError

SUPPORTED_UNIVERSES = ("hs300", "sz50", "zz500")

_EXPECTED_COLUMNS = ("updateDate", "code", "code_name")
_EXPECTED_COUNTS = {
    "hs300": 300,
    "sz50": 50,
    "zz500": 500,
}
_BAOSTOCK_CODE = re.compile(r"(sh|sz|bj)\.(\d{6})", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class UniverseSnapshot:
    """Immutable normalized contents and identity of one stock-pool snapshot."""

    name: str
    snapshot_date: date
    instruments: tuple[str, ...]
    sha256: str


def load_universe(value: str) -> UniverseSnapshot:
    """Normalize a universe name and load its cached package snapshot."""

    return _load_universe(normalize_universe_name(value))


def normalize_universe_name(value: object) -> str:
    """Return a canonical built-in universe name or raise a query error."""

    if not isinstance(value, str) or not value.strip():
        raise InvalidQueryError("universe must be a non-empty string")
    name = value.strip().lower()
    if name not in _EXPECTED_COUNTS:
        raise InvalidQueryError(
            f"Unknown universe {value!r}; supported universes: {list(SUPPORTED_UNIVERSES)}"
        )
    return name


@lru_cache(maxsize=len(SUPPORTED_UNIVERSES))
def _load_universe(name: str) -> UniverseSnapshot:
    resource = files("quant_data").joinpath("resources", "universes", f"{name}.csv")
    try:
        payload = resource.read_bytes()
    except OSError as exc:
        raise SchemaMismatchError(
            f"Unable to read built-in universe snapshot {name!r}: {exc}"
        ) from exc
    return _parse_universe_csv(name, payload)


def _parse_universe_csv(name: str, payload: bytes) -> UniverseSnapshot:
    """Parse one package resource and enforce the built-in snapshot contract."""

    expected_count = _EXPECTED_COUNTS.get(name)
    if expected_count is None:
        raise SchemaMismatchError(f"Unsupported built-in universe snapshot {name!r}")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SchemaMismatchError(
            f"Built-in universe snapshot {name!r} is not valid UTF-8"
        ) from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = tuple(next(reader))
    except StopIteration as exc:
        raise SchemaMismatchError(f"Built-in universe snapshot {name!r} is empty") from exc
    if header != _EXPECTED_COLUMNS:
        raise SchemaMismatchError(
            f"Built-in universe snapshot {name!r} has columns {header}; "
            f"expected {_EXPECTED_COLUMNS}"
        )

    snapshot_dates: set[date] = set()
    instruments: list[str] = []
    seen: set[str] = set()
    for row_number, row in enumerate(reader, start=2):
        if len(row) != len(_EXPECTED_COLUMNS):
            raise SchemaMismatchError(
                f"Built-in universe snapshot {name!r} row {row_number} "
                f"has {len(row)} columns; expected {len(_EXPECTED_COLUMNS)}"
            )
        raw_date, raw_code, code_name = (item.strip() for item in row)
        if not raw_date or not raw_code or not code_name:
            raise SchemaMismatchError(
                f"Built-in universe snapshot {name!r} row {row_number} "
                "contains an empty required value"
            )
        try:
            snapshot_dates.add(date.fromisoformat(raw_date))
        except ValueError as exc:
            raise SchemaMismatchError(
                f"Built-in universe snapshot {name!r} row {row_number} "
                f"has invalid updateDate {raw_date!r}"
            ) from exc

        match = _BAOSTOCK_CODE.fullmatch(raw_code)
        if match is None:
            raise SchemaMismatchError(
                f"Built-in universe snapshot {name!r} row {row_number} "
                f"has invalid Baostock code {raw_code!r}"
            )
        exchange, digits = match.groups()
        instrument = f"{digits}.{exchange.upper()}"
        if instrument in seen:
            raise SchemaMismatchError(
                f"Built-in universe snapshot {name!r} contains duplicate instrument {instrument!r}"
            )
        seen.add(instrument)
        instruments.append(instrument)

    if len(snapshot_dates) != 1:
        rendered_dates = sorted(value.isoformat() for value in snapshot_dates)
        raise SchemaMismatchError(
            f"Built-in universe snapshot {name!r} must contain exactly one "
            f"updateDate; found {rendered_dates}"
        )
    if len(instruments) != expected_count:
        raise SchemaMismatchError(
            f"Built-in universe snapshot {name!r} contains {len(instruments)} "
            f"instruments; expected {expected_count}"
        )

    return UniverseSnapshot(
        name=name,
        snapshot_date=next(iter(snapshot_dates)),
        instruments=tuple(instruments),
        sha256=hashlib.sha256(payload).hexdigest(),
    )

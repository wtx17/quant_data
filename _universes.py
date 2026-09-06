"""Load and select the package's historical instrument-universe panels."""

from __future__ import annotations

import csv
import hashlib
import io
import re
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from importlib.resources import files

from .exceptions import InvalidQueryError, SchemaMismatchError

SUPPORTED_UNIVERSES = ("hs300", "zz500", "zz1000")

_EXPECTED_COUNTS = {
    "hs300": 300,
    "zz500": 500,
    "zz1000": 1000,
}
_CANONICAL_INSTRUMENT = re.compile(r"[0-9]{6}\.(?:SH|SZ|BJ)\Z")
_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")


@dataclass(frozen=True, slots=True)
class UniversePanel:
    """Immutable contents and identity of one historical universe panel.

    ``change_dates[i]`` is the date on which ``masks[i]`` becomes effective.
    The instrument order is the order of the panel header and is therefore
    also the order returned by :meth:`select`.
    """

    name: str
    change_dates: tuple[date, ...]
    instruments: tuple[str, ...]
    masks: tuple[tuple[int, ...], ...]
    sha256: str

    @property
    def first_change_date(self) -> date:
        """Return the first date represented by this panel."""

        return self.change_dates[0]

    @property
    def last_change_date(self) -> date:
        """Return the last date represented by this panel."""

        return self.change_dates[-1]

    def select(self, start: date, end: date) -> tuple[str, ...]:
        """Return instruments belonging to the universe at any selected state.

        The query interval is closed.  A change takes effect on its own date,
        so the state at ``start`` and every state changed during
        ``(start, end]`` contribute to the union.  Before the first panel row
        there is no state; after the last row the last state remains effective.
        """

        if not isinstance(start, date) or isinstance(start, datetime):
            raise InvalidQueryError("universe selection start must be a date")
        if not isinstance(end, date) or isinstance(end, datetime):
            raise InvalidQueryError("universe selection end must be a date")
        if start > end:
            raise InvalidQueryError("universe selection start must be earlier than or equal to end")

        selected = [False] * len(self.instruments)
        state_index = bisect_right(self.change_dates, start) - 1
        if state_index >= 0:
            self._or_row(selected, self.masks[state_index])

        first_changed = bisect_right(self.change_dates, start)
        last_changed = bisect_right(self.change_dates, end)
        for row in self.masks[first_changed:last_changed]:
            self._or_row(selected, row)

        return tuple(
            instrument for instrument, included in zip(self.instruments, selected) if included
        )

    @staticmethod
    def _or_row(selected: list[bool], row: tuple[int, ...]) -> None:
        for index, value in enumerate(row):
            if value:
                selected[index] = True


def load_universe(value: str) -> UniversePanel:
    """Normalize a universe name and load its cached package panel."""

    return _load_universe(normalize_universe_name(value))


def normalize_universe_name(value: object) -> str:
    """Return a canonical supported universe name or raise a query error."""

    if not isinstance(value, str) or not value.strip():
        raise InvalidQueryError("universe must be a non-empty string")
    name = value.strip().lower()
    if name not in _EXPECTED_COUNTS:
        raise InvalidQueryError(
            f"Unknown universe {value!r}; supported universes: {list(SUPPORTED_UNIVERSES)}"
        )
    return name


def normalize_universe_names(value: object) -> tuple[str, ...]:
    """Normalize a name or non-empty list, removing duplicates in input order."""

    if isinstance(value, str):
        return (normalize_universe_name(value),)
    if not isinstance(value, list) or not value:
        raise InvalidQueryError("universe must be a name or a non-empty list of names")
    return tuple(dict.fromkeys(normalize_universe_name(name) for name in value))


@lru_cache(maxsize=len(SUPPORTED_UNIVERSES))
def _load_universe(name: str) -> UniversePanel:
    resource = files("quant_data").joinpath("resources", "universes", f"{name}_panel.csv")
    try:
        payload = resource.read_bytes()
    except OSError as exc:
        raise SchemaMismatchError(
            f"Unable to read built-in universe panel {name!r}: {exc}"
        ) from exc
    return _parse_universe_csv(name, payload)


def _parse_universe_csv(name: str, payload: bytes) -> UniversePanel:
    """Parse one package panel and enforce its strict CSV contract."""

    expected_count = _EXPECTED_COUNTS.get(name)
    if expected_count is None:
        raise SchemaMismatchError(f"Unsupported built-in universe panel {name!r}")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SchemaMismatchError(f"Built-in universe panel {name!r} is not valid UTF-8") from exc

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header = tuple(next(reader))
    except StopIteration as exc:
        raise SchemaMismatchError(f"Built-in universe panel {name!r} is empty") from exc
    if not header or header[0] != "change_date":
        raise SchemaMismatchError(
            f"Built-in universe panel {name!r} must have 'change_date' as its first column"
        )
    instruments = header[1:]
    if not instruments:
        raise SchemaMismatchError(
            f"Built-in universe panel {name!r} must contain at least one security column"
        )
    seen_instruments: set[str] = set()
    for instrument in instruments:
        if not _CANONICAL_INSTRUMENT.fullmatch(instrument):
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} has invalid instrument code {instrument!r}; "
                "expected a canonical code such as '000001.SZ'"
            )
        if instrument in seen_instruments:
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} contains duplicate instrument {instrument!r}"
            )
        seen_instruments.add(instrument)

    change_dates: list[date] = []
    masks: list[tuple[int, ...]] = []
    for row_number, row in enumerate(reader, start=2):
        if len(row) != len(header):
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} row {row_number} has {len(row)} columns; "
                f"expected {len(header)}"
            )
        raw_date = row[0]
        if not _ISO_DATE.fullmatch(raw_date):
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} row {row_number} has invalid change_date "
                f"{raw_date!r}"
            )
        try:
            change_date = date.fromisoformat(raw_date)
        except ValueError as exc:
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} row {row_number} has invalid change_date "
                f"{raw_date!r}"
            ) from exc
        if change_dates and change_date <= change_dates[-1]:
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} change_date values must be strictly increasing; "
                f"row {row_number} has {raw_date!r}"
            )

        mask: list[int] = []
        for column_number, value in enumerate(row[1:], start=2):
            if value not in {"0", "1"}:
                raise SchemaMismatchError(
                    f"Built-in universe panel {name!r} row {row_number} column "
                    f"{column_number} has invalid mask value {value!r}; expected 0 or 1"
                )
            mask.append(int(value))
        if sum(mask) != expected_count:
            raise SchemaMismatchError(
                f"Built-in universe panel {name!r} row {row_number} contains {sum(mask)} "
                f"instruments; expected {expected_count}"
            )
        change_dates.append(change_date)
        masks.append(tuple(mask))

    if not change_dates:
        raise SchemaMismatchError(
            f"Built-in universe panel {name!r} must contain at least one change row"
        )

    return UniversePanel(
        name=name,
        change_dates=tuple(change_dates),
        instruments=tuple(instruments),
        masks=tuple(masks),
        sha256=hashlib.sha256(payload).hexdigest(),
    )

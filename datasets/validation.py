"""Registration validation shared by the dataset factories."""

from __future__ import annotations

from collections.abc import Mapping
from zoneinfo import ZoneInfo

from ..exceptions import DatasetRegistrationError


def _validate_name(name: str) -> None:
    if not name.strip():
        raise DatasetRegistrationError("Dataset name cannot be empty")


def _validate_key_columns(time_column: str, instrument_column: str) -> None:
    if not time_column or not instrument_column:
        raise DatasetRegistrationError("Key column names cannot be empty")
    if time_column == instrument_column:
        raise DatasetRegistrationError("Time and instrument columns must be different")


def _validate_timezone(timezone: str | None) -> None:
    if timezone:
        try:
            ZoneInfo(timezone)
        except Exception as exc:
            raise DatasetRegistrationError(f"Invalid timezone: {timezone!r}") from exc


def _validate_disclosure_windows(
    disclosure_lag: int, fetch_buffer_days: int, fetch_margin_days: int
) -> None:
    for label, value in (
        ("disclosure_lag", disclosure_lag),
        ("fetch_buffer_days", fetch_buffer_days),
        ("fetch_margin_days", fetch_margin_days),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise DatasetRegistrationError(f"{label} must be non-negative")


def _validate_mapping_keys(fixed_params: Mapping[str, object]) -> None:
    if not isinstance(fixed_params, Mapping):
        raise DatasetRegistrationError("Tushare fixed_params must be a mapping")
    invalid_param_keys = [key for key in fixed_params if not isinstance(key, str) or not key]
    if invalid_param_keys:
        raise DatasetRegistrationError("Tushare fixed_params keys must be non-empty strings")

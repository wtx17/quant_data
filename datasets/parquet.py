"""Generic local Parquet dataset registration."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa

from ..backends import parquet as parquet_backend
from ..models import Dataset, Query
from .observation import observation_read_panel
from .validation import _validate_key_columns, _validate_name, _validate_timezone


def parquet_dataset(
    name: str,
    paths: Sequence[str | Path],
    *,
    time_column: str = "time",
    instrument_column: str = "ts_code",
    frequency: str | None = None,
    timezone: str | None = None,
    version: str | None = None,
) -> Dataset:
    """Register local Parquet files as one ordinary-observation dataset.

    Raises
    ------
    DatasetRegistrationError
        If the name, key columns, paths, or timezone are invalid.
    SchemaMismatchError
        If the file schemas cannot be unified.
    """

    _validate_name(name)
    _validate_key_columns(time_column, instrument_column)
    _validate_timezone(timezone)
    files = parquet_backend.resolve_parquet_paths(paths)
    schema = parquet_backend.inspect_parquet_schema(files)
    parquet_backend.validate_parquet_keys(files, time_column, instrument_column)

    def reader(fields: tuple[str, ...], query: Query) -> pa.Table:
        return parquet_backend.scan_parquet(
            files, name, time_column, instrument_column, fields, query
        )

    return Dataset(
        schema=schema,
        time_column=time_column,
        instrument_column=instrument_column,
        read_panel=observation_read_panel(
            name, reader, schema, time_column, instrument_column, None
        ),
        fingerprint=lambda: parquet_backend.parquet_fingerprint(files),
        query_timezone=None,
        frequency=frequency,
        version=version,
        requires_range=False,
        instrument_suffixes=None,
        adjustment=None,
    )

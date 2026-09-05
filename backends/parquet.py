"""DuckDB-backed readers for generic local Parquet datasets."""

from __future__ import annotations

import glob
import hashlib
import os
from collections.abc import Iterable, Sequence
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ..exceptions import DatasetRegistrationError, SchemaMismatchError
from ..models import Query

BUILTIN_MEMBERSHIP_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("code", pa.string()),
        ("membership", pa.int8()),
    ]
)
MEMBERSHIP_EVENTS_PATH = (
    Path(__file__).resolve().parents[1] / "resources/universes/membership_events.parquet"
)


def quote_identifier(value: str) -> str:
    """Quote one DuckDB identifier, neutralizing embedded quotes."""

    return '"' + value.replace('"', '""') + '"'


def read_membership_events(path: Path) -> pa.Table:
    """Read the packaged full membership event history."""

    return pq.read_table(path)


def membership_events_fingerprint(path: Path) -> dict[str, object]:
    """Return the packaged event file identity for a query audit."""

    return {
        "events_path": str(path),
        "events_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }


def resolve_parquet_paths(paths: Sequence[str | Path]) -> tuple[Path, ...]:
    """Expand files, directories, and glob patterns into sorted unique files."""

    resolved: set[Path] = set()
    for raw in paths:
        value = Path(raw).expanduser()
        matches: Iterable[Path]
        if value.is_dir():
            matches = value.rglob("*.parquet")
        elif glob.has_magic(str(value)):
            matches = (Path(item) for item in glob.glob(str(value), recursive=True))
        else:
            matches = (value,)
        for match in matches:
            if match.is_file() and match.suffix.lower() == ".parquet":
                resolved.add(match.resolve())
    if not resolved:
        raise DatasetRegistrationError("No Parquet files matched the supplied paths")
    return tuple(sorted(resolved, key=str))


def inspect_parquet_schema(files: tuple[Path, ...]) -> pa.Schema:
    """Merge the schemas of every file with permissive Arrow promotion."""

    schemas: list[pa.Schema] = []
    try:
        for path in files:
            schemas.append(pq.read_schema(path))
        return pa.unify_schemas(schemas, promote_options="permissive")
    except (pa.ArrowException, OSError) as exc:
        raise SchemaMismatchError(f"Unable to unify Parquet schemas: {exc}") from exc


def validate_parquet_keys(
    files: tuple[Path, ...],
    time_column: str,
    instrument_column: str,
) -> None:
    """Require both key columns in every matched file."""

    for path in files:
        file_schema = pq.read_schema(path)
        missing_keys = {time_column, instrument_column}.difference(file_schema.names)
        if missing_keys:
            raise DatasetRegistrationError(
                f"Parquet file {path} is missing key columns: {sorted(missing_keys)}"
            )


def scan_parquet(
    files: tuple[Path, ...],
    dataset_name: str,
    time_column: str,
    instrument_column: str,
    fields: tuple[str, ...],
    query: Query,
) -> pa.Table:
    """Project and filter generic Parquet observations with DuckDB.

    Parameters
    ----------
    files
        Resolved Parquet files forming one logical table.
    dataset_name
        Name included in error messages.
    time_column, instrument_column
        Panel key columns.
    fields
        Non-key columns to project.
    query
        Normalized closed bounds and instruments.

    Returns
    -------
    pyarrow.Table
        Long table ordered by time and instrument.

    Raises
    ------
    SchemaMismatchError
        If DuckDB fails to read or convert the matched files.
    """

    time_col = quote_identifier(time_column)
    instrument_col = quote_identifier(instrument_column)
    projected = [
        f"CAST({time_col} AS TIMESTAMP) AS {time_col}",
        instrument_col,
        *[quote_identifier(field) for field in fields],
    ]
    sql = f"SELECT {', '.join(projected)} FROM read_parquet(?, union_by_name = true) AS source"
    params: list[object] = [[str(path) for path in files]]
    clauses: list[str] = []

    if query.start is not None:
        clauses.append(f"CAST({time_col} AS TIMESTAMP) >= ?")
        params.append(query.start)
    if query.end is not None:
        clauses.append(f"CAST({time_col} AS TIMESTAMP) <= ?")
        params.append(query.end)

    connection = duckdb.connect(database=":memory:")
    try:
        if query.instruments is not None:
            requested = pa.table({instrument_column: list(query.instruments)})
            connection.register("requested_instruments", requested)
            sql += f" INNER JOIN requested_instruments AS requested USING ({instrument_col})"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += f" ORDER BY {time_col}, {instrument_col}"
        return connection.execute(sql, params).to_arrow_table()
    except (duckdb.Error, pa.ArrowException) as exc:
        raise SchemaMismatchError(
            f"Parquet query failed for dataset {dataset_name!r}: {exc}"
        ) from exc
    finally:
        connection.close()


def parquet_fingerprint(files: tuple[Path, ...]) -> dict[str, object]:
    """Return current file statistics for a query audit."""

    fingerprints: list[dict[str, object]] = []
    for path in files:
        stat = os.stat(path)
        fingerprints.append(
            {"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
        )
    return {"backend": "parquet", "files": fingerprints}

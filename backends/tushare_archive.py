"""Manifest-backed local Tushare Parquet archive reader."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..exceptions import DatasetRegistrationError, InvalidQueryError, SchemaMismatchError
from ..models import Query
from .parquet import quote_identifier
from .tushare_catalog import MembershipSemantics, TushareDatasetCatalog
from .tushare_common import coerce_frame, fixed_param_value


@dataclass(frozen=True, slots=True)
class ArchivePartition:
    """Store one validated manifest partition."""

    key: str
    relative_path: str
    path: Path
    rows: int
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class TushareArchive:
    """Store validated manifest state for one local logical dataset."""

    data_dir: Path
    manifest_path: Path
    manifest_version: int
    dataset: str
    schema_hash: str
    range_start: date
    range_end: date
    updated_at: str | None
    fixed_params: dict[str, object]
    partitions: tuple[ArchivePartition, ...]


# Fixed parameters a local snapshot can reconstruct from stored columns.
_LOCAL_FIXED_PARAM_COLUMNS: dict[str, dict[str, str]] = {
    "daily_basic": {},
    "income": {
        "ann_date": "ann_date",
        "f_ann_date": "f_ann_date",
        "report_type": "report_type",
        "comp_type": "comp_type",
    },
    "balancesheet": {
        "ann_date": "ann_date",
        "report_type": "report_type",
        "comp_type": "comp_type",
    },
    "cashflow": {
        "ann_date": "ann_date",
        "f_ann_date": "f_ann_date",
        "report_type": "report_type",
        "comp_type": "comp_type",
    },
    "fina_indicator": {"ann_date": "ann_date"},
    "express": {"ann_date": "ann_date"},
    "forecast": {"ann_date": "ann_date", "type": "type"},
    "stk_holdernumber": {"ann_date": "ann_date", "enddate": "end_date"},
    "ci_index_member": {
        "l1_code": "l1_code",
        "l2_code": "l2_code",
        "l3_code": "l3_code",
        "is_new": "is_new",
    },
    "index_member_all": {
        "l1_code": "l1_code",
        "l2_code": "l2_code",
        "l3_code": "l3_code",
        "is_new": "is_new",
    },
}


# Tushare's statement APIs default to the latest consolidated statement when
# ``report_type`` is omitted. The archive deliberately contains all twelve
# report types, so local scans must make that remote default explicit.
LOCAL_DEFAULT_FIXED_PARAMS: dict[str, dict[str, object]] = {
    "income": {"report_type": "1"},
    "balancesheet": {"report_type": "1"},
    "cashflow": {"report_type": "1"},
}


def effective_local_fixed_params(
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
) -> dict[str, object]:
    """Merge archive statement defaults with caller-provided parameters."""

    return {
        **LOCAL_DEFAULT_FIXED_PARAMS.get(catalog.name, {}),
        **dict(fixed_params),
    }


def validate_local_fixed_params(
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
) -> None:
    """Reject parameters a stored snapshot cannot reconstruct."""

    allowed = _LOCAL_FIXED_PARAM_COLUMNS[catalog.name]
    unsupported = set(fixed_params).difference(allowed)
    if unsupported:
        raise DatasetRegistrationError(
            f"Tushare Parquet dataset {catalog.name!r} cannot reconstruct "
            f"fixed parameters: {sorted(unsupported)}"
        )
    invalid_values = [
        key
        for key, value in fixed_params.items()
        if value is None
        or isinstance(value, (Mapping, Sequence))
        and not isinstance(value, str)
    ]
    if invalid_values:
        raise DatasetRegistrationError(
            "Tushare Parquet fixed_params values must be non-null scalars: "
            f"{sorted(invalid_values)}"
        )


def load_archive(
    data_dir: str | Path,
    dataset_name: str,
    catalog: TushareDatasetCatalog,
    fixed_params: Mapping[str, object],
) -> TushareArchive:
    """Load and fully validate one manifest-backed local dataset.

    Raises
    ------
    DatasetRegistrationError
        If the directory, manifest, version, range, fields, or partitions
        are invalid.
    SchemaMismatchError
        If a partition conflicts with the Tushare catalog schema.
    """

    root = Path(data_dir).expanduser().resolve()
    if not root.is_dir():
        raise DatasetRegistrationError(
            f"Tushare Parquet data directory does not exist: {root}"
        )
    manifest_path = root / dataset_name / "_manifest.json"
    manifest = _load_manifest(manifest_path)
    if manifest.get("manifest_version") != 1:
        raise DatasetRegistrationError(
            f"Unsupported manifest version for {dataset_name!r}: "
            f"{manifest.get('manifest_version')!r}"
        )
    if manifest.get("dataset") != dataset_name:
        raise DatasetRegistrationError(
            f"Manifest dataset differs for {dataset_name!r}: {manifest.get('dataset')!r}"
        )
    range_start = _parse_manifest_date(manifest.get("range_start"), f"{dataset_name} range_start")
    range_end = _parse_manifest_date(manifest.get("range_end"), f"{dataset_name} range_end")
    if range_start > range_end:
        raise DatasetRegistrationError(f"Manifest range is reversed for {dataset_name!r}")
    schema_hash = manifest.get("schema_hash")
    if not isinstance(schema_hash, str) or not schema_hash:
        raise DatasetRegistrationError(f"Manifest for {dataset_name!r} has no schema_hash")
    _validate_manifest_fields(manifest, catalog)
    partitions = _resolve_manifest_partitions(root, manifest, catalog, dataset_name)
    updated_at = manifest.get("updated_at")
    if updated_at is not None and not isinstance(updated_at, str):
        raise DatasetRegistrationError(f"Manifest updated_at is invalid for {dataset_name!r}")
    return TushareArchive(
        data_dir=root,
        manifest_path=manifest_path,
        manifest_version=1,
        dataset=dataset_name,
        schema_hash=schema_hash,
        range_start=range_start,
        range_end=range_end,
        updated_at=updated_at,
        fixed_params=dict(fixed_params),
        partitions=partitions,
    )


def validate_snapshot_bounds(
    archive: TushareArchive,
    dataset_name: str,
    start: datetime | None,
    end: datetime | None,
    *,
    fetch_buffer_days: int,
    is_disclosure: bool,
) -> None:
    """Validate explicit query bounds and the PIT carry-in buffer.

    Raises
    ------
    InvalidQueryError
        If a bound falls outside the archived range, or a disclosure panel
        would need carry-in events predating the archive.
    """

    if start is not None and start.date() < archive.range_start:
        raise InvalidQueryError(
            f"Dataset {dataset_name!r} starts at {archive.range_start.isoformat()}; "
            f"requested start is {start.date().isoformat()}"
        )
    if end is not None and end.date() > archive.range_end:
        raise InvalidQueryError(
            f"Dataset {dataset_name!r} ends at {archive.range_end.isoformat()}; "
            f"requested end is {end.date().isoformat()}"
        )
    if (
        is_disclosure
        and start is not None
        and (start - timedelta(days=fetch_buffer_days)).date() < archive.range_start
    ):
        earliest = archive.range_start + timedelta(days=fetch_buffer_days)
        raise InvalidQueryError(
            f"Dataset {dataset_name!r} PIT panel requires start on or after "
            f"{earliest.isoformat()} to preserve the {fetch_buffer_days}-day "
            "carry-in buffer"
        )


def read_archive_frame(
    archive: TushareArchive,
    dataset_name: str,
    catalog: TushareDatasetCatalog,
    query: Query,
    columns: tuple[str, ...],
    *,
    date_column: str | None = None,
    membership: MembershipSemantics | None = None,
    order_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Read normalized archive rows with DuckDB.

    Parameters
    ----------
    archive
        Validated manifest state.
    dataset_name
        Registration name included in error messages.
    catalog
        Logical dataset catalog providing the instrument column.
    query
        Buffered or direct request with closed bounds and instruments.
    columns
        Projection in output order.
    date_column
        Column receiving closed string date bounds.
    membership
        Interval semantics; applies overlap predicates instead of a date
        range.
    order_columns
        Deterministic ordering columns.

    Returns
    -------
    pandas.DataFrame
        Coerced frame restricted to ``columns``.

    Raises
    ------
    SchemaMismatchError
        If DuckDB fails or the archive conflicts with the catalog.
    """

    if query.instruments == ():
        return coerce_frame(pd.DataFrame(columns=columns), catalog.schema)
    partitions = select_archive_partitions(archive, query)
    if not partitions:
        return coerce_frame(pd.DataFrame(columns=columns), catalog.schema)

    projected = ", ".join(quote_identifier(column) for column in columns)
    sql = f"SELECT {projected} FROM read_parquet(?, union_by_name = true) AS source"
    params: list[object] = [[str(partition.path) for partition in partitions]]
    clauses: list[str] = []
    fixed_columns = _LOCAL_FIXED_PARAM_COLUMNS[catalog.name]
    for key, value in archive.fixed_params.items():
        column = fixed_columns[key]
        clauses.append(f"source.{quote_identifier(column)} = ?")
        params.append(fixed_param_value(value))

    if date_column is not None:
        if query.start is not None:
            clauses.append(f"source.{quote_identifier(date_column)} >= ?")
            params.append(query.start.strftime("%Y%m%d"))
        if query.end is not None:
            clauses.append(f"source.{quote_identifier(date_column)} <= ?")
            params.append(query.end.strftime("%Y%m%d"))
    elif membership is not None:
        start_col = quote_identifier(membership.interval_start_column)
        end_col = quote_identifier(membership.interval_end_column)
        if query.start is not None:
            clauses.append(
                f"(NULLIF(CAST(source.{end_col} AS VARCHAR), '') IS NULL "
                f"OR source.{end_col} >= ?)"
            )
            params.append(query.start.strftime("%Y%m%d"))
        if query.end is not None:
            clauses.append(f"NULLIF(CAST(source.{start_col} AS VARCHAR), '') IS NOT NULL")
            clauses.append(f"source.{start_col} <= ?")
            params.append(query.end.strftime("%Y%m%d"))

    connection = duckdb.connect(database=":memory:")
    try:
        if query.instruments is not None:
            requested = pa.table({catalog.instrument_column: list(query.instruments)})
            connection.register("requested_instruments", requested)
            instrument = quote_identifier(catalog.instrument_column)
            sql += f" INNER JOIN requested_instruments AS requested USING ({instrument})"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        available_order = [column for column in order_columns if column in columns]
        if available_order:
            sql += " ORDER BY " + ", ".join(
                quote_identifier(column) for column in available_order
            )
        frame = connection.execute(sql, params).fetchdf()
    except (duckdb.Error, pa.ArrowException, ValueError, TypeError) as exc:
        raise SchemaMismatchError(
            f"Tushare Parquet query failed for dataset {dataset_name!r}: {exc}"
        ) from exc
    finally:
        connection.close()
    return coerce_frame(frame, catalog.schema)


def select_archive_partitions(
    archive: TushareArchive,
    query: Query,
) -> tuple[ArchivePartition, ...]:
    """Prune daily partitions to the closed query range."""

    if archive.dataset != "daily_basic":
        return archive.partitions
    if query.start is None or query.end is None:
        raise InvalidQueryError("Local daily_basic queries require both start and end")
    start_key = query.start.strftime("%Y%m%d")
    end_key = query.end.strftime("%Y%m%d")
    return tuple(
        partition for partition in archive.partitions if start_key <= partition.key <= end_key
    )


def archive_fingerprint(
    archive: TushareArchive,
    calendar_connection: str,
) -> dict[str, object]:
    """Return manifest metadata and current partition file statistics."""

    partitions: list[dict[str, object]] = []
    for item in archive.partitions:
        stat = item.path.stat()
        partitions.append(
            {
                "key": item.key,
                "path": str(item.path),
                "relative_path": item.relative_path,
                "rows": item.rows,
                "expected_size": item.bytes,
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": item.sha256,
            }
        )
    return {
        "backend": "parquet",
        "format": "tushare-archive",
        "dataset": archive.dataset,
        "data_dir": str(archive.data_dir),
        "manifest": str(archive.manifest_path),
        "manifest_version": archive.manifest_version,
        "schema_hash": archive.schema_hash,
        "range_start": archive.range_start.isoformat(),
        "range_end": archive.range_end.isoformat(),
        "updated_at": archive.updated_at,
        "calendar_connection": calendar_connection,
        "fixed_params": {
            str(key): str(value) for key, value in archive.fixed_params.items()
        },
        "partitions": partitions,
    }


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DatasetRegistrationError(f"Tushare manifest does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetRegistrationError(f"Unable to read Tushare manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DatasetRegistrationError(f"Tushare manifest is not an object: {path}")
    return payload


def _parse_manifest_date(value: object, label: str) -> date:
    if not isinstance(value, str):
        raise DatasetRegistrationError(f"{label} must use YYYYMMDD format")
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise DatasetRegistrationError(f"{label} must use YYYYMMDD format: {value!r}") from exc


def _validate_manifest_fields(
    manifest: Mapping[str, Any], catalog: TushareDatasetCatalog
) -> None:
    fields = manifest.get("fields")
    if not isinstance(fields, list):
        raise DatasetRegistrationError(f"Manifest fields are invalid for {catalog.name!r}")
    names = {
        item.get("name")
        for item in fields
        if isinstance(item, Mapping) and isinstance(item.get("name"), str)
    }
    missing = set(catalog.schema.names).difference(names)
    if missing:
        raise DatasetRegistrationError(
            f"Manifest for {catalog.name!r} is missing catalog fields: {sorted(missing)}"
        )


def _resolve_manifest_partitions(
    data_dir: Path,
    manifest: Mapping[str, Any],
    catalog: TushareDatasetCatalog,
    logical_name: str,
) -> tuple[ArchivePartition, ...]:
    raw_partitions = manifest.get("partitions")
    if not isinstance(raw_partitions, Mapping) or not raw_partitions:
        raise DatasetRegistrationError(f"Manifest for {logical_name!r} has no partitions")
    result: list[ArchivePartition] = []
    seen: set[Path] = set()
    for raw_key in sorted(raw_partitions, key=str):
        entry = raw_partitions[raw_key]
        if not isinstance(raw_key, str) or not isinstance(entry, Mapping):
            raise DatasetRegistrationError(f"Manifest partition is invalid for {logical_name!r}")
        if logical_name == "daily_basic":
            _parse_manifest_date(raw_key, f"{logical_name} partition key")
        relative = entry.get("relative_path")
        rows = entry.get("rows")
        size = entry.get("bytes")
        sha256 = entry.get("sha256")
        if not isinstance(relative, str) or not relative:
            raise DatasetRegistrationError(f"Manifest partition {raw_key!r} has no relative_path")
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            raise DatasetRegistrationError(f"Manifest partition {raw_key!r} has invalid rows")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise DatasetRegistrationError(f"Manifest partition {raw_key!r} has invalid bytes")
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise DatasetRegistrationError(f"Manifest partition {raw_key!r} has invalid sha256")
        path = (data_dir / relative).resolve()
        if not path.is_relative_to(data_dir):
            raise DatasetRegistrationError(
                f"Manifest partition escapes the data directory: {relative!r}"
            )
        if path in seen:
            raise DatasetRegistrationError(
                f"Manifest contains a duplicate partition path: {relative!r}"
            )
        seen.add(path)
        if not path.is_file() or path.suffix.lower() != ".parquet":
            raise DatasetRegistrationError(f"Parquet partition does not exist: {path}")
        stat = path.stat()
        if stat.st_size != size:
            raise DatasetRegistrationError(
                f"Parquet partition size differs from manifest: {path}"
            )
        try:
            parquet = pq.ParquetFile(path)
            file_schema = parquet.schema_arrow
            actual_rows = parquet.metadata.num_rows
        except (pa.ArrowException, OSError) as exc:
            raise SchemaMismatchError(
                f"Unable to inspect Tushare Parquet partition {path}: {exc}"
            ) from exc
        if actual_rows != rows:
            raise DatasetRegistrationError(
                f"Parquet partition row count differs from manifest: {path}"
            )
        missing = set(catalog.schema.names).difference(file_schema.names)
        if missing:
            raise SchemaMismatchError(
                f"Parquet partition {path} is missing catalog fields: {sorted(missing)}"
            )
        for field in catalog.schema:
            stored = file_schema.field(field.name)
            if not _archive_type_compatible(stored.type, field.type):
                raise SchemaMismatchError(
                    f"Parquet partition {path} field {field.name!r} has type "
                    f"{stored.type}, expected archive-compatible {field.type}"
                )
        result.append(
            ArchivePartition(
                key=raw_key,
                relative_path=relative,
                path=path,
                rows=rows,
                bytes=size,
                sha256=sha256,
            )
        )
    return tuple(result)


def _archive_type_compatible(stored: pa.DataType, public: pa.DataType) -> bool:
    if pa.types.is_date32(public):
        return bool(pa.types.is_string(stored) or pa.types.is_date32(stored))
    return bool(stored.equals(public))

"""DuckDB-backed local Parquet scanners."""

from __future__ import annotations

import glob
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..exceptions import (
    DatasetRegistrationError,
    InvalidQueryError,
    SchemaMismatchError,
)
from ..models import (
    DataQuery,
    DatasetContract,
    DatasetDefinition,
    DatasetSpec,
    RegisteredDataset,
    TushareParquetDatasetSpec,
)
from .tushare import TushareBackend
from .tushare_catalog import (
    DisclosureSemantics,
    MembershipSemantics,
    TushareDatasetCatalog,
)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class _TradingCalendarProvider(Protocol):
    def has_connection(self, name: str) -> bool:
        """Return whether a named Tushare connection has been configured."""

        ...

    def fetch_calendar(
        self, connection: str, exchange: str, start: datetime, end: datetime
    ) -> list[date]:
        """Fetch open sessions for a closed calendar range."""

        ...


@dataclass(frozen=True, slots=True)
class _ArchivePartition:
    key: str
    relative_path: str
    path: Path
    rows: int
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _TushareParquetSource:
    data_dir: Path
    manifest_path: Path
    manifest_version: int
    dataset: str
    schema_hash: str
    range_start: date
    range_end: date
    updated_at: str | None
    fixed_params: dict[str, object]
    partitions: tuple[_ArchivePartition, ...]


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
_LOCAL_DEFAULT_FIXED_PARAMS: dict[str, dict[str, object]] = {
    "income": {"report_type": "1"},
    "balancesheet": {"report_type": "1"},
    "cashflow": {"report_type": "1"},
}


class DuckDBParquetBackend:
    """Read generic or Tushare-shaped local Parquet data with DuckDB.

    Parameters
    ----------
    calendar_provider
        Optional provider used only by ``TushareParquetDatasetSpec`` panel
        queries. Generic Parquet queries never use it.

    Notes
    -----
    Generic datasets resolve files and merge their schemas. Manifest-backed
    Tushare snapshots instead use the shared logical catalog for their public
    schema and reproduce disclosure, membership, and observation panels locally.
    Every scan uses a short-lived in-memory DuckDB connection.
    """

    def __init__(self, calendar_provider: Any | None = None) -> None:
        self._calendar_provider = cast(_TradingCalendarProvider | None, calendar_provider)

    def prepare(self, definition: DatasetDefinition) -> RegisteredDataset:
        """Resolve and validate a generic or Tushare-shaped Parquet dataset.

        Parameters
        ----------
        definition
            A generic :class:`quant_data.DatasetSpec` or a manifest-backed
            :class:`quant_data.TushareParquetDatasetSpec`.

        Returns
        -------
        RegisteredDataset
            Dataset with a public Arrow schema, source descriptor, and query
            contract.

        Raises
        ------
        DatasetRegistrationError
            If paths, manifests, connections, or required columns are invalid.
        SchemaMismatchError
            If generic schemas cannot be unified or an archive footer conflicts
            with the Tushare catalog.
        """

        if isinstance(definition, DatasetSpec):
            return self._prepare_generic(definition)
        if isinstance(definition, TushareParquetDatasetSpec):
            return self._prepare_tushare(definition)
        raise DatasetRegistrationError(
            "Parquet backend requires DatasetSpec or TushareParquetDatasetSpec"
        )

    def _prepare_generic(self, definition: DatasetSpec) -> RegisteredDataset:
        files = self._resolve_paths(definition.paths)
        schema = self._inspect_schema(tuple(str(path) for path in files))
        for path in files:
            file_schema = pq.read_schema(path)
            missing_keys = {
                definition.time_column,
                definition.instrument_column,
            }.difference(file_schema.names)
            if missing_keys:
                raise DatasetRegistrationError(
                    f"Parquet file {path} is missing key columns: {sorted(missing_keys)}"
                )
        contract = DatasetContract(
            source_time_column=definition.time_column,
            instrument_column=definition.instrument_column,
            panel_time_column=definition.time_column,
            panel_frequency=definition.frequency,
            timezone=definition.timezone,
            version=definition.version,
        )
        return RegisteredDataset(
            spec=definition,
            schema=schema,
            source=tuple(files),
            contract=contract,
        )

    def _prepare_tushare(self, definition: TushareParquetDatasetSpec) -> RegisteredDataset:
        logical_name = definition.dataset or definition.name
        catalog = TushareBackend._catalog(logical_name)
        TushareBackend._validate_definition(definition, catalog)
        self._validate_local_fixed_params(definition, catalog)
        if self._calendar_provider is None:
            raise DatasetRegistrationError(
                "Tushare Parquet datasets require a trading-calendar provider"
            )
        if not self._calendar_provider.has_connection(definition.calendar_connection):
            raise DatasetRegistrationError(
                f"Tushare calendar connection {definition.calendar_connection!r} is not configured"
            )

        data_dir = Path(definition.data_dir).expanduser().resolve()
        if not data_dir.is_dir():
            raise DatasetRegistrationError(
                f"Tushare Parquet data directory does not exist: {data_dir}"
            )
        manifest_path = data_dir / logical_name / "_manifest.json"
        manifest = self._load_manifest(manifest_path)
        if manifest.get("manifest_version") != 1:
            raise DatasetRegistrationError(
                f"Unsupported manifest version for {logical_name!r}: "
                f"{manifest.get('manifest_version')!r}"
            )
        if manifest.get("dataset") != logical_name:
            raise DatasetRegistrationError(
                f"Manifest dataset differs for {logical_name!r}: {manifest.get('dataset')!r}"
            )
        range_start = self._parse_manifest_date(
            manifest.get("range_start"), f"{logical_name} range_start"
        )
        range_end = self._parse_manifest_date(
            manifest.get("range_end"), f"{logical_name} range_end"
        )
        if range_start > range_end:
            raise DatasetRegistrationError(f"Manifest range is reversed for {logical_name!r}")
        schema_hash = manifest.get("schema_hash")
        if not isinstance(schema_hash, str) or not schema_hash:
            raise DatasetRegistrationError(f"Manifest for {logical_name!r} has no schema_hash")
        self._validate_manifest_fields(manifest, catalog)
        partitions = self._resolve_manifest_partitions(data_dir, manifest, catalog, logical_name)
        updated_at = manifest.get("updated_at")
        if updated_at is not None and not isinstance(updated_at, str):
            raise DatasetRegistrationError(f"Manifest updated_at is invalid for {logical_name!r}")
        effective_fixed_params = {
            **_LOCAL_DEFAULT_FIXED_PARAMS.get(logical_name, {}),
            **definition.fixed_params,
        }
        source = _TushareParquetSource(
            data_dir=data_dir,
            manifest_path=manifest_path,
            manifest_version=1,
            dataset=logical_name,
            schema_hash=schema_hash,
            range_start=range_start,
            range_end=range_end,
            updated_at=updated_at,
            fixed_params=effective_fixed_params,
            partitions=partitions,
        )
        return RegisteredDataset(
            spec=definition,
            schema=catalog.schema,
            source=source,
            contract=TushareBackend._contract(definition, catalog),
        )

    def _inspect_schema(self, files: tuple[str, ...]) -> pa.Schema:
        schemas: list[pa.Schema] = []
        try:
            for path in files:
                schemas.append(pq.read_schema(path))
            return pa.unify_schemas(schemas, promote_options="permissive")
        except (pa.ArrowException, OSError) as exc:
            raise SchemaMismatchError(f"Unable to unify Parquet schemas: {exc}") from exc

    def fingerprint(self, dataset: RegisteredDataset) -> dict[str, object]:
        """Return sanitized file or manifest provenance for a query audit.

        Parameters
        ----------
        dataset
            Prepared Parquet dataset.

        Returns
        -------
        dict[str, object]
            Generic file stats, or archive manifest metadata and partition
            checksums with current file stats.
        """

        if isinstance(dataset.source, _TushareParquetSource):
            source = dataset.source
            partitions: list[dict[str, object]] = []
            for item in source.partitions:
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
            spec = dataset.spec
            if not isinstance(spec, TushareParquetDatasetSpec):
                raise SchemaMismatchError("Invalid Tushare Parquet specification")
            return {
                "backend": "parquet",
                "format": "tushare-archive",
                "dataset": source.dataset,
                "data_dir": str(source.data_dir),
                "manifest": str(source.manifest_path),
                "manifest_version": source.manifest_version,
                "schema_hash": source.schema_hash,
                "range_start": source.range_start.isoformat(),
                "range_end": source.range_end.isoformat(),
                "updated_at": source.updated_at,
                "calendar_connection": spec.calendar_connection,
                "fixed_params": {
                    str(key): str(value) for key, value in source.fixed_params.items()
                },
                "partitions": partitions,
            }

        fingerprints: list[dict[str, object]] = []
        for path in self._generic_files(dataset):
            stat = os.stat(path)
            fingerprints.append(
                {"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
            )
        return {"backend": "parquet", "files": fingerprints}

    def scan(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Scan Parquet observations for panel construction.

        Parameters
        ----------
        dataset
            Prepared Parquet dataset.
        query
            Normalized fields, closed bounds, instruments.

        Returns
        -------
        pyarrow.Table
            Ordered Arrow long table with both method-specific keys.

        Raises
        ------
        SchemaMismatchError
            If DuckDB, stored values, or prepared state conflict with the
            registered schema.
        """

        if isinstance(dataset.spec, DatasetSpec):
            return self._scan_generic(dataset, query)
        if isinstance(dataset.spec, TushareParquetDatasetSpec):
            return self._scan_tushare_observations(dataset, query)
        raise SchemaMismatchError("Invalid Parquet registered dataset")

    def _scan_generic(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        spec = dataset.spec
        if not isinstance(spec, DatasetSpec):
            raise SchemaMismatchError("Invalid generic Parquet registered dataset")
        time_col = _quote_identifier(spec.time_column)
        instrument_col = _quote_identifier(spec.instrument_column)
        projected = [
            f"CAST({time_col} AS TIMESTAMP) AS {time_col}",
            instrument_col,
            *[_quote_identifier(field) for field in query.fields],
        ]
        sql = f"SELECT {', '.join(projected)} FROM read_parquet(?, union_by_name = true) AS source"
        params: list[object] = [[str(path) for path in self._generic_files(dataset)]]
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
                requested = pa.table({spec.instrument_column: list(query.instruments)})
                connection.register("requested_instruments", requested)
                sql += f" INNER JOIN requested_instruments AS requested USING ({instrument_col})"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += f" ORDER BY {time_col}, {instrument_col}"
            return connection.execute(sql, params).to_arrow_table()
        except (duckdb.Error, pa.ArrowException) as exc:
            raise SchemaMismatchError(
                f"Parquet query failed for dataset {spec.name!r}: {exc}"
            ) from exc
        finally:
            connection.close()

    def _scan_tushare_observations(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        _, _, catalog = self._tushare_state(dataset)
        selected = (
            dataset.contract.panel_time_column,
            dataset.contract.instrument_column,
            *query.fields,
        )
        if query.instruments == ():
            return TushareBackend._empty_arrow(catalog.schema, selected)
        remote_fields = TushareBackend._remote_columns(selected, catalog)
        semantics = catalog.semantics
        frame = self._read_archive_frame(
            dataset,
            query,
            remote_fields,
            date_column=dataset.contract.panel_time_column,
            order_columns=semantics.source_order,
        )
        frame = TushareBackend._filter_time(frame, dataset.contract.panel_time_column, query)
        frame = TushareBackend._sort_by(frame, semantics.source_order)
        return TushareBackend._frame_to_arrow(frame, catalog.schema, selected)

    def normalize_snapshot_query(
        self,
        dataset: RegisteredDataset,
        query: DataQuery,
    ) -> DataQuery:
        """Validate panel bounds and archived history for the PIT carry-in buffer."""

        spec, source, catalog = self._tushare_state(dataset)
        start = query.start
        end = query.end
        if start is not None and start.date() < source.range_start:
            raise InvalidQueryError(
                f"Dataset {spec.name!r} starts at {source.range_start.isoformat()}; "
                f"requested start is {start.date().isoformat()}"
            )
        if end is not None and end.date() > source.range_end:
            raise InvalidQueryError(
                f"Dataset {spec.name!r} ends at {source.range_end.isoformat()}; "
                f"requested end is {end.date().isoformat()}"
            )
        if (
            isinstance(catalog.semantics, DisclosureSemantics)
            and start is not None
            and (start - timedelta(days=spec.fetch_buffer_days)).date() < source.range_start
        ):
            earliest = source.range_start + timedelta(days=spec.fetch_buffer_days)
            raise InvalidQueryError(
                f"Dataset {spec.name!r} PIT panel requires start on or after "
                f"{earliest.isoformat()} to preserve the {spec.fetch_buffer_days}-day "
                "carry-in buffer"
            )
        return replace(query, start=start, end=end)

    def panel_kind(self, dataset: RegisteredDataset) -> str:
        """Return the catalog's panel construction kind."""

        _, _, catalog = self._tushare_state(dataset)
        if isinstance(catalog.semantics, DisclosureSemantics):
            return "disclosure"
        if isinstance(catalog.semantics, MembershipSemantics):
            return "membership"
        return "observation"

    def scan_disclosure_events(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Read local disclosure events needed by a PIT panel."""

        spec, _, catalog = self._tushare_state(dataset)
        semantics = catalog.semantics
        if not isinstance(semantics, DisclosureSemantics):
            raise InvalidQueryError(
                f"Tushare Parquet dataset {catalog.name!r} is not disclosure data"
            )
        if query.start is None or query.end is None:
            raise InvalidQueryError(
                f"Dataset {spec.name!r} point-in-time panel requires both start and end"
            )
        selected = self._unique_columns(
            (
                semantics.disclosure_column,
                catalog.instrument_column,
                semantics.period_column,
                *semantics.identity_columns,
                *query.fields,
            )
        )
        remote_fields = TushareBackend._remote_columns(selected, catalog)
        fetch_query = replace(
            query,
            start=query.start - timedelta(days=spec.fetch_buffer_days),
        )
        frame = self._read_archive_frame(
            dataset,
            fetch_query,
            remote_fields,
            date_column=semantics.disclosure_column,
            order_columns=self._unique_columns(
                (
                    semantics.disclosure_column,
                    catalog.instrument_column,
                    semantics.period_column,
                    *semantics.revision_order,
                )
            ),
        )
        frame = TushareBackend._filter_time(frame, semantics.disclosure_column, fetch_query)
        frame = TushareBackend._sort_by(
            frame,
            self._unique_columns(
                (
                    semantics.disclosure_column,
                    catalog.instrument_column,
                    semantics.period_column,
                    *semantics.revision_order,
                )
            ),
        )
        return TushareBackend._frame_to_arrow(frame, catalog.schema, selected)

    def trade_calendar(self, dataset: RegisteredDataset, query: DataQuery) -> list[date]:
        """Fetch the buffered Tushare calendar for a local PIT panel."""

        spec, _, _ = self._tushare_state(dataset)
        if query.start is None or query.end is None:
            raise InvalidQueryError(f"Dataset {spec.name!r} panel requires both start and end")
        provider = self._calendar()
        return provider.fetch_calendar(
            spec.calendar_connection,
            spec.calendar_exchange,
            query.start - timedelta(days=spec.fetch_buffer_days),
            query.end + timedelta(days=spec.fetch_margin_days),
        )

    def pit_panel_semantics(self, dataset: RegisteredDataset) -> tuple[str, str, tuple[str, ...]]:
        """Return disclosure, report-period, and revision-order columns."""

        _, _, catalog = self._tushare_state(dataset)
        semantics = catalog.semantics
        if not isinstance(semantics, DisclosureSemantics):
            raise SchemaMismatchError(
                f"Tushare Parquet dataset {catalog.name!r} is not disclosure data"
            )
        return (
            semantics.disclosure_column,
            semantics.period_column,
            semantics.revision_order,
        )

    def scan_membership_panel(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Expand local membership intervals over the remote trade calendar."""

        spec, _, catalog = self._tushare_state(dataset)
        semantics = catalog.semantics
        if not isinstance(semantics, MembershipSemantics):
            raise SchemaMismatchError(
                f"Tushare Parquet dataset {catalog.name!r} is not membership data"
            )
        if query.start is None or query.end is None:
            raise InvalidQueryError(
                f"Dataset {spec.name!r} membership panel requires both start and end"
            )
        selected_raw = self._unique_columns(
            (
                semantics.interval_start_column,
                catalog.instrument_column,
                semantics.interval_end_column,
                *semantics.identity_columns,
                *query.fields,
            )
        )
        remote_fields = TushareBackend._remote_columns(selected_raw, catalog)
        frame = self._read_archive_frame(
            dataset,
            query,
            remote_fields,
            membership=semantics,
            order_columns=semantics.source_order,
        )
        frame = TushareBackend._filter_membership_overlap(frame, semantics, query)
        calendar = self._calendar().fetch_calendar(
            spec.calendar_connection,
            spec.calendar_exchange,
            query.start,
            query.end,
        )
        selected_panel = self._unique_columns(
            (
                semantics.panel_time_column,
                catalog.instrument_column,
                *query.fields,
            )
        )
        expanded = TushareBackend._expand_membership_panel(
            frame,
            semantics,
            catalog.instrument_column,
            query,
            calendar,
            selected_panel,
        )
        return TushareBackend._membership_frame_to_arrow(
            expanded,
            catalog.schema,
            semantics.panel_time_column,
            selected_panel,
        )

    def _read_archive_frame(
        self,
        dataset: RegisteredDataset,
        query: DataQuery,
        columns: tuple[str, ...],
        *,
        date_column: str | None = None,
        membership: MembershipSemantics | None = None,
        order_columns: tuple[str, ...] = (),
    ) -> pd.DataFrame:
        spec, source, catalog = self._tushare_state(dataset)
        if query.instruments == ():
            return TushareBackend._coerce_frame(pd.DataFrame(columns=columns), catalog.schema)
        partitions = self._select_archive_partitions(source, catalog, query)
        if not partitions:
            return TushareBackend._coerce_frame(pd.DataFrame(columns=columns), catalog.schema)

        projected = ", ".join(_quote_identifier(column) for column in columns)
        sql = f"SELECT {projected} FROM read_parquet(?, union_by_name = true) AS source"
        params: list[object] = [[str(partition.path) for partition in partitions]]
        clauses: list[str] = []
        fixed_columns = _LOCAL_FIXED_PARAM_COLUMNS[catalog.name]
        for key, value in source.fixed_params.items():
            column = fixed_columns[key]
            clauses.append(f"source.{_quote_identifier(column)} = ?")
            params.append(self._fixed_param_value(value))

        if date_column is not None:
            if query.start is not None:
                clauses.append(f"source.{_quote_identifier(date_column)} >= ?")
                params.append(query.start.strftime("%Y%m%d"))
            if query.end is not None:
                clauses.append(f"source.{_quote_identifier(date_column)} <= ?")
                params.append(query.end.strftime("%Y%m%d"))
        elif membership is not None:
            start_col = _quote_identifier(membership.interval_start_column)
            end_col = _quote_identifier(membership.interval_end_column)
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
                instrument = _quote_identifier(catalog.instrument_column)
                sql += f" INNER JOIN requested_instruments AS requested USING ({instrument})"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            available_order = [column for column in order_columns if column in columns]
            if available_order:
                sql += " ORDER BY " + ", ".join(
                    _quote_identifier(column) for column in available_order
                )
            frame = connection.execute(sql, params).fetchdf()
        except (duckdb.Error, pa.ArrowException, ValueError, TypeError) as exc:
            raise SchemaMismatchError(
                f"Tushare Parquet query failed for dataset {spec.name!r}: {exc}"
            ) from exc
        finally:
            connection.close()
        return TushareBackend._coerce_frame(frame, catalog.schema)

    def close(self) -> None:
        """Release backend resources.

        Notes
        -----
        DuckDB scans close their own connections. The shared calendar provider
        is owned and closed by :class:`quant_data.DataClient`.
        """

        return None

    def _tushare_state(
        self, dataset: RegisteredDataset
    ) -> tuple[
        TushareParquetDatasetSpec,
        _TushareParquetSource,
        TushareDatasetCatalog,
    ]:
        spec = dataset.spec
        source = dataset.source
        if not isinstance(spec, TushareParquetDatasetSpec) or not isinstance(
            source, _TushareParquetSource
        ):
            raise SchemaMismatchError("Invalid Tushare Parquet registered dataset")
        return spec, source, TushareBackend._catalog(source.dataset)

    def _calendar(self) -> _TradingCalendarProvider:
        if self._calendar_provider is None:
            raise SchemaMismatchError("Tushare Parquet calendar provider is unavailable")
        return self._calendar_provider

    @staticmethod
    def _unique_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
        result: list[str] = []
        for column in columns:
            if column not in result:
                result.append(column)
        return tuple(result)

    @staticmethod
    def _fixed_param_value(value: object) -> object:
        if isinstance(value, datetime):
            return value.strftime("%Y%m%d")
        if isinstance(value, date):
            return value.strftime("%Y%m%d")
        return value

    @staticmethod
    def _select_archive_partitions(
        source: _TushareParquetSource,
        catalog: TushareDatasetCatalog,
        query: DataQuery,
    ) -> tuple[_ArchivePartition, ...]:
        if catalog.name != "daily_basic":
            return source.partitions
        if query.start is None or query.end is None:
            raise InvalidQueryError("Local daily_basic queries require both start and end")
        start_key = query.start.strftime("%Y%m%d")
        end_key = query.end.strftime("%Y%m%d")
        return tuple(
            partition for partition in source.partitions if start_key <= partition.key <= end_key
        )

    @staticmethod
    def _validate_local_fixed_params(
        definition: TushareParquetDatasetSpec,
        catalog: TushareDatasetCatalog,
    ) -> None:
        allowed = _LOCAL_FIXED_PARAM_COLUMNS[catalog.name]
        unsupported = set(definition.fixed_params).difference(allowed)
        if unsupported:
            raise DatasetRegistrationError(
                f"Tushare Parquet dataset {catalog.name!r} cannot reconstruct "
                f"fixed parameters: {sorted(unsupported)}"
            )
        invalid_values = [
            key
            for key, value in definition.fixed_params.items()
            if value is None
            or isinstance(value, (Mapping, Sequence))
            and not isinstance(value, str)
        ]
        if invalid_values:
            raise DatasetRegistrationError(
                "Tushare Parquet fixed_params values must be non-null scalars: "
                f"{sorted(invalid_values)}"
            )

    @staticmethod
    def _load_manifest(path: Path) -> dict[str, Any]:
        if not path.is_file():
            raise DatasetRegistrationError(f"Tushare manifest does not exist: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DatasetRegistrationError(
                f"Unable to read Tushare manifest {path}: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise DatasetRegistrationError(f"Tushare manifest is not an object: {path}")
        return payload

    @staticmethod
    def _parse_manifest_date(value: object, label: str) -> date:
        if not isinstance(value, str):
            raise DatasetRegistrationError(f"{label} must use YYYYMMDD format")
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError as exc:
            raise DatasetRegistrationError(f"{label} must use YYYYMMDD format: {value!r}") from exc

    @staticmethod
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

    @staticmethod
    def _resolve_manifest_partitions(
        data_dir: Path,
        manifest: Mapping[str, Any],
        catalog: TushareDatasetCatalog,
        logical_name: str,
    ) -> tuple[_ArchivePartition, ...]:
        raw_partitions = manifest.get("partitions")
        if not isinstance(raw_partitions, Mapping) or not raw_partitions:
            raise DatasetRegistrationError(f"Manifest for {logical_name!r} has no partitions")
        result: list[_ArchivePartition] = []
        seen: set[Path] = set()
        for raw_key in sorted(raw_partitions, key=str):
            entry = raw_partitions[raw_key]
            if not isinstance(raw_key, str) or not isinstance(entry, Mapping):
                raise DatasetRegistrationError(
                    f"Manifest partition is invalid for {logical_name!r}"
                )
            if logical_name == "daily_basic":
                DuckDBParquetBackend._parse_manifest_date(
                    raw_key,
                    f"{logical_name} partition key",
                )
            relative = entry.get("relative_path")
            rows = entry.get("rows")
            size = entry.get("bytes")
            sha256 = entry.get("sha256")
            if not isinstance(relative, str) or not relative:
                raise DatasetRegistrationError(
                    f"Manifest partition {raw_key!r} has no relative_path"
                )
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
                if not DuckDBParquetBackend._archive_type_compatible(stored.type, field.type):
                    raise SchemaMismatchError(
                        f"Parquet partition {path} field {field.name!r} has type "
                        f"{stored.type}, expected archive-compatible {field.type}"
                    )
            result.append(
                _ArchivePartition(
                    key=raw_key,
                    relative_path=relative,
                    path=path,
                    rows=rows,
                    bytes=size,
                    sha256=sha256,
                )
            )
        return tuple(result)

    @staticmethod
    def _archive_type_compatible(stored: pa.DataType, public: pa.DataType) -> bool:
        if pa.types.is_date32(public):
            return bool(pa.types.is_string(stored) or pa.types.is_date32(stored))
        return cast(bool, stored.equals(public))

    @staticmethod
    def _generic_files(dataset: RegisteredDataset) -> tuple[Path, ...]:
        source = dataset.source
        if not isinstance(source, tuple) or not all(isinstance(path, Path) for path in source):
            raise SchemaMismatchError("Invalid generic Parquet dataset source")
        return source

    @staticmethod
    def _resolve_paths(paths: Sequence[str | Path]) -> list[Path]:
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
        return sorted(resolved, key=str)

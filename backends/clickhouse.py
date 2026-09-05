"""ClickHouse backend implemented with clickhouse-connect."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Callable

import pyarrow as pa

from .clickhouse_catalog import MINGHU_TABLE_COLUMN_TYPES
from ..exceptions import (
    BackendConnectionError,
    DatasetRegistrationError,
    RemoteQueryError,
    SchemaMismatchError,
)
from ..models import (
    ClickHouseConfig,
    ClickHouseDatasetSpec,
    DataQuery,
    DatasetContract,
    DatasetDefinition,
    PriceAdjustment,
    RegisteredDataset,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MINGHU_DAILY_PRICE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "pclose",
    "ztprice",
    "dtprice",
    "omax_op",
    "omin_op",
)
_QUERY_TABLE_ALIAS = "_q"


@dataclass(frozen=True, slots=True)
class ClickHouseSource:
    """Store prepared ClickHouse source metadata.

    Parameters
    ----------
    connection
        Named connection profile.
    table
        Unquoted ``database.table`` identifier.
    column_types
        Mapping of column names to ClickHouse type strings.
    schema_hash
        Stable hash of the prepared schema.
    schema_source
        ``"catalog"`` for built-in schemas or ``"remote"`` after a
        ``DESCRIBE TABLE`` lookup.
    time_expression
        SQL expression for a synthesized minute timestamp, if applicable.
    """

    connection: str
    table: str
    column_types: dict[str, str]
    schema_hash: str
    schema_source: str
    time_expression: str | None = None


def _quote_identifier(value: str) -> str:
    parts = value.split(".")
    if not parts or not all(_IDENTIFIER.fullmatch(part) for part in parts):
        raise DatasetRegistrationError(f"Invalid ClickHouse identifier: {value!r}")
    return ".".join(f"`{part}`" for part in parts)


def _qualified_identifier(value: str, table_alias: str) -> str:
    return f"{_quote_identifier(table_alias)}.{_quote_identifier(value)}"


class ClickHouseBackend:
    """Scan ClickHouse tables through ``clickhouse-connect``.

    Parameters
    ----------
    client_factory
        Optional callable compatible with ``clickhouse_connect.get_client``.
        The factory is invoked lazily and supports offline testing.

    Notes
    -----
    Built-in Minghu schemas come from
    ``quant_data.backends.clickhouse_catalog``. Their registration performs
    no network access. Custom tables require remote schema discovery.
    """

    def __init__(self, client_factory: Callable[..., Any] | None = None) -> None:
        self._configs: dict[str, ClickHouseConfig] = {}
        self._clients: dict[str, Any] = {}
        self._client_factory = client_factory

    def add_connection(self, name: str, config: ClickHouseConfig) -> None:
        """Add or replace a validated connection profile.

        Parameters
        ----------
        name
            Identifier used by dataset specifications.
        config
            Connection and timeout settings.

        Raises
        ------
        DatasetRegistrationError
            If the name, host, port, or timeout is invalid.

        Notes
        -----
        Adding a profile does not open a connection. Replacing an open profile
        closes its cached client first.
        """

        if not name or not _IDENTIFIER.fullmatch(name):
            raise DatasetRegistrationError(f"Invalid ClickHouse connection name: {name!r}")
        if not config.host:
            raise DatasetRegistrationError("ClickHouse host cannot be empty")
        if config.port <= 0 or config.port > 65535:
            raise DatasetRegistrationError("ClickHouse port must be between 1 and 65535")
        if config.connect_timeout <= 0 or config.query_timeout <= 0:
            raise DatasetRegistrationError("ClickHouse timeouts must be positive")
        if name in self._clients:
            self._clients.pop(name).close()
        self._configs[name] = config

    def prepare(self, definition: DatasetDefinition) -> RegisteredDataset:
        """Prepare a ClickHouse specification and Arrow schema.

        Parameters
        ----------
        definition
            ClickHouse dataset specification referencing a configured profile.

        Returns
        -------
        RegisteredDataset
            Prepared schema, source fingerprint inputs, and any recognized
            price-adjustment policy.

        Raises
        ------
        DatasetRegistrationError
            If the definition, profile, identifiers, or configured columns are
            invalid.
        RemoteQueryError
            If a custom table cannot be described remotely.
        SchemaMismatchError
            If a ClickHouse type cannot be mapped to Arrow.
        """

        if not isinstance(definition, ClickHouseDatasetSpec):
            raise DatasetRegistrationError("ClickHouse backend requires ClickHouseDatasetSpec")
        table = _quote_identifier(definition.table)
        self._connection_config(definition.connection)
        catalog_columns = MINGHU_TABLE_COLUMN_TYPES.get(definition.table)
        if catalog_columns is None:
            column_types = self._describe_column_types(definition, table)
            schema_source = "remote"
        else:
            column_types = dict(catalog_columns)
            schema_source = "catalog"

        time_expression = None
        result_column_types = dict(column_types)
        if definition.time_column == "date_time" and {"date", "time_int"} <= column_types.keys():
            date_type = self._arrow_type(column_types["date"])
            if not (pa.types.is_date(date_type) or pa.types.is_integer(date_type)):
                raise DatasetRegistrationError(
                    "Minute date must be Date, Date32 or YYYYMMDD integer"
                )
            if not pa.types.is_integer(self._arrow_type(column_types["time_int"])):
                raise DatasetRegistrationError(
                    "Minute time_int must be integer milliseconds since midnight"
                )
            day = _qualified_identifier("date", _QUERY_TABLE_ALIAS)
            if pa.types.is_integer(date_type):
                day = f"YYYYMMDDToDate({day})"
            time_expression = (
                f"(toDateTime64({day}, 3, 'Asia/Shanghai') + "
                f"toIntervalMillisecond({_qualified_identifier('time_int', _QUERY_TABLE_ALIAS)}))"
            )
            result_column_types["date_time"] = "DateTime64(3, 'Asia/Shanghai')"

        required = {definition.time_column, definition.instrument_column}
        if definition.partition_column:
            required.add(definition.partition_column)
        required.update(definition.order_columns)
        missing = required.difference(result_column_types)
        if missing:
            raise DatasetRegistrationError(
                f"ClickHouse table {definition.table!r} is missing configured columns: "
                f"{sorted(missing)}"
            )

        try:
            schema = pa.schema(
                [
                    pa.field(name, self._arrow_type(type_name))
                    for name, type_name in result_column_types.items()
                ]
            )
        except Exception as exc:
            raise SchemaMismatchError(
                f"Unable to map ClickHouse schema for {definition.table!r}: {exc}"
            ) from exc
        normalized = json.dumps(sorted(column_types.items()), separators=(",", ":"))
        source = ClickHouseSource(
            definition.connection,
            definition.table,
            column_types,
            hashlib.sha256(normalized.encode()).hexdigest(),
            schema_source,
            time_expression,
        )
        adjustment = None
        if definition.table.lower() == "stock_base.daily" and "hfq" in column_types:
            adjustment = PriceAdjustment(
                factor_column="hfq",
                fields=tuple(
                    field for field in _MINGHU_DAILY_PRICE_FIELDS if field in column_types
                ),
            )
        requires_time_range = bool(
            definition.require_time_range is True
            or (definition.require_time_range is None and definition.partition_column is not None)
        )
        contract = DatasetContract(
            source_time_column=definition.time_column,
            instrument_column=definition.instrument_column,
            panel_time_column=definition.time_column,
            panel_frequency=definition.frequency,
            timezone="Asia/Shanghai" if time_expression else definition.timezone,
            version=definition.version,
            panel_requires_time_range=requires_time_range,
        )
        return RegisteredDataset(
            spec=definition,
            schema=schema,
            source=source,
            contract=contract,
            adjustment=adjustment,
        )

    def scan(self, dataset: RegisteredDataset, query: DataQuery) -> pa.Table:
        """Run a parameterized ClickHouse query.

        Parameters
        ----------
        dataset
            Prepared ClickHouse dataset.
        query
            Normalized projection and filter request.

        Returns
        -------
        pyarrow.Table
            Ordered Arrow result from ``query_arrow``.

        Raises
        ------
        BackendConnectionError
            If the lazy connection cannot be created.
        RemoteQueryError
            If ClickHouse rejects or fails the query.
        SchemaMismatchError
            If the prepared source state is inconsistent.

        Notes
        -----
        Time, partition, and instrument values are bound parameters.
        Minghu ``code`` values are projected and filtered with their ``.SZ``,
        ``.SH``, or ``.BJ`` suffix derived from ``exg``. The Shanghai/Shenzhen
        flow table derives its suffix from the stock code prefix.
        """

        spec = dataset.spec
        source = dataset.source
        if not isinstance(spec, ClickHouseDatasetSpec) or not isinstance(source, ClickHouseSource):
            raise SchemaMismatchError("Invalid ClickHouse registered dataset")
        client = self._client(source.connection)
        selected = (spec.time_column, spec.instrument_column, *query.fields)
        add_code_suffix = self._adds_code_suffix(spec, source)
        code_expression = (
            self._suffixed_code_expression(_QUERY_TABLE_ALIAS, source) if add_code_suffix else None
        )
        projection = self._projection(
            selected,
            source.column_types,
            table_alias=_QUERY_TABLE_ALIAS,
            suffixed_column=spec.instrument_column if add_code_suffix else None,
            time_expression=source.time_expression,
            code_expression=code_expression,
        )
        sql = (
            f"SELECT {projection} FROM {_quote_identifier(source.table)} "
            f"AS {_quote_identifier(_QUERY_TABLE_ALIAS)}"
        )
        clauses: list[str] = []
        parameters: dict[str, object] = {}

        time_type = (
            "DateTime64(3, 'Asia/Shanghai')"
            if source.time_expression
            else source.column_types[spec.time_column]
        )
        time_expression = source.time_expression or _qualified_identifier(
            spec.time_column, _QUERY_TABLE_ALIAS
        )
        if query.start is not None:
            clauses.append(f"{time_expression} >= {{start:{time_type}}}")
            parameters["start"] = (
                query.start.date()
                if time_type.startswith("Date") and not time_type.startswith("DateTime")
                else query.start
            )
        if query.end is not None:
            clauses.append(f"{time_expression} <= {{end:{time_type}}}")
            parameters["end"] = (
                query.end.date()
                if time_type.startswith("Date") and not time_type.startswith("DateTime")
                else query.end
            )
        # Bind local timestamp text to preserve milliseconds with older drivers too.
        if source.time_expression:
            if query.start is not None:
                parameters["start"] = query.start.strftime("%Y-%m-%d %H:%M:%S.%f")
            if query.end is not None:
                parameters["end"] = query.end.strftime("%Y-%m-%d %H:%M:%S.%f")
        if spec.partition_column and query.start is not None and query.end is not None:
            partition = _qualified_identifier(spec.partition_column, _QUERY_TABLE_ALIAS)
            partition_type = source.column_types[spec.partition_column]
            clauses.extend(
                [
                    f"{partition} >= {{partition_start:{partition_type}}}",
                    f"{partition} <= {{partition_end:{partition_type}}}",
                ]
            )
            parameters["partition_start"] = query.start.date()
            parameters["partition_end"] = query.end.date()
            if (
                source.time_expression
                and spec.partition_column == "date"
                and pa.types.is_integer(self._arrow_type(partition_type))
            ):
                parameters["partition_start"] = int(query.start.strftime("%Y%m%d"))
                parameters["partition_end"] = int(query.end.strftime("%Y%m%d"))
        if query.instruments is not None:
            instrument = _qualified_identifier(spec.instrument_column, _QUERY_TABLE_ALIAS)
            if code_expression is not None:
                instrument = code_expression
            # clickhouse-connect serializes lists as ClickHouse Array literals (`[...]`).
            # Tuples become SQL tuple literals (`(...)`) and cannot bind to Array(String).
            clauses.append(f"{instrument} IN {{instruments:Array(String)}}")
            parameters["instruments"] = list(query.instruments)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        order_columns = spec.order_columns or (spec.time_column, spec.instrument_column)
        sql += " ORDER BY " + ", ".join(
            time_expression
            if item == spec.time_column
            else _qualified_identifier(item, _QUERY_TABLE_ALIAS)
            for item in order_columns
        )
        try:
            return client.query_arrow(sql, parameters=parameters, use_strings=True)
        except Exception as exc:
            raise RemoteQueryError(
                f"ClickHouse query failed for dataset {spec.name!r}: {exc}"
            ) from exc

    def fingerprint(self, dataset: RegisteredDataset) -> dict[str, object]:
        """Return sanitized connection, table, and schema provenance.

        Parameters
        ----------
        dataset
            Prepared ClickHouse dataset.

        Returns
        -------
        dict[str, object]
            JSON-serializable metadata excluding username and password.

        Raises
        ------
        SchemaMismatchError
            If the dataset was not prepared by this backend.
        """

        spec = dataset.spec
        source = dataset.source
        if not isinstance(spec, ClickHouseDatasetSpec) or not isinstance(source, ClickHouseSource):
            raise SchemaMismatchError("Invalid ClickHouse registered dataset")
        config = self._configs[source.connection]
        return {
            "backend": "clickhouse",
            "connection": source.connection,
            "host": config.host,
            "port": config.port,
            "secure": config.secure,
            "table": source.table,
            "schema_hash": source.schema_hash,
            "schema_source": source.schema_source,
            "time_expression": source.time_expression,
        }

    def close(self) -> None:
        """Close all cached ClickHouse clients."""

        for client in self._clients.values():
            client.close()
        self._clients.clear()

    def _client(self, name: str) -> Any:
        existing = self._clients.get(name)
        if existing is not None:
            return existing
        config = self._connection_config(name)
        password = config.password
        if password is None and config.password_env:
            password = os.environ.get(config.password_env)
            if password is None:
                raise BackendConnectionError(
                    f"ClickHouse password environment variable {config.password_env!r} is not set"
                )
        factory = self._client_factory
        if factory is None:
            try:
                from clickhouse_connect import get_client
            except ImportError as exc:
                raise BackendConnectionError(
                    "ClickHouse support is not installed; install quant-data[clickhouse]"
                ) from exc
            factory = get_client
        try:
            client = factory(
                host=config.host,
                port=config.port,
                username=config.username,
                password=password or "",
                secure=config.secure,
                connect_timeout=config.connect_timeout,
                send_receive_timeout=config.query_timeout,
            )
        except Exception as exc:
            raise BackendConnectionError(
                f"Unable to connect to ClickHouse profile {name!r} at {config.host}:{config.port}: {exc}"
            ) from exc
        self._clients[name] = client
        return client

    def _connection_config(self, name: str) -> ClickHouseConfig:
        config = self._configs.get(name)
        if config is None:
            raise DatasetRegistrationError(f"ClickHouse connection {name!r} is not configured")
        return config

    def _describe_column_types(
        self, definition: ClickHouseDatasetSpec, quoted_table: str
    ) -> dict[str, str]:
        client = self._client(definition.connection)
        try:
            description = client.query_arrow(f"DESCRIBE TABLE {quoted_table}", use_strings=True)
            names = description.column("name").to_pylist()
            types = description.column("type").to_pylist()
            return {str(name): str(type_name) for name, type_name in zip(names, types)}
        except Exception as exc:
            raise RemoteQueryError(
                f"Unable to inspect ClickHouse table {definition.table!r}: {exc}"
            ) from exc

    @staticmethod
    def _adds_code_suffix(spec: ClickHouseDatasetSpec, source: ClickHouseSource) -> bool:
        return spec.instrument_column == "code" and (
            "exg" in source.column_types or source.table == "zhangruiqi.zb_cj_flow_min"
        )

    @staticmethod
    def _suffixed_code_expression(table_alias: str, source: ClickHouseSource) -> str:
        code = _qualified_identifier("code", table_alias)
        if source.table == "zhangruiqi.zb_cj_flow_min":
            suffix = (
                f"multiIf(startsWith({code}, '6'), '.SH', "
                f"startsWith({code}, '0') OR startsWith({code}, '3'), '.SZ', '')"
            )
            return f"concat({code}, {suffix})"
        exchange = _qualified_identifier("exg", table_alias)
        suffix = f"multiIf({exchange} = 1, '.SZ', {exchange} = 2, '.SH', {exchange} = 3, '.BJ', '')"
        return f"concat({code}, {suffix})"

    @staticmethod
    def _projection(
        columns: tuple[str, ...],
        column_types: dict[str, str],
        *,
        table_alias: str,
        suffixed_column: str | None,
        time_expression: str | None = None,
        code_expression: str | None = None,
    ) -> str:
        expressions = []
        for column in columns:
            output = _quote_identifier(column)
            qualified = _qualified_identifier(column, table_alias)
            if column == "date_time" and time_expression:
                expressions.append(f"{time_expression} AS {output}")
            elif column == suffixed_column and code_expression is not None:
                expressions.append(f"{code_expression} AS {output}")
            elif column_types[column].startswith("FixedString"):
                expressions.append(f"toString({qualified}) AS {output}")
            else:
                expressions.append(f"{qualified} AS {output}")
        return ", ".join(expressions)

    @staticmethod
    def _arrow_type(type_name: str) -> pa.DataType:
        value = type_name.strip()
        for wrapper in ("Nullable", "LowCardinality"):
            prefix = f"{wrapper}("
            if value.startswith(prefix) and value.endswith(")"):
                return ClickHouseBackend._arrow_type(value[len(prefix) : -1])

        integer_types: dict[str, pa.DataType] = {
            "Int8": pa.int8(),
            "Int16": pa.int16(),
            "Int32": pa.int32(),
            "Int64": pa.int64(),
            "UInt8": pa.uint8(),
            "UInt16": pa.uint16(),
            "UInt32": pa.uint32(),
            "UInt64": pa.uint64(),
        }
        if value in integer_types:
            return integer_types[value]
        if value == "Float32":
            return pa.float32()
        if value == "Float64":
            return pa.float64()
        if value in {"String", "UUID", "IPv4", "IPv6"} or value.startswith(
            ("FixedString(", "Enum8(", "Enum16(")
        ):
            return pa.string()
        if value in {"Date", "Date32"}:
            return pa.date32()
        if value == "Bool":
            return pa.bool_()

        datetime_match = re.fullmatch(r"DateTime(?:\('([^']+)'\))?", value)
        if datetime_match:
            return pa.timestamp("s", tz=datetime_match.group(1))
        datetime64_match = re.fullmatch(r"DateTime64\((\d+)(?:,\s*'([^']+)')?\)", value)
        if datetime64_match:
            precision = int(datetime64_match.group(1))
            unit = (
                "s"
                if precision == 0
                else "ms"
                if precision <= 3
                else "us"
                if precision <= 6
                else "ns"
            )
            return pa.timestamp(unit, tz=datetime64_match.group(2))

        decimal_match = re.fullmatch(r"Decimal(?:128|256)?\((\d+),\s*(\d+)\)", value)
        if decimal_match:
            precision, scale = map(int, decimal_match.groups())
            if precision <= 38:
                return pa.decimal128(precision, scale)
            return pa.decimal256(precision, scale)
        if value.startswith("Array(") and value.endswith(")"):
            return pa.list_(ClickHouseBackend._arrow_type(value[6:-1]))

        raise SchemaMismatchError(f"Unsupported ClickHouse type: {type_name!r}")

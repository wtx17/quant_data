# Repository Map

以下地图用于快速定位。签名省略实现细节，但保留参数顺序、关键类型和默认值。

## 根目录

### `pyproject.toml`

Hatchling 构建配置、Python/依赖范围、pytest marker、ruff 和 mypy 配置；wheel
显式包含 `_universes.py` 和 `resources/`。

### `AGENTS.md` 与 `.agent/`

Agent 工作约定、架构说明和本文件。

### `__init__.py`

公共导出：

- `DataClient`
- `DatasetSpec`, `ClickHouseDatasetSpec`, `TushareDatasetSpec`,
  `TushareParquetDatasetSpec`
- `ClickHouseConfig`, `TushareConfig`
- 全部公共异常
- `__version__`

### `_version.py`

- `__version__: str`

### `_universes.py` 与 `resources/universes/`

- 加载、校验并缓存 `hs300`、`sz50`、`zz500` 的版本化 CSV 快照。
- 将 Baostock 代码规范化为 `000001.SZ` 形式，并提供快照日期与 SHA-256。
- `SUPPORTED_UNIVERSES: tuple[str, ...]`
- `UniverseSnapshot(name, snapshot_date, instruments, sha256)`
- `load_universe(value) -> UniverseSnapshot`
- `normalize_universe_name(value) -> str`
- `_parse_universe_csv(name, payload) -> UniverseSnapshot`

资源契约：表头固定为 `updateDate,code,code_name`，单一快照日期、证券代码唯一，
CSV 行序即面板列序；预期数量为沪深 300、中证 500、上证 50 的名义成分数。

### `client.py`

`class DataClient`

- `DataClient(audit_dir=".quant_data/audit", *, clickhouse_client_factory=None, tushare_client_factory=None)`
- `add_clickhouse_connection(name: str, config: ClickHouseConfig) -> None`
- `add_tushare_connection(name: str, config: TushareConfig) -> None`
- `register(spec: DatasetDefinition) -> None`
- `get_panel(dataset, fields, start=None, end=None, instruments=None, adjusted=None, *, universe: str | None = None) -> dict[str, pd.DataFrame]`
- `get_table(dataset, fields, start=None, end=None, instruments=None, limit=None, adjusted=None) -> pa.Table`
- `close() -> None`
- `__enter__() -> DataClient`
- `__exit__(exc_type, exc_value, traceback) -> None`

关键内部入口：

- `_execute(mode, dataset, fields, start, end, instruments, limit, adjusted, universe)`
- `_build_tushare_disclosure_panels(dataset_name, dataset, query, record, backend)`
- `_build_panels(table, dataset_name, dataset, query, record, apply_adjustment)`
- `_prepare_query(mode, dataset, fields, start, end, instruments, limit) -> DataQuery`
- `_resolve_adjustment(dataset, adjusted) -> bool`
- `_adjust_prices(table, dataset) -> pa.Table`
- `_validate_table_keys(table, dataset, *, time_column, instrument_column)`
- `_table_columns(dataset, fields) -> tuple[str, ...]`
- `_attach_table_metadata(table, query_id, dataset, parameters) -> pa.Table`

### `models.py`

不可变 dataclass，除 `QueryAudit` 外均为 `frozen=True, slots=True`。

- `DatasetSpec(name, paths, time_column="time", instrument_column="ts_code", frequency=None, timezone=None, version=None, backend="parquet")`
- `ClickHouseConfig(host, port=8123, username=None, password=None, password_env=None, secure=False, connect_timeout=10, query_timeout=300)`
- `ClickHouseDatasetSpec(name, connection, table, time_column, instrument_column="code", partition_column=None, order_columns=(), frequency=None, timezone="Asia/Shanghai", version=None, panel_compatible=True, require_time_range=None)`；`backend="clickhouse"`
- `TushareConfig(token=None, token_env="TUSHARE_TOKEN")`
- `TushareDatasetSpec(name, connection, dataset=None, fixed_params=<factory>, timezone="Asia/Shanghai", version=None, disclosure_lag=0, calendar_exchange="SSE", fetch_buffer_days=180, fetch_margin_days=31)`；`backend="tushare"`
- `TushareParquetDatasetSpec(name, data_dir, calendar_connection, dataset=None, fixed_params=<factory>, timezone="Asia/Shanghai", version=None, disclosure_lag=0, calendar_exchange="SSE", fetch_buffer_days=180, fetch_margin_days=31)`；`backend="parquet"`
- `DatasetContract(table_time_column, instrument_column, table_identity_columns=(), table_frequency=None, panel_time_column=None, panel_frequency=None, timezone=None, version=None, panel_compatible=True, table_requires_time_range=False, panel_requires_time_range=False)`
- `RegisteredDataset(spec, schema, source, contract, adjustment=None)`
- `PriceAdjustment(factor_column, fields, default=True)`
- `DataQuery(fields, start=None, end=None, instruments=None, limit=None)`
- `FileFingerprint(path, size, mtime_ns)`
- `QueryAudit(query_id, dataset, fields, parameters, started_at, framework_version, operation="panel", ...)`
- `DatasetDefinition = DatasetSpec | ClickHouseDatasetSpec | TushareDatasetSpec | TushareParquetDatasetSpec`

### `initialize.py`

- `_ClickHouseRegistration(name, table, time_column, partition_column=None, order_columns=(), frequency=None, panel_compatible=True)`
- `clickhouse_dataset_specs(connection="minghu") -> tuple[ClickHouseDatasetSpec, ...]`
- `tushare_dataset_specs(connection="tushare") -> tuple[TushareDatasetSpec, ...]`
- `tushare_parquet_dataset_specs(data_dir, calendar_connection="tushare") -> tuple[TushareParquetDatasetSpec, ...]`
- `registered_dataset_names() -> tuple[str, ...]`
- `initialize_data_client(*, audit_dir=None, register_clickhouse=True, register_tushare=True, clickhouse_connection="minghu", clickhouse_host=None, clickhouse_port=None, clickhouse_username=None, clickhouse_password=None, clickhouse_password_env=None, clickhouse_secure=None, tushare_connection="tushare", tushare_data_dir=None, tushare_remote_datasets=None, tushare_token=None, tushare_token_env=None) -> DataClient`
- `initialize(**kwargs) -> DataClient`
- `main() -> None`

内部环境解析：`_resolve_tushare_remote_datasets(...)`, `_first_env(...)`,
`_env_int(...)`, `_env_bool(...)`。

Tushare 注册集合：

- `_TUSHARE_DATASETS`：全部受支持的远端和本地逻辑数据集，包含 `daily_basic`。
- `tushare_parquet_dataset_specs()` 为全部 Tushare 数据集生成本地快照规格。
- 配置 `tushare_data_dir` 后默认全部使用本地快照；
  `tushare_remote_datasets` 中的数据集改用远端 API。

默认 ClickHouse 数据集：

- `minghu_daily -> stock_base.daily`
- `minghu_index_daily -> index_base.daily`
- `minghu_m1 -> stock_base.m1`
- `minghu_tk -> stock_base.tk`
- `minghu_zb -> stock_base.zb`

默认 Tushare 数据集：

- `daily_basic`
- `income`, `balancesheet`, `cashflow`, `fina_indicator`, `express`, `forecast`
- `stk_holdernumber`, `ci_index_member`, `index_member_all`, `stk_holdertrade`

### `audit.py`

`class AuditWriter`

- `AuditWriter(root: str | Path)`
- `write(record: QueryAudit) -> Path`

按 UTC 日期分区，临时文件 `fsync` 后通过 `os.replace` 原子落盘。

### `exceptions.py`

基类：`QuantDataError`。

子类：`DatasetNotFoundError`, `DatasetRegistrationError`, `FieldNotFoundError`,
`InvalidQueryError`, `SchemaMismatchError`, `DuplicateObservationError`,
`AuditWriteError`, `BackendConnectionError`, `RemoteQueryError`。

## Backend

### `backends/__init__.py`

导出 `ClickHouseBackend`、`DuckDBParquetBackend` 和 `TushareBackend`。

### `backends/base.py`

`class DataBackend(Protocol)`

- `prepare(spec: DatasetDefinition) -> RegisteredDataset`
- `scan(dataset: RegisteredDataset, query: DataQuery) -> pa.Table`
- `fingerprint(dataset: RegisteredDataset) -> dict[str, object]`
- `close() -> None`

`class TushareSemanticBackend(Protocol)`

- `panel_kind(dataset) -> str`
- `scan_disclosure_events(dataset, query) -> pa.Table`
- `trade_calendar(dataset, query) -> list[date]`
- `pit_panel_semantics(dataset) -> tuple[str, str, tuple[str, ...]]`
- `scan_membership_panel(dataset, query) -> pa.Table`
- `normalize_snapshot_query(dataset, query, mode) -> DataQuery`

### `backends/parquet.py`

`class DuckDBParquetBackend`

- `DuckDBParquetBackend(calendar_provider=None)`
- `prepare(definition) -> RegisteredDataset`
- `scan(dataset, query) -> pa.Table`
- `fingerprint(dataset) -> dict[str, object]`
- `normalize_snapshot_query(dataset, query, mode) -> DataQuery`
- `panel_kind(dataset) -> str`
- `scan_disclosure_events(dataset, query) -> pa.Table`
- `trade_calendar(dataset, query) -> list[date]`
- `pit_panel_semantics(dataset) -> tuple[str, str, tuple[str, ...]]`
- `scan_membership_panel(dataset, query) -> pa.Table`
- `close() -> None`

关键内部对象和入口：

- `_TradingCalendarProvider(Protocol)`
- `_ArchivePartition(key, relative_path, path, rows, bytes, sha256)`
- `_TushareParquetSource(data_dir, manifest_path, manifest_version, dataset, schema_hash, range_start, range_end, updated_at, fixed_params, partitions)`
- `_prepare_generic(definition)`, `_prepare_tushare(definition)`
- `_read_archive_frame(dataset, query, columns, *, date_column=None, membership=None, order_columns=(), limit=None)`
- `_validate_manifest_fields(manifest, catalog)`
- `_resolve_manifest_partitions(data_dir, manifest, catalog, logical_name)`
- `_archive_type_compatible(stored, public) -> bool`

### `backends/clickhouse.py`

`ClickHouseSource(connection, table, column_types, schema_hash, schema_source)`

标识符辅助：`_quote_identifier(value) -> str`,
`_qualified_identifier(value, table_alias) -> str`。

`class ClickHouseBackend`

- `ClickHouseBackend(client_factory=None)`
- `add_connection(name: str, config: ClickHouseConfig) -> None`
- `prepare(definition) -> RegisteredDataset`
- `scan(dataset, query) -> pa.Table`
- `fingerprint(dataset) -> dict[str, object]`
- `close() -> None`

关键内部入口：

- `_describe_column_types(definition, quoted_table) -> dict[str, str]`
- `_projection(columns, column_types, *, table_alias, suffixed_column) -> str`
- `_arrow_type(type_name: str) -> pa.DataType`
- `_suffixed_code_expression(table_alias: str) -> str`

### `backends/clickhouse_catalog.py`

- `MINGHU_TABLE_COLUMN_TYPES: dict[str, tuple[tuple[str, str], ...]]`

内置表的有序 ClickHouse 类型目录。

### `backends/tushare_schemas.py`

- `_fields(data_type, names: list[str]) -> tuple[pa.Field, ...]`
- `_strings(names)`, `_dates(names)`, `_integers(names)`, `_floats(names)`
- `_schema(*groups) -> pa.Schema`
- `TUSHARE_SCHEMAS: dict[str, pa.Schema]`

物理 schema：`daily_basic`, `income`, `balancesheet`, `cashflow`, `fina_indicator`,
`express`, `forecast`, `stk_holdernumber`, `stk_holdertrade`, `industry_member`。

### `backends/tushare_catalog.py`

查询形状：

- `PeriodQuery(period_param="period")`
- `DateRangeQuery(start_param="start_date", end_param="end_date")`
- `TradeDateQuery(date_param="trade_date", max_rows=6000)`
- `UnboundedQuery()`
- `MembershipQuery(status_param="is_new", status_values=("Y", "N"))`

Catalog 对象：

- `TushareApiRoute(api_name, universe, table_query, disclosure_query=None, instrument_param="ts_code")`
- `DisclosureSemantics(period_column, disclosure_column, identity_columns, revision_order, table_order, table_frequency="q", panel_time_column="trade_date", panel_frequency="d")`
- `MembershipSemantics(interval_start_column, interval_end_column, identity_columns, table_order, table_time_column="in_date", panel_time_column="date", panel_frequency="d")`
- `ObservationSemantics(table_time_column, identity_columns, table_order, table_frequency="d", panel_time_column="trade_date", panel_frequency="d")`
- `EventSemantics(table_time_column, identity_columns, table_order, table_frequency="d")`
- `TushareDatasetCatalog(name, schema, semantics, routes, instrument_column="ts_code")`
- `build_tushare_catalogs(schemas) -> dict[str, TushareDatasetCatalog]`
- `TUSHARE_DATASETS`

- `daily_basic` 使用 `ObservationSemantics` 和 `TradeDateQuery`；键为
  `trade_date × ts_code`，表和面板查询都要求闭区间。
- `ci_index_member` 和 `index_member_all` 共用 `industry_member` schema。

### `backends/tushare.py`

`TushareSource(connection, dataset, schema_hash, fixed_params)`

`class TushareBackend`

- `TushareBackend(client_factory=None)`
- `add_connection(name: str, config: TushareConfig) -> None`
- `has_connection(name: str) -> bool`
- `fetch_calendar(connection, exchange, start, end) -> list[date]`
- `prepare(definition) -> RegisteredDataset`
- `scan(dataset, query) -> pa.Table`
- `scan_disclosure_events(dataset, query) -> pa.Table`
- `trade_calendar(dataset, query) -> list[date]`
- `pit_panel_semantics(dataset) -> tuple[str, str, tuple[str, ...]]`
- `route_name(dataset, query) -> str | None`
- `panel_kind(dataset) -> str`
- `table_columns(dataset, fields) -> tuple[str, ...]`
- `scan_membership_panel(dataset, query) -> pa.Table`
- `normalize_snapshot_query(dataset, query, mode) -> DataQuery`
- `fingerprint(dataset) -> dict[str, object]`
- `close() -> None`

关键内部入口：

- `_select_route(catalog, instruments) -> TushareApiRoute`
- `_fetch_table_frames(client, fixed_params, route, query, fields, *, trade_dates=None)`
- `_fetch_disclosure_route_frames(client, fixed_params, route, query, fields)`
- `_route_params(fixed_params, route, query, fields, *, period, membership_status, trade_date)`
- `_normalize_remote_frames(frames, catalog, columns, route) -> pd.DataFrame`
- `_filter_instruments(frame, instrument_column, instruments) -> pd.DataFrame`
- `_expand_membership_panel(frame, semantics, instrument_column, query, calendar, columns)`
- `_coerce_frame(frame, schema) -> pd.DataFrame`
- `_frame_to_arrow(frame, schema, selected) -> pa.Table`
- `_periods(start, end) -> tuple[str, ...] | None`

## Transform

### `transforms/panel.py`

- `build_panels(table, *, dataset_name, time_column, instrument_column, fields, instruments) -> dict[str, pd.DataFrame]`

### `transforms/pit.py`

- `build_daily_panels(table, *, dataset_name, disclosure_column, instrument_column, period_column, fields, instruments, calendar, panel_start, panel_end, disclosure_lag, revision_order=(), index_name="trade_date") -> dict[str, pd.DataFrame]`
- `_resolve_revisions(frame, *, dataset_name, instrument_column, period_column, fields, revision_order) -> pd.DataFrame`
- `_active_state_events(frame, *, instrument_column, period_column) -> pd.DataFrame`
- `_empty_panels(fields, instruments, index, time_column, instrument_column)`

`transforms/__init__.py` 导出 `build_panels` 和 `build_daily_panels`。

## 文档和工具

### `tools/generate_dataset_catalog.py`

- `CatalogError(RuntimeError)`
- `DatasetReference(name, fields, table_time_column, instrument_column, table_identity_columns, panel_time_column)`；属性 `panel_compatible`
- `DatasetNotes(title, summary, fields)`
- `collect_references() -> tuple[DatasetReference, ...]`
- `load_notes(references) -> dict[str, DatasetNotes]`
- `render_catalog(references, notes) -> str`
- `expected_output() -> str`
- `sync(*, check: bool) -> int`
- `main() -> int`

内部校验和渲染辅助：`_add_reference`, `_nonempty_text`, `_field_role`,
`_escape_cell`, `_anchor`, `_unique`, `_code_list`。

输入：源码 catalog 和 `tools/dataset_descriptions.toml`。输出：`DATASETS.md`。

### `tools/dataset_descriptions.toml`

数据集标题、摘要和逐字段中文说明。字段集合必须与源码 schema 完全一致。

### `README.md`

安装、初始化、命名股票池以及远端/本地混合 Tushare 示例。

### `DATASETS.md`

生成的数据集、字段、类型、键和可用查询模式手册。

## 测试地图

- `tests/test_universes.py`：包内快照数量、顺序、代码转换、名称归一化和资源损坏校验。
- `tests/test_client.py`：通用 Parquet、命名股票池展开、查询校验、调价、审计、重复键。
- `tests/test_clickhouse.py`：离线 ClickHouse fake、股票池参数绑定、SQL、类型、代码后缀、注册。
- `tests/test_clickhouse_integration.py`：真实 Minghu schema drift 和 smoke test。
- `tests/test_initialize.py`：默认 spec、注册名称、离线初始化。
- `tests/test_dataset_catalog.py`：生成文档离线且无漂移。
- `tests/test_tushare_pit.py`：财务披露 route、修订、lag、PIT 状态。
- `tests/test_tushare_daily_basic.py`：交易日逐日请求、普通宽表、本地证券过滤、闭区间和
  6000 行截断保护。
- `tests/test_tushare_industry.py`：中信/申万有效区间和面板展开。
- `tests/test_tushare_parquet.py`：manifest、本地语义、混合远端/本地初始化。
- `tests/test_tushare_schemas.py`：Tushare schema 字段数量、顺序和类型 hash。

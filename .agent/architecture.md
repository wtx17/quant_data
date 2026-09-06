# 当前架构

`DataClient.get_panel()` 是唯一查询入口，返回字段名到 `time × instrument` Pandas
宽表的映射。参数风格、校验、调价、命名股票池和审计契约保持不变。

## 调用链

`DataClient → registry[name] → Dataset.read_panel(Query, QueryAudit) → 宽表`。
`Dataset` 保存 schema、查询约束、读取函数和来源指纹；不再保留 Spec、RegisteredDataset
或各来源数据集类。注册工厂直接完成配置校验和函数绑定。

- `datasets/parquet.py`：自定义本地观测表。
- `datasets/clickhouse.py`：ClickHouse 观测表和调价配置。
- `datasets/builtin.py`：包内 membership_events，使用 ClickHouse 行情日期和证券。
- `datasets/tushare.py`：本地存档的普通观测、财务 PIT、行业区间三种处理。
- `datasets/validation.py`：注册参数校验。
- `backends/`：连接、查询、存档校验和 Arrow/日期规范化。
- `transforms/`：透视、财务 PIT、有效区间展开等纯变换。

## Tushare 本地链路

所有 Tushare 数据只读带 manifest 的 Parquet 存档，无 SDK、token、远端 API、VIP
路由或远端回退。`tushare_catalog.py` 用一个平铺 TypedDict 保存每个数据集的 schema、
处理类型、时间列、身份列、修订顺序和本地过滤字段；不再为语义或路由建类。

`initialize_data_client()` 要求 `tushare_data_dir` 或 `QUANT_DATA_TUSHARE_DATA_DIR`，
未提供则报错；只使用 ClickHouse 可关闭 `register_tushare`。初始化将所有默认
Tushare 名称注册到同一存档根目录，并复用 `clickhouse_connection` 获取日历。
手动 `register_tushare()` 必须提供 `data_dir`；`calendar_connection` 指向已注册
ClickHouse 连接，默认 minghu。连接及凭据在真正读取日历时才使用。

- daily_basic：直接扫描日期分区并透视，无需 ClickHouse 或交易日历。
- 财务：本地读取公告窗口内事件，保留 carry-in buffer、公告滞后、报告期和修订排序，
  用 `build_daily_panels()` 构造 PIT；新报告的显式空值不可沿用旧报告。
- 行业：本地读取有效区间，按 in_date、is_new 解决覆盖，冲突报错，展开并透视。
- PIT 和行业：从 `stock_base.daily` 查询有界 `SELECT DISTINCT date`，只传输日期，
  不受 instruments/universe 过滤。PIT 保留前向 buffer 和后向 margin 日历窗口。
  无日历缓存、工作日猜测或失败回退。ClickHouse 缺失的交易日不会凭空补齐。

manifest 版本、数据集名、分区路径、范围、文件大小/行数和 schema 校验仍严格执行。
固定过滤条件在 SQL 下推；财务默认 report_type=1 可显式覆盖。内部保留 Arrow 长表
与既有财务/区间变换；未增加跨来源中间抽象。

## 公共约束与审计

自定义来源频率可省略，不限制分钟及以上粒度。ClickHouse 分钟时间由 date 和
以毫秒表示的 time_int 合成，保留时区；调价策略与原行为一致。
命名股票池仅 hs300/zz500/zz1000，必须给闭区间，与 instruments 互斥；在审计初始化
后、数据读取前展开历史状态证券并集。保持顺序、CSV SHA-256、完整证券列表与日期元数据。
不对结果额外施加逐日成分掩码。裸字符串 instruments 必须报错。

每次查询包括失败都写 JSON 审计，不记录凭据。Tushare 来源包含存档指纹；事件面板
另记 ClickHouse 日历来源和 calendar_table。日历失败也保留来源，可追溯。

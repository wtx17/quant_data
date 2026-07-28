# 架构设计

## 核心边界

```text
DatasetSpec
    -> DataClient.register()
    -> Backend.prepare()
    -> RegisteredDataset(schema, source, contract, adjustment)

get_panel()/get_table()
    -> 参数规范化与审计初始化
    -> Backend.scan() 或 Tushare 语义扫描
    -> Arrow 长表校验
    -> 调价 / PIT / 区间展开 / 普通透视
    -> Pandas 面板或 Arrow 长表
    -> 审计落盘
```

`DataClient` 是唯一的公共编排层。Backend 负责存储访问，Transform 负责纯数据变换，
Catalog 负责静态 schema 和数据语义。

## 分层

| 层 | 文件 | 职责 |
| --- | --- | --- |
| 公共 API | `__init__.py`, `client.py` | 注册、查询、校验、调价、结果元数据、审计编排 |
| 初始化 | `initialize.py` | 默认连接配置和默认数据集注册 |
| 数据模型 | `models.py` | Spec、Contract、Prepared state、Query、Audit |
| Backend 协议 | `backends/base.py` | 通用扫描协议和 Tushare 语义扩展协议 |
| Parquet | `backends/parquet.py` | DuckDB 扫描、schema 合并、manifest 快照校验 |
| ClickHouse | `backends/clickhouse.py` | 参数化 SQL、类型映射、连接复用、代码后缀 |
| Tushare | `backends/tushare.py` | API 路由、远端请求、日期规范化、交易日历 |
| Catalog | `backends/*_catalog.py`, `backends/tushare_schemas.py` | 内置字段、类型、路由和时间语义 |
| Transform | `transforms/` | 普通宽表透视和 point-in-time 状态构建 |
| 审计 | `audit.py` | 原子写入 JSON 查询记录 |
| 文档生成 | `tools/generate_dataset_catalog.py` | 合并源码 catalog 与人工说明，生成 `DATASETS.md` |

## Backend 设计

### Parquet

- 普通 `DatasetSpec`：递归解析 Parquet，合并 Arrow schema，通过 DuckDB 投影和过滤。
- `TushareParquetDatasetSpec`：读取 `_manifest.json` 和分区，复用远端 Tushare catalog。
- 本地 Tushare 表查询不调用数据 API；披露和成分面板只通过 Tushare 获取
  `trade_cal`，普通观测面板保持全本地。
- 本地 `daily_basic` 按查询闭区间裁剪 `trade_date` 分区，再通过一次 DuckDB
  扫描读取；不复用远端逐交易日请求逻辑。
- manifest 字段、分区字段、行数、类型和固定参数在注册时校验。

### ClickHouse

- 内置 Minghu 表从 `MINGHU_TABLE_COLUMN_TYPES` 离线注册。
- 自定义表通过 `DESCRIBE TABLE` 获取 schema。
- 查询使用参数绑定；标识符单独校验和引用。
- Minghu `code + exg` 在查询层转换为 `.SZ/.SH/.BJ` 代码。
- `stock_base.daily` 可按 `hfq` 对价格字段做乘法复权。

### Tushare

- `TUSHARE_SCHEMAS` 定义有序 Arrow schema。
- `TUSHARE_DATASETS` 将 schema、API route 和时间语义组合成逻辑 catalog。
- 财务披露数据的单证券查询使用普通 API，全市场查询使用 VIP API；不做失败后的隐式
  route 回退。
- `TradeDateQuery` 描述必须按开市日切片的 API；`ObservationSemantics` 描述可直接透视的
  唯一 `time × instrument` 观测。
- `daily_basic` 同时要求 `start/end`，复用缓存的 `trade_cal`，然后仅携带
  `trade_date` 逐日请求。指定证券时先取当日全市场数据，再在标准化后本地过滤。
- `daily_basic` 单日响应达到 6000 行即视为可能截断并失败，不返回静默缺行的数据。
- 审计同时记录 `daily_basic` 数据 API 和用于枚举开市日的 `trade_cal`。
- 返回值统一转换为 catalog 类型，日期字符串转换为 `date32`。
- 交易日历按连接、交易所、年月缓存。

## 数据语义

### 长表

- 输出顺序：时间键、证券键、catalog 身份列、请求字段。
- 财务披露长表保留全部公告和修订。
- 行业成分长表保留原始有效区间。
- 股东增减持等一对多事件只支持长表。

### 普通面板

- `transforms.panel.build_panels()` 校验键非空和键对唯一。
- 每个请求字段生成一个 Pandas DataFrame。
- 调用方指定的证券顺序必须保留；无数据证券补全为空列。
- `daily_basic` 先由 Backend 合并逐交易日响应为 `trade_date × ts_code` 长表，再直接复用
  此普通透视路径；不在 `DataClient` 中维护专用宽表分支。

### Point-in-time 面板

- 披露日先对齐到下一交易日，再应用交易日 lag。
- 状态按证券和报告期维护；最新报告期生效。
- 同报告期修订按 catalog 的 `revision_order` 决胜。
- 新报告中的显式空值不继承旧报告值。

### 成分面板

- 有效区间与查询范围求交。
- 在交易日历上展开后再透视。
- 当前和历史成分按 route 配置请求。

## 不变量

- `RegisteredDataset.schema` 是查询字段和类型的运行时事实来源。
- `DatasetContract` 是键、频率、时间范围要求和面板能力的事实来源。
- 注册名称唯一；重复注册替换旧 prepared state。
- 查询边界闭区间；需要分区或 PIT 的数据集要求同时提供起止时间。
- `TradeDateQuery` 数据集也必须同时提供起止时间；不能退化为可能受行数上限影响的无界查询。
- 所有成功和失败查询都必须写审计记录。
- 审计 fingerprint 只能包含经过清洗的来源信息。
- 内置 catalog、生成文档和 schema 签名测试必须同步。
- 配置 Tushare 归档目录后，全部逻辑数据集默认使用本地快照；仅
  `tushare_remote_datasets` 指定的数据集使用远端 API。

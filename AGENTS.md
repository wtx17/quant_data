# quant_data Agent 指南

## 项目功能

`quant_data` 是量化研究数据访问库，统一读取本地 Parquet、ClickHouse 和 Tushare。

- `DataClient.get_panel()` 返回 `time × instrument` 的 Pandas 宽表。
- `get_panel(universe=...)` 支持 `hs300`、`zz500`、`zz1000` 三个包内
  历史成分面板。
- ClickHouse 支持内置 Minghu 表和自定义表。
- Tushare 支持远端 API，以及带 manifest 的本地 Parquet 快照。
- Tushare `daily_basic` 支持普通日频宽表；
- Tushare 财务披露数据支持交易日对齐的 point-in-time 面板。
- 行业成分支持有效区间展开。
- 每次查询都写入不含凭据的 JSON 审计记录。

默认数据集和字段见 `DATASETS.md`，由 `initialize.py` 统一注册。

## 运行环境

- Python：`>=3.11`
- 环境：`conda activate qt`。
- 非交互 shell：

```bash
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate qt
```

conda 环境已经配置好以下环境变量：

- ClickHouse：`QUANT_DATA_CLICKHOUSE_*`、`MINGHU_CLICKHOUSE_*`。
- Tushare：`TUSHARE_TOKEN`。


## 测试环境

默认使用 `qt` Conda 环境，从仓库根目录执行。

安全的离线测试：

```bash
pytest -m "not clickhouse"
ruff check .
mypy .
python tools/generate_dataset_catalog.py --check
```

全量测试：

```bash
pytest
```

真实 ClickHouse 集成测试：

```bash
pytest -m clickhouse tests/test_clickhouse_integration.py
```

集成测试需要 `MINGHU_CLICKHOUSE_HOST`、`MINGHU_CLICKHOUSE_USERNAME`、
`MINGHU_CLICKHOUSE_PASSWORD`；端口、TLS 和测试日期可分别由
`MINGHU_CLICKHOUSE_PORT`、`MINGHU_CLICKHOUSE_SECURE`、
`MINGHU_CLICKHOUSE_TEST_DATE` 配置。

仅检查本次修改的 Python 文件格式：

```bash
ruff format --check path/to/changed.py
```

修改命名股票池或其解析逻辑时，至少运行：

```bash
pytest tests/test_universes.py tests/test_client.py tests/test_clickhouse.py
```

同时构建 wheel，并确认 `quant_data/resources/universes/*.csv` 已被打包。

## 修改约束

- `DATASETS.md` 由 `tools/generate_dataset_catalog.py` 生成，不要手工修改。
- `get_panel()` 的 `universe` 只接受 `hs300`、`zz500`、`zz1000`（忽略大小写和
  首尾空白），必须同时提供闭区间 `start/end`，并与 `instruments` 互斥。
  查询方法必须拒绝裸字符串形式的 `instruments`，单证券也应放入列表。
- 股票池资源位于 `resources/universes/`，运行时不得依赖仓库外的源文件。
  `<name>_panel.csv` 的第一列为 `change_date`，后续列为全历史证券，行是从该日期
  起生效的 0/1 状态；日期、代码、行宽和掩码严格校验，每行 1 的数量分别为
  300/500/1000。
- 命名股票池必须在审计初始化后、Backend 查询前展开。对 `[start, end]`，选择
  `start` 当日状态以及之后至 `end` 的所有调仓状态的证券并集；首行之前为空，末行
  之后延续末状态。审计和面板参数需要保留规范化名称、panel 首末 change date、
  选中数量、CSV SHA-256 以及完整展开列表；解析失败也必须写失败审计。
- 展开后的股票池与手工传入完整 `instruments` 列表使用相同 Backend 路由。不要隐式
  改成全市场查询；远端 Tushare 财务数据因此可能产生逐证券请求。
- 修改 Tushare 字段时，同步更新：
  - `backends/tushare_schemas.py`
  - `tools/dataset_descriptions.toml`
  - `tests/test_tushare_schemas.py`
  - 重新生成 `DATASETS.md`
- 修改 ClickHouse 内置字段时，同步更新 `backends/clickhouse_catalog.py` 和集成校验。
- 配置 `tushare_data_dir` 后，全部 Tushare 数据集（包括 `daily_basic`）默认注册为
  本地数据源；只有 `tushare_remote_datasets` 指定的数据集使用远端 API。
- 内部扫描仍使用 Arrow 长表；保留财务 PIT 的公告、修订及行业成分的有效区间。
- 自定义数据源不要求声明频率，也不检查分钟及以上粒度。
- 不要在审计、异常、日志或 `repr` 中写入密码和 token。
- 不要在未确认兼容策略时放宽 Tushare Parquet manifest 和分区 schema 校验。

架构说明见 `.agent/architecture.md`，代码定位见 `.agent/repo-map.md`。

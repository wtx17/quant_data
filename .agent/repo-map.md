# 代码定位

- `client.py`：连接/数据集注册、get_panel 查询校验与编排、审计和 attrs。
- `models.py`：Dataset、Query、QueryAudit、ClickHouseConfig 等小型数据结构。
- `initialize.py`：默认 ClickHouse 和本地 Tushare 注册、环境配置。
- `_universes.py`：包内股票池资源严格校验、历史状态展开。
- `datasets/`：各来源注册工厂；`observation.py` 共用观测面板读取编排。
- `datasets/tushare.py`：唯一的本地 Tushare 读取流程，根据 catalog 的 kind 分支。
- `backends/clickhouse.py`：ClickHouseSession、表描述/扫描、read_trade_calendar。
- `backends/clickhouse_catalog.py`：内置表字段。
- `backends/parquet.py`：本地观测扫描、membership_events 读取。
- `backends/tushare_archive.py`：manifest/分区校验、本地扫描和类型规范化。
- `backends/tushare_catalog.py`：平铺 TUSHARE_DATASETS 和本地过滤配置。
- `backends/tushare_schemas.py`：固定有序 Arrow schema，不包含来源路由。
- `transforms/pit.py`、`transforms/intervals.py`：财务披露与行业区间纯变换。
- `tools/generate_dataset_catalog.py`：从 catalog 与描述源生成 DATASETS.md。
- `tests/test_panel_compatibility.py`：加载 Git 680ab80 原实现，严格比较相同数据/日历下输出。
- `tests/tushare_fixtures.py`：本地 manifest 快照及 ClickHouse 日期响应测试夹具。
- `tests/test_calendar.py`：休市日、连接复用、日期 schema、失败审计。
- `tests/test_clickhouse_integration.py`：真实只读 ClickHouse 验证。

当前来源契约以 architecture.md、AGENTS.md 和测试为准；quant_data_refactor_plan.md
仅保留最初方案历史，远端 Tushare 部分已被本地唯一来源决策替代。

# quant_data

`quant_data` 是面向量化研究的统一数据访问库，支持本地 Parquet、ClickHouse 和
Tushare。唯一取数接口 `get_panel()` 返回每个字段对应的 `time × instrument` Pandas 宽表。
内置数据集提供分钟线、日线、财务 PIT 和行业成分面板；自定义数据源的频率仍是可选元数据。

## 安装

项目要求 Python 3.11 或更高版本：

```bash
python -m pip install -e .
```

按实际使用的数据源安装远程后端：

```bash
python -m pip install -e ".[clickhouse,tushare]"
```

## 使用

`initialize_data_client()` 会注册项目支持的默认数据集：

```python
from quant_data.initialize import initialize_data_client

with initialize_data_client() as data:
    close = data.get_panel(
        "minghu_daily",
        ["close"],
        start="2026-01-01",
        end="2026-01-31",
        instruments=["000001.SZ"],
    )["close"]

    income = data.get_panel(
        "income",
        ["total_revenue"],
        start="2025-01-01",
        end="2025-12-31",
        instruments=["000001.SZ"],
    )["total_revenue"]
```

财务面板的 `start/end` 表示交易日查询区间，数值是当时已披露的最新报告期状态。

ClickHouse 分钟表统一注册 `time_column="date_time"`。当源表包含 `date` 和
`time_int` 时，SQL 会用日期加当日零点起的毫秒数合成时间，物理 `date_time` 列可省略；
即使存在也不参与取数。`date` 支持 `Date`、`Date32` 或 `YYYYMMDD` 整数，
可继续设置 `partition_column="date"` 做分区过滤，无需声明 `frequency`。
输出索引为带 `Asia/Shanghai` 时区的 Pandas `DatetimeIndex`，保留毫秒精度。

`get_panel()` 也可以通过 `universe` 选择内置股票池：

```python
with initialize_data_client() as data:
    hs300_close = data.get_panel(
        "minghu_daily",
        ["close"],
        start="2026-01-01",
        end="2026-01-31",
        universe="hs300",
    )["close"]
```

当前支持 `hs300`、`zz500` 和 `zz1000`。这些股票池由包内
`resources/universes/<name>_panel.csv` 的历史变更面板提供。`universe` 查询必须同时
给出闭区间 `start/end`，返回区间内曾经属于该指数的证券并集：变更日当天的新状态
立即生效，区间首日的有效状态也会纳入，查询区间跨越多次调仓时保留所有相关证券。
`universe` 与 `instruments` 不能同时使用。股票池名称、首末变更日期、内容哈希和展开
后的证券列表会写入查询审计及面板元数据。展开后的列表沿用现有后端路由，因此远端
Tushare 财务面板可能产生逐证券 API 请求。

配置 `tushare_data_dir` 后，全部 Tushare 数据集默认从同一 Parquet 归档读取。
只有 `tushare_remote_datasets` 中列出的数据集继续调用远端 API：

```python
with initialize_data_client(
    tushare_data_dir="/data/tushare",
    tushare_remote_datasets={"forecast"},
) as data:
    income = data.get_panel(
        "income", ["total_revenue"], start="2026-07-01", end="2026-07-31"
    )["total_revenue"]
    forecast = data.get_panel(
        "forecast", ["p_change_min", "p_change_max"],
        start="2026-07-01", end="2026-07-31",
    )
```

不传 `tushare_data_dir` 时，全部 Tushare 数据集使用远端 API。财务披露和行业成分的
本地 `get_panel()` 为了交易日对齐，仍会通过配置的 Tushare 连接读取 `trade_cal`；
本地 `daily_basic` 直接扫描请求范围内的日期分区，不调用 Tushare API。

全部数据集、字段类型及字段含义见 [默认数据集手册](DATASETS.md)。

字段说明维护在 `tools/dataset_descriptions.toml`。源码 catalog 发生变化后运行：

```bash
python tools/generate_dataset_catalog.py
python tools/generate_dataset_catalog.py --check
```

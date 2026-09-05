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

当前支持 `hs300`、`sz50`、`zz500` 和 `zz1000`。前三者是 `2026-07-20` 的固定
成分股快照，`zz1000` 是 `2026-07-28` 的固定成分股快照；它们会应用于整个查询区间，
并不表示历史时点成分。`universe` 与 `instruments` 不能同时使用。股票池版本、内容
哈希和展开后的证券列表会写入查询审计及面板元数据。展开后的列表沿用现有后端路由，
因此远端 Tushare 财务面板可能产生逐证券 API 请求。

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

# quant_data

`quant_data` 是面向量化研究的统一数据访问库，支持本地 Parquet、ClickHouse 和
Tushare。使用者通过 `get_panel()` 获取 `time × instrument` Pandas 宽表，通过
`get_table()` 获取保留事件和修订记录的 Arrow 长表。

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

    income = data.get_table(
        "income",
        ["total_revenue"],
        start="2025-01-01",
        end="2025-12-31",
        instruments=["000001.SZ"],
    )
```

配置 `tushare_data_dir` 后，全部 Tushare 数据集默认从同一 Parquet 归档读取。
只有 `tushare_remote_datasets` 中列出的数据集继续调用远端 API：

```python
with initialize_data_client(
    tushare_data_dir="/data/tushare",
    tushare_remote_datasets={"forecast"},
) as data:
    income = data.get_table("income", ["total_revenue"])
    forecast = data.get_table("forecast", ["p_change_min", "p_change_max"])
```

不传 `tushare_data_dir` 时，全部 Tushare 数据集使用远端 API。财务披露和行业成分的
本地 `get_panel()` 为了交易日对齐，仍会通过配置的 Tushare 连接读取 `trade_cal`；
本地 `daily_basic` 直接扫描请求范围内的日期分区，不调用 Tushare API。

全部数据集、可用方法、字段类型及字段含义见 [默认数据集手册](DATASETS.md)。

字段说明维护在 `tools/dataset_descriptions.toml`。源码 catalog 发生变化后运行：

```bash
python tools/generate_dataset_catalog.py
python tools/generate_dataset_catalog.py --check
```

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
python -m pip install -e ".[clickhouse]"
```

## 使用

`initialize_data_client()` 会注册项目支持的默认数据集：

```python
from quant_data.initialize import initialize_data_client

with initialize_data_client(tushare_data_dir="/data/tushare") as data:
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
with initialize_data_client(tushare_data_dir="/data/tushare") as data:
    hs300_close = data.get_panel(
        "minghu_daily",
        ["close"],
        start="2026-01-01",
        end="2026-01-31",
        universe="hs300",
    )["close"]
```

也支持传入非空名称列表，例如 `universe=["hs300", "zz500"]`，表示各股票池在查询
区间内的成分并集。名称忽略大小写和首尾空白；重复名称和证券去重，证券列按列表
顺序及各股票池 CSV 列顺序保留首次出现的位置。空列表或非法名称会报错。

当前支持 `hs300`、`zz500` 和 `zz1000`。这些股票池由包内
`resources/universes/<name>_panel.csv` 的历史变更面板提供。`universe` 查询必须同时
给出闭区间 `start/end`，返回区间内曾经属于该指数的证券并集：变更日当天的新状态
立即生效，区间首日的有效状态也会纳入，查询区间跨越多次调仓时保留所有相关证券。
`universe` 与 `instruments` 不能同时使用。股票池名称、首末变更日期、内容哈希和展开
后的证券列表会写入查询审计及面板元数据，并直接用于本地扫描过滤。
列表调用的 `parameters["universe"]` 包含规范化去重的 `names`、各池来源信息
`panels` 和并集证券总数 `count`；字符串调用保持原有元数据结构。

全部 Tushare 数据集只读取带 manifest 的本地 Parquet 存档。初始化必须提供
`tushare_data_dir`，或设置 `QUANT_DATA_TUSHARE_DATA_DIR`；缺少配置立即报错。
只使用 ClickHouse 时可设置 `register_tushare=False`。不再需要 Tushare SDK 或 token。

`daily_basic` 完全本地读取，无需交易日历。财务 PIT 和行业成分面板使用 ClickHouse
`stock_base.daily` 的去重日期对齐，日历不受请求股票过滤影响，只传输日期。
初始化时使用 `clickhouse_connection`；手动注册通过 `calendar_connection` 指定
已添加的 ClickHouse 连接（默认 `minghu`）。本地缺文件、日历连接失败都直接报错，
不回退到远端 Tushare。manifest 和分区 schema 的严格校验保持不变。

全部数据集、字段类型及字段含义见 [默认数据集手册](DATASETS.md)。

字段说明维护在 `tools/dataset_descriptions.toml`。源码 catalog 发生变化后运行：

```bash
python tools/generate_dataset_catalog.py
python tools/generate_dataset_catalog.py --check
```

### 历史指数成分归属

开启 ClickHouse 注册时，`initialize_data_client()` 同时注册 `membership_events`：

```python
membership = client.get_panel(
    "membership_events",
    fields=["membership"],
    instruments=["000001.SZ", "600000.SH"],  # 或 universe="hs300"
    start="2024-01-01",
    end="2024-12-31",
)["membership"]
```

输出交易日 × 股票代码的 `int8` 宽表：0 表示不属于三个指数，1 / 2 / 3 分别表示
沪深300 / 中证500 / 中证1000。事件从 `change_date` 当日起生效，查询起点继承此前状态；
首个事件前为 0，最后事件后延续状态。包内事件文件不会自动更新。

交易日和全市场股票来自 ClickHouse `stock_base.daily`，只读取日期及代码。
全市场证券并集取 `[start 向前一个自然月, end]`（月末按目标月最后一天对齐），
输入证券必须在该扩展区间出现过，否则报错，以减少停牌股被误判的情况。
输出交易日仍严格限定在原始 `start/end`；不要求每个证券每天都有行情。
证券缺少某日行情时仍按事件状态填值。省略 `instruments/universe` 查询上述扩展区间的全市场，
空 `instruments=[]` 保留交易日索引、返回零列。`start/end` 必填。
手动注册可使用 `client.register_builtin("membership_events", connection="minghu")`。

### 自定义注册与旧接口迁移

`get_panel()` 的参数和返回结构保持不变。注册时直接传参数，不再构造 `DatasetSpec`
等对象。默认数据集名称保留；初始化与注册中的 Tushare 远端配置已删除。

| 旧注册对象 | 当前方法 |
| --- | --- |
| `DatasetSpec` | `client.register_parquet(name, paths, ...)` |
| `ClickHouseDatasetSpec` | `client.register_clickhouse(name, connection=..., table=..., time_column=..., ...)` |
| `TushareDatasetSpec` | 已删除远端链路；先准备本地存档，再使用下方本地注册 |
| `TushareParquetDatasetSpec` | `client.register_tushare(name, data_dir=..., calendar_connection=..., dataset=..., ...)` |
| `BuiltInDatasetSpec` | `client.register_builtin(name, connection=..., dataset=...)` |

例如：

```python
from quant_data import DataClient

with DataClient() as data:
    data.register_parquet(
        "factors", ["/data/factors/*.parquet"],
        time_column="date", instrument_column="code",
    )
    factors = data.get_panel("factors", ["value"], start="2026-01-01", end="2026-01-31")
```

### 验证重构兼容性

在 `qt` 环境、仓库根目录执行：

```bash
pytest -m "not clickhouse"
pytest -m clickhouse tests/test_clickhouse_integration.py tests/test_panel_compatibility.py
ruff check .
mypy .
python tools/generate_dataset_catalog.py --check
```

`tests/test_panel_compatibility.py` 从本地 Git 历史加载重构前的 `680ab80`，在独立模块
命名空间中运行原实现与当前实现。两边使用同一份文件和相同的模拟响应，只适配注册
语法，不修改查询代码；还包含四个默认 ClickHouse 数据集的真实查询对照。
比较覆盖面板值、缺失值、dtype、轴名称/顺序、稳定 attrs 和审计、模拟 SQL 请求。
查询 UUID、开始时间和耗时不要求相等；Tushare 日历来源元数据按新契约单独验证。
本地 Tushare 对照向新旧实现提供相同交易日，严格比较数值与结构。没有本地基线提交时会明确跳过该组测试，
不下载旧代码；验收时需确认这组测试实际执行。

测试配置优先从当前 checkout 导入包，避免另一目录的 editable 安装干扰工作树验证。
构建 wheel 可使用 `python -m pip wheel . --no-deps --wheel-dir dist`；从源码目录外
安装验证，并检查 `datasets/` 模块和 `resources/universes/` 下的 CSV、Parquet 已打包。

# 本地 Tushare 收尾与兼容性验收

验证日期：2026-09-06。当前工作区基于 c5c5d19；新旧对照基线为
`680ab80ed536f959ce6ce563e073dfd87f482cbe`。本文替代之前双来源架构的验收记录。

## 结果

- 离线测试：274 passed，12 个 ClickHouse 测试按 marker 排除。
- 真实只读 ClickHouse：12 passed，含四个默认表的新旧查询对照，以及本地行业面板
  使用真实 stock_base.daily 日历的完整链路；确认 2024-01-01 休市被排除。
- 以上合计 286 项通过；其中新旧架构直接对照 143 项（139 离线、4 真实 ClickHouse）。
- ruff check、mypy（28 个源码文件）、修改文件格式、目录生成检查和 git diff --check 通过。
- wheel 构建通过，在源码目录外解包并验证导入、本地 daily_basic 查询、三个股票池 CSV
  与 membership_events Parquet。wheel 不含远端 Tushare 模块、公共配置或 SDK 依赖。

## 对照方法与边界

测试从本地 Git 导出原架构，使用独立模块命名空间运行原始源码，不修改其 get_panel。
两边提供相同 get_panel 参数、同一份 Parquet/manifest、相同模拟日期和数据；只适配
注册接口。历史原实现的交易日历协议由测试适配器提供，生产代码没有远端 Tushare 链路。

严格比较字段顺序、数值、缺失值、dtype、索引/列类型、名称、顺序、精度及时区，使用
assert_frame_equal(check_exact=True)，不做数值容差或类型转换。比较稳定 attrs、审计、
失败异常类型和文本；SQL 数据查询亦比较调用参数。UUID、起始时间、耗时不要求相等。

本次明确更换的 calendar_api、calendar_connection 和 calendar 来源元数据不要求
新旧相等，改为单独断言新审计来源为 ClickHouse stock_base.daily，且每次事件查询
只有一条去重日期 SQL。daily_basic 必须零日历调用。其他稳定元数据仍严格比较。

| 对照范围 | 项数 |
| --- | ---: |
| get_panel 参数签名 | 1 |
| 自定义 Parquet 类型与证券选择 | 12 |
| 默认 ClickHouse 表、选择与调整标志 | 24 |
| 复权、非价格字段、缺失因子等 | 24 |
| 七个本地财务数据集、lag 0/1、三种证券选择 | 42 |
| 本地 daily_basic、三种证券选择 | 3 |
| 两个本地行业数据集、is_new 筛选、三种证券选择 | 12 |
| 三个股票池、三类数据源 | 9 |
| 非法输入与失败审计 | 12 |
| 默认 ClickHouse 表真实对照 | 4 |

财务和行业在相同交易日输入下保持原输出。真实运行中，交易日改以 ClickHouse 表中
实际存在的日期为准；若该表与原 Tushare 日历覆盖不同，输出日期也会不同，这是本次
来源切换的预期行为，不声称两个不同日历无条件等价。本地存档测试使用严格 manifest
夹具，未扫描用户全部生产存档。

## 当前接口

get_panel 全部功能和参数风格保留。register_tushare 必须给 data_dir；PIT/行业的
calendar_connection 指向 ClickHouse。initialize_data_client 必须给 tushare_data_dir
或 QUANT_DATA_TUSHARE_DATA_DIR，也可 register_tushare=False。
删除 TushareConfig、add_tushare_connection、远端 client factory、API/VIP 路由、
token、remote datasets 选择器和 Tushare SDK extra。schema、manifest 校验和本地过滤
约束保持不变。catalog 改为平铺字典，无来源/语义包装类。

复现：

```bash
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate qt
pytest -m "not clickhouse"
pytest -m clickhouse tests/test_clickhouse_integration.py tests/test_panel_compatibility.py
ruff check .
mypy .
python tools/generate_dataset_catalog.py --check
```

# 真实数据新旧架构对照验收

验证日期：2026-09-06。原架构基线 `680ab80ed536f959ce6ce563e073dfd87f482cbe`；新架构为当前工作区。

**15 个默认数据集、30 组成功查询全部精确一致。** 另有 6 组源数据质量案例：4 组指数重复键、2 组预告冲突修订，新旧均拒绝查询；未进行去重或修改源数据。

合计严格比较 2,500,870 个单元格，其中 1,782,506 个非空值。

## 数据和边界

- ClickHouse：真实 Minghu 服务，stock_base.daily、index_base.daily、stock_base.m1、zhangruiqi.zb_cj_flow_min。
- Tushare：`/Users/wtx/Sync/Quant/quant_data_infra/tushare/data`，真实 Parquet 和原始 manifest，全程只读。
- 普通日频/财务/行业/归属：短区间 2025-04-28—2025-04-30，500 只；长区间 2025-01-01—2025-06-30，5 只。
- 分钟：短区间 2026-03-02—2026-03-03，500 只；长区间 2026-03-02—2026-03-11，5 只。包含完整交易时段，最长 10 个自然日。
- 指数成功补测：2026-03-02—2026-03-04 的全部 16 个指数；2026-01-01—2026-06-30 的 000902.SH、000984.SH。指数表仅有 16 个标的，无法构造 500 个真实指数。半年区间其他指数有重复记录，不能生成宽表。
- forecast 成功补测仍使用相同日期范围，证券从真实存档中选择：在包含 180 天 carry-in 的 2024-07-05—2025-06-30 公告窗口内，按 ts_code、ann_date、end_date、first_ann_date 分组无重复；取 500 只，长区间取前 5 只。原始随机股票池的冲突案例保留，未隐瞒。
- 股票从真实行情区间内按确定性哈希抽样，包含不同市场；精确证券列表、字段、范围和耗时见 summary.json。
- Tushare 比较全部可查询字段；股票/指数/分钟行情比较 open、close、volume；资金流比较 cj_all_mn_min、cj_psell_xl_td_min；归属比较 membership。

## 每个数据集的结果

| 数据集 | 字段数 | 短区间：行 × 列 | 长区间：行 × 列 | 结果 |
| --- | ---: | --- | --- | --- |
| minghu_daily | 3 | 3 × 500 | 117 × 5 | 精确一致 |
| minghu_index_daily | 3 | 3 × 16 | 116 × 2 | 精确一致 |
| minghu_m1 | 3 | 482 × 500 | 1928 × 5 | 精确一致 |
| zb_cj_flow_min | 2 | 482 × 500 | 1928 × 5 | 精确一致 |
| membership_events | 1 | 3 × 500 | 117 × 5 | 精确一致 |
| daily_basic | 17 | 3 × 500 | 117 × 5 | 精确一致 |
| income | 93 | 3 × 500 | 117 × 5 | 精确一致 |
| balancesheet | 157 | 3 × 500 | 117 × 5 | 精确一致 |
| cashflow | 96 | 3 × 500 | 117 × 5 | 精确一致 |
| fina_indicator | 166 | 3 × 500 | 117 × 5 | 精确一致 |
| express | 31 | 3 × 500 | 117 × 5 | 精确一致 |
| forecast | 11 | 3 × 500 | 117 × 5 | 精确一致 |
| stk_holdernumber | 3 | 3 × 500 | 117 × 5 | 精确一致 |
| ci_index_member | 10 | 3 × 500 | 117 × 5 | 精确一致 |
| index_member_all | 10 | 3 × 500 | 117 × 5 | 精确一致 |

## 比较方法和发现

两份源码在不同模块命名空间运行，使用相同 get_panel 参数和同一份真实存档。原架构 get_panel、扫描和变换源码未修改。原架构仅在旧日历客户端的传输接口处接入真实 ClickHouse 日期；新架构直接查询同一张表。因此验证的是**相同真实数据和交易日历下**的重构一致性，不是 Tushare 线上日历与 ClickHouse 日期覆盖的一致性。

`assert_frame_equal(check_exact=True)` 精确比较每个字段的值、缺失位置、dtype、索引和列的类型/名称/顺序/时区，不转换结果、不设置数值容差。字段字典顺序、稳定 attrs 和审计也比较。排除查询 UUID、开始时刻、耗时、已明确替换的日历来源标记；包内 membership 事件允许不同 checkout 的绝对路径前缀不同，但内容 SHA-256 严格一致。

最初在 2025 和 2026 年四组指数查询中发现重复日期/代码，双方均拒绝查询。重复错误的示例记录顺序可能不同，不将其视为结果面板差异；对应失败审计保留于 index-errors、index-2026。没有通过删除原始数据、静默去重或修改 get_panel 绕过错误。

forecast 初始两组查询在原始存档中遇到同优先级但值不同的修订，双方均报 SchemaMismatchError，证券、报告期及生效日期均一致。使用无冲突证券补测全部 11 个字段，仍使用原始存档，不过滤字段或修改底层行。

`membership_events` 初次检查只因审计 events_path 的源码目录前缀不同失败；当次面板值、dtype、轴与 attrs 已完全一致。后续核对全部稳定审计和事件 SHA-256 一致，详见 membership-audit-check.json。

存档中的 stk_holdertrade 不在新旧项目的默认注册/catalog 中，未计入 15 个数据集，也未临时添加支持。未对用户未指定的自定义 Parquet 数据集虚构生产数据。查询耗时包含网络、服务端及客户端缓存影响，不作为性能基准。

## 复现

```bash
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate qt
python tests/validate_real_panels.py --archive /Users/wtx/Sync/Quant/quant_data_infra/tushare/data
python tests/validate_real_panels.py --archive /Users/wtx/Sync/Quant/quant_data_infra/tushare/data --datasets minghu_index_daily --daily-year 2026 --clean-index --output .agent/real-panel-validation/index-clean
python tests/validate_real_panels.py --archive /Users/wtx/Sync/Quant/quant_data_infra/tushare/data --datasets forecast --instruments-file .agent/real-panel-validation/forecast-clean-instruments.json --output .agent/real-panel-validation/forecast-clean
```

默认全量运行遇到真实重复键会保留双方错误并返回非零状态；补测命令提供可返回面板的指数场景。原始 results.json 保留首次检测结果；summary.json 汇总已完成补验的最终结论。

# 候选质量证明 v1 开发表

## 1. 阶段主线

下一阶段只做一件事：

`把系统从“能生成候选”升级成“能证明候选质量”。`

本文档是开发执行表，不是愿景文档。所有任务必须能落地产物、测试、验收结果和阻断判断。

执行优先级：

1. `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
2. `docs/CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md`
3. 本文档

## 2. 不做范围

本阶段明确不做：

- UI 扩展
- 新概念页面
- 自动实盘交易
- 人工挑选样本美化结果
- 临时更改 benchmark 口径
- 用单次漂亮收益替代长期样本证明

## 3. 验收总标准

本阶段结束时，系统必须能用产物回答：

1. 今天候选使用的数据可靠吗？
2. 候选从哪些数据和产物生成？
3. 真实交易约束下候选是否仍有效？
4. 候选发布后真实表现如何？
5. 候选失败的主要原因是什么？
6. 当前观察名单的组合风险是什么？
7. 每个候选为什么值得观察，什么情况下失效？

## 4. P0 数据质量与血缘

| ID | 任务 | 开发内容 | 产物 | 测试 | 验收 | 阻断规则 |
| --- | --- | --- | --- | --- | --- | --- |
| P0-1 | 数据质量报告 | 检查缺失交易日、价格跳变、成交量为 0、异常涨跌幅、数据延迟 | `data_quality_report_latest.json` | 单元测试覆盖缺失、异常、延迟样例 | 每日生成质量报告 | 报告缺失则候选不得进入正式观察名单 |
| P0-2 | 股票级数据质量分 | 为每只股票输出 `data_quality_score`、`quality_level`、`blocking_reasons` | `stock_data_quality_latest.csv` | 构造好/坏样本测试评分 | 候选能读取质量分 | `quality_level=blocked` 不得入选 |
| P0-3 | 候选血缘 | 每个候选绑定数据日期、生成 run、输入文件、模型版本、候选来源 | `candidate_lineage_latest.json` | 校验字段完整性 | 候选详情可追溯来源 | 缺 `run_id/source_files/data_as_of` 阻断 |
| P0-4 | 数据门禁 | 在候选生成后增加质量门禁 | `candidate_data_quality_gate_latest.json` | 覆盖通过/阻断/降级 | 质量不达标候选自动降级或剔除 | 数据质量门禁 failed 阻断正式发布 |

最低完成标准：

- `candidate_lineage_latest.json`
- `data_quality_report_latest.json`
- `candidate_data_quality_gate_latest.json`
- 候选生成流程读取并执行数据质量门禁

## 5. P1 真实交易约束回测

| ID | 任务 | 开发内容 | 产物 | 测试 | 验收 | 阻断规则 |
| --- | --- | --- | --- | --- | --- | --- |
| P1-1 | 交易日历约束 | 回测只允许在有效交易日计算和成交 | `realistic_backtest_latest.json` | 非交易日样例 | 非交易日无成交 | 交易日历缺失阻断真实约束回测 |
| P1-2 | 停牌/无成交约束 | 停牌、成交量为 0、价格缺失不可成交 | 同上 | 停牌样例 | 不再产生虚假成交 | 出现停牌成交阻断 |
| P1-3 | 涨跌停约束 | 涨停不可买、跌停不可卖或按配置降级 | 同上 | 涨跌停样例 | 成交符合 A 股约束 | 违反涨跌停成交阻断 |
| P1-4 | 成本模型 | 加入佣金、滑点、冲击成本、换手成本 | `transaction_cost_breakdown_latest.json` | 成本计算测试 | 输出毛收益/扣费/扣滑点/扣冲击后收益 | 成本字段缺失阻断 |
| P1-5 | 容量约束 | 成交量容量、单票成交占比、流动性上限 | `capacity_constraint_report_latest.json` | 小成交量样例 | 容量不足自动降权 | 容量超限阻断或降级 |

最低完成标准：

- 同一批候选同时输出 `ideal_backtest` 和 `realistic_backtest`
- 报告必须展示收益衰减原因
- 如果真实约束后收益失效，候选质量不得标为通过

## 6. P2 候选闭环样本

| ID | 任务 | 开发内容 | 产物 | 测试 | 验收 | 阻断规则 |
| --- | --- | --- | --- | --- | --- | --- |
| P2-1 | 每日观察名单冻结 | 每个交易日冻结一个正式观察名单 | `candidate_observation_snapshot_YYYYMMDD.json` | 重复冻结测试 | 快照不可被后续重写 | 无快照不得进入闭环统计 |
| P2-2 | 观察 ledger | 记录入选日期、入选理由、观察周期、收益、回撤、命中状态 | `candidate_observation_ledger.jsonl` | ledger append 测试 | 每个候选有闭环记录 | 缺 ledger 阻断质量报告 |
| P2-3 | 观察结果计算 | 计算 5D/20D/60D 收益、超额收益、最大回撤、是否命中 | `candidate_observation_result_latest.json` | 收益计算样例 | 可复算候选表现 | 价格缺失必须标注，不可静默跳过 |
| P2-4 | 失败归因 | 将失败归因到数据、模型、风控、市场、流动性、时机 | `candidate_failure_attribution_latest.json` | 归因分类测试 | 失败样本必须有原因 | 未归因失败样本不得进入通过结论 |
| P2-5 | 20 样本报告 | 满 20 个正式样本后生成阶段质量报告 | `candidate_quality_20_sample_report.json` | 样本不足/充足测试 | 报告包含胜率、超额、回撤、失败分布 | 样本不足禁止晋级 |

最低完成标准：

- 系统不再只展示“今天候选”
- 必须能回看历史候选表现
- 满 20 样本前只能显示 `继续观察`

## 7. P3 组合优化与风险暴露

| ID | 任务 | 开发内容 | 产物 | 测试 | 验收 | 阻断规则 |
| --- | --- | --- | --- | --- | --- | --- |
| P3-1 | 观察组合权重 | 从 Top N 排名升级为观察组合权重 | `candidate_portfolio_latest.json` | 权重求和测试 | 输出单票权重 | 权重异常阻断 |
| P3-2 | 行业暴露控制 | 行业权重上限、行业集中度惩罚 | `portfolio_exposure_report_latest.json` | 行业集中样例 | 暴露超限自动降权 | 行业超限阻断通过 |
| P3-3 | 相关性控制 | 高相关候选降权或替换 | 同上 | 高相关样例 | 组合相关性可解释 | 相关性数据缺失降级 |
| P3-4 | 流动性容量 | 组合容量、单票容量、预估冲击 | `portfolio_capacity_report_latest.json` | 低流动性样例 | 容量不足降级 | 容量不可计算阻断 |
| P3-5 | 风险成本联合评分 | 预期收益、风险、成本、容量合成组合质量分 | `candidate_portfolio_quality_latest.json` | 综合评分测试 | 输出组合级 pass/review/fail | 组合质量 fail 阻断晋级 |

最低完成标准：

- 候选不再只是排名表
- 输出观察组合、权重、行业暴露、容量风险
- 组合风险必须能解释

## 8. P4 风控状态机

| ID | 任务 | 开发内容 | 产物 | 测试 | 验收 | 阻断规则 |
| --- | --- | --- | --- | --- | --- | --- |
| P4-1 | 状态定义 | 固定 `watch/review/degrade/invalid/closed` | `candidate_risk_state_latest.json` | 状态枚举测试 | 每个候选有状态 | 未知状态阻断 |
| P4-2 | 状态转移规则 | 数据异常、回撤、模型退化、观察结束触发转移 | `candidate_risk_state_transition.jsonl` | 转移规则测试 | 状态变化有原因 | 无原因转移阻断 |
| P4-3 | 降级/失效逻辑 | 候选触发风险后自动降级或失效 | 同上 | 回撤/数据异常样例 | 不再展示为正常观察 | 失效候选不得继续列为正常 |
| P4-4 | 状态审计 | 每次状态变化写入审计记录 | `candidate_state_audit_latest.json` | 审计字段测试 | 可追溯谁/何时/为何变化 | 审计缺失阻断 |

最低完成标准：

- 每个候选都有状态
- 每次状态变化都有原因
- 状态决定外部展示和质量评分

## 9. P5 候选解释层

| ID | 任务 | 开发内容 | 产物 | 测试 | 验收 | 阻断规则 |
| --- | --- | --- | --- | --- | --- | --- |
| P5-1 | 外部解释 | 每个候选输出为什么关注、主要风险、何时失效、下一步观察 | `candidate_public_explanation_latest.json` | 字段完整性测试 | 外部页面只读这四句话 | 缺解释不得进入外部展示 |
| P5-2 | 内部解释 | 因子贡献、模型置信度、历史相似样本、排除原因 | `candidate_internal_explanation_latest.json` | 字段完整性测试 | 内部可审计 | 内部解释缺失标记 review |
| P5-3 | 反例与排除 | 展示高分但被排除候选及原因 | `candidate_rejection_explanation_latest.json` | 排除样例测试 | 不隐藏被风控剔除的强票 | 排除原因缺失阻断 |

最低完成标准：

- 外部只讲人话
- 内部保留完整证据
- 每个候选都能回答“为什么”和“什么时候错”

## 10. 开发顺序

| 周期 | 必做任务 | 完成标志 |
| --- | --- | --- |
| Week 1 | P0-1 到 P0-4 | 候选具备数据质量门禁和血缘 |
| Week 2 | P1-1 到 P1-5 | 回测具备真实交易约束 |
| Week 3 | P2-1 到 P2-5 | 候选进入闭环 ledger |
| Week 4 | P3-1 到 P5-3 | 组合、状态机、解释层形成 v1 |

## 11. Release Gate

候选质量证明 v1 不允许通过，除非同时满足：

- `data_quality_report_latest.json` 存在且 status 不为 failed
- `candidate_lineage_latest.json` 覆盖所有展示候选
- `realistic_backtest_latest.json` 存在
- `candidate_observation_ledger.jsonl` 可追加、可回放
- `candidate_quality_20_sample_report.json` 在样本不足时明确输出 blocked
- `candidate_portfolio_latest.json` 输出权重与暴露
- `candidate_risk_state_latest.json` 覆盖所有候选
- `candidate_public_explanation_latest.json` 覆盖所有外部展示候选

## 12. 禁止晋级条件

出现以下任一情况，禁止声称候选质量已证明：

- 数据质量门禁缺失
- 候选血缘缺失
- 只使用理想回测，不使用真实约束回测
- 样本不足但输出正向质量结论
- 失败样本未归因
- 组合风险不可解释
- 外部解释缺失
- benchmark 口径临时变化

## 13. 当前下一步

当前已完成 `P0-1`、`P0-2` 的候选输出接入、`P0-3`。

下一步立即进入 `P0-4 数据门禁`：

1. 正式观察名单生成前执行 `candidate_data_quality_gate_latest.json`
2. `quality_level=blocked` 不得进入正式观察名单
3. `quality_level=review` 必须降级并写明原因
4. 门禁结果必须写入候选 summary、lineage、run status

历史第一批代码任务已完成：

1. `src/candidate_quality/data_quality.py`
2. `scripts/build_candidate_data_quality_report.py`
3. `tests/test_candidate_data_quality_report.py`
4. `data/experiments/data_quality_report_latest.json`
5. `data/experiments/candidate_data_quality_gate_latest.json`

## 14. 执行记录

### 2026-05-07 P0-1 已落地

已完成：

1. 新增 `src/candidate_quality/data_quality.py`
2. 新增 `scripts/build_candidate_data_quality_report.py`
3. 新增 `tests/test_candidate_data_quality_report.py`
4. 刷新 `data/experiments/data_quality_report_latest.json`
5. 刷新 `data/experiments/stock_data_quality_latest.csv`
6. 刷新 `data/experiments/candidate_data_quality_gate_latest.json`
7. `run_top_candidates.py` 已读取 `data_quality_report_latest.json` 并写出候选级数据质量门禁
8. 候选 CSV/Markdown 已携带 `data_quality_score`、`data_quality_level`、`data_quality_blocking_reasons`

本次真实运行结果：

- `data_quality_report_latest.json`: `status=passed`
- `candidate_data_quality_gate_latest.json`: `status=passed`
- 数据质量覆盖股票数: `5517`
- 数据质量阻断股票数: `110`
- 当前候选数: `3`
- 阻断候选数: `0`
- 复核候选数: `0`

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_top_candidates_universe.py -k data_quality_gate`: 通过

下一步严格进入：

1. `P0-3 候选血缘`：生成 `candidate_lineage_latest.json`
2. `P0-4 数据门禁`：将 `quality_level=blocked` 从“写 gate”升级为“正式观察名单前剔除或降级”

### 2026-05-07 P0-3 已落地

已完成：

1. 新增 `src/candidate_quality/lineage.py`
2. 新增 `tests/test_candidate_lineage.py`
3. `run_top_candidates.py` 已在候选输出时同步写出 `candidate_lineage_latest.json`
4. 每个候选已绑定 `run_id`、`data_as_of`、`source_files`、`model_version`、`candidate_source`
5. 血缘校验已覆盖缺 `run_id/source_files/data_as_of` 的阻断规则
6. 候选运行状态已输出 `latest_lineage_path`

本次真实运行结果：

- `candidate_lineage_latest.json`: `status=passed`
- `run_id`: `candidate-20260507_093017`
- `data_as_of`: `20260506`
- 当前候选数: `3`
- 候选来源: `formal`
- 覆盖候选: `688256.SH`、`688521.SH`、`600487.SH`

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_lineage.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_top_candidates_universe.py -k "data_quality_gate or lineage"`: 通过

下一步严格进入：

1. `P0-4 数据门禁`：将 `quality_level=blocked` 从“写 gate”升级为“正式观察名单前剔除或降级”

### 2026-05-07 P0-4 已落地

已完成：

1. 正式候选输出前执行 `candidate_data_quality_gate_latest.json`
2. `quality_level=blocked` 已从正式 `candidates_top_latest.csv` 剔除
3. `quality_level=review` 已标记 `basket_risk_flag=data_quality_review`
4. summary 已写入 `data_quality_gate_enforced`、`data_quality_original_candidate_count`、`data_quality_removed_count`、`data_quality_removed_codes`
5. gate 仍保留原始候选阻断证据，lineage 只覆盖最终正式展示候选

本次真实运行结果：

- `candidate_data_quality_gate_latest.json`: `status=passed`
- `data_quality_gate_enforced`: `true`
- 原始候选数: `3`
- 数据质量剔除数: `0`
- 当前正式候选数: `3`
- `candidate_lineage_latest.json`: `status=passed`
- `run_id`: `candidate-20260507_093902`
- 覆盖候选: `600487.SH`、`688521.SH`、`300548.SZ`

验证：

- `pytest -q stock_ultimate_system/tests/test_top_candidates_universe.py -k "data_quality_gate or lineage"`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P1-1 交易日历约束`
2. `P1-2 停牌/无成交约束`
3. `P1-3 涨跌停约束`

### 2026-05-07 P1-1/P1-2/P1-3 已落地

已完成：

1. 新增 `src/candidate_quality/realistic_backtest.py`
2. 新增 `scripts/build_candidate_realistic_backtest.py`
3. 新增 `tests/test_candidate_realistic_backtest.py`
4. 生成 `data/experiments/realistic_backtest_latest.json`
5. 回测以 `candidate_lineage_latest.json` 的 `data_as_of` 为锚点
6. 买入日期必须来自 SQLite 实际交易日序列，非交易日不成交
7. 缺行情、零成交量、零成交额、价格无效时阻断
8. 涨停不可买、跌停不可卖

本次真实运行结果：

- `realistic_backtest_latest.json`: `status=failed`
- 候选数: `3`
- 通过数: `0`
- 阻断数: `3`
- 阻断原因: `insufficient_future_trade_dates`
- 解释: 当前 `data_as_of=20260506`，本地库没有足够后续交易日，禁止伪造未来收益

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_realistic_backtest.py`: 通过

下一步严格进入：

1. `P1-4 成本模型`：输出佣金、滑点、冲击成本、换手成本拆解
2. `P1-5 容量约束`：按成交额容量和单票成交占比降级或阻断

### 2026-05-07 P1-4/P1-5 已落地

已完成：

1. `realistic_backtest_latest.json` 已纳入佣金、滑点、印花税、冲击成本
2. 新增 `transaction_cost_breakdown_latest.json`
3. 新增 `capacity_constraint_report_latest.json`
4. 容量约束按单票名义资金 / 买卖日成交额计算参与率
5. 参与率超过观察阈值标记 `review`
6. 参与率超过阻断阈值标记 `blocked`
7. CLI `scripts/build_candidate_realistic_backtest.py` 已同时输出真实回测、成本拆解、容量报告三份产物

本次真实运行结果：

- `realistic_backtest_latest.json`: `status=failed`
- `transaction_cost_breakdown_latest.json`: `status=failed`
- `capacity_constraint_report_latest.json`: `status=failed`
- 候选数: `3`
- 阻断数: `3`
- 阻断原因: `insufficient_future_trade_dates`
- 解释: 当前 `data_as_of=20260506` 后没有足够未来交易日，因此禁止输出成本后收益或容量通过结论

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P2-1 每日观察名单冻结`
2. `P2-2 观察 ledger`
3. 样本不足时继续保持 `blocked/继续观察`，不得输出质量已证明结论

### 2026-05-07 P2-1/P2-2 已落地

已完成：

1. 新增 `src/candidate_quality/observation.py`
2. 新增 `scripts/freeze_candidate_observation.py`
3. 新增 `tests/test_candidate_observation.py`
4. 生成 `data/experiments/candidate_observation_snapshot_20260506.json`
5. 生成 `data/experiments/candidate_observation_snapshot_latest.json`
6. 生成并追加 `data/experiments/candidate_observation_ledger.jsonl`
7. 同日重复冻结不会覆盖 `candidate_observation_snapshot_YYYYMMDD.json`
8. ledger 按 `observation_id=YYYYMMDD:ts_code` 去重，重复运行不重复追加

本次真实运行结果：

- 快照日期: `20260506`
- 快照状态: `frozen`
- 候选数: `3`
- ledger 新增: `3`
- 重复执行结果: `already_frozen`
- 重复执行 ledger 新增: `0`
- ledger 当前行数: `3`
- 当前 observation: `600487.SH`、`688521.SH`、`300548.SZ`

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P2-3 观察结果计算`：计算 5D/20D/60D 收益、超额收益、最大回撤、命中状态
2. `P2-4 失败归因`：失败样本必须归因
3. `P2-5 20 样本报告`：样本不足时输出 blocked，不得晋级

### 2026-05-07 P2-3/P2-4/P2-5 已落地

已完成：

1. `src/candidate_quality/observation.py` 新增观察结果计算
2. 新增 `scripts/build_candidate_observation_closure.py`
3. 新增 `tests/test_candidate_observation_closure.py`
4. 生成 `data/experiments/candidate_observation_result_latest.json`
5. 生成 `data/experiments/candidate_failure_attribution_latest.json`
6. 生成 `data/experiments/candidate_quality_20_sample_report.json`
7. 观察结果计算覆盖 `5D/20D/60D` 收益、超额收益、最大回撤、命中状态
8. 价格缺失、交易日不足、ledger 缺失均显式标注，不静默跳过
9. 失败归因按 `insufficient_observation_window`、`data_or_calendar_insufficient`、`risk_control_failure`、`negative_absolute_return`、`benchmark_underperformance` 分类
10. 20 样本不足时固定输出 `blocked` 和 `continue_observation`

本次真实运行结果：

- `candidate_observation_result_latest.json`: `status=pending`
- 候选数: `3`
- completed: `0`
- pending: `3`
- blocked: `0`
- `candidate_failure_attribution_latest.json`: `status=passed`
- 归因类别: `insufficient_observation_window=3`
- `candidate_quality_20_sample_report.json`: `status=blocked`
- 已完成样本数: `0/20`
- 结论: `continue_observation`

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P3-1 观察组合权重`
2. `P3-2 行业暴露控制`
3. `P3-3 相关性控制`

### 2026-05-07 P3-1/P3-2/P3-3 已落地

已完成：

1. 新增 `src/candidate_quality/portfolio.py`
2. 新增 `scripts/build_candidate_portfolio.py`
3. 新增 `tests/test_candidate_portfolio.py`
4. 生成 `data/experiments/candidate_portfolio_latest.json`
5. 生成 `data/experiments/portfolio_exposure_report_latest.json`
6. 观察组合权重从冻结观察名单生成，不反向修改候选生成逻辑
7. 权重显式归一，校验 `weight_sum=1.0`
8. 单票权重上限默认 `35%`
9. 行业暴露分为目标上限 `50%` 和硬上限 `65%`
10. 行业超过硬上限时自动降权并尝试向其他行业再分配
11. 相关性基于 SQLite 历史收盘价计算，默认 `60` 个交易日窗口
12. 相关性数据不足时输出 `review` 和 `correlation_data_insufficient`，不静默通过
13. 高相关候选默认阈值 `0.85`，触发后对较小权重候选降权并记录调整

本次真实运行结果：

- `candidate_portfolio_latest.json`: `status=review`
- 候选数: `3`
- 权重合计: `1.0`
- 最大单票权重: `0.35`
- 行业权重: `通信设备=0.65`，`半导体=0.35`
- 顶部行业: `通信设备`
- 顶部行业权重: `0.65`
- 复核原因: `industry_target_limit_exceeded`
- 阻断原因: 无
- 相关性状态: `passed`
- 相关性样本点: `59`
- 相关性对数: `3`
- 最高相关性未触发高相关降权

真实结论：

- 当前观察组合不是 P3 完全通过，而是 `review`
- 原因不是单票超限，也不是相关性拥挤，而是行业集中度仍高
- 通信设备权重达到硬上限 `65%`，未超过硬阻断线，但明显高于目标上限 `50%`
- 在只有 `3` 个冻结候选且其中 `2` 个同属通信设备时，系统不能伪造分散，只能标注复核

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_portfolio.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P3-4 流动性容量`
2. `P3-5 风险成本联合评分`

### 2026-05-07 P3-4/P3-5 已落地

已完成：

1. `src/candidate_quality/portfolio.py` 新增组合容量报告
2. `src/candidate_quality/portfolio.py` 新增风险成本联合质量评分
3. `scripts/build_candidate_portfolio.py` 扩展为一次生成 P3 四类产物
4. `tests/test_candidate_portfolio.py` 新增低容量阻断和组合质量阻断测试
5. 生成 `data/experiments/portfolio_capacity_report_latest.json`
6. 生成 `data/experiments/candidate_portfolio_quality_latest.json`
7. 容量报告使用冻结日或之前最近可用成交额，不依赖未来交易窗口
8. 单票容量输出目标名义金额、成交额、参与率、预估冲击成本
9. 组合容量输出最差参与率、总预估冲击成本、组合可承载名义金额
10. 风险成本联合评分合成行业集中度、HHI、容量、冲击成本、交易成本、真实回测可评估性
11. 质量分低于最低线时阻断晋级，不允许以 UI 或候选排名替代真实容量与成本证明

本次真实运行结果：

- `portfolio_capacity_report_latest.json`: `status=review`
- 候选数: `3`
- 容量状态分布: `passed=2`，`review=1`
- 最差参与率: `0.0512722`
- 容量复核原因: `capacity_participation_exceeds_watch_limit`
- 预估总冲击成本: `33.3719`
- 预估冲击成本: `0.3337 bps`
- `300548.SZ` 参与率 `0.0512722`，超过观察线 `0.05`

- `candidate_portfolio_quality_latest.json`: `status=blocked`
- 组合质量分: `23.6456`
- 阻断原因: `portfolio_quality_score_below_minimum`
- 复核原因:
  - `industry_target_limit_exceeded`
  - `capacity_participation_exceeds_watch_limit`
  - `transaction_cost_unavailable`
  - `realistic_backtest_not_evaluable`
- 主要扣分:
  - 组合行业复核: `12.0`
  - 容量复核: `14.0`
  - 行业集中度: `12.0`
  - HHI: `2.1`
  - 容量参与率: `10.2544`
  - 交易成本不可用: `8.0`
  - 真实回测阻断: `18.0`

真实结论：

- P3-4 容量不是完全通过，是 `review`
- P3-5 组合质量是 `blocked`
- 当前组合不能进入“候选质量已证明”的下一阶段
- 核心原因不是模型排序，而是组合层证据不足：行业集中、单票容量轻微吃紧、真实交易约束回测尚不可评估、交易成本报告无有效候选

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_portfolio.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P4-1 观察状态机`
2. `P4-2 降级规则`
3. `P4-3 失效退出规则`

### 2026-05-07 P4-1/P4-2/P4-3/P4-4 已落地

已完成：

1. 新增 `src/candidate_quality/risk_state.py`
2. 新增 `scripts/build_candidate_risk_state.py`
3. 新增 `tests/test_candidate_risk_state.py`
4. 生成 `data/experiments/candidate_risk_state_latest.json`
5. 生成 `data/experiments/candidate_risk_state_transition.jsonl`
6. 生成 `data/experiments/candidate_state_audit_latest.json`
7. 固定候选状态枚举：`watch/review/degrade/invalid/closed`
8. 每个候选必须有状态、原因和证据
9. 每次状态变化写入 transition JSONL
10. 审计检查 unknown state、missing reason、missing evidence
11. `invalid` 候选不得继续作为正常观察展示
12. `degrade` 候选不得继续作为正常 watch 展示

状态转移规则：

- 数据质量失败 -> `invalid`
- 观察数据或交易日历不可用 -> `invalid`
- 最大回撤低于失效阈值 -> `invalid`
- 收益低于失效阈值 -> `invalid`
- 观察窗口完成且无 pending -> `closed`
- 组合质量阻断 -> `degrade`
- 单票容量阻断或复核 -> `degrade`
- 回撤或收益触发降级阈值 -> `degrade`
- 观察窗口未完成 -> `review`
- 无风险且仍在观察中 -> `watch`

本次真实运行结果：

- `candidate_risk_state_latest.json`: `status=passed`
- 候选数: `3`
- 状态分布: `degrade=3`
- 阻断原因: 无
- transition 记录数: `3`
- 审计结果: `passed`
- unknown state: `0`
- missing reason: `0`
- missing evidence: `0`

候选状态：

- `600487.SH`: `degrade`，原因 `portfolio_quality_blocked`
- `688521.SH`: `degrade`，原因 `portfolio_quality_blocked`
- `300548.SZ`: `degrade`，原因 `portfolio_quality_blocked`、`capacity_review`

真实结论：

- 当前 3 个候选不得继续作为正常 `watch` 展示
- 系统应对外表达为“候选仍在观察，但组合质量证明未通过，已降级复核”
- 这不是选股失败结论，而是质量证明链在组合风控层未满足晋级要求

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_risk_state.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_risk_state.py stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P5-1 外部解释模板`
2. `P5-2 内部解释模板`
3. `P5-3 解释边界和非投资建议声明`

### 2026-05-07 P5-1/P5-2/P5-3 已落地

已完成：

1. 新增 `src/candidate_quality/explanation.py`
2. 新增 `scripts/build_candidate_explanations.py`
3. 新增 `tests/test_candidate_explanation.py`
4. 生成 `data/experiments/candidate_public_explanation_latest.json`
5. 生成 `data/experiments/candidate_internal_explanation_latest.json`
6. 生成 `data/experiments/candidate_rejection_explanation_latest.json`
7. 外部解释固定为四句话：
   - 为什么关注
   - 主要风险
   - 何时失效
   - 下一步观察什么
8. 外部解释固定加入边界声明：`本内容只用于研究观察，不构成买入、卖出或持有建议。`
9. 内部解释保留因子贡献、数据质量、血缘、组合权重、容量证据、观察结果、失败归因、风控状态证据
10. 降级或失效候选进入 rejection explanation，不再作为正常外部展示对象
11. 外部解释字段缺失时阻断外部展示
12. 内部解释证据缺失时标记 `review`
13. 排除原因缺失时阻断

本次真实运行结果：

- `candidate_public_explanation_latest.json`: `status=passed`
- `candidate_internal_explanation_latest.json`: `status=passed`
- `candidate_rejection_explanation_latest.json`: `status=passed`
- 候选解释数: `3`
- 排除解释数: `3`
- 外部展示正常允许数: `0`

外部解释状态：

- `600487.SH`: `degrade`，`external_display_allowed=false`
- `688521.SH`: `degrade`，`external_display_allowed=false`
- `300548.SZ`: `degrade`，`external_display_allowed=false`

排除原因：

- `600487.SH`: `portfolio_quality_blocked`
- `688521.SH`: `portfolio_quality_blocked`
- `300548.SZ`: `portfolio_quality_blocked`、`capacity_review`

真实结论：

- 当前外部页面不应把这 3 个对象展示为正常观察名单
- 可以展示为“降级复核对象”，但必须附带四句解释和非投资建议边界
- 内部仍可查看完整证据链，用于修复组合质量、容量和样本闭环问题

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_explanation.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_explanation.py stock_ultimate_system/tests/test_candidate_risk_state.py stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py`: 通过

下一步严格进入：

1. `P6-1 候选质量证明总报告`
2. `P6-2 外部页面只读解释层和状态机`
3. `P6-3 UI 同步服务器一致性`

### 2026-05-07 P6-1/P6-2/P6-3 已落地

已完成：

1. 新增 `src/candidate_quality/proof_report.py`
2. 新增 `scripts/build_candidate_quality_proof_report.py`
3. 新增 `tests/test_candidate_quality_proof_report.py`
4. `src/dashboard_context.py` 外部候选卡优先读取 `candidate_public_explanation_latest.json`
5. `src/dashboard_context.py` 候选详情优先读取解释层四句话
6. `src/dashboard_context.py` 同步读取 `candidate_risk_state_latest.json`
7. `scripts/run_server_sync_preflight.py` required sync files 纳入 P0-P6 新增模块和脚本
8. 生成 `data/experiments/candidate_quality_proof_report_latest.json`
9. 外部页面候选卡不再直接用 `candidates_top_latest.csv` 的工程字段作为首要展示事实
10. 同步 manifest 中 P0-P6 新增代码均归入 allowed files

本次真实运行结果：

- `candidate_quality_proof_report_latest.json`: `status=blocked`
- `quality_proven`: `false`
- `external_page_mode`: `degraded_review`
- 样本数: `0`
- 组合质量分: `23.6456`
- 风控状态分布: `degrade=3`
- 正常外部展示数: `0`
- 排除解释数: `3`

禁止声称：

- `candidate_quality_proven`
- `portfolio_quality_passed`
- `normal_external_watchlist`

页面读取层真实检查：

- `candidate_public_explanation_latest.json` 可生成 `3` 张外部候选卡
- 首张卡 `600487.SH` 状态为 `降级复核`
- 风险边界为 `不可正常展示`
- 候选详情只展示四句解释：为什么关注、主要风险、何时失效、下一步观察

服务器同步一致性检查：

- `src/candidate_quality/proof_report.py`: allowed
- `src/candidate_quality/explanation.py`: allowed
- `src/candidate_quality/risk_state.py`: allowed
- `src/dashboard_context.py`: allowed
- `scripts/build_candidate_quality_proof_report.py`: allowed
- `scripts/build_candidate_explanations.py`: allowed
- `scripts/build_candidate_risk_state.py`: allowed
- manifest `unclassified_total=0`

真实结论：

- P6 已完成集成，但质量证明总报告是 `blocked`
- 外部 UI 只能展示“降级复核”，不能展示“正常观察名单”
- 当前系统已经从“能生成候选”推进到“能证明候选尚未达标”，这是正确的质量证明结果

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_quality_proof_report.py stock_ultimate_system/tests/test_dashboard_context.py stock_ultimate_system/tests/test_build_server_sync_file_list.py stock_ultimate_system/tests/test_run_server_sync_preflight.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_quality_proof_report.py stock_ultimate_system/tests/test_candidate_explanation.py stock_ultimate_system/tests/test_candidate_risk_state.py stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py stock_ultimate_system/tests/test_dashboard_context.py stock_ultimate_system/tests/test_build_server_sync_file_list.py stock_ultimate_system/tests/test_run_server_sync_preflight.py`: 通过

下一步严格进入：

1. 修复质量证明阻断项：完成观察样本、补真实交易窗口、降低行业集中度
2. UI 只展示 `degraded_review`，不得恢复正常观察名单话术
3. 服务器发布前跑同步 preflight 和页面冒烟

### 2026-05-07 阻断项修复计划已落地

已完成：

1. 新增 `src/candidate_quality/remediation.py`
2. 新增 `scripts/build_candidate_quality_remediation_plan.py`
3. 新增 `tests/test_candidate_quality_remediation.py`
4. 生成 `data/experiments/candidate_quality_remediation_plan_latest.json`
5. `scripts/run_server_sync_preflight.py` required sync files 纳入 remediation 模块和脚本
6. 修复动作全部声明 `can_be_fixed_now=false`，避免伪造样本、伪造未来交易窗口或直接调分

本次真实修复计划：

- `candidate_quality_remediation_plan_latest.json`: `status=blocked`
- action_count: `6`
- next_run_order: `R1 -> R2 -> R3 -> R4 -> R5 -> R6`

动作清单：

1. `R1` `P0`：补观察样本
   - blocker: `insufficient_completed_samples`
   - 当前: `0`
   - 目标: `20`
   - 动作: 持续每日冻结观察名单并写 ledger，直到 20 个 completed 样本
   - 不能立即修复：需要真实观察窗口完成，不能补造样本

2. `R2` `P0`：补真实交易窗口
   - blocker: `insufficient_future_trade_dates`
   - 当前阻断候选: `3`
   - 目标: `0`
   - 动作: 等未来交易日形成后重跑 realistic backtest 和 transaction cost
   - 不能立即修复：不能用未来函数替代真实交易日

3. `R3` `P1`：降低行业集中度
   - blocker: `industry_target_limit_exceeded`
   - 当前通信设备权重: `0.65`
   - 目标: `0.50`
   - 需要降低: `0.15`
   - 优先处理对象: `300548.SZ 长芯博创`
   - 原因: 同属通信设备，权重 `0.30`，且容量状态 `review`
   - 动作: 下一轮扩展候选池，替换或下调主导行业候选

4. `R4` `P1`：修容量参与率
   - blocker: `capacity_participation_exceeds_watch_limit`
   - 当前最差参与率: `0.0512722`
   - 目标: `0.05`
   - 动作: 降低或替换容量复核候选

5. `R5` `P0`：恢复组合质量分
   - blocker: `portfolio_quality_score_below_minimum`
   - 当前质量分: `23.6456`
   - 目标: `60.0`
   - 分数缺口: `36.3544`
   - 动作: 先清除样本、回测、行业、容量阻断，再重算质量分
   - 不能直接调分

6. `R6` `P2`：解除状态机降级
   - blocker: `risk_state_degrade`
   - 当前降级候选: `3`
   - 目标: `0`
   - 动作: 上游质量和容量通过后重跑风险状态机

同步检查：

- `src/candidate_quality/remediation.py`: allowed
- `scripts/build_candidate_quality_remediation_plan.py`: allowed
- manifest `unclassified_total=0`

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_quality_remediation.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_quality_remediation.py stock_ultimate_system/tests/test_candidate_quality_proof_report.py stock_ultimate_system/tests/test_candidate_explanation.py stock_ultimate_system/tests/test_candidate_risk_state.py stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py stock_ultimate_system/tests/test_dashboard_context.py stock_ultimate_system/tests/test_build_server_sync_file_list.py stock_ultimate_system/tests/test_run_server_sync_preflight.py`: 通过

下一步严格执行：

1. 每个交易日收盘后重跑 `freeze_candidate_observation.py`
2. 每个交易日收盘后重跑 `build_candidate_observation_closure.py`
3. 未来交易窗口满足后重跑 `build_candidate_realistic_backtest.py`
4. 下一轮候选生成必须扩大候选池，优先寻找非通信设备替代，目标 `top_industry_weight <= 0.50`
5. 重跑 `build_candidate_portfolio.py`、`build_candidate_risk_state.py`、`build_candidate_explanations.py`、`build_candidate_quality_proof_report.py`

### 2026-05-07 R1-R6 日常闭环 Runner 已落地

已完成：

1. 新增 `scripts/run_candidate_quality_daily_closure.py`
2. 新增 `tests/test_candidate_quality_daily_closure.py`
3. 生成 `data/experiments/candidate_quality_daily_closure_latest.json`
4. 日常闭环 runner 默认不重跑候选生成，避免覆盖当前正式样本链
5. 如需下一轮扩池降集中度，必须显式传入 `--generate-candidates --expanded-universe-size N`
6. `scripts/run_server_sync_preflight.py` required sync files 纳入 daily closure runner

闭环执行顺序：

1. `R3_candidate_generation`
   - 默认 `skipped`
   - 原因：候选生成会覆盖正式样本链，必须显式开启
2. `R1_freeze_observation`
3. `R1_observation_closure`
4. `R2_realistic_backtest`
5. `R3_R4_R5_portfolio_quality`
6. `R6_risk_state`
7. `P5_explanations`
8. `P6_quality_proof_report`
9. `remediation_plan`

本次真实运行结果：

- `candidate_quality_daily_closure_latest.json`: `status=blocked`
- `quality_proof_status`: `blocked`
- `remediation_status`: `blocked`
- `remediation_action_count`: `6`
- `failed_step_ids`: `[]`
- `next_run_order`: `R1 -> R2 -> R3 -> R4 -> R5 -> R6`

分步状态：

- `R3_candidate_generation`: `skipped`
- `R1_freeze_observation`: `frozen` / `already_frozen`
- `R1_observation_closure`: `pending`
- `R2_realistic_backtest`: `failed`，原因 `insufficient_future_trade_dates`
- `R3_R4_R5_portfolio_quality`: portfolio `review`，quality `blocked`
- `R6_risk_state`: `passed`
- `P6_quality_proof_report`: `blocked`
- `remediation_plan`: `blocked`

同步检查：

- `scripts/run_candidate_quality_daily_closure.py`: allowed
- manifest `unclassified_total=0`

验证：

- `pytest -q stock_ultimate_system/tests/test_candidate_quality_daily_closure.py stock_ultimate_system/tests/test_candidate_quality_remediation.py`: 通过
- `pytest -q stock_ultimate_system/tests/test_candidate_quality_daily_closure.py stock_ultimate_system/tests/test_candidate_quality_remediation.py stock_ultimate_system/tests/test_candidate_quality_proof_report.py stock_ultimate_system/tests/test_candidate_explanation.py stock_ultimate_system/tests/test_candidate_risk_state.py stock_ultimate_system/tests/test_candidate_portfolio.py stock_ultimate_system/tests/test_candidate_observation_closure.py stock_ultimate_system/tests/test_candidate_observation.py stock_ultimate_system/tests/test_candidate_realistic_backtest.py stock_ultimate_system/tests/test_candidate_lineage.py stock_ultimate_system/tests/test_candidate_data_quality_report.py stock_ultimate_system/tests/test_dashboard_context.py stock_ultimate_system/tests/test_build_server_sync_file_list.py stock_ultimate_system/tests/test_run_server_sync_preflight.py`: 通过

下一步严格执行：

1. 每日收盘后运行 `python scripts/run_candidate_quality_daily_closure.py --json`
2. 下一轮扩池时运行 `python scripts/run_candidate_quality_daily_closure.py --generate-candidates --expanded-universe-size 300 --top-n 5 --json`
3. 扩池后必须检查 `candidate_portfolio_latest.json.summary.top_industry_weight <= 0.50`
4. 只有 `candidate_quality_proof_report_latest.json.status=passed` 才能恢复正常外部观察名单

### 2026-05-07 运行观察期自动化与同步稳定性补齐

阶段定义：

- 功能开发冻结，进入 2-4 周运行验证期
- 不新增大能力模块，不包装 UI，不人工修复质量分
- 只保留自动化定时、服务器同步、数据更新稳定性、页面只读一致性和 bug 修复

已完成：

1. 新增 `install_candidate_quality_daily_closure_launchd.py`
2. launchd 默认每天 `16:10` 执行 `scripts/run_candidate_quality_daily_closure.py --json`
3. launchd 默认不带 `--generate-candidates`，不会覆盖当前正式样本链
4. 支持 `--dry-run` 审计 plist，不写入本机 LaunchAgents
5. 支持 `--no-load` 只写 plist，不立即加载 launchd
6. 新增 `tests/test_install_candidate_quality_daily_closure_launchd.py`
7. `scripts/run_server_sync_preflight.py` required sync files 纳入 `install_candidate_quality_daily_closure_launchd.py`

安装命令：

- 只审计不安装：`python install_candidate_quality_daily_closure_launchd.py --dry-run`
- 写入并加载：`python install_candidate_quality_daily_closure_launchd.py`
- 只写 plist 不加载：`python install_candidate_quality_daily_closure_launchd.py --no-load`

同步检查：

- `install_candidate_quality_daily_closure_launchd.py`: allowed
- `scripts/run_candidate_quality_daily_closure.py`: allowed
- manifest `unclassified_total=0`
- server sync preflight `status=passed`

验证：

- `pytest -q stock_ultimate_system/tests/test_install_candidate_quality_daily_closure_launchd.py stock_ultimate_system/tests/test_candidate_quality_daily_closure.py stock_ultimate_system/tests/test_run_server_sync_preflight.py`: 通过
- `python stock_ultimate_system/scripts/run_server_sync_preflight.py --json`: 通过

观察期硬边界：

1. 每日收盘 runner 只记录真实闭环结果
2. `insufficient_future_trade_dates` 只能等未来交易日自然满足
3. `completed` 样本只能由真实 5D/20D/60D 观察窗口形成
4. 下一轮扩池必须显式手工执行，并优先降低通信设备集中度
5. `portfolio_quality_score` 未达到 60 前，外部页面继续保持 `degraded_review`
6. `completed < 20` 前不允许恢复正常外部观察名单

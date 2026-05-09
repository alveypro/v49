# Airivo ensemble_core 顶层组合研究线开发计划

版本：`v1.0`
日期：`2026-05-07`
状态：`严格执行计划`
上位约束：`docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md`、`docs/AIRIVO_CURRENT_STAGE_STRATEGY_PRODUCTION_READINESS_PLAN.md`、`docs/AIRIVO_PROFESSIONAL_SYSTEM_BLUEPRINT.md`
适用范围：`ensemble_core`、alpha sleeve fact chain、Tushare Pro PIT 特征、multi-horizon attribution、shadow portfolio、正式池对照。

## 1. 当前真实结论

`ensemble_core` 已经从概念研究推进到可审计 fact chain，但仍不是正式策略。

已完成事实：

- Tushare Pro 本地落库数据已经接入 PIT alpha feature chain。
- `momentum / reversal / money_flow / sector_rotation / quality_low_vol / event_risk` 已能形成 sleeve 级样本。
- 已能计算 `1/3/5` 日 IC、RankIC 和 decay profile。
- 当前 `10/20` 日 forward window 不足，不能完整判断 alpha 生命周期。
- 正式 top 仍是 `v4 / v5 / v9`。
- `ensemble_core` 仍为 `research_only`，不得进入正式池或 observation pool。

当前 sleeve 审计结论：

| sleeve | 当前用途 | 真实理由 |
| --- | --- | --- |
| `momentum` | `positive_alpha_candidate` | 1D/5D IC 为正，但 3D 为负，RankIC 不稳，只能轻权重候选 |
| `reversal` | `positive_alpha_candidate` | 3D/5D 有正贡献，更像 3-5 日修复型 alpha |
| `money_flow` | `research_blocked_negative_ic` | 5D IC 为负，不能正向使用 |
| `quality_low_vol` | `risk_filter_candidate` | 3D/5D IC 为负，只能做防守过滤 |
| `sector_rotation` | `risk_filter_candidate` | 1D/3D/5D IC 整体负，不得正向加权 |
| `event_risk` | `research_blocked_insufficient_event_sample` | 样本不足，不能独立成 alpha sleeve |

## 2. 不可偏移边界

以下事项在本计划内禁止：

- 不把 `ensemble_core` 加入正式 top、正式 eligible pool 或默认生产路径。
- 不用简单加权平均替代 portfolio construction。
- 不把负 IC sleeve 当作正向 alpha。
- 不用 5D 单点 IC 证明 alpha 成熟。
- 不用当前 `1/3/5` 日 decay 替代完整 `1/3/5/10/20` 日生命周期验证。
- 不靠降低阈值、扩大 top_n、修改标签来制造 alpha。
- 不在 backtest 运行中实时请求 Tushare Pro。
- 不把页面展示、自然语言报告或临时 JSON 当作晋级证据。
- `hard_event_alpha_candidate` 当前仅允许保持 `research/blocked`；任何后续改动只能围绕 `risk-off alpha` 本体修复、预声明规则冻结、同批窗口重跑和 `unthrottled` 证明展开。不得用 allocator throttle、现金回退、UI 展示或 observation 包装替代 alpha 证明，也不得进入 `formal`、`top` 或 `production`。
- `hard_event_alpha_candidate` 的任何 repair passed 结论最多只能写入 `observation_watch_discussion_allowed`。必须先落入 `Research Repair Flow` 审计整改系统，记录目标、预声明规则、rule hash、固定窗口、unthrottled/throttled artifacts、attribution、repair review、watch risk register 和 prohibited actions；缺少流程记忆、同批窗口校验、rule hash 校验或 watch risk register 时，不得继续调参、提交晋级或包装结果。

允许事项：

- 读取本地已落库 Tushare Pro/PIT 表。
- 补全 forward replay 数据。
- 扩展事件表、停复牌和涨跌停表。
- 构建 research-only shadow portfolio。
- 对负 IC sleeve 自动降权、关闭或转为 risk filter。

## 3. 顶级设计对标

行业顶级组合策略系统不是单策略排行榜，而是以下闭环：

1. PIT data lineage
2. Alpha sleeve attribution
3. Multi-horizon IC/RankIC/decay
4. Cross-sleeve correlation
5. Portfolio construction
6. Cost, slippage, capacity and limit-state replay
7. Walk-forward shadow benchmark
8. Regime split and risk contribution
9. Promotion, demotion and kill switch

本计划只推进 `ensemble_core` 从第 2 层走到第 7 层。未通过第 7 层前，不讨论正式策略晋级。

### 3.1 Research Repair Flow 审计整改系统

`hard_event_alpha_candidate` 的修复链路必须通过 `Research Repair Flow` 记录完整流程记忆：

1. `research_repair_iteration_flow` 记录 repair attempt 的目标、禁止目标、规则版本、rule hash、固定窗口、benchmark 配置、artifacts、裁决状态和下一步。
2. `research_rule_freeze_registry` 记录预声明规则，不允许 shadow benchmark 中途改变规则。
3. `research_artifact_registry` 索引 rule freeze、unthrottled benchmark、throttled benchmark、allocator attribution、monitor、repair review、watch risk review 和 OOS monitoring plan。
4. `observation_watch_risk_register` 登记 observation watch 风险，尤其是 neutral signal sparsity、cash weight high、source concentration、over-veto 和 same-window overfit。
5. `tools/governance_gate.py` 必须阻断 repair artifact 中任何 `formal_candidate_allowed=true`、`formal_ranking_allowed=true` 或 `production_candidate_allowed=true`。

当前 v5 若通过 repair review，也只表示 `observation_watch_discussion_allowed`，不表示 formal/top/production。

### 3.2 v5 Watch Risk Review + OOS Monitoring Plan

`hard_event_alpha_candidate` v5 在 repair passed 后必须先进入 watch risk review，不得直接进入 observation watch：

1. 冻结当前 v5 evidence manifest、rule hash、10 窗口 unthrottled/throttled benchmark 和 watch risk register，不再改 v5 参数。
2. 生成 `watch_risk_review` artifact，独立审查 over-veto、neutral signal sparsity、neutral cash weight high，以及是否存在“靠拒绝交易改善”的假修复。
3. 生成 `oos_monitoring_plan` artifact，预声明 OOS window set、paired unthrottled/throttled run requirement、same rule hash requirement 和 regime split requirement。
4. OOS pass 条件必须至少包括：neutral unthrottled after-cost excess return > 0、neutral hit rate >= 0.5、unthrottled turnover <= 0.75、coverage 不过低、risk-off 修复不回退、无 block-level watch risk。
5. 若当前 flow 存在 block-level watch risk，governance gate 必须要求 `watch_risk_review_artifact` 和 `oos_monitoring_plan_artifact`，否则阻断。
6. OOS 执行必须生成 `oos_monitoring_result` artifact，校验预声明窗口、paired unthrottled/throttled、same rule hash、regime split、neutral alpha/hit/turnover/coverage 和 OOS watch risk。即使 OOS passed，也只能写为 `oos_monitoring_passed_discussion_only`，最多允许重新讨论 `observation_watch_discussion_allowed`。

当前 v5 的 `over_veto_risk` 是 block-level risk，因此只允许 `watch_risk_register_review` 与 `out_of_sample_monitoring_plan`；formal/top/production 继续硬阻断。

### 3.3 v6 Predeclared Alpha Repair

v5 OOS 若结论为 `oos_monitoring_failed_blocked`，下一步只能启动 `v6 predeclared alpha repair`：

1. 先登记 v5 OOS failure attribution，再冻结 v6 repair objective、规则候选、rule hash、repair window set 和 OOS window set。
2. v6 规则候选必须显式版本化为 `hard_event_alpha_candidate.neutral_over_veto_rebalance_guard.v6`，不得覆盖 v5 rule hash。
3. v6 只允许修 alpha 本体的 over-veto、neutral alpha/hit、turnover 和 risk-off 回退，不得使用 allocator throttle、现金回退、UI 或 observation 包装替代 alpha 证明。
4. v6 repair 必须重新产出 paired unthrottled/throttled benchmark、attribution、repair review、watch risk register 和 OOS result。
5. 只有 v6 unthrottled 在 risk-off、neutral、risk-on 都站住，且没有 block-level watch risk，才允许重新讨论 `observation_watch_discussion_allowed`。formal/top/production 继续硬阻断。

### 3.4 Failed Repair Freeze 与 v7 Go/No-Go

若 v6 结论仍为 `repair_review_blocked` 或 `oos_monitoring_failed_blocked`：

1. 冻结 v6 failed repair attempt，保留 final manifest、rule hash、repair replay、attribution、repair review、watch risk register 和 OOS result。
2. 生成 `failure_attribution_comparison`，对照 v5/v6 的 over-veto、neutral coverage、benchmark validity、regime evidence 和 risk-off regression。
3. 生成 `v7_go_no_go_review`。只有存在明确、非事后择优的 alpha 本体机制，才允许后续启动 v7 predeclared repair。
4. v7 go 条件必须包括：repair objective、相对 v6 的唯一修复点、固定窗口、rule hash、OOS 通过条件和放弃条件。
5. 若无法满足 v7 go 条件，`hard_event_alpha_candidate` 应归档为 failed research candidate，不再消耗 observation/formal 资源。
6. 归档必须生成 `failed_research_candidate_archive` artifact，记录 archive reasons、reopen conditions 和 prohibited actions。归档后默认不得进入 observation watch、formal、top 或 production；重开必须先有新的 alpha 本体机制、rule hash、固定窗口和治理批准。

## 4. 阶段路线

### P0. 审计冻结与基线确认

目标：

- 固定当前事实口径，防止后续开发漂移。

必须保持：

- 正式 eligible pool：`v4 / v5 / v9 / v8 / combo`
- 当前 top：`v4 / v5 / v9`
- `stable`：observation
- `v6 / v7`：diagnostic
- `ai / ensemble_core`：research-only

验收命令：

```bash
python tools/strategy_optimization_stage_audit.py --rejected-artifacts logs/openclaw/rejected_backtest_artifacts.jsonl --json
```

验收标准：

- `blocking_reasons=[]`
- `top_strategies=["v4","v5","v9"]`
- `ensemble_core` 不出现在正式 top 或正式可执行策略输出中。

### P1. Forward replay 数据补齐

目标：

- 让 `1/3/5/10/20` 日 forward return 全部可计算。

开发任务：

- 补齐 `2026-03-20` 后至少 20 个交易日的 `daily_trading_data`。
- 或选择一个已有足够 forward window 的历史 as-of 日期，重新生成 v4/v5/v8/v9/combo/v6/v7 source scans。
- `build_data_version(as_of_date=...)` 必须继续截断到 as-of，不得被当前最新数据污染。

验收 artifact：

- `logs/openclaw/repair_YYYYMMDD_ensemble_forward_window_probe_*/all_strategy_evidence_run_*.json`

验收标准：

- `multi_horizon_decay.*.horizons.10.available=true`
- `multi_horizon_decay.*.horizons.20.available=true`
- 若不可用，必须输出 `insufficient_forward_price_window`，不得静默跳过。

### P2. Event risk 样本扩展

目标：

- 让 `event_risk` 从样本不足变成可评估 sleeve。

数据范围：

- 龙虎榜：`top_list`
- 涨跌停：limit up/down 表或等价本地落库表
- 停复牌：suspension 表或等价本地落库表
- 公告事件：回购、减持、业绩预告、监管问询、重大合同、股权激励

开发任务：

- 新增或扩展 event feature builder，只读本地 PIT 表。
- 每个事件必须有：
  - `event_type`
  - `event_date`
  - `visible_as_of_date`
  - `source_table`
  - `ts_code`
  - `directional_prior`

验收标准：

- `event_risk.active_signal_count >= 10`
- `event_risk` 至少 3 个 horizon 可算 IC/RankIC。
- 未满足前，`event_risk` 必须保持 `research_blocked_insufficient_event_sample`。

### P3. Sleeve policy 审计固化

目标：

- 把 sleeve 用途判定制度化，不能靠人工解释。

规则：

- `positive_alpha_candidate`
  - active sample > 0
  - 5D IC > 0
  - 至少两个 horizon IC 为正
  - RankIC 不得长期反向
- `risk_filter_candidate`
  - 收益 IC 不支持正向 alpha
  - 但可解释为风险、防守或行业过滤
- `research_blocked_negative_ic`
  - 5D IC <= 0 且不属于明确风控过滤 sleeve
- `research_blocked_insufficient_event_sample`
  - event 样本不足

开发任务：

- 保持 `sleeve_use_policy` 为单一事实输出。
- 将负 IC sleeve 从 portfolio alpha candidate 中排除。
- 在 artifact 中保留被排除原因。

验收标准：

- `sector_rotation` 当前不得是 `positive_alpha_candidate`。
- `quality_low_vol` 当前不得是 `positive_alpha_candidate`。
- `money_flow` 当前不得是 `positive_alpha_candidate`。

### P3A. Alpha 重建候选预声明 walk-forward

目标：

- 对失败归因后产生的新 alpha 修复假设做预声明验证，防止事后择优。

当前唯一允许假设：

- `hard_event_alpha_candidate` 只能测试 `risk_off_gate`。
- `risk_off_gate` 定义固定为：`avg_pct_chg <= -1.0` 或 `advance_ratio <= 0.35` 的 as-of window 排除。
- 不允许叠加 `source_strategy_filter` 删除 v6/v8/v9 来制造通过。

开发任务：

- 使用不参与上一轮 gate contrast 的 fresh as-of windows。
- 输出 `research_only` 的预声明 walk-forward artifact。
- artifact 必须保留 retained windows、excluded risk-off windows、sample retention、IC、RankIC 和 blocking reasons。

验收标准：

- 至少 4 个 retained windows。
- 至少 1 个被排除的 fresh risk-off validation window。
- retained windows 必须全部 IC/RankIC 为正。
- sample retention >= 0.5。
- 即使通过，也只能进入 `sleeve policy candidate discussion`，不能直接进入 shadow portfolio、observation 或 formal pool。

标准命令：

```bash
python tools/ensemble_alpha_predeclared_gate_walk_forward.py \
  --as-of-dates YYYY-MM-DD,YYYY-MM-DD,YYYY-MM-DD,YYYY-MM-DD,YYYY-MM-DD \
  --calibration-as-of-dates 2026-02-04,2026-02-11,2026-02-18,2026-02-25,2026-03-04 \
  --candidate hard_event_alpha_candidate \
  --gate-name risk_off_gate \
  --operator-name codex_hard_event_alpha_predeclared_gate_walk_forward_YYYYMMDD \
  --output-dir logs/openclaw/repair_YYYYMMDD_hard_event_alpha_predeclared_gate_walk_forward
```

禁止：

- 用同一批失败归因窗口证明 gate 成熟。
- 用多个 gate 方案同场比较后选择最优。
- 把 risk-off 排除解释成正向 alpha 本体。
- 把本阶段结果当作 observation/formal 晋级证据。

### P4. Research-only shadow portfolio constructor

目标：

- 构建 shadow portfolio，不进入正式推荐，不生成生产买入清单。

第一版允许使用：

- alpha sleeves：`momentum`、`reversal`
- filter sleeves：`quality_low_vol`、`sector_rotation`
- blocked sleeves：`money_flow`、`event_risk`

组合约束：

- 单票上限
- 行业上限
- 成交额容量约束
- 换手预算
- 回撤预算
- 风险预算

禁止：

- 简单平均 score。
- 无容量约束权重。
- 无行业约束权重。
- 把 risk filter 当正向 alpha 加权。

输出要求：

- `shadow_weights`
- `excluded_sleeves`
- `constraint_hits`
- `risk_budget`
- `industry_exposure`
- `capacity_usage`
- `turnover_estimate`
- `not_for_production=true`

验收标准：

- 输出明确 `research_only=true`。
- 不写入正式 top stock。
- 不改变 `unified_recommendation.top_strategies`。

### P5. Cost, slippage, capacity replay

目标：

- 让 shadow portfolio 从纸面组合变成 after-cost 可比较组合。

成本模型最低要求：

- 固定佣金/印花税
- 按成交额估算冲击成本
- 涨停不可买、跌停不可卖
- 停牌不可交易
- 成交额容量上限
- 开盘跳空处理

验收字段：

- `gross_return`
- `cost_bps`
- `slippage_bps`
- `capacity_blocked_names`
- `limit_state_blocked_names`
- `net_return`
- `turnover`

未完成前：

- `ensemble_core` 不能进入 observation pool。

### P6. Walk-forward shadow benchmark

目标：

- 与正式池做真实 shadow 对照。

benchmark：

- `v4`
- `v5`
- `v8`
- `v9`
- `combo`

最低窗口：

- 至少 5 个 as-of windows。
- 每个窗口必须有完整 data_version。
- 每个窗口必须有 after-cost portfolio result。

验收指标：

- after-cost excess return
- max drawdown
- hit rate
- turnover
- capacity utilization
- industry concentration
- regime split
- risk contribution

晋级规则：

- 只允许从 `research_only` 推进到 `observation`。
- 不允许直接进入 formal eligible。
- 必须显著优于正式池基线，且风险不恶化。

### P7. Observation gate hardening

目标：

- 防止 5 窗口、单一 regime 或高换手结果被包装成 observation 晋级。
- 将 `hard_event_alpha_candidate` 从 shadow benchmark 推进到 observation review 前，先做独立硬门禁。

最低门槛：

- 至少 8 个 fresh as-of windows，目标 10 个。
- `risk_on` 至少 3 个窗口，`neutral` 至少 2 个窗口。
- 必要 regime 的 after-cost excess return 必须为正，hit rate 不低于 0.50。
- 总体 hit rate 不低于 0.60。
- 平均 turnover 不高于 0.75。
- industry concentration 必须低于 0.30，不能触顶通过。
- capacity utilization 不高于 0.10。

产物：

- `openclaw/services/ensemble_observation_gate_service.py`
- `tools/ensemble_observation_gate.py`
- `tests/test_ensemble_observation_gate_service.py`
- `logs/openclaw/repair_YYYYMMDD_hard_event_alpha_observation_gate/ensemble_observation_gate_*.json`

晋级规则：

- 通过 P7 只代表可进入 observation promotion review。
- P7 artifact 不得直接修改 strategy pool。
- 未通过 P7 时，`ensemble_core` 必须继续保持 `research_only`。

## 5. 代码落点

优先落点：

- `openclaw/services/ensemble_alpha_sleeve_service.py`
- `openclaw/services/tushare_pro_alpha_feature_service.py`
- `openclaw/services/ensemble_core_contract_service.py`
- `openclaw/research/all_strategy_evidence_run.py`
- `openclaw/services/unified_strategy_recommendation_service.py`
- `tools/all_strategy_evidence_run.py`

后续可新增：

- `openclaw/services/ensemble_shadow_portfolio_service.py`
- `openclaw/services/ensemble_execution_cost_service.py`
- `tests/test_ensemble_shadow_portfolio_service.py`
- `tests/test_ensemble_execution_cost_service.py`

禁止落点：

- 页面层成熟度判断。
- 临时脚本直接决定策略晋级。
- 修改正式池 top 逻辑绕过审计服务。

## 6. 标准执行命令

### 6.1 生成 historical source scans

```bash
python tools/all_strategy_evidence_run.py \
  --strategies v4,v5,v8,v9,combo,v6,v7 \
  --date-from 2025-10-01 \
  --date-to YYYY-MM-DD \
  --sweep-max-runs 1 \
  --per-run-timeout-sec 90 \
  --offline-stock-limit 80 \
  --scan-limit 20 \
  --operator-name codex_ensemble_source_scans_YYYYMMDD \
  --output-dir logs/openclaw/repair_YYYYMMDD_ensemble_source_scans \
  --no-research-only
```

### 6.2 生成 ensemble_core fact chain

```bash
python tools/all_strategy_evidence_run.py \
  --strategies ensemble_core \
  --date-from 2025-10-01 \
  --date-to YYYY-MM-DD \
  --sweep-max-runs 1 \
  --per-run-timeout-sec 90 \
  --offline-stock-limit 80 \
  --scan-limit 20 \
  --operator-name codex_ensemble_core_probe_YYYYMMDD \
  --output-dir logs/openclaw/repair_YYYYMMDD_ensemble_core_probe
```

### 6.3 运行测试

```bash
pytest \
  tests/test_ensemble_alpha_sleeve_service.py \
  tests/test_tushare_pro_alpha_feature_service.py \
  tests/test_ensemble_core_contract_service.py \
  tests/test_all_strategy_evidence_run.py \
  tests/test_unified_strategy_recommendation_service.py \
  tests/test_strategy_optimization_stage_audit_tool.py
```

### 6.4 运行阶段审计

```bash
python tools/strategy_optimization_stage_audit.py \
  --rejected-artifacts logs/openclaw/rejected_backtest_artifacts.jsonl \
  --json
```

## 7. 每轮开发验收清单

每轮结束必须回答：

- 本轮是否改变正式 top？
- `ensemble_core` 是否仍为 research-only？
- 是否新增或改变 PIT 事实源？
- 是否有 artifact 可复现？
- 是否有测试覆盖？
- 是否有 stage audit？
- 是否有负 IC sleeve 被错误当作正向 alpha？
- 是否有 10/20 日 forward window 缺失？
- 是否引入页面层或自然语言晋级判断？

每轮必须留下：

- JSON artifact
- Markdown artifact
- 测试结果
- 审计 artifact
- 明确的下一步 blocking reasons

## 8. 当前下一步唯一允许任务

按优先级执行：

1. 补齐 `10/20` 日 forward replay。
2. 扩展 `event_risk` PIT 事件样本。
3. 对 `hard_event_alpha_candidate` 只能做预声明 `risk_off_gate` fresh walk-forward，不能用同样本 contrast 替代。
4. 固化 sleeve policy 到 shadow portfolio 输入。
5. 新增 research-only `ensemble_shadow_portfolio_service.py`。
6. 接入成本、滑点、容量和 limit-state replay。
7. 做正式池 shadow benchmark。

不得先做：

- 直接 portfolio 正式推荐。
- v6/v7 standalone 继续硬救。
- 把 `sector_rotation` 正向加权。
- 把 `quality_low_vol` 当收益 alpha。
- 把 `money_flow` 当前版本当收益 alpha。

## 9. 晋级裁决

`ensemble_core` 晋级顺序固定：

```text
research_only -> shadow_research -> observation -> formal_candidate -> formal_eligible
```

当前只允许停留在：

```text
research_only
```

进入 `observation` 的最低条件：

- `1/3/5/10/20` 日全部可 replay。
- 至少两个 alpha sleeve 通过正向 alpha 候选规则。
- risk filter 不被当作收益 alpha。
- shadow portfolio 有 after-cost / after-slippage / after-capacity 结果。
- walk-forward shadow benchmark 不劣于正式池，且有风险改善或收益改善。
- stage audit 通过。

进入 `formal_candidate` 的最低条件：

- observation 连续多个窗口稳定。
- regime split 不崩。
- risk contribution 可解释。
- turnover 和 capacity 可执行。
- 有人工裁决 artifact。

进入 `formal_eligible` 的最低条件：

- 与现有正式池使用同一套 backtest credibility、quality floor、execution evidence 和 release gate。
- 不允许降低任何正式池门槛。

## 10. 最终判断

`ensemble_core` 的正确方向是顶层组合研究线，不是新单策略版本号。

当前开发必须围绕以下事实推进：

- 先证明 alpha 生命周期，再做组合。
- 先证明 after-cost，再谈收益。
- 先 shadow benchmark，再谈观察池。
- 先观察池稳定，再谈正式候选。

任何偏离该路线、直接包装成正式策略的开发，都应被审计系统阻断。

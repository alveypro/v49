# Airivo 策略优化升级阶段开发落地文档

版本：`v1.0`
日期：`2026-05-05`
状态：`下一阶段执行文档`
上位约束：`docs/AIRIVO_PROFESSIONAL_SYSTEM_BLUEPRINT.md`、`docs/AIRIVO_CURRENT_STAGE_STRATEGY_PRODUCTION_READINESS_PLAN.md`
专项约束：`docs/AIRIVO_ENSEMBLE_CORE_SHADOW_PORTFOLIO_DEVELOPMENT_PLAN.md`
适用范围：`v5 / v8 / v9 / combo / v4 / v6 / v7 / stable / ai` 的回测、实验、优化、统一竞争、执行证据和晋级裁决。

## 1. 阶段结论

当前阶段允许进入 `策略优化升级`，但不允许宣称 `策略成熟`、`生产闭环完成` 或 `对标顶级系统完成`。

本阶段的真实目标是：

- 让生产策略和实验策略用同一套事实要求进入统一竞争。
- 让任何参数优化都只能来自可信 backtest artifact。
- 让失败策略留下可解释诊断，而不是通过降阈值、补标签或页面包装进入榜单。
- 让策略晋级依赖 `signal_runs -> decision_events -> execution evidence -> release evidence`，不依赖人工印象。

行业顶级设计对应关系：

- Point-in-time replay：防未来函数。
- Walk-forward validation：防全样本调参。
- Cost and capacity aware backtest：防纸面收益。
- Experiment registry：防实验不可复现。
- Shadow/canary validation：防回测直接进生产。
- Kill switch and demotion：防策略劣化后继续运行。

## 2. 强制边界

### 2.1 禁止项

- 不降低 `score_threshold` 换取信号数量。
- 不把 `eligible_for_formal_ranking=false` 的 sweep artifact 写入运行默认。
- 不补录 `backtest_credibility`、`execution_evidence` 或治理标签伪造证据。
- 不从 observation pool 生成正式 top list。
- 不在页面层新增策略成熟度判断。
- 不把实验策略直接加入默认生产路径。
- 不用单次胜率、单窗口收益、单日榜单证明策略有效。

### 2.2 允许项

- 允许优化 `holding_days`、`sample_size`、止损止盈、ATR cap、因子权重、combo 权重、market regime multiplier。
- 允许实验策略进入统一竞争，但必须先过同一套 backtest credibility 和 quality floor。
- 允许失败策略保留在诊断池和 observation pool。
- 允许把可信 sweep 结果作为候选参数，但必须经过质量过滤和人工裁决。

## 3. 单一事实源

本阶段只允许以下事实源定义策略优化结果：

- `signal_runs`
- `signal_items`
- `decision_events`
- `execution_orders`
- `execution_fills`
- `execution_attribution`
- `logs/openclaw/backtest_sweep_*.json`
- `logs/openclaw/backtest_sweep_*.csv`
- `logs/openclaw/backtest_sweep_*.md`
- `logs/openclaw/rejected_backtest_artifacts.jsonl`

页面、session state、临时 JSON、日报自然语言和人工备注只能消费事实，不得定义事实。

## 4. 统一竞争入口

所有策略分两类治理身份，但进入统一竞争时使用同一事实要求。

生产策略：

- `v5`
- `v8`
- `v9`
- `combo`

实验策略：

- `v4`
- `v6`
- `v7`
- `stable`
- `ai`

统一竞争最低要求：

- 有成功 `signal_runs`。
- 有非空 `signal_items`。
- 有 `data_version`、`code_version`、`param_version`。
- 有通过的 `backtest_credibility`。
- 有通过的 `strategy_backtest_diagnostics.eligible_for_formal_ranking`。
- 对正式 top list，必须不在 observation pool。

## 5. Backtest 可信度门槛

每个候选 artifact 必须同时满足：

- `point_in_time_data=true`
- `suspension_and_limit_handling=true`
- `volume_constraint=true`
- `cost_model=true`
- `slippage_model=true`
- `in_sample_out_of_sample_split=true`
- `parameter_sensitivity=true`
- `failed_backtests_recorded=true`
- `metrics.signal_density > 0`
- `metrics.test_windows > 0`

质量线：

- `win_rate >= 0.45`
- `max_drawdown <= 0.25`
- `signal_density > 0`

不满足可信度门槛的策略只能进入诊断池；不满足质量线但可信度完整的策略只能进入 observation pool。

## 6. 开发任务分解

### 6.1 可信 artifact 过滤

目标：

- 防止失败 sweep、弱质量 sweep、不可正式排名 sweep 自动进入运行默认。

落点：

- `strategies/center_config.py`
- `openclaw/config/strategy_center.yaml`
- `tests/test_center_config.py`

要求：

- `find_latest_backtest_best()` 跳过不可排名 artifact。
- 自动应用 best 参数时，`score_threshold` 不得低于 registry/default/center floor。
- CLI 显式覆盖可以保留，但必须在审计输出中标明 `source=cli_override`。

验收：

- `pytest tests/test_center_config.py`

### 6.2 策略 backtest 诊断

目标：

- 所有失败策略必须能解释失败原因。

落点：

- `openclaw/services/strategy_backtest_diagnostic_service.py`
- `openclaw/research/backtest_param_sweep.py`
- `tests/test_strategy_backtest_diagnostic_service.py`
- `tests/test_backtest_param_sweep.py`

必须诊断：

- `no_successful_rolling_test_window`
- `zero_signal_density`
- `missing_execution_constraint_evidence`
- `missing_cost_or_slippage_evidence`
- `weak_out_of_sample_win_rate`
- `drawdown_above_quality_floor`
- `combo_component_consensus_diagnostic_required`
- `factor_score_distribution_diagnostic_required`
- `backtest_handler_missing_or_not_credible`

验收：

- 每个 sweep artifact 写入 `strategy_backtest_diagnostics`。
- 失败策略也必须有 `failure_classes` 和 `next_actions`。

### 6.3 生产策略优化

目标：

- 优化生产策略的稳定性，不追求短期漂亮收益。

策略要求：

- `v5`：保持主执行策略基线，禁止因失败 sweep 降阈值；重点降低回撤和假信号。
- `v8`：冻结 `advanced_score / pre_market_score / market_penalty / risk_penalty` 分布；识别是否因风控过强导致信号塌缩。
- `v9`：按 market regime 分桶输出样本外表现；不能只靠单窗口胜率。
- `combo`：输出组件通过率、pair agreement、weighted consensus gap；判断失败来自组件弱、权重错配还是共识门槛。

落点：

- `openclaw/runtime/v49_handlers.py`
- `openclaw/runtime/v8_signal_evaluator.py`
- `openclaw/runtime/combo_signal_evaluator.py`
- `tests/test_v49_handlers_normalize.py`
- `tests/test_v8_signal_evaluator.py`
- `tests/test_combo_signal_evaluator.py`

验收：

- `pytest tests/test_v49_handlers_normalize.py tests/test_v8_signal_evaluator.py tests/test_combo_signal_evaluator.py`

### 6.4 实验策略优化

目标：

- 实验策略和生产策略使用同一套可信证据门槛，但不自动进入默认生产路径。

策略要求：

- `v6`：必须使用 point-in-time context；输出因子分布和阈值附近分布。
- `v7`：补齐失败诊断；不得因历史定位进入生产候选。
- `stable`：只作为防御候选，不得用低回撤包装为生产成熟。
- `ai`：未有真实 runtime backtest handler 前，不得进入统一正式排名。

落点：

- `openclaw/runtime/v6_backtest_context.py`
- `openclaw/runtime/v49_handlers.py`
- `openclaw/services/experiment_governance_service.py`
- `tests/test_v6_backtest_context.py`
- `tests/test_experiment_governance_service.py`

验收：

- `pytest tests/test_v6_backtest_context.py tests/test_experiment_governance_service.py`

### 6.5 统一推荐输出

目标：

- 统一竞争已经成立后，正式输出必须只来自合格策略。

落点：

- `openclaw/services/unified_strategy_recommendation_service.py`
- `tests/test_unified_strategy_recommendation_service.py`

要求：

- 实验策略可参与评分，但 `strategy_tier` 只作信息标签。
- `eligible_for_daily_top3=false` 的策略不得进入 `top_strategies`。
- `top_stocks` 只聚合合格策略贡献。
- backtest 可信但质量线失败时，必须阻断正式排名。

验收：

- `pytest tests/test_unified_strategy_recommendation_service.py`

### 6.6 执行证据反向校验

目标：

- 策略升级不能止于 backtest，必须进入 shadow/canary 或准真实执行证据复盘。

落点：

- `openclaw/services/execution_evidence_service.py`
- `openclaw/services/broker_execution_report_service.py`
- `tests/test_execution_evidence_service.py`
- `tests/test_broker_execution_report_service.py`

最小样本类型：

- `filled`
- `partial_fill`
- `cancelled`
- `expired`
- `manual_override`
- `high_slippage`
- `decision_deviation`

验收：

- 执行证据必须能追到 `decision_id -> based_on_run_id -> signal_run`。
- 不能只提供 `linked_decision_ids` 作为实验策略晋级证据。

### 6.7 阶段验收裁决

目标：

- 把第 11 节验收清单变成可执行、可测试、只读的事实审计。

落点：

- `openclaw/services/strategy_optimization_stage_service.py`
- `openclaw/services/rejected_backtest_artifact_ledger_service.py`
- `tools/strategy_optimization_stage_audit.py`
- `tools/rejected_backtest_artifacts.py`
- `tools/governance_gate.py`
- `tests/test_strategy_optimization_stage_service.py`
- `tests/test_strategy_optimization_stage_audit_tool.py`
- `tests/test_rejected_backtest_artifact_ledger_service.py`
- `tests/test_governance_gate_strategy_optimization.py`

要求：

- 只能读取事实链和统一推荐结果，不得生成榜单或修改策略状态。
- 必须阻断 observation pool 策略进入正式 top list。
- 必须阻断缺执行证据的 `promote_candidate` / `experiment_promote_candidate`。
- 必须记录已拒绝 artifact 的拒绝原因，并阻断其作为 runtime default 复用。
- 被拒 artifact 必须进入 `logs/openclaw/rejected_backtest_artifacts.jsonl`，字段至少包含 `artifact_path`、`strategy`、`reason`、`reused_as_runtime_default`。
- 必须生成 JSON/Markdown 审计产物，供本地和 CI 留档。
- 治理门必须把本阶段文档和审计工具列为主线必备文件；显式启用时必须运行阶段审计。

治理门显式启用方式：

```bash
AIRIVO_ENABLE_STRATEGY_OPTIMIZATION_STAGE_GATE=1 \
AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH=/path/to/fact.db \
AIRIVO_REJECTED_BACKTEST_ARTIFACTS_FILE=logs/openclaw/rejected_backtest_artifacts.jsonl \
python tools/governance_gate.py
```

记录 rejected artifact：

```bash
python tools/rejected_backtest_artifacts.py record \
  --artifact-path logs/openclaw/backtest_sweep_v8_failed.json \
  --strategy v8 \
  --reason eligible_for_formal_ranking_false \
  --operator-name reviewer
```

验收：

- `pytest tests/test_strategy_optimization_stage_service.py tests/test_strategy_optimization_stage_audit_tool.py tests/test_rejected_backtest_artifact_ledger_service.py tests/test_governance_gate_strategy_optimization.py`

## 7. 工作流

标准开发流：

1. 注册策略优化假设。
2. 固定数据窗口、代码版本、参数网格。
3. 运行 backtest sweep。
4. 生成 backtest credibility。
5. 生成 strategy diagnostics。
6. 过滤不可排名 artifact。
7. 进入 observation pool。
8. 选择少量候选进入 shadow/canary。
9. 生成 execution evidence。
10. 写入 decision event：`promote_candidate`、`continue_observe`、`reject` 或 `degrade`。

禁止跳步：

- 不得从第 3 步直接到生产默认参数。
- 不得从第 7 步直接生成正式榜。
- 不得没有第 9 步就宣称执行闭环有效。

## 8. 产物要求

每次策略优化必须留下：

- sweep JSON
- sweep CSV
- sweep Markdown
- `backtest_credibility`
- `strategy_backtest_diagnostics`
- 参数来源说明
- 失败窗口诊断
- 决策事件，若发生晋级/拒绝/降级
- 执行证据摘要，若进入 shadow/canary

## 9. 测试门禁

最小局部测试：

```bash
pytest tests/test_center_config.py \
  tests/test_backtest_credibility_service.py \
  tests/test_strategy_backtest_diagnostic_service.py \
  tests/test_backtest_param_sweep.py \
  tests/test_unified_strategy_recommendation_service.py \
  tests/test_strategy_optimization_stage_service.py \
  tests/test_strategy_optimization_stage_audit_tool.py \
  tests/test_rejected_backtest_artifact_ledger_service.py \
  tests/test_governance_gate_strategy_optimization.py
```

策略运行诊断测试：

```bash
pytest tests/test_backtest_engine.py \
  tests/test_combo_signal_evaluator.py \
  tests/test_v6_backtest_context.py \
  tests/test_v8_signal_evaluator.py \
  tests/test_v49_handlers_normalize.py \
  tests/test_v49_adapter.py
```

执行归因卫生审计（dry-run，发现缺口即阻断）：

```bash
python tools/backfill_execution_attribution.py \
  --db-path /path/to/fact.db \
  --statuses created,submitted \
  --stale-minutes 30 \
  --max-orders 500
```

阶段收口测试：

```bash
pytest
```

验收规则：

- 局部测试必须过。
- 全量测试必须可收集。
- 若全量测试失败，必须证明失败与本阶段无关，并记录原因；否则不得收口。

## 10. 角色与裁决

系统允许的裁决结果：

- `promote_candidate`：进入生产候选，但不等于生产默认。
- `continue_observe`：留在 observation pool。
- `reject`：拒绝本轮优化。
- `degrade`：生产候选降级或暂停。

裁决必须写入 `decision_events`，并引用：

- `run_id`
- artifact path
- backtest credibility
- strategy diagnostics
- execution evidence，若存在

## 11. 阶段验收清单

本阶段结束必须能回答：

- 哪些策略进入统一竞争，依据是什么？
- 哪些策略被拒绝，失败原因是什么？
- 哪些参数来自可信 artifact？
- 哪些 artifact 被拒绝自动应用，原因是什么？
- 实验策略是否与生产策略用了同一套 backtest 门槛？
- observation pool 是否没有生成正式榜？
- 是否存在任何通过降阈值、补标签、页面描述包装成熟度的路径？
- 执行证据是否能反向追到信号和决策？

若任一问题答不清楚，本阶段不得宣布完成。

## 12. 行业标杆级竞争与 Top5 组合审计

统一推荐结果只能作为候选输入，不等于正式 Top5 或生产买入清单。进入行业标杆级竞争必须额外产出
`strategy_competition_portfolio_audit.v1`：

- 每个 alpha 必须有 `model_card`、`hypothesis`、`rule_hash`、`data_hash`、`code_hash`。
- 每次竞争必须提供固定候选池，并生成 `ranking_method_hash`；不得赛后补入候选。
- `failed`、`research_only`、`archived`、`diagnostic`、`observation` 状态不得进入正式 Top5。
- 每个 Top5 股票必须解释来源策略、信号引用、组合权重、行业/流动性风险、成本估计和约束检查。
- 每个晋级动作必须由 `independent_validator` 独立批准，并登记 reviewed artifacts。
- 生产前必须具备 shadow execution 和 pre-trade risk controls。

落地文件：

- 服务：`openclaw/services/strategy_competition_audit_service.py`
- CLI：`tools/strategy_competition_portfolio_audit.py`
- 当前事实审计生成器：`tools/build_current_strategy_competition_audit.py`
- DB：`scripts/migrations/006_strategy_competition_audit.sql`
- Gate env：`AIRIVO_COMPETITION_AUDIT_FILE`

硬阻断规则：

- 没有 competition audit artifact，不得把历史 `unified_recommendation.top_stocks` 宣称为正式 Top5。
- 没有 independent validation，不得晋级。
- 没有 shadow execution 或 pre-trade controls，不得进入 production。
- 失败研究候选不得通过 UI、allocator、现金回退、throttle 或解释文案进入正式结果。

当前事实审计生成器允许从真实 DB 的 unified recommendation 取固定候选池并生成审计 artifact。
如果没有真实 independent validation、shadow execution 或 pre-trade controls 输入，生成器必须写入 blocked
stub，产物不得通过。

可使用 `--derive-pre-trade-risk-controls` 从当前 Top5 与 DB 行情事实派生交易前风控证据。该证据只覆盖单票权重、
行业权重、流动性、涨跌停代理、停牌代理、换手预算和订单金额限制；即使通过，也不能替代 independent validation
或 shadow execution。

可使用 `tools/build_strategy_competition_shadow_execution_plan.py` 从已通过 pre-trade 的 competition audit 生成
shadow execution plan。该工具只创建 shadow plan run、decision event 和待执行 shadow orders；不会写入成交、
不会写入 attribution，也不会让 shadow execution 通过。只有后续真实 shadow 回报、成交/未成交原因和执行归因完整，
才能把 shadow execution 从 blocked 推进为 passed。

可使用 `tools/record_strategy_competition_shadow_feedback.py` 录入 shadow order 的真实回报并生成
`strategy_competition_shadow_execution_evidence.v1`。反馈必须引用既有 shadow plan order；filled/partial_fill
必须带 fills，cancelled/rejected/expired/manual_override 必须带 miss_reason，manual_override 必须带人工原因，
每笔反馈必须带 close_price/reference price 用于 attribution。未提供反馈时只能生成 blocked evidence；即使
shadow evidence passed，也只代表 shadow execution 证据完整，仍不能替代 independent validation，也不能单独打开
formal Top5 / production。

可使用 `tools/build_strategy_competition_independent_validation.py` 对外部独立验证人报告做系统级复核。该工具不允许
系统自批，验证人报告必须包含 `validator_name`、`validator_role=independent_validator`、利益冲突声明、已审
artifact 清单、完整 review scope 和结论摘要；系统还会反查 competition audit 中的 fixed pool、model cards、
Top5 explainability、shadow execution、pre-trade controls 和 promotion boundaries。上游 shadow 或 pre-trade
未通过时，即使外部报告写 approved，系统产物也必须是 blocked。

可使用 `tools/build_strategy_competition_production_readiness.py` 生成最终发布前 readiness artifact。该闸只接受
已经 `industry_benchmark_competition_passed` 的 competition audit，并额外要求 kill switch、rollback plan、
live monitoring、incident owner、单笔订单限额、单票仓位限额和人工批准声明。blocked readiness 不能释放实盘订单；
readiness passed 也只是发布前条件具备，仍保留人工最终批准要求。

可使用 `tools/build_strategy_competition_operational_controls.py` 先生成标准化运行控制包。该工具会校验 kill switch、
rollback、监控、负责人、订单限额、仓位限额和人工批准声明，并确保运行时限额不宽于 competition audit /
pre-trade 中的预声明约束。没有真实运行控制输入时只能生成 blocked controls，不能拿默认值补齐。

可使用 `tools/build_strategy_competition_evidence_intake_packet.py` 生成证据补齐包。该包汇总当前 audit、shadow、
independent validation、operational controls 和 production readiness 的缺口，并输出待填写的 shadow feedback、
independent validator decision、operational controls input 模板。intake packet 不是审批，不得作为 production
evidence 使用；它只用于把后续人工/系统补证动作格式化、可追踪化。生成器会同时落独立模板文件、README 和源
artifact SHA256，防止后续补证混用不同批次证据。

可使用 `tools/review_strategy_competition_evidence_submission.py` 对填报后的补证文件做 intake submission review。
该 review 只判断补证材料是否可进入正式验证工具：校验源 artifact hash 未漂移、shadow order 集合一致、独立验证
reviewed artifacts 对齐、运行控制限额不超过 packet 约束。submission review 通过不等于 shadow/independent/
operational/readiness 通过，更不能直接打开 production。治理门禁可通过
`AIRIVO_EVIDENCE_SUBMISSION_REVIEW_FILE` 独立校验 submission review artifact，blocked review 可作为非生产证据，
但任何 production eligibility 声明都会被拒绝。

可使用 `tools/build_strategy_competition_formal_validation_handoff.py` 在 submission review 通过后生成
`strategy_competition_formal_validation_handoff.v1`。该 handoff 只负责把同一批补证材料转换为固定顺序的正式验证
工作单：shadow evidence、independent validation、operational controls、competition audit rerun、production
readiness、release-chain adjudication。它记录 intake packet 与 submission review hash、每一步输入 hash 和命令
顺序；handoff ready 不等于任何正式验证已通过，也不得产生 production eligibility。治理门禁可通过
`AIRIVO_FORMAL_VALIDATION_HANDOFF_FILE` 独立校验 handoff artifact。

可使用 `tools/review_strategy_competition_formal_validation_results.py` 回收并复核 handoff 后的正式验证输出。该 review
要求 handoff 必须 ready，并按固定顺序核验 shadow evidence、independent validation、operational controls、
competition audit rerun、production readiness 和 release-chain adjudication 的 artifact version、passed 状态和
hash。任一步缺失或失败，后续步骤都不得视为有效推进。result review 通过也不是交易指令，不能替代最终人工发布批准；
治理门禁可通过 `AIRIVO_FORMAL_VALIDATION_RESULT_REVIEW_FILE` 独立校验。

可使用 `tools/build_strategy_competition_human_release_approval.py` 生成最终人工发布批准 artifact。该层只在 formal
result review accepted 且 release-chain adjudication passed-for-human-approval 后工作，并要求独立
`release_approver` 提交 approval decision、利益冲突声明、reviewed artifacts、approval ticket 和 summary。
blocked human approval 不得释放 live orders；approved human approval 是唯一能授予 `live_order_authority_granted`
的系统 artifact，但它不允许修改任何上游策略证据。治理门禁可通过 `AIRIVO_HUMAN_RELEASE_APPROVAL_FILE` 独立校验。

可使用 `tools/check_strategy_competition_live_order_authority.py` 在任何 broker/live submission 前执行实盘订单出口
硬闸。该工具只校验 `human_release_approved`、`live_order_authority_granted=true`、订单意图的 approval hash 与
competition run 是否匹配、订单数量和字段是否有效；它不创建订单、不提交订单。blocked authority check 不能进入
broker 层；即使 authority allowed，也仍需后续 broker adapter 与执行回报链路。治理门禁可通过
`AIRIVO_LIVE_ORDER_AUTHORITY_FILE` 独立校验。

可使用 `tools/check_strategy_competition_broker_submission_guard.py` 在 broker adapter 前执行提交守卫。该层要求
live order authority 已 allowed，broker submission intent 引用同一 authority hash，声明 broker adapter、
idempotency key 和 submission mode（`dry_run` 或 `controlled_submit`）。guard 不执行订单、不记录成交；任何
blocked guard 都不得调用 broker adapter。即使 guard passed，仍必须用单独的 broker response / execution
feedback 证据记录提交结果、成交、拒单、撤单和 attribution。治理门禁可通过
`AIRIVO_BROKER_SUBMISSION_GUARD_FILE` 独立校验。

可使用 `tools/review_strategy_competition_broker_submission_response.py` 复核 broker adapter 的提交回报。该层要求
broker guard 已 passed，response 引用同一 guard hash、idempotency key、broker adapter 和订单集合；可记录
dry-run ack、submitted、accepted、rejected、adapter error，但不得包含 fills，也不得把 submission confirmed
包装成 filled。成交、拒单后的执行归因和费用滑点必须进入独立 broker execution report / execution feedback
链路。治理门禁可通过 `AIRIVO_BROKER_SUBMISSION_RESPONSE_FILE` 独立校验。

可使用 `tools/review_strategy_competition_broker_execution_feedback.py` 复核最终 broker execution feedback。该层要求
broker submission response accepted，feedback 引用同一 response hash、idempotency key 和订单集合；filled /
partial_fill 必须有 fills、费用、滑点和 close/reference price，rejected/cancelled/expired/manual_override 必须有
miss reason，所有订单必须有 execution attribution。`submitted` 或 `accepted` 仍不是终态，不得标记执行完成。
治理门禁可通过 `AIRIVO_BROKER_EXECUTION_FEEDBACK_FILE` 独立校验。

可使用 `tools/reconcile_strategy_competition_post_trade.py` 做 post-trade reconciliation。该层要求 broker execution
feedback accepted，并对现金差异、仓位数量差异、成本/滑点偏差、异常 owner 与 resolution plan、operations
signoff 做统一复核。execution feedback complete 只表示订单终态反馈完整，不等于组合现金/仓位已经对账完成；只有
post-trade reconciliation passed 才能把本次交易生命周期标记为 complete。治理门禁可通过
`AIRIVO_POST_TRADE_RECONCILIATION_FILE` 独立校验。

可使用 `tools/adjudicate_strategy_competition_trade_lifecycle.py` 生成 post-release trade lifecycle 的 court-of-record。
该层串联 human release approval、live order authority、broker submission guard、broker submission response、
broker execution feedback 和 post-trade reconciliation，输出当前阻断阶段、root blockers 和允许下一步。它不创建
新的交易许可；blocked lifecycle stage 不能被下游 artifact 包装跳过。治理门禁可通过
`AIRIVO_TRADE_LIFECYCLE_ADJUDICATION_FILE` 独立校验。

可使用 `tools/build_strategy_competition_evidence_chain_manifest.py` 生成全链路 evidence-chain manifest。该层把
competition audit、evidence intake、submission review、formal handoff、formal result review、release-chain
adjudication、human release、live order authority、broker guard/response、execution feedback、post-trade
reconciliation 和 trade lifecycle adjudication 汇总为一份证据库存，记录每个 artifact 的 hash、状态、阻断点、
缺失项和允许下一步。manifest 是库存与审计索引，不是审批；即使所有证据完整，也不得由 manifest 自身授予
production eligibility、production release authorization 或 live order authority。治理门禁可通过
`AIRIVO_EVIDENCE_CHAIN_MANIFEST_FILE` 独立校验，防止半成品或局部证据被包装成生产结论。

可使用 `tools/build_strategy_competition_evidence_remediation_work_order.py` 从 evidence-chain manifest 生成补证工单。
该层把每个未通过 artifact 拆成 owner role、required evidence、validator tool、blocking reasons 和 acceptance
rule，方便执行人员逐项补真实 shadow feedback、independent validator decision、operational controls、broker /
execution / post-trade 证据。工单不是验证通过；工单完成后仍必须重跑对应 validator、manifest 和 court-of-record。
治理门禁可通过 `AIRIVO_EVIDENCE_REMEDIATION_WORK_ORDER_FILE` 校验，禁止把工单、半成品或局部关闭项包装成
production / live order authority。

可使用 `tools/build_strategy_competition_remediation_closure_submission.py` 从补证工单和已完成的 validator artifact 生成
closure submission。该层只把 designated validator artifacts 按 work item 打包成统一提交，校验同一 work order hash、
同一 manifest hash 和每个 work item 是否真的 closed；即使 submission ready，也只是提交给 remediation closure
review，不是 closure review accepted，更不是 formal validation pass。治理门禁可通过
`AIRIVO_REMEDIATION_CLOSURE_SUBMISSION_FILE` 校验，防止半成品 submission 被包装成 closure review 或生产证据。

可使用 `tools/review_strategy_competition_remediation_closure.py` 审核补证工单关闭提交。该层校验 closure submission
是否引用同一 work order hash 与 manifest hash、每个 work item 是否由指定 validator tool 关闭、validator artifact
hash 是否匹配且 payload passed。closure review accepted 只表示可以按顺序重跑 formal validators、manifest 和
court-of-record；它不是 formal validation pass，也不能创建 production eligibility、release authorization 或 live
order authority。治理门禁可通过 `AIRIVO_REMEDIATION_CLOSURE_REVIEW_FILE` 独立校验。

可使用 `tools/build_strategy_competition_formal_rerun_plan.py` 在 closure review accepted 后生成固定顺序的 formal
rerun plan。该计划要求依次重跑 shadow execution evidence、independent validation、operational controls、
competition audit rerun、production readiness、release-chain adjudication、formal result review、evidence-chain
manifest 和 release-chain recheck；每一步输出必须收集且 passed 后才能进入下一步。formal rerun plan ready 不是
validator pass，不得授予 production 或 live order authority。治理门禁可通过 `AIRIVO_FORMAL_RERUN_PLAN_FILE` 校验。

可使用 `tools/build_strategy_competition_formal_rerun_output_submission.py` 从 formal rerun plan 和逐步输出 artifact
生成 rerun output submission。该层只把固定顺序的 step output 打包为统一提交，校验同一 rerun plan hash、固定
step 顺序、每个 output payload 是否 passed；它只是提交给 formal rerun result review，不是 result review accepted，
更不是 release approval。治理门禁可通过 `AIRIVO_FORMAL_RERUN_OUTPUT_SUBMISSION_FILE` 校验，防止局部 rerun 输出
被包装成正式审核通过或 production 证据。

可使用 `tools/review_strategy_competition_formal_rerun_results.py` 回收并审核 formal rerun 输出。该层要求 rerun plan
ready、submission 引用同一 rerun plan hash、所有 step output 按固定顺序存在、artifact hash 匹配且 payload
passed；任一步缺失或失败，后续步骤不得视为有效。formal rerun result accepted 仍不是 release approval，只允许
重建 evidence-chain manifest 和 court-of-record。治理门禁可通过 `AIRIVO_FORMAL_RERUN_RESULT_REVIEW_FILE` 校验。

可使用 `tools/build_strategy_competition_rerun_court_rebuild_submission.py` 从 accepted formal rerun result review、重建的
evidence-chain manifest 和 release-chain adjudication 生成 rerun court rebuild submission。该层只把 rebuilt manifest /
release-chain 及其 hash 统一打包，不授予 release approval 或 live authority；它只是提交给 rerun court rebuild review。
治理门禁可通过 `AIRIVO_RERUN_COURT_REBUILD_SUBMISSION_FILE` 校验，防止局部重建产物被包装成 court rebuild review accepted。

可使用 `tools/review_strategy_competition_rerun_court_rebuild.py` 审核 formal rerun 后的 court-of-record 重建结果。
该层要求 formal rerun result review accepted，并校验新 evidence-chain manifest 与 release-chain adjudication
均存在、hash 可追踪、引用同一 rerun result review hash，且不得在重建产物中直接声明 production / release /
live order authority。rerun court rebuild accepted 仍不是人工发布批准；后续仍必须走 release-chain adjudication 与
human release approval。治理门禁可通过 `AIRIVO_RERUN_COURT_REBUILD_REVIEW_FILE` 校验。

可使用 `tools/build_strategy_competition_post_rerun_release_readiness_submission.py` 先把 rerun 后发布就绪证据打包成
submission。它要求 rerun court rebuild review accepted、release-chain adjudication 已 passed-for-human-approval、
human release approval 已 approved，且三者都引用同一 court rebuild hash。submission 只负责封装证据，不授予
production release、不创建 live order authority、不提交 broker orders；治理门禁可通过
`AIRIVO_POST_RERUN_RELEASE_READINESS_SUBMISSION_FILE` 校验。

可使用 `tools/review_strategy_competition_post_rerun_release_readiness.py` 做 rerun 后发布就绪复核。该层要求
rerun court rebuild accepted、release-chain adjudication 已 passed-for-human-approval、human release approval 已
approved，且两者都引用同一 court rebuild hash。该层最多只允许进入 live order authority check；它本身不授予
production release、不创建 live order authority、不提交 broker orders。治理门禁可通过
`AIRIVO_POST_RERUN_RELEASE_READINESS_FILE` 校验。

可使用 `tools/build_strategy_competition_post_rerun_live_authority_submission.py` 先把 rerun 后 live authority
证据打包成 submission。它要求 post-rerun release readiness ready、live order authority allowed，且二者引用同一
readiness hash。submission 只负责封装证据，不授予 production release、不创建 live order authority、不提交
broker orders；治理门禁可通过 `AIRIVO_POST_RERUN_LIVE_AUTHORITY_SUBMISSION_FILE` 校验。

可使用 `tools/review_strategy_competition_post_rerun_live_authority.py` 复核 rerun 后 live authority 出口。该层要求
post-rerun release readiness ready、live order authority allowed，且 live authority artifact 引用同一 readiness hash；
它只允许进入 broker submission guard，不调用 broker adapter、不确认提交、不确认成交。治理门禁可通过
`AIRIVO_POST_RERUN_LIVE_AUTHORITY_REVIEW_FILE` 校验，防止把 authority ready 包装成 broker submission 或 execution。

可使用 `tools/build_strategy_competition_post_rerun_broker_guard_submission.py` 先把 rerun 后 broker guard
证据打包成 submission。它要求 post-rerun live authority review ready、broker submission guard passed，且二者引用
同一 live authority review hash。submission 只负责封装证据，不调用 broker adapter、不确认提交、不确认成交；
治理门禁可通过 `AIRIVO_POST_RERUN_BROKER_GUARD_SUBMISSION_FILE` 校验。

可使用 `tools/review_strategy_competition_post_rerun_broker_guard.py` 复核 rerun 后 broker guard 出口。该层要求
post-rerun live authority review ready、broker submission guard passed，且 guard artifact 引用同一
live authority review hash，并声明 broker adapter、idempotency key 和 submission mode。该 review 不调用 broker
adapter、不确认 submission、不确认 fills；通过后也只允许进入 broker adapter 调用并记录 broker submission
response，成交、费用、滑点和 post-trade reconciliation 仍必须走后续独立证据链。治理门禁可通过
`AIRIVO_POST_RERUN_BROKER_GUARD_REVIEW_FILE` 校验，防止把 guard ready 包装成 broker response、filled 或 trade
lifecycle complete。

可使用 `tools/review_strategy_competition_post_rerun_broker_response.py` 复核 rerun 后 broker response。该层要求
post-rerun broker guard review ready、broker submission response evidence accepted，且 response 引用同一
broker submission guard hash。该层可以确认 broker submission response 已被接受，但仍不得确认 fills、费用、滑点、
execution attribution 或 post-trade reconciliation；通过后只允许进入 broker execution feedback。治理门禁可通过
`AIRIVO_POST_RERUN_BROKER_RESPONSE_REVIEW_FILE` 校验，防止把 submitted / accepted 包装成 filled、reconciled 或
trade lifecycle complete。

可使用 `tools/review_strategy_competition_post_rerun_broker_execution_feedback.py` 复核 rerun 后 broker execution
feedback。该层要求 post-rerun broker response review ready、broker execution feedback accepted，且 feedback 引用
同一 response evidence hash；filled / partial_fill 必须包含 fills、费用、滑点和 close price，rejected / cancelled /
expired / manual_override 必须包含 miss reason，所有订单必须有 execution attribution。该层只允许进入 post-trade
reconciliation，不得把 execution feedback packed 成 trade complete。治理门禁可通过
`AIRIVO_POST_RERUN_BROKER_EXECUTION_FEEDBACK_REVIEW_FILE` 校验，防止把 submitted、accepted 或 execution feedback
完整性包装成 post-trade completion。

可使用 `tools/reconcile_strategy_competition_post_rerun_post_trade.py` 复核 rerun 后 post-trade reconciliation。该层要求
post-rerun broker execution feedback review ready、reconciliation input 引用同一 execution feedback review hash，
并对现金差异、仓位差异、成本/滑点偏差、异常 owner、resolution plan 和 operations signoff 做统一复核。该层只允许
进入 post-rerun trade lifecycle adjudication，不得把对账通过包装成新的交易权限。治理门禁可通过
`AIRIVO_POST_RERUN_POST_TRADE_RECONCILIATION_FILE` 校验。

可使用 `tools/adjudicate_strategy_competition_post_rerun_trade_lifecycle.py` 复核 rerun 后 trade lifecycle。
该层把 post-rerun broker guard review、post-rerun broker response review、post-rerun broker execution feedback
review 和 post-rerun post-trade reconciliation 串成 court-of-record，输出当前阻断阶段、root blockers 和允许下一步。
它不创建新交易许可，也不把局部通过包装成 trade lifecycle complete。治理门禁可通过
`AIRIVO_POST_RERUN_TRADE_LIFECYCLE_ADJUDICATION_FILE` 校验。

可使用 `tools/build_strategy_competition_post_rerun_evidence_chain_manifest.py` 生成 post-rerun evidence-chain manifest。
该层把 post-rerun release readiness、post-rerun live authority review、post-rerun broker guard review、
post-rerun broker response review、post-rerun broker execution feedback review、post-rerun post-trade reconciliation
和 post-rerun trade lifecycle adjudication 汇总成库存索引，记录每个 artifact 的 hash、状态、阻断点、缺失项和允许
下一步。它是 inventory，不是 approval；即使 complete，也不得由 manifest 自身授予 production eligibility、production
release authorization 或 live order authority。治理门禁可通过
`AIRIVO_POST_RERUN_EVIDENCE_CHAIN_MANIFEST_FILE` 校验。
对应的 remediation work order 会把每个 blocked post-rerun artifact 路由到明确 owner 和 validator；其中
`post_rerun_human_release_approval_review` 由 `human_release_approver` 负责，并使用
`tools/review_strategy_competition_post_rerun_human_release_approval_review.py` 作为指定 validator。

可使用 `tools/review_strategy_competition_post_rerun_human_release_approval_review.py` 复核 post-rerun human release
approval review。该层把 post-rerun evidence-chain manifest 和独立 human approval decision 串成最后一跳证据，
只验证人审是否独立、是否与当前 manifest 同批、是否包含 conflict attestation 和 reviewed_artifacts；即使
review 通过，也不得直接授予 live order authority、production release authorization 或任何 broker 权限。治理门禁可通过
`AIRIVO_POST_RERUN_HUMAN_RELEASE_APPROVAL_REVIEW_FILE` 校验。

可使用 `tools/adjudicate_strategy_competition_release_chain.py` 生成
`strategy_competition_release_chain_adjudication.v1`。该裁决层把 competition audit、shadow execution、
independent validation、operational controls、evidence submission review 和 production readiness 串成一个
固定顺序的发布证据链，输出当前阻断关口、root blockers、源 artifact hash 和允许的下一步动作。adjudication
不是交易指令；blocked gate 不能被跳过；模板、submission review 或局部通过证据不得包装成 production evidence。
治理门禁可通过 `AIRIVO_RELEASE_CHAIN_ADJUDICATION_FILE` 独立校验该裁决 artifact。

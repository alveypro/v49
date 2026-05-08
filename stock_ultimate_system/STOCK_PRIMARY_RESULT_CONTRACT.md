# STOCK_PRIMARY_RESULT_CONTRACT.md

## `/stock` 主结果契约

版本：v1.0  
状态：冻结版  
性质：核心业务结果契约文件  
适用范围：`airivo.online/stock` 主结果卡、主分析区、主结论表达层

---

## 1. 文档目的

本文件用于冻结 `/stock` 主结果卡的职责、边界与不变量，确保主结果长期稳定成为 Airivo 的核心竞争力中心。

---

## 2. 主结果卡定位

`/stock` 主结果卡是股票业务系统中的主结果表达组件。

它负责：

- 输出业务主判断
- 输出当前对象与主阶段
- 输出业务解释链中的必要核心信息
- 成为用户理解“当前结果是什么、为何如此”的第一入口

它不负责：

- 输出治理系统本体
- 承担平台入口职责
- 代替 `/T12` 输出治理摘要

---

## 3. 固定输出区块

`/stock` 主结果卡固定围绕以下区块组织：

1. 当前对象
2. 主阶段
3. 风险提示
4. 同步/来源说明
5. 历史验证与解释辅助
6. 禁用与失效解释

这些区块服务于业务主结果理解，不服务于治理系统承载。

---

## 4. 哪些内容属于业务结论

以下内容属于业务结论层：

- 当前对象是谁
- 当前结果处于什么主阶段
- 当前业务结果类型是什么
- 当前业务风险提示是什么
- 当前历史验证记录是什么

业务结论层的目标是帮助用户理解业务主判断，而不是治理裁决。

---

## 4.1 主结果生命周期工件

`/stock` 主结果阶段必须由事实工件驱动，不得由页面文案或人工口头判断推进。

当前执行规则：

- L2 candidate：来自 `candidates_top_latest.csv`、`daily_research_status_latest.json`、`buylist_latest.json`
- L3 audit：只能由 per-result `primary_result_audit_latest.json` 推进
- L4 execution：只能由 per-result `primary_result_execution_latest.json` 推进
- L4 rollback decision：来自 per-result `primary_result_rollback_latest.json`
- L4 observation：来自 per-result `primary_result_observation_latest.json`
- L5 archive：只能由显式 `primary_result_terminal_latest.json` 推进，其中 `success` 必须基于 completed observation
- 系统级 `governance_audit_latest.json` 可以表达失败或待审风险，但不能单独把主结果认定为已审核通过
- `primary_result_audit_latest.json`、`primary_result_execution_latest.json`、`primary_result_rollback_latest.json`、`primary_result_observation_latest.json`、`primary_result_terminal_latest.json` 必须绑定 `result_id` 或 `ts_code`，防止旧工件误套到新对象

这意味着 `/stock` 现在不再把“内容质量通过”等同于“审核通过”。内容质量只是生产候选门槛，L3 必须有独立审核工件。

L4 当前定义为本地执行协议 ready/blocked/running/completed/failed/cancelled，不代表已经接入券商、外部交易平台或真实下单系统。

L5 当前只能由显式终局记录触发，且必须提供 reason。系统不得为了消除缺失字段或提升完成度而自动伪造 success、archived 或其他终局结论。若终局为 success，必须先存在 `observation_status=completed`。

`observation_status=completed` 不允许空跑，必须至少包含：

- observation window start/end
- observed_return
- benchmark_return
- max_drawdown
- completion criteria

当前本地协议默认完成条件：

- `observed_return >= 0.0`
- `max_drawdown >= -0.08`

观察期指标可以由 `run_primary_result_observation_metrics.py` 从本地价格 CSV 自动计算。该脚本不接第三方行情或外部分析系统，只读取本地 `ts_code,trade_date,close` 结构数据，并生成：

- `primary_result_observation_metrics_latest.json`
- 更新后的 `primary_result_observation_latest.json`

本地价格 CSV 必须先经过 `PrimaryResultPriceHistoryIngest` 或 `PrimaryResultPriceHistorySqliteIngest` 导入为 canonical CSV，再生成 `PrimaryResultPriceHistoryArtifact` manifest。CSV ingest 接受本地 CSV，SQLite ingest 只读指定本地 SQLite 表；二者都只负责过滤目标 `ts_code` 与 benchmark 的观察窗口数据，记录 source/output 证据，不允许更新数据库、不抓取第三方行情、不改变 observation 状态。manifest 负责记录 CSV path、hash、必需列、observed/benchmark row counts 和可计算指标结果。关闭 observation 前必须支持 `PrimaryResultObservationClosurePreflight`。preflight 只读取当前主结果、open observation、window、price history CSV、price history manifest 和 benchmark code，判断是否具备关闭条件，以及关闭后是否会达到 terminal success 的前置条件。preflight 不写 observation、不记录 terminal、不登记 performance ledger。

本地指标计算至少覆盖：

- observed_return
- benchmark_return
- excess_return
- max_drawdown
- observed/benchmark price window summary
- blocking data quality checks

已关闭的 observation 必须进入 `PrimaryResultPerformanceLedger`，形成长期绩效账本。账本使用 append-only JSONL，不允许把仍处于 `observing` 的结果计入长期能力曲线。summary 只从已登记 entries 计算，不从当前页面状态推断。

绩效账本至少追踪：

- entry_total
- success_total
- failed_total
- success_rate
- average_observed_return
- average_excess_return
- worst_max_drawdown
- latest_entry_id

关闭后的 observation 还必须支持 `PrimaryResultFailureAttribution`。失败归因不修改 ledger 统计，只生成独立解释工件，用来回答失败或弱成功的主要原因。仍处于 `observing` 的结果不得做失败归因。

失败归因至少覆盖：

- data_quality_failure
- risk_control_failure
- benchmark_underperformance
- negative_absolute_return
- market_drag
- source_risk_mismatch
- weak_source_signal
- weak_success
- unclassified_failure

弱成功定义为 completion criteria 通过但 `excess_return` 未达到归因阈值。弱成功不得直接作为高置信样本进入后续模型学习或能力宣传。

失败归因之后必须支持 `PrimaryResultLearningFeedback`。learning feedback 只把归因转成可审核的改进输入，不得自动修改候选生成、风控阈值、执行规则或 baseline。所有 feedback 默认 `do_not_auto_apply=true`。

learning feedback 至少包含：

- affected_module
- recommendation
- severity
- requires_baseline_revalidation
- evidence_category
- do_not_auto_apply

任何涉及候选选择、风控、执行时点、市场过滤的建议，都必须标记 `requires_baseline_revalidation=true`，进入 benchmark 与 baseline policy 流程后才允许成为正式规则。

learning feedback 必须进入 `PrimaryResultFeedbackReviewQueue` 后才允许被纳入改进排期。review queue 维护当前 item 状态，同时使用 append-only decision history 记录每一次入队和决策。

review queue 状态至少包含：

- open
- accepted
- rejected
- needs_benchmark
- closed

队列决策不得改变 `do_not_auto_apply=true` 的安全边界。`accepted` 或 `needs_benchmark` 只表示进入评审或 benchmark 计划，不表示规则已应用。

状态为 `needs_benchmark` 的 review item 必须能够生成 `PrimaryResultBenchmarkPlan`。benchmark plan 是验证计划，不是变更执行。它必须记录：

- affected_modules
- recommended_changes
- required_tests
- expected_evidence_artifacts
- release_gates_required
- baseline_policy_required
- requires_baseline_revalidation
- do_not_auto_apply

benchmark plan 必须保存不可变 history snapshot，并用 `current.json` 只保存当前计划指针。只有完成 benchmark report、benchmark diff、release gates、release evidence bundle 和 baseline policy 后，才允许进入正式变更讨论。

benchmark plan 可以生成 `PrimaryResultBenchmarkPlanExecution`。execution evidence 只记录 plan 中 `required_tests` 的执行结果，不代表 release gates 已全部通过，不代表 baseline 已晋升，也不代表策略已变更。execution evidence 必须继续保持 `do_not_auto_apply=true`。

benchmark plan execution 至少包含：

- source_plan_hash
- command
- required_tests
- exit_code
- status
- stdout/stderr
- release_gates_required
- baseline_policy_required
- expected_evidence_artifacts
- do_not_auto_apply

benchmark plan execution 可以回写 `PrimaryResultFeedbackReviewQueue`，但只允许在 review item 当前状态为 `needs_benchmark` 时执行：

- execution `passed` -> review item `accepted`
- execution `failed` -> review item `rejected`

该回写必须追加 decision history，并记录 source execution hash。即使回写为 `accepted`，也仍不表示生产规则已应用；它只表示计划内测试已经通过，后续仍需 release gates、release evidence bundle 和 baseline policy。

状态为 `accepted` 的 review item 必须能够生成 `PrimaryResultReleaseEvidenceChecklist`。checklist 只判断发布和 baseline 讨论所需证据是否齐全，不执行上线、不晋升 baseline、不应用策略变更。它必须记录：

- benchmark_report
- benchmark_diff
- release_gates
- release_evidence_bundle
- manifest
- baseline_policy_decision
- missing_evidence
- blocking_gate_reason
- do_not_auto_apply

release evidence checklist 必须保存不可变 history snapshot，并用 `current.json` 只保存当前 checklist 指针。checklist 状态只允许表达 readiness：`complete` 表示证据齐全且未发现 blocking gate；`incomplete` 表示仍缺证据；`blocked` 表示 release gates 出现阻断信号。`complete` 仍不是上线命令，只是进入正式发布或 baseline promotion 讨论的最低证据条件。

状态为 `complete` 的 checklist 必须生成 `PrimaryResultReleaseDecision` 后才允许进入 baseline promotion。release decision 是人工/制度批准工件，不是自动发布动作。它必须记录：

- decision
- actor
- reason
- checklist_id
- source_checklist_hash
- release_pipeline_allowed
- baseline_promotion_allowed
- do_not_auto_apply

只有 `decision=approved`、`baseline_promotion_allowed=true`、且来源 checklist 为 `complete` 时，baseline promotion 才允许执行。`rejected` decision 可以用于留档阻断原因，但不得允许 promotion。release decision 必须保存不可变 history snapshot，并用 `current.json` 只保存当前 decision 指针。

baseline promotion 完成后，还必须支持 `PrimaryResultProductionReadiness`。production readiness 不是上线开关，而是最终证据账本，用于回答“是否具备生产就绪证据”。它必须至少校验：

- release decision 已 approved
- baseline current pointer 指向不可变 snapshot
- baseline snapshot 绑定 release decision hash
- terminal outcome 为 explicit success
- performance ledger 最新闭环结果为 success
- do_not_auto_apply

production readiness 必须保存不可变 history snapshot，并用 `current.json` 只保存当前 readiness 指针。状态只允许表达证据结论：`ready` 表示证据齐全且全部通过；`blocked` 表示仍存在阻断原因。`ready` 仍不自动部署、不自动交易、不自动修改策略。

在生成正式 production readiness 前，必须支持 `PrimaryResultProductionReadinessPreflight`。preflight 是诊断入口，只读取当前 release decision pointer、baseline current pointer、terminal artifact 与 performance ledger，输出缺失工件和阻断原因，不写 history，不改变 current pointer。

服务器运行时应优先使用 `run_primary_result_lifecycle.py` 统一推进 L3/L4 链路并生成 `primary_result_lifecycle_evidence_latest.json`。单步脚本保留为诊断工具，不应作为常规人工操作顺序。

通过后的 lifecycle evidence 必须进入 `PrimaryResultLifecycleRegistry`，形成不可变 history snapshot，并由 `artifacts/primary_result_lifecycle/current.json` 只保存当前指针。登记层负责拒绝 failed evidence、blocking failure、缺失 step hash、最终 payload 状态不达标等情况。rollback 只切换 current pointer，不覆盖旧 snapshot。

这意味着 `/stock` 主结果现在不只是“能展示当前状态”，而是具备了：

- 单次生命周期运行证据
- 不可变历史登记
- 当前结果指针
- 可回滚历史指针
- 审核、执行、回滚、观察四步的 hash 级追溯

---

## 5. 哪些内容属于解释层

以下内容属于解释层：

- 同步说明
- 历史来源说明
- 历史验证提示
- 禁用解释
- 失效解释

解释层的作用是帮助理解业务主结果，不改变业务事实本身。

---

## 6. 哪些内容必须留在 `/T12`

以下内容必须留在 `/T12`，不得进入 `/stock` 作为主结果组成部分：

- Governance Summary
- 治理只读摘要
- 制度状态总览
- 治理边界说明的主承载区
- 任何把治理系统本体带入 `/stock` 的模块

允许：

- `/stock` 继续保留必要的业务内解释字段

不允许：

- 把 `/T12` 视图、治理摘要区、治理只读系统挂入 `/stock`

---

## 7. 主结果契约不变量

### 7.1 单一主判断不变量

`/stock` 主结果必须始终是股票业务主判断的中心载体。

### 7.2 业务解释权不变量

`/stock` 拥有股票业务解释权，不能把解释权让渡给主站或 `/T12`。

### 7.3 治理边界不变量

治理信息可以被引用，但治理系统本体不得进入主结果卡。

### 7.4 结果优先不变量

主结果卡优先表达“结果是什么”，再表达“为什么”，最后表达“有什么边界”。

### 7.5 不跨层不变量

主结果卡不得承载：

- 平台母层身份表达
- `/T12` 治理摘要主表达
- 主站入口导航职责

---

## 8. 禁止事项

- 禁止把治理摘要接入 `/stock`
- 禁止把主站说明文案替代业务主结论
- 禁止让解释层反向定义统一事实层
- 禁止把主结果卡改成平台总览或治理总览

---

## 9. 为什么主结果必须成为核心竞争力中心

原因固定如下：

- 用户真正付费与长期留存，依赖的是业务主结果质量
- 主站只能放大认知，不能替代业务结果
- `/T12` 只能稳定边界，不能替代业务判断
- 行业前 3 的核心壁垒一定建立在主结果质量、解释稳定性和长期可信度上

因此：

- `/stock` 主结果卡不是普通 UI 模块
- 它是 Airivo 业务竞争力最集中的产品接口

---

## 10. 结论

`/stock` 主结果卡必须长期保持：

- 业务主判断中心
- 解释稳定中心
- 竞争力表达中心

任何改动都必须优先保护这三个中心地位。

# Airivo 当前阶段生产/实验策略开发与审计计划

版本：`v1.0`
日期：`2026-05-01`
状态：`当前阶段执行文档`
上位约束：`docs/AIRIVO_PROFESSIONAL_SYSTEM_BLUEPRINT.md`

## 1. 结论

当前系统不能宣称已经对标行业顶级生产策略系统。

更准确的判断是：

- 研究与信号侧已经进入专业化改造后期，具备可追溯、可审计基础。
- 生产策略侧已经具备事实链骨架，但尚未完成足够真实执行样本验证。
- 实验策略侧已有 `signal_runs` 基础，但尚未形成完整实验生命周期治理。
- 发布/回滚侧已经具备 dry-run、趋势观察和候选硬门规则，但尚未完成真实 release gate 演练。

因此，当前定位仍是：`准生产系统 -> 专业级系统过渡期`。

## 2. 不允许的表述

以下表述在本阶段禁止作为正式结论使用：

- `已达到顶级生产策略系统`
- `已完成专业级闭环`
- `release gate 已真实验证`
- `执行链已完成实盘闭环`
- `实验策略可直接晋级生产`

允许表述：

- `四条事实链已成形`
- `发布 dry-run 已进入 CI 观察期`
- `生产策略仍需执行事实链实证`
- `实验策略仍需实验治理和晋级规则`

## 3. 对标顶级系统的真实差距

### 3.1 信号事实链

当前状态：

- 扫描、回测、失败回测和部分核心评分口径已进入 `signal_runs`。
- v8/v9/combo 关键评分口径持续从页面/大类迁入 runtime/service。

差距：

- 参数实验尚未有完整实验注册与晋级制度。
- 样本外验证、参数敏感性和过拟合审计仍不足。

顶级系统要求：

- 每个信号必须有 `data_version`、`code_version`、`param_version`。
- 每次实验必须能追到假设、参数、样本、结果、失败原因和晋级裁决。

### 3.2 决策事实链

当前状态：

- `decision_events` 与 `decision_snapshot` 已成形。
- 决策可记录风险门、发布门、理由和操作人。

差距：

- 生产策略晋级、降级、灰度、回滚决策还缺统一生命周期裁决。
- 实验策略进入生产策略池的门槛尚未制度化。

顶级系统要求：

- 决策必须可重放。
- 生产策略资格必须来自事实链，不来自页面开关或自然语言判断。

### 3.3 执行事实链

当前状态：

- `execution_orders`、`execution_fills`、`execution_attribution` 已建立。
- 准发布样本已覆盖成交、未成交、撤单、部分成交、滑点异常、决策偏离。

差距：

- 真实或准真实订单样本不足。
- 滑点、未成交、人工覆盖、偏离归因尚未经过多轮真实数据复盘。
- TCA 仍处于结构具备阶段，不是稳定生产结论。

顶级系统要求：

- 执行结果必须能追到订单、成交、滑点、延迟、未成交原因和决策偏离。
- 生产策略质量必须由执行结果反向校验，而不是只看回测收益。

### 3.4 发布事实链

当前状态：

- release dry-run 已有 payload、fixture、负样本、趋势摘要和 CI artifact。
- `hard_gate_upgrade_candidates` 有候选规则。

差距：

- 完整 `release_gate.sh` 尚未真实执行。
- 回滚能力仍主要是记录与演练证据，不是多轮真实事故/回滚验证。

顶级系统要求：

- 发布前验证、发布动作、失败阻断、回滚依据和回滚验证都必须结构化。
- dry-run 只能作为进入真实 gate 前的观察证据，不能替代真实发布门。

## 4. 当前阶段开发主线

本阶段只允许围绕四条事实链推进。

优先级如下：

1. CI dry-run 观察期
2. 执行事实链实证化
3. 实验策略治理
4. 回测可信度审计
5. v49 主入口减债的低风险收口

不再优先推进：

- release 工具继续扩张
- 真实 `release_gate.sh` 硬门接入
- 页面新增
- 新策略页面包装
- 未经事实链证明的生产就绪宣传

## 5. 阶段一：CI dry-run 观察期

目标：

- 连续观察 2-3 轮 CI artifact。
- 确认 `hard_gate_upgrade_candidates` 是否稳定出现。
- 区分事实链缺口、环境缺失和工具误报。

必须审阅的产物：

- `artifacts/release_dry_run/current_readiness_payload.json`
- `artifacts/release_dry_run/readiness_trend.json`
- `artifacts/release_dry_run/readiness_trend.md`

进入真实 release gate 候选的必要条件：

- 同一 validation 最新连续 N 次失败。
- 失败原因不是环境缺失。
- 每次失败样本都有 rollback reference。
- 失败原因可解释、可复现、可修复。

禁止：

- 单次失败即接入硬门。
- 环境缺失被视为生产事实链失败。
- 无回滚引用的失败进入硬门候选。

真实发布硬门接入边界：

- `tools/release_gate.sh` 只允许通过显式环境变量开启事实链 readiness 硬门。
- 开启条件为 `AIRIVO_ENABLE_RELEASE_FACT_GATE=1` 且提供 `AIRIVO_RELEASE_DB_PATH`。
- 该 DB 必须是发布候选环境的只读事实库，不得使用 CI 自动生成的 fixture DB 伪装生产事实。
- 开启后必须去掉 `--non-blocking`，`allow_release_gate=false` 即在远端哈希、组合回归、健康检查之前阻断。
- 默认未开启时只保留观察与人工审计结论，不宣称生产发布已放行。

验收：

- 观察期报告能明确给出 `continue_observe`、`fix_fact_chain_gap` 或 `promote_to_release_gate_candidate`。

## 6. 阶段二：执行事实链实证化

目标：

- 用真实或准真实订单样本验证执行事实链。
- 把执行结果反向连接到决策和信号。

最小样本集：

- `filled`
- `partial_fill`
- `cancelled`
- `expired`
- `manual_override`
- `high_slippage`
- `decision_deviation`

每个样本必须具备：

- `decision_id`
- `order_id`
- `status`
- `fill_id`，若有成交
- `fill_ratio`
- `slippage_bp`
- `miss_reason_code`
- `broker_ref`
- `source_type`

验收：

- `professional_audit_service` 对执行链无缺失归因。
- 失败、撤单、未成交样本有结构化 `miss_reason_code`。
- 可以从执行结果追回 `decision_events.based_on_run_id`。

当前落点：

- `openclaw/services/execution_evidence_service.py`
- 执行证据摘要只读取 `execution_orders`、`execution_fills`、`execution_attribution`、`decision_events`、`signal_runs`
- 检查 `broker_ref`、`source_type`、成交记录、未成交原因和信号 lineage
- 输出 `status_counts`、`miss_reason_counts`、`linked_run_ids`、`cases`

裁决：

- 实验策略的 shadow/canary 样本不能只提供 `linked_decision_ids`。
- 晋级前必须通过执行证据摘要复盘。
- 执行证据服务不新增执行事实源，不替代执行表。

## 7. 阶段三：实验策略治理

目标：

- 区分生产策略与实验策略。
- 建立实验策略从研究到生产候选的晋级路径。

生产策略池保持：

- `v5`
- `v8`
- `v9`
- `combo`

研究策略池保持：

- `v4`
- `v6`
- `v7`
- `stable`
- `ai`

实验策略不得直接进入默认生产路径。

实验策略晋级至少需要：

- 绑定 `signal_runs`
- 明确 `param_version`
- 明确数据范围与样本外范围
- 有回测可信度审计
- 有执行链 shadow 或 canary 样本
- 有 decision 事件记录晋级/拒绝原因

本阶段不新增第二套实验事实表，优先使用 `signal_runs.summary_json` 和 `decision_events.decision_payload_json` 承载最小治理证据。只有当重复字段稳定后，才考虑独立 experiment registry。

当前落点：

- `openclaw/services/experiment_governance_service.py`
- 实验事实写入 `signal_runs`，`run_type=experiment`
- 实验假设、样本范围、样本外范围、回测可信度、shadow/canary 执行样本写入 `summary_json`
- 实验观察、拒绝、生产候选裁决写入 `decision_events`

裁决：

- 服务只判断实验策略是否具备“生产候选裁决”资格。
- 服务不直接修改 `strategies.registry`。
- 服务不把实验策略直接放入默认生产路径。
- 生产池变更必须另走决策事实链和发布事实链。
- `backtest_audit.passed=true` 不足以支持晋级，必须通过完整回测可信度审计字段。
- shadow/canary 执行样本必须给出 `linked_decision_ids`，不能只写样本数量。
- `linked_decision_ids` 必须能通过执行证据摘要复盘，不能指向空决策或缺失执行事实。

验收：

- 能回答某个实验策略为什么未进入生产。
- 能回答某个生产候选策略基于哪些 `run_id` 晋级。
- 能回答参数、数据、代码是否可复现。
- 能回答实验策略是否通过 PIT、停牌/涨跌停、成交量、成本、滑点、样本外、参数敏感性和失败回测落账检查。

## 8. 阶段四：回测可信度审计

目标：

- 不再只看收益率和胜率。
- 先判断回测是否可信，再讨论策略是否优秀。

必须审计：

- 数据是否 point-in-time。
- 是否处理停牌、涨跌停、成交量约束。
- 成本、手续费、滑点、冲击成本是否建模。
- 参数是否过拟合。
- 样本内、样本外是否分离。
- 是否有失败回测落账。

验收：

- 每个进入生产候选的策略必须有回测可信度摘要。
- 回测可信度不足时，不允许用收益率作为生产晋级依据。

当前落点：

- `openclaw/services/backtest_credibility_service.py`
- 回测可信度证据读取 `signal_runs.summary_json.backtest_credibility`
- 实验策略可复用 `summary_json.backtest_audit`

最小可信度字段：

- `point_in_time_data`
- `suspension_and_limit_handling`
- `volume_constraint`
- `cost_model`
- `slippage_model`
- `in_sample_out_of_sample_split`
- `parameter_sensitivity`
- `failed_backtests_recorded`

裁决：

- 回测可信度服务只判断证据是否足够可信。
- 不使用收益率或胜率单独作为生产晋级依据。
- 不直接修改生产策略池。

## 9. 阶段五：v49 主入口减债

目标：

- 保持 `v49_app.py` 继续退化为 UI 壳。

当前判断：

- `v49_app.py` 仍超过 10000 行，治理门高风险提示正确。
- 本阶段不以行数下降作为第一目标。
- 只有当改动不打断发布/回滚观察期时，才做低风险减债。

允许：

- 迁出依赖装配 helper。
- 收口入口分发薄壳。
- 删除不再使用的兼容路径。

禁止：

- 页面层重新定义事实。
- 为减行数机械搬大块逻辑。
- 在页面里新增生产判断。

## 10. 开发准入规则

任何新开发必须回答：

1. 属于哪条事实链？
2. 权威事实源是哪张表或哪个服务？
3. 是否影响生产策略、实验策略或发布路径？
4. 如何审计？
5. 如何回滚？
6. 是否引入第二口径？

答不清楚，不进入开发。

## 11. 测试与验证要求

最小验证组合：

- 相关单测
- `python -m py_compile`，覆盖被改 Python 文件
- `python tools/governance_gate.py --all-files`

涉及发布路径时：

- 只运行 dry-run 预检，除非明确批准，不运行完整 `release_gate.sh`。

涉及执行事实链时：

- 必须覆盖成功、失败、撤单、未成交或偏差样本中的至少一种。

涉及实验策略时：

- 必须能追溯 `run_id`、`param_version`、`decision_id` 或明确说明为何仍处观察期。

## 12. 当前阶段退出条件

满足以下条件后，才允许讨论“进入专业级生产闭环”：

- CI dry-run 观察期完成，并形成趋势结论。
- 执行事实链有多类真实或准真实样本。
- 实验策略有明确晋级/拒绝证据。
- 回测可信度审计能解释主要生产候选策略。
- 至少一次非真实发布演练报告可复盘。
- 如需真实 release gate，必须先有稳定硬门候选证据。

在这些条件完成前，系统仍按 `准生产系统` 管理。

# 运维入口地图与补跑分级说明

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 的运维入口地图与补跑分级。

本文档只回答四件事：

- 生产运维有哪些正式入口
- 每个入口属于什么动作类型
- 哪些入口只能读，哪些能重算，哪些能改 latest，哪些能改主链
- 补跑与恢复动作在什么边界内允许执行

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. 本文档

本文档不是部署教程，不是脚本索引全表，不是值班手册。

---

## 2. 总原则

### 2.1 入口必须分级

后续所有生产入口都必须归入以下四级之一：

- `R0 只读检查`
- `R1 重算但不改 pointer`
- `R2 重建 latest / registry 辅助产物`
- `R3 正式主链改写`

未分级入口，默认不得执行。

### 2.2 主链改写权极窄

只有正式主链入口允许改：

- `current_result_pointer`
- 对应 `result_registry current`
- 对应 `run_registry current`

默认允许改主链的入口只有：

- [run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_lifecycle.py:1)

### 2.3 补跑不等于放宽纪律

补跑、重算、重建 latest、部署恢复，都必须继续服从：

- fail-closed
- guard
- 证据留痕
- 前后状态验收

### 2.4 部署链与主结果链分离

部署相关入口只处理：

- 服务态
- nginx
- systemd
- deploy registry

部署入口不应直接替代：

- 主结果 lifecycle
- 主结果 pointer 裁决
- release gate 裁决

---

## 3. 分级定义

## 3.1 R0 只读检查

定义：

- 允许读取、审计、报告
- 不允许写 production latest
- 不允许写 pointer
- 不允许写 current registry

典型用途：

- 健康检查
- 污染检查
- 指针一致性检查
- 发布门禁检查

## 3.2 R1 重算但不改 pointer

定义：

- 允许重算分析结果或验收结果
- 允许写临时/派生输出
- 不允许改 `current_result_pointer`
- 不允许改主结果 current identity

典型用途：

- 重算报告
- 重算证据摘要
- 重算评分或 readiness

## 3.3 R2 重建 latest / registry 辅助产物

定义：

- 允许重建 latest、feedback、evidence、scoreboard、gate 这类辅助产物
- 仍不允许直接改主结果 pointer
- 必须有污染审计、重建留痕与二次验收

典型用途：

- latest 污染清洗
- evidence 重建
- handoff gate 重建
- feedback queue / scoreboard 刷新

## 3.4 R3 正式主链改写

定义：

- 允许正式写入主结果主链身份
- 允许更新 `current_result_pointer`
- 允许形成新的主结果 current
- 必须满足最严格的产物链、guard 和验收纪律

典型用途：

- 正常 lifecycle 主链推进

---

## 4. 正式入口地图

## 4.1 R0 只读检查入口

以下入口属于 `R0`：

- [check_current_result_pointer_integrity.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/check_current_result_pointer_integrity.py:1)
  - 用途：检查 pointer/result/run/artifact 一致性
  - 禁止：修复 pointer

- [check_release_gates.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/check_release_gates.py:1)
  - 用途：做 release gate 检查
  - 禁止：绕过 gate 直接判定通过

- [inspect_artifact_source_pollution.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/inspect_artifact_source_pollution.py:1)
  - 用途：审计 pytest/tmp 污染
  - 禁止：直接替代重建动作

- [run_server_post_deploy_verification.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_post_deploy_verification.py:1)
  - 用途：部署后验收
  - 禁止：当成部署动作本身

- `run_server_domain_preflight.py`
- `run_server_sync_preflight.py`

### R0 共同规则

- 允许失败并留痕
- 不允许失败后自动修复 production 状态
- 所有结论只能作为后续动作依据

---

## 4.2 R1 重算但不改 pointer 入口

以下入口属于 `R1`：

- [build_primary_result_performance_evidence.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/build_primary_result_performance_evidence.py:1)
- [build_primary_result_promotion_readiness_gate.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/build_primary_result_promotion_readiness_gate.py:1)
- [build_primary_result_daily_operations_scoreboard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/build_primary_result_daily_operations_scoreboard.py:1)
- [build_primary_result_competitive_gap_assessment.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/build_primary_result_competitive_gap_assessment.py:1)
- [run_primary_result_failure_attribution.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_failure_attribution.py:1)
- [run_primary_result_learning_feedback.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_learning_feedback.py:1)
- [run_primary_result_feedback_loop.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_feedback_loop.py:1)

### R1 共同规则

- 允许生成派生证据
- 不允许更新 `current_result_pointer`
- 不允许自行宣布主结果变更

---

## 4.3 R2 重建 latest / registry 辅助产物入口

以下入口属于 `R2`：

- [run_stock_entry_guard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_stock_entry_guard.py:1)
  - 允许：刷新 `stock_entry_guard_latest.json`
  - 禁止：决定新主结果对象

- [refresh_primary_result_operations_artifacts.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/refresh_primary_result_operations_artifacts.py:1)
  - 允许：刷新 handoff gate / performance evidence / promotion readiness / scoreboard 等 operations artifacts
  - 禁止：交易、handoff、baseline promote、部署

- [run_primary_result_candidate_basket_observation.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_candidate_basket_observation.py:1)
  - 允许：刷新 candidate basket observation / performance 产物
  - 禁止：改主结果 pointer

- [build_primary_result_candidate_handoff_gate.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/build_primary_result_candidate_handoff_gate.py:1)
  - 允许：重建 handoff gate latest
  - 禁止：执行 handoff

- `run_current_primary_result_daily_closure.py`
- `run_current_candidate_basket_observation.py`

### R2 共同规则

- 允许重建 latest
- 允许重建辅助 evidence
- 不允许直接改主结果 current pointer
- 结束后必须补二次验收

---

## 4.4 R3 正式主链改写入口

以下入口属于 `R3`：

- [run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_lifecycle.py:1)
  - 允许：
    - 运行 audit / execution / rollback / observation / terminal
    - 写 registry
    - 在通过条件满足时更新 `current_result_pointer`
  - 禁止：
    - 缺 evidence 时强行写 current

补充说明：

- 目前只有这一条正式主链入口被授权改 `current_result_pointer`
- 其他脚本即使能重建 latest，也不能被视为新的主结果裁决入口

---

## 4.5 部署链入口

以下入口不属于 `R3 主结果改写`，而属于 `部署链`：

- [build_server_activation_plan.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/build_server_activation_plan.py:1)
- `run_server_activation_plan.py`
- [run_server_post_deploy_verification.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_post_deploy_verification.py:1)
- `build_server_deploy_evidence_bundle.py`
- `register_server_deploy.py`
- [run_server_deploy_rollback.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_deploy_rollback.py:1)

说明：

- 这条链可以改 deploy registry current
- 这条链不可以替代主结果主链 current

---

## 5. service / timer 对应地图

以下 systemd 资产属于正式生产入口：

### 5.1 服务态入口

- `stock-ultimate-main-site.service`
- `stock-ultimate-dashboard.service`
- `stock-ultimate-t12.service`

### 5.2 数据更新入口

- `stock-ultimate-update.service`
- `stock-ultimate-update.timer`

### 5.3 研究入口

- `stock-ultimate-daily-research.service`
- `stock-ultimate-daily-research.timer`
- `stock-ultimate-nightly-research.service`
- `stock-ultimate-nightly-research.timer`
- `stock-ultimate-weekly-long.service`
- `stock-ultimate-weekly-long.timer`

### 5.4 主链守卫入口

- `stock-ultimate-entry-guard.service`
- `stock-ultimate-entry-guard.timer`

### 5.5 健康检查入口

- `stock-ultimate-healthcheck.service`
- `stock-ultimate-healthcheck.timer`

### 5.6 apex 生产链入口

- `airivo-apex-entry-guard.service`
- `airivo-apex-entry-guard.timer`
- `airivo-apex-primary-result-daily-closure.service`
- `airivo-apex-primary-result-daily-closure.timer`
- `airivo-apex-candidate-basket-observation.service`
- `airivo-apex-candidate-basket-observation.timer`
- `airivo-apex-candidate-handoff-gate.service`
- `airivo-apex-candidate-handoff-gate.timer`
- `airivo-apex-performance-evidence.service`
- `airivo-apex-performance-evidence.timer`
- `airivo-apex-primary-result-feedback-loop.service`
- `airivo-apex-primary-result-feedback-loop.timer`
- `airivo-apex-daily-operations-scoreboard.service`
- `airivo-apex-daily-operations-scoreboard.timer`
- `airivo-apex-promotion-readiness-gate.service`
- `airivo-apex-promotion-readiness-gate.timer`

说明：

- 这些 timer/service 的正式职责，后续必须与脚本入口逐一绑定
- 不允许存在“已经上线但职责未归类”的 service/timer

---

## 6. 补跑分级说明

## 6.1 A 类：只读核查

允许动作：

- 看 pointer
- 看 registry
- 看 guard
- 看 deploy verification
- 看污染审计

不允许动作：

- 写 latest
- 写 current
- 手动纠错

适用入口：

- R0 全部入口

## 6.2 B 类：重算派生结果

允许动作：

- 重算 evidence
- 重算 scoreboard
- 重算 attribution / feedback

不允许动作：

- 改 `current_result_pointer`
- 改主结果 current identity

适用入口：

- R1 全部入口

## 6.3 C 类：重建 latest / 辅助产物

允许动作：

- 清洗并重建 latest
- 刷新 candidate basket observation
- 刷新 handoff gate / performance evidence / promotion readiness

不允许动作：

- 直接把 rebuilt latest 当作新主结果
- 绕过二次污染审计

适用入口：

- R2 全部入口

## 6.4 D 类：正式主链重写

允许动作：

- 通过 lifecycle 正式改写主结果 current

不允许动作：

- 手工写 current
- 通过其他入口旁路改 pointer

适用入口：

- R3 正式主链入口

## 6.5 E 类：服务态回滚

允许动作：

- rollback activation
- post-rollback verification
- 更新 deploy registry current

不允许动作：

- 用服务回滚替代主结果回滚
- 不验证直接改 deploy current

适用入口：

- 服务器回滚链

---

## 7. 高风险误用场景

以下场景必须视为误用：

### 7.1 用 latest rebuild 替代 lifecycle

错误原因：

- latest rebuild 只能修复辅助产物，不具备主结果裁决权

### 7.2 用 entry guard 刷新替代主链推进

错误原因：

- guard 只能验证，不负责生成新主结果

### 7.3 用 deploy 成功替代主链成功

错误原因：

- deploy 只代表服务态成功，不代表主结果链已通过

### 7.4 用回滚指针替代正式 rollback

错误原因：

- 只改 current 指针不会留下完整 rollback 执行证据

### 7.5 用 AI 或脚本摘要替代 L1 主事实

错误原因：

- 这会直接破坏 [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1) 的分层边界

---

## 8. 执行动作前检查

执行任何高风险入口前，必须先回答：

1. 这是 `R0 / R1 / R2 / R3 / 部署链` 哪一类？
2. 这次动作是否允许改 latest？
3. 这次动作是否允许改 pointer？
4. 这次动作结束后必须产出什么 JSON / artifact？
5. 失败后是回到只读检查，还是进入正式回滚？

答不清，禁止执行。

---

## 9. 阻断规则

以下情况必须阻断继续执行：

- 入口未分级
- 想用非 R3 入口改 `current_result_pointer`
- latest 重建没有污染审计
- rollback 没有 post-deploy verification
- deploy 没有 evidence bundle
- 只读检查入口被当成修复入口使用

默认动作：

- 停止执行
- 保留现场
- 回到文档与人工复核

---

## 10. 下一步衔接

本文档冻结后，后续生产动作应先对照：

- 它属于哪一级
- 它能不能改 latest
- 它能不能改 pointer
- 它的验收产物是什么

下一份建议补的文档是：

- `AI 接入边界说明`

---

## 11. 阶段结论

当前结论：

- 生产入口必须先分级，再允许执行
- 只有正式 lifecycle 主线拥有主结果 current 改写权
- latest rebuild 只是修复动作，不是裁决动作
- deploy / rollback 只处理服务态，不自动替代主结果治理
- 后续补跑与恢复动作，必须先对照本入口地图与补跑分级说明

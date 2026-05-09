# P0 完成核销

## 1. 文档定位

本文档用于对照 [P0_ACCEPTANCE_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/P0_ACCEPTANCE_CHECKLIST.md:1) 做一次实际核销。

本文档只回答三件事：

- `P0` 哪些 blocking 项已经完成
- 还剩哪些工程化整理项
- 当前是否允许进入 `P1`

结论优先级高于口头判断，低于真实代码行为。

---

## 2. 当前结论

当前结论：

- `P0 pass`
- `允许进入 P1`

但有两个前提必须记录：

- `P0` 仍有少量收尾整理工作，不影响是否通过
- `P1` 开始前，必须先处理 release pipeline 测试过慢问题，否则后续验证成本过高

---

## 3. Blocking Checklist 核销

## A. 主结果唯一入口

- [x] `/stock` 主结果只来自唯一 `current_result_pointer`
- [x] pointer 包含 `result_id`
- [x] pointer 包含 `run_id`
- [x] pointer 包含 `artifact_ids`
- [x] pointer 包含 `lifecycle_id`
- [x] pointer 包含 `as_of_date`
- [x] pointer 缺失时系统 fail closed

证据：

- [current_result_pointer.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/current_result_pointer.py:1)
- [dashboard_context.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/dashboard_context.py:440)
- [run_dashboard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/run_dashboard.py:281)

## B. 主结果读路径

- [x] `unified_result_builder` 已改为 pointer-first
- [x] `unified_result_builder` 不再以多份 latest 推断主事实
- [x] `dashboard_context` 不再直接决定 primary result 主事实
- [x] `first_place_evidence_cockpit` 不再承担主事实拼装职责
- [x] `primary_result_candidate_handoff_gate` 不再被当成主结果真相源

证据：

- [unified_result_builder.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/unified_result_builder.py:392)
- [first_place_evidence_cockpit.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/first_place_evidence_cockpit.py:179)
- [primary_result_candidate_handoff_gate.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_candidate_handoff_gate.py:86)

## C. 统一身份与链路

- [x] 主结果可追溯到唯一 `result_id`
- [x] 主结果可追溯到唯一 `run_id`
- [x] 主结果可追溯到明确 `artifact_ids`
- [x] `result_registry` 已建立并可查询
- [x] `run_registry` 已建立并可查询
- [x] `artifact_registry` 能稳定关联主结果链路

证据：

- [result_registry.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/result_registry.py:1)
- [run_registry.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/run_registry.py:1)
- [run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_lifecycle.py:86)
- [run_stock_release_pipeline.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_stock_release_pipeline.py:154)

## D. fail closed 行为

- [x] pointer 不完整时不输出误导性正向结论
- [x] artifact 缺失时不输出误导性正向结论
- [x] 状态冲突时不自动推断为可推进
- [x] 缺证据时页面明确显示阻断/证据不足/人工复核

证据：

- [unified_result_builder.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/unified_result_builder.py:397)
- [test_unified_result_builder.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_unified_result_builder.py:1)
- [test_dashboard_context.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_dashboard_context.py:1)

## E. gate 与约束

- [x] 已建立 latest-path gate
- [x] 已建立 pointer-integrity gate
- [x] gate 能对 blocking 问题输出 fail
- [x] 未新增新的 primary result latest 主读路径

证据：

- [check_release_gates.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/check_release_gates.py:1)
- [check_current_result_pointer_integrity.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/check_current_result_pointer_integrity.py:1)
- [test_check_release_gates.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_check_release_gates.py:1)

## F. 最小可回放能力

- [x] 至少存在一条完整主结果 evidence chain 样本
- [x] 样本可证明 pointer -> result -> run -> artifacts 全链路可追溯
- [x] 该样本可作为 P1/P2 的继续基线

证据：

- [test_run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_run_primary_result_lifecycle.py:1)
- [test_current_result_pointer_integrity.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/tests/test_current_result_pointer_integrity.py:1)

---

## 4. Non-blocking Checklist 核销

- [x] 相关测试覆盖已补齐
- [x] 文档和代码字段命名一致
- [x] 新 registry 与旧 latest 迁移关系已说明
- [x] P1 入口依赖已标注清楚

说明：

- 这里的“迁移关系已说明”以 `STRICT_CONTINUATION_EXECUTION_STANDARD.md`、`P0_EXECUTION_PLAN.md`、本核销文档为准
- 这里的“P1 入口依赖”当前最关键的是：`P0` 通过后先处理测试提速，再做 `run_dashboard.py` 拆分

---

## 5. 剩余收尾项

这些项不再阻断 `P0`，但建议在进入 `P1` 前完成：

1. 把 `P0_TASK_BACKLOG.md` 中已完成任务标记为 done，保留未做项或转移项
2. 把 `P0_ACCEPTANCE_CHECKLIST.md` 从模板状态更新为已核销状态，避免下次重复人工判断
3. 整理一份 `latest -> pointer/registry` 的迁移说明给后续维护者

---

## 6. 风险提示

当前最大的现实风险已经不是事实链绕路，而是：

- release pipeline 测试过慢
- 后续任何 `P1` 改动都会被高验证成本拖慢

这不是测试“多跑一会儿”的小问题，而是后续工程节奏问题。

---

## 7. 进入 P1 的前提

允许进入 `P1`，但顺序必须是：

1. 先完成 release pipeline 测试提速/拆分
2. 再进入 `run_dashboard.py` 读模型拆分

如果反过来做，`P1` 每轮验证成本会持续偏高，工程节奏会失控。

# P0 验收清单

## 1. 文档定位

本文档用于 `P0` 阶段验收。

当前实际核销结果见：

- [P0_COMPLETION_REVIEW.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/P0_COMPLETION_REVIEW.md:1)

用途只有一个：

`判断 P0 是否真实完成，而不是判断团队是否投入了很多工作。`

如本清单有任一 blocking 项未通过，则 `P0` 不允许结束，也不允许进入 `P1`。

---

## 2. 验收规则

- 验收以代码行为和产物为准，不以口头说明为准
- 验收以调用链为准，不以“数据结构已存在”为准
- 验收以 fail closed 为准，不以“页面看起来正常”为准

---

## 3. Blocking Checklist

## A. 主结果唯一入口

- [ ] `/stock` 主结果只来自唯一 `current_result_pointer`
- [ ] pointer 包含 `result_id`
- [ ] pointer 包含 `run_id`
- [ ] pointer 包含 `artifact_ids`
- [ ] pointer 包含 `lifecycle_id`
- [ ] pointer 包含 `as_of_date`
- [ ] pointer 缺失时系统 fail closed

## B. 主结果读路径

- [ ] `unified_result_builder` 已改为 pointer-first
- [ ] `unified_result_builder` 不再以多份 latest 推断主事实
- [ ] `dashboard_context` 不再直接决定 primary result 主事实
- [ ] `first_place_evidence_cockpit` 不再承担主事实拼装职责
- [ ] `primary_result_candidate_handoff_gate` 不再被当成主结果真相源

## C. 统一身份与链路

- [ ] 主结果可追溯到唯一 `result_id`
- [ ] 主结果可追溯到唯一 `run_id`
- [ ] 主结果可追溯到明确 `artifact_ids`
- [ ] `result_registry` 已建立并可查询
- [ ] `run_registry` 已建立并可查询
- [ ] `artifact_registry` 能稳定关联主结果链路

## D. fail closed 行为

- [ ] pointer 不完整时不输出误导性正向结论
- [ ] artifact 缺失时不输出误导性正向结论
- [ ] 状态冲突时不自动推断为可推进
- [ ] 缺证据时页面明确显示阻断/证据不足/人工复核

## E. gate 与约束

- [ ] 已建立 latest-path gate
- [ ] 已建立 pointer-integrity gate
- [ ] gate 能对 blocking 问题输出 fail
- [ ] 未新增新的 primary result latest 主读路径

## F. 最小可回放能力

- [ ] 至少存在一条完整主结果 evidence chain 样本
- [ ] 样本可证明 pointer -> result -> run -> artifacts 全链路可追溯
- [ ] 该样本可作为 P1/P2 的继续基线

---

## 4. Non-blocking Checklist

以下项不通过不会单独阻断 `P0`，但必须记录：

- [ ] 相关测试覆盖已补齐
- [ ] 文档和代码字段命名一致
- [ ] 新 registry 与旧 latest 迁移关系已说明
- [ ] P1 入口依赖已标注清楚

---

## 5. 验收结论模板

每次验收只允许输出以下三种结论之一：

### 结论 A：Pass

条件：

- 所有 blocking 项通过

输出模板：

- `P0 pass`
- `允许进入 P1`

### 结论 B：Carry-over

条件：

- 存在未通过 blocking 项，但方向正确，继续留在 P0

输出模板：

- `P0 not complete`
- `继续停留在 P0`
- `列出未通过 blocking 项`

### 结论 C：Rollback / Rework

条件：

- 本轮改造引入新绕路、新 latest 主依赖、或破坏 fail closed

输出模板：

- `P0 regression detected`
- `必须回退或返工`
- `列出破坏项`

---

## 6. 周五验收问题

每周五验收时，只回答以下问题：

1. `/stock` 主结果是否更接近唯一 pointer 入口
2. latest 是否继续被降级，而不是重新扩权
3. 主结果是否更接近可追溯、可回放
4. fail closed 是否比上周更硬
5. 是否出现新的绕路事实源

如第 5 条答案为“是”，本周原则上不算高质量推进。

---

## 7. 最终 P0 通过结论

只有当以下判断全部成立时，才能宣布：

`P0 完成`

必须全部成立：

- 唯一主结果入口已建立
- 主结果读路径已收口
- registry 已承接统一身份
- fail closed 已成为默认行为
- 存在最小可回放样本

任何一项不成立，都不能宣布 P0 完成。

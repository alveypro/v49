# P0 任务台账

## 1. 文档定位

本文档用于把 `P0_EXECUTION_PLAN.md` 拆成可执行任务。

每个任务都必须具备：

- task id
- owner
- write scope
- 输入
- 输出
- 依赖
- 验收标准

未进入本台账的工作，不应作为 `P0` 主任务推进。

---

## 2. Owner 角色定义

为避免同文件多人同时改动，`P0` 统一使用以下 owner 角色：

- `owner-fact-source`
- `owner-read-path`
- `owner-registry`
- `owner-gate`
- `owner-test`

如后续指定到具体人名，可在不改任务结构的前提下替换。

---

## 3. 任务列表

## P0-001 定义 current_result_pointer 结构

- owner: `owner-fact-source`
- priority: `blocking`
- write scope:
  - 新增 pointer schema/loader
  - primary result pointer 默认路径定义
- input:
  - `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
  - `docs/P0_EXECUTION_PLAN.md`
- output:
  - pointer 结构定义
  - pointer 读写接口
- dependency:
  - none
- done criteria:
  - pointer 字段齐全
  - pointer 支持版本字段
  - pointer 支持 fail closed 读取

## P0-002 建立 result_registry

- owner: `owner-registry`
- priority: `blocking`
- write scope:
  - `src/result_registry.py`
- input:
  - current primary result identity model
  - artifact registry 现状
- output:
  - result registry 实现
  - result registry history/current 约定
- dependency:
  - `P0-001`
- done criteria:
  - 能按 `result_id` 查询结果主身份
  - 能关联 `run_id`
  - 能关联 `artifact_ids`

## P0-003 建立 run_registry

- owner: `owner-registry`
- priority: `blocking`
- write scope:
  - `src/run_registry.py`
- input:
  - 当前研究、审核、发布脚本的 run 语义
- output:
  - run registry 实现
- dependency:
  - none
- done criteria:
  - 能按 `run_id` 取回基础 metadata
  - 支持 `run_type/status/config_hash/data_snapshot_id/code_revision`

## P0-004 扩展 artifact_registry 兼容 P0 主线

- owner: `owner-registry`
- priority: `high`
- write scope:
  - `src/artifact_registry.py`
- input:
  - 现有 artifact registry
  - `P0-002`
  - `P0-003`
- output:
  - artifact registry 扩展方案
- dependency:
  - `P0-002`
  - `P0-003`
- done criteria:
  - artifact entry 可稳定关联 `result_id/run_id`
  - parent chain 字段完整

## P0-005 改造 unified_result_builder 为 pointer-first

- owner: `owner-fact-source`
- priority: `blocking`
- write scope:
  - `src/unified_result_builder.py`
- input:
  - `P0-001`
  - `P0-002`
  - `P0-003`
- output:
  - pointer-first 主结果构建逻辑
- dependency:
  - `P0-001`
  - `P0-002`
  - `P0-003`
- done criteria:
  - 主结果先读 pointer
  - 不再以多份 latest 推断主事实
  - 缺证据默认 fail closed

## P0-006 改造 dashboard_context 不再决定主事实

- owner: `owner-read-path`
- priority: `blocking`
- write scope:
  - `src/dashboard_context.py`
- input:
  - `P0-005`
- output:
  - dashboard context 改为消费 pointer 派生事实
- dependency:
  - `P0-005`
- done criteria:
  - dashboard_context 不再承担 primary result 主推断逻辑
  - `/stock` 主结论不再直接来源于多份 latest

## P0-007 调整 first_place_evidence_cockpit 的角色边界

- owner: `owner-read-path`
- priority: `medium`
- write scope:
  - `src/first_place_evidence_cockpit.py`
- input:
  - `P0-005`
- output:
  - cockpit 从主结果真相源退回为证据展示层
- dependency:
  - `P0-005`
- done criteria:
  - cockpit 不再承担主事实拼装
  - 只展示 evidence summary

## P0-008 调整 handoff gate 只保留 gate 语义

- owner: `owner-read-path`
- priority: `medium`
- write scope:
  - `src/primary_result_candidate_handoff_gate.py`
- input:
  - `P0-005`
- output:
  - handoff gate 与主结果真相源解耦
- dependency:
  - `P0-005`
- done criteria:
  - handoff gate 继续做 gate
  - 不再被当成主结果真相源

## P0-009 建立 latest-path gate

- owner: `owner-gate`
- priority: `blocking`
- write scope:
  - `scripts/check_release_gates.py`
  - 新增辅助检查脚本
- input:
  - `P0_EXECUTION_PLAN.md`
- output:
  - latest 主读路径检查
- dependency:
  - none
- done criteria:
  - 能识别新增 primary result 相关 latest 主读路径
  - 能作为 gate 输出 pass/fail

## P0-010 建立 pointer-integrity gate

- owner: `owner-gate`
- priority: `blocking`
- write scope:
  - 新增 pointer 校验脚本
  - tests
- input:
  - `P0-001`
  - `P0-002`
  - `P0-003`
- output:
  - pointer 完整性检查
- dependency:
  - `P0-001`
  - `P0-002`
  - `P0-003`
- done criteria:
  - 缺 `result_id/run_id/artifact_ids` 时 fail
  - pointer 指向不存在工件时 fail

## P0-011 建立 fail-closed 测试

- owner: `owner-test`
- priority: `blocking`
- write scope:
  - relevant tests for primary result and dashboard read path
- input:
  - `P0-005`
  - `P0-006`
- output:
  - 缺证据、缺 pointer、冲突状态测试
- dependency:
  - `P0-005`
  - `P0-006`
- done criteria:
  - 缺证据时系统不输出误导性正向结论
  - 测试明确覆盖 fail closed 行为

## P0-012 建立最小可回放样本

- owner: `owner-test`
- priority: `high`
- write scope:
  - test fixtures / sample artifacts
- input:
  - `P0-001`
  - `P0-002`
  - `P0-003`
- output:
  - 至少一条完整主结果 evidence chain 样本
- dependency:
  - `P0-001`
  - `P0-002`
  - `P0-003`
- done criteria:
  - 可用该样本证明主结果可追溯与可回放

---

## 4. 任务依赖关系

推荐执行顺序：

1. `P0-001`
2. `P0-002`
3. `P0-003`
4. `P0-004`
5. `P0-005`
6. `P0-006`
7. `P0-007`
8. `P0-008`
9. `P0-009`
10. `P0-010`
11. `P0-011`
12. `P0-012`

不得跳过 `P0-005` 直接改页面主结论读路径。

---

## 5. 本阶段不接受的“伪完成”

以下情况一律不算任务完成：

- 只加了数据结构，调用链没有切换
- 只改了文档，没有改读路径
- 只保留了旧 latest 逻辑，再外包一层 pointer 名称
- 只在 UI 上隐藏异常，没有 fail closed
- 只靠人工说明“以后会改”

---

## 6. 台账使用规则

- 每周一只允许新增或调整本台账中的任务，不单独口头派活
- 每周三检查依赖是否阻塞
- 每周五只按 done criteria 验收
- 未通过 done criteria 的任务，状态只能是 `carry-over`，不能算完成

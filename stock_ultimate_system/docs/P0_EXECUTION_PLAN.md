# P0 执行计划

## 1. 文档定位

本文档是 `STRICT_CONTINUATION_EXECUTION_STANDARD.md` 在 `P0` 阶段的执行计划。

本文档只回答以下问题：

- `P0` 到底做什么
- `P0` 不做什么
- `P0` 影响哪些文件和模块
- `P0` 如何分阶段推进
- `P0` 何时允许进入 `P1`

如本文档与其他 P0 讨论材料冲突，以本文档为准。

---

## 2. P0 目标冻结

`P0` 的唯一目标是：

`收口主结果真相源，结束多份 latest 拼接主结论的状态。`

`P0` 不以页面更好看、模块更多、文档更多、命名更完整作为成功标准。

`P0` 的成功标准只有四类：

- `/stock` 主结果只来自唯一 pointer
- 主结果对象拥有统一身份
- 主结果事实链可追溯
- 缺证据时系统 fail closed

---

## 3. P0 范围边界

### 3.1 P0 必做范围

`P0` 只覆盖以下四个工作包：

1. `current_result_pointer`
2. `result_registry`
3. `run_registry`
4. 主结果读路径收口

### 3.2 P0 不做范围

`P0` 明确不做以下事项：

- 大规模 UI 重做
- `/T12` 页面扩权
- `run_dashboard.py` 全量重构
- 策略评分体系重写
- 新增更多治理叙事模块
- 大面积重命名
- 研究链新功能扩张

这些事项即使合理，也统一推迟到 `P1` 或 `P2` 后讨论。

---

## 4. P0 入场门

只有同时满足以下条件，`P0` 才视为正式开始：

- 冻结新增首页叙事需求
- 冻结 `/T12` 新交互需求
- 冻结新增 primary result 主读路径
- 冻结新增多文件 latest 拼装逻辑

若出现紧急生产修复，可例外，但必须：

- 不新增结构性绕路
- 修复后补登记到 `P0_TASK_BACKLOG.md`

---

## 5. P0 工作包

## 5.1 工作包 A：建立唯一 current_result_pointer

目标：

- 为 `/stock` 主结果建立唯一入口

必须包含字段：

- `pointer_version`
- `result_id`
- `run_id`
- `lifecycle_id`
- `artifact_ids`
- `as_of_date`
- `updated_at`
- `source_scope`

要求：

- `/stock` 主结果只能先读 pointer
- pointer 只允许指向 immutable artifacts
- pointer 缺失或损坏时 fail closed

## 5.2 工作包 B：建立 result_registry

目标：

- 让研究、候选、审核、观察、终局结果围绕 `result_id` 统一挂接

最低要求：

- 一个 `result_id` 对应一条主身份记录
- 每条记录可追到 `run_id`
- 每条记录可追到 `artifact_ids`
- 每条记录带 `lifecycle_stage`

## 5.3 工作包 C：建立 run_registry

目标：

- 为每次研究、生成、审核、观察、发布动作建立统一 run 身份

最低要求：

- `run_id`
- `run_type`
- `created_at`
- `producer`
- `config_hash`
- `data_snapshot_id`
- `code_revision`
- `status`

## 5.4 工作包 D：主结果读路径收口

目标：

- 结束当前主结果通过多份 latest 文件拼接推断的状态

要求：

- `unified_result_builder` 先读 pointer，再按 `result_id` 拉链路
- `dashboard_context` 不再直接决定主事实
- `first_place_evidence_cockpit` 不再承担主事实拼装责任
- `primary_result_candidate_handoff_gate` 继续保留校验职责，但不再成为主结果真相源

---

## 6. 文件级范围

### 6.1 P0 一级修改范围

- `src/unified_result_builder.py`
- `src/dashboard_context.py`
- `src/first_place_evidence_cockpit.py`
- `src/primary_result_candidate_handoff_gate.py`
- `src/artifact_registry.py`
- 新增 `src/result_registry.py`
- 新增 `src/run_registry.py`

### 6.2 P0 二级配套范围

- `scripts/check_release_gates.py`
- 与 primary result pointer / verification 相关的新增脚本
- 对应测试文件

### 6.3 P0 禁止随意改动范围

无明确 P0 任务时，禁止大改以下文件：

- `run_dashboard.py`
- `src/main_site_home.py`
- `src/t12_governance_summary.py`
- `src/dashboard_operations.py`
- `src/dashboard_reports.py`

原因：

- 这些文件主要属于 `P1` 读模型与职责拆分范围

---

## 7. 推进顺序

P0 只能按以下顺序推进：

### 第一步：定义对象与指针结构

- 定义 `current_result_pointer`
- 定义 `result_registry`
- 定义 `run_registry`
- 明确字段与版本

### 第二步：让主结果 builder 改读 pointer

- `unified_result_builder` 改为 pointer-first
- 移除主事实对多份 latest 的依赖

### 第三步：让 dashboard 读路径服从 pointer

- `dashboard_context` 只消费 pointer 派生事实
- 页面不再反推主结果

### 第四步：补校验与 gate

- 检查是否仍新增 latest 主读路径
- 检查 `/stock` 是否绕过 pointer
- 检查缺证据时是否 fail closed

不得跳步骤推进。

---

## 8. 风险清单

`P0` 的核心风险如下：

### 风险 1：边收口边新增 latest 绕路

处理：

- 新增 latest 主读路径一律拦截

### 风险 2：多人同时改主结果读路径

处理：

- 按 write-scope 分工
- 同一文件同一阶段只允许一个 owner

### 风险 3：pointer 建了，但页面仍不服从

处理：

- 验收以调用链为准，不以数据结构存在为准

### 风险 4：过早开始 dashboard 大拆

处理：

- 未通过 P0 出场门，不允许将大拆作为主任务

---

## 9. P0 出场门

只有同时满足以下条件，`P0` 才算完成并允许进入 `P1`：

1. `/stock` 主结果只来自唯一 `current_result_pointer`
2. `unified_result_builder` 不再以多份 latest 推断主事实
3. `dashboard_context` 不再直接决定主事实
4. `result_id / run_id / artifact_ids` 已贯通到主结果
5. pointer 缺失、artifact 缺失、状态冲突时系统 fail closed
6. 至少一条完整主结果 evidence chain 可被回放

如果其中任一项未通过，则 `P0` 不算完成。

---

## 10. 阶段结论

`P0` 的本质不是“做一轮重构”，而是：

`把主结果从文件拼装逻辑中解放出来，收回到唯一事实入口。`

这是后续 `P1` 和 `P2` 能否成立的前提。

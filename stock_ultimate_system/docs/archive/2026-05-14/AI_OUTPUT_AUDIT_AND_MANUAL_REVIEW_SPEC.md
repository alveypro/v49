# AI 输出留痕与人工复核规范

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 中 AI 输出的留痕规则、人工复核规则、阻断规则与闭环规则。

本文档只回答五件事：

- AI 输出最少必须留什么痕
- 哪些 AI 输出可以自动展示，哪些必须先复核
- 哪些 AI 输出一旦异常必须阻断
- 复核动作由谁承接、怎么闭环
- AI 输出与现有主链、review queue、release gate 如何衔接

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. [OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md:1)
5. [AI_INTEGRATION_BOUNDARY_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_BOUNDARY_SPEC.md:1)
6. 本文档

本文档不是 prompt 设计说明，不是模型效果报告，不是 UI 交互稿。

---

## 2. 总原则

### 2.1 没有留痕，就等于没有 AI 输出

后续任何 AI 输出，只要进入系统可见范围，就必须留痕。

没有留痕的 AI 输出，不得：

- 出现在 `/stock`
- 出现在 `/T12`
- 出现在报告正文
- 出现在运维复核建议
- 出现在发布说明

### 2.2 先留痕，再展示

系统流程顺序必须是：

1. 生成 AI 输出
2. 写入留痕对象
3. 判断是否需要人工复核
4. 决定能否展示或采用

禁止顺序：

1. 先展示
2. 后补留痕

### 2.3 复核优先级高于体验优先级

如果一段 AI 输出同时满足：

- 对外可见
- 影响理解、判断、行动建议

则必须先判断是否需要人工复核，而不是先追求“页面更完整”。

### 2.4 AI 输出只能被采纳，不能被默认信任

AI 输出的制度状态只有三类：

- `仅展示`
- `待复核`
- `已复核采纳`

禁止状态：

- `默认可信`
- `默认生效`
- `直接改主链`

---

## 3. AI 输出对象分级

## 3.1 A0 展示级输出

定义：

- 只影响阅读体验
- 不触发动作建议
- 不影响任何治理判断

典型例子：

- 图表说明
- FAQ 文案
- 页面导读
- tooltip 文案

要求：

- 必须留痕
- 可不经人工逐条审核上线
- 出错时只允许降级隐藏

## 3.2 A1 说明级输出

定义：

- 解释 L1/L2 现有事实
- 可能影响操作者理解
- 不直接触发动作

典型例子：

- 主结果解释摘要
- 阻断原因重述
- 执行状态摘要
- 观察期说明

要求：

- 必须留痕
- 可自动展示
- 若命中冲突或高敏感词，转入人工复核

## 3.3 A2 建议级输出

定义：

- 给出人工应考虑的下一步建议
- 可能影响补跑、复核、部署、发布判断
- 但不能直接触发动作

典型例子：

- 运维复核 checklist
- 证据缺口补齐建议
- 补跑范围建议
- 发布前阅读摘要

要求：

- 必须留痕
- 必须有复核状态
- 未复核不得作为正式行动依据

## 3.4 A3 高敏感输出

定义：

- 涉及发布、回滚、主链、审计、治理阻断
- 一旦误导，会直接造成错误动作

典型例子：

- 发布说明摘要
- 回滚原因建议
- release evidence 复核建议
- “应升格 current”式推荐文案

要求：

- 必须留痕
- 默认进入人工复核
- 未人工采纳前不得展示为正式结论

---

## 4. 最低留痕字段

每次 AI 输出最少必须记录以下字段：

- `trace_id`
- `generated_at`
- `model_id`
- `model_provider`
- `prompt_version`
- `input_schema_version`
- `output_schema_version`
- `output_level`
- `output_role`
- `source_module`
- `source_scope`
- `source_object_id`
- `source_result_id`
- `source_run_id`
- `source_lifecycle_id`
- `source_as_of_date`
- `input_field_whitelist`
- `input_source_paths`
- `output_text`
- `output_structured_payload`
- `display_label`
- `needs_manual_review`
- `review_status`
- `review_reason`
- `adoption_status`
- `adopted_by`
- `adopted_at`
- `superseded_by_trace_id`
- `error_status`
- `error_message`

硬话直接说：
少于这组字段，后续几乎不可能把问题追清楚。

---

## 5. 留痕级别

## 5.1 L-A 基础留痕

适用：

- A0 展示级输出
- A1 说明级输出

最低要求：

- 保留最小字段全集
- 保留输入字段白名单
- 保留输出版本与模型标识

## 5.2 L-B 审阅留痕

适用：

- A2 建议级输出

额外要求：

- 记录建议对象范围
- 记录触发原因
- 记录人工是否采用
- 记录采用后的关联动作编号

## 5.3 L-C 治理留痕

适用：

- A3 高敏感输出

额外要求：

- 记录复核人
- 记录复核结论
- 记录是否进入 release / rollback / review queue
- 记录被拒绝原因
- 记录最终替代文本或人工结论

---

## 6. 人工复核触发条件

以下情况必须触发人工复核：

### 6.1 触及高敏感语义

如果 AI 输出出现以下语义之一，必须转人工复核：

- 建议发布
- 建议回滚
- 建议升格 current
- 建议跳过 gate
- 建议忽略 evidence 缺口
- 建议替代人工判断

### 6.2 涉及 A2/A3 输出等级

只要输出等级属于：

- `A2 建议级输出`
- `A3 高敏感输出`

就必须有复核状态。

### 6.3 与 L1 或 guard 冲突

如果 AI 输出与以下任一对象冲突，必须阻断自动展示并进入复核：

- `current_result_pointer`
- `stock_entry_guard`
- `primary_result_lifecycle_evidence`
- release gate
- deploy rollback 证据

### 6.4 输入来源不完整

如果出现以下情况，必须阻断并进入人工复核：

- 缺 `result_id`
- 缺 `run_id`
- 缺输入字段清单
- 缺来源路径
- 输入引用了未授权字段

### 6.5 模型输出异常

如果出现以下情况，必须阻断并进入人工复核：

- 输出为空但状态为 success
- 输出格式不符合 schema
- 输出包含未授权动作建议
- 输出出现明显事实幻觉

---

## 7. 复核状态机

AI 输出复核状态只允许以下流转：

- `generated`
- `needs_review`
- `reviewed_accepted`
- `reviewed_rejected`
- `superseded`
- `expired`

禁止出现以下状态：

- `implicitly_trusted`
- `auto_approved`
- `effective_without_review`

### 7.1 generated

说明：

- 输出已生成
- 已留痕
- 尚未判断是否需要复核

### 7.2 needs_review

说明：

- 命中人工复核条件
- 未复核前不得作为正式建议使用

### 7.3 reviewed_accepted

说明：

- 已人工复核
- 允许作为说明性或建议性材料继续使用

注意：

- 这不等于它升格为 L1
- 这不等于它可以改 pointer 或 gate

### 7.4 reviewed_rejected

说明：

- 已人工认定不可采用
- 应保留原记录并禁止继续展示为当前版本

### 7.5 superseded

说明：

- 已被同一对象后续版本替代

### 7.6 expired

说明：

- 时间敏感输出已过期
- 不得继续展示为当前建议

---

## 8. 人工复核职责边界

## 8.1 复核人职责

复核人只负责：

- 判断 AI 输出是否可采用
- 判断是否需要改写为人工文本
- 判断是否需要进入 review queue 或升级治理处理

复核人不负责：

- 用 AI 输出替代正式主链证据
- 在无证据时给出制度豁免

## 8.2 复核不等于批准主链动作

人工复核 `AI 建议`，不等于批准以下动作：

- 改 `current_result_pointer`
- 跑 `R3 lifecycle`
- 做 release pass
- 做 deploy rollback

正式动作仍必须走原有治理链。

## 8.3 高敏感复核必须双向留痕

对于 A3 输出，至少要同时保留：

- AI 原文
- 人工复核结论

禁止只保留“人工改过后的最终版本”，而抹掉 AI 原文。

---

## 9. 与现有治理对象的衔接

## 9.1 与 feedback review queue 的衔接

当 AI 输出满足以下条件之一时，应考虑进入受控 review queue，而不是停留在页面层：

- 连续多次提示同一证据缺口
- 连续多次提示同一治理冲突
- 对同一对象提出高敏感人工处理建议

衔接原则：

- AI 只能触发“进入受控复核”
- AI 不能替代现有 `feedback_review_queue` 治理流程

参考对象：

- [primary_result_feedback_review_queue.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_feedback_review_queue.py:1)

## 9.2 与 promotion / release gate 的衔接

AI 输出可以做：

- gate 摘要
- gate 证据阅读辅助
- gate 缺口提示

AI 输出不可以做：

- 直接判 `promotion_review_allowed`
- 直接判 release pass
- 直接关闭 open review items

## 9.3 与 T12 的衔接

AI 在 `/T12` 范围内只允许做：

- 治理摘要解释
- 复核提示说明

AI 在 `/T12` 范围内不允许做：

- 第二主结论
- 第二 current 推荐
- 绕开只读治理摘要边界

---

## 10. 展示规则

## 10.1 必须显式标注

所有 AI 输出展示时至少应包含以下之一：

- `AI 解释`
- `AI 摘要`
- `AI 建议`
- `AI 风险提示`
- `待人工复核`

### 10.2 未复核建议不得伪装成正式结论

未复核的 A2/A3 输出不得以以下形式展示：

- “系统结论”
- “正式建议”
- “已批准动作”

### 10.3 被拒绝输出不得继续当前展示

一旦状态进入 `reviewed_rejected`：

- 不得继续作为当前输出展示
- 只能在审计或历史记录中可见

---

## 11. 阻断条件

出现以下任一情况，AI 输出必须立即阻断：

- 缺少最小留痕字段
- 缺少输入字段白名单
- 命中未授权动作建议
- 触及 L1 改写
- 触及 pointer / registry current / guard / release decision / baseline current
- 输出与 guard 结论冲突
- 输出与 lifecycle evidence 冲突
- 输出试图放宽 fail-closed

阻断后允许的后续动作只有：

- 隐藏输出
- 标记错误
- 路由人工复核
- 写审计记录

阻断后不允许：

- 继续展示
- 自动重写为“更温和”的建议后继续上线

---

## 12. 最低测试与校验要求

后续任何 AI 接入，最少应补以下校验：

- AI 输出无留痕时不可展示
- AI 输出缺 `source_result_id` 时阻断
- A2/A3 输出未复核时不可作为正式建议
- AI 输出被拒绝后当前页面不再继续显示
- AI 服务失败时 `/stock` 与 `/T12` 仍可显示非 AI 主事实
- AI 输出不能影响 `current_result_pointer`
- AI 输出不能影响 `stock_entry_guard`
- AI 输出不能替代 review queue 或 release gate 结论

---

## 13. 最低执行口径

如果未来要快速上线 AI 输出能力，最低只允许采用以下保守执行口径：

1. 先接 A0/A1，不接 A2/A3
2. 先做只读说明，不做行动建议
3. 先留痕，再展示
4. 先支持人工拒绝，再谈自动采用
5. 先接页面说明，再考虑运维复核辅助

做不到这五条，就不应该匆忙上线。

---

## 14. 下一步文档

本文档之后，如果继续补治理文档，下一份建议优先补：

- `AI 输入字段白名单与输出 schema 约束表`

原因：

- 本文档解决“接了之后怎么管”
- 下一份文档要解决“具体允许喂什么、吐什么”

---

## 15. 结论

本文档正式冻结以下规则：

- AI 输出必须先留痕，再展示
- AI 输出只能被采纳，不能被默认信任
- A2/A3 输出必须进入人工复核状态机
- 高敏感 AI 输出必须保留 AI 原文与人工结论双向留痕
- AI 输出不得替代主链、guard、release gate、deploy rollback 正式治理链

硬话直接说：
如果后面谁想跳过留痕、跳过复核、直接把 AI 建议当系统结论，那不是“提效”，而是在给主链治理埋雷。

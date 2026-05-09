# AI 输入字段白名单与输出 Schema 约束表

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 中 AI 接入时允许输入哪些字段、禁止输入哪些字段、允许输出什么结构、禁止输出什么结构。

本文档只回答五件事：

- AI 输入字段如何分层白名单
- `/stock`、`/T12`、运维复核、报告层分别能喂给 AI 什么
- AI 输出 schema 分成哪几类
- 哪些字段绝不能出现在输入或输出里
- 字段白名单与 schema 校验失败时系统必须怎么阻断

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. [OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md:1)
5. [AI_INTEGRATION_BOUNDARY_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_BOUNDARY_SPEC.md:1)
6. [AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md:1)
7. 本文档

本文档不是 prompt 模板库，不是模型配置文件，不是字段字典全集。

---

## 2. 总原则

### 2.1 输入不是“能读到什么就喂什么”

AI 输入必须先做字段白名单。

禁止做法：

- 把整个 `context` 原样喂给 AI
- 把整个 `primary_result` 原始对象原样喂给 AI
- 把整个 registry / evidence / deploy 记录原样喂给 AI
- 把日志、凭据、环境变量整包喂给 AI

### 2.2 输出不是“模型想说什么就收什么”

AI 输出必须先过 schema 校验。

禁止做法：

- 任意自由文本直接进入页面
- 任意 JSON 结构直接写入系统对象
- 未声明字段直接被系统消费

### 2.3 白名单与 schema 都必须按场景收窄

后续 AI 接入不得存在“一份通用白名单通吃所有场景”的偷懒方案。

最少按以下场景分开：

- `/stock` 说明层
- `/T12` 治理摘要解释层
- 运维/补跑复核辅助层
- 报告/展示增强层

### 2.4 白名单失败与 schema 失败必须阻断

只要出现以下任一情况，系统都必须阻断：

- 输入字段超出白名单
- 输入缺少必需字段
- 输出结构不符合约束
- 输出字段超出声明范围
- 输出触及禁止字段

硬话直接说：
如果这些失败不阻断，白名单和 schema 就只是摆设。

---

## 3. 输入字段分层

## 3.1 I1 主结果说明输入层

用途：

- `/stock` 的 AI 解释摘要
- 阻断原因重述
- 运行态说明增强

允许输入字段：

### 3.1.1 身份锚点

- `result_id`
- `run_id`
- `lifecycle_id`
- `as_of_date`
- `ts_code`
- `stock_name`

### 3.1.2 主结果事实副本

- `result_lifecycle_stage`
- `result_type`
- `audit_status`
- `execution_status`
- `terminal_outcome`
- `disabled_reason`
- `invalid_reason`

说明：

- 这些字段允许作为只读解释输入
- 不代表 AI 可以反向改写它们

### 3.1.3 受控说明字段

- `headline_tone`
- `headline_detail`
- `summary_lines`
- `data_sync_note`
- `history_source_file`
- `history_generation_mode`
- `decision_semantics`
- `blocker_semantics`
- `execution_semantics`
- `evidence_semantics`
- `governance_semantics`

### 3.1.4 允许的辅助说明字段

- `observation_wait_status`
- `current_basket_pointer_status`
- `current_basket_pointer_basket_id`
- `current_basket_pointer_updated_at`
- `latest_basket_attempt_status`
- `latest_basket_attempt_generated_at`
- `latest_basket_attempt_blocking_reason`

禁止输入字段：

- `current_result_pointer` 原始对象
- `result_registry` 原始对象
- `run_registry` 原始对象
- `artifact_registry` 原始对象
- `stock_entry_guard_latest.json` 原始全文
- `primary_result_lifecycle_evidence` 原始全文
- 未脱敏路径、secret、token

## 3.2 I2 T12 治理摘要输入层

用途：

- `/T12` 的 AI 只读治理解释

允许输入字段严格限定为 [t12_governance_summary.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/t12_governance_summary.py:1) 的允许事实键：

- `disabled_reason`
- `audit_status`
- `promotion_status`
- `terminal_outcome`
- `risk_level`
- `data_sync_note`
- `result_lifecycle_stage`
- `result_type`
- `execution_status`
- `candidate_status`
- `research_status`

补充允许字段：

- `review_status`
- `review_generated_at`

禁止输入字段：

- 任意候选榜排序字段
- 任意推荐对象字段
- 任意第二主结论字段
- 任意 pointer current 对象全文

硬话直接说：
`/T12` 只允许解释治理摘要，不允许偷偷喂它一套“更像主结果”的输入。

## 3.3 I3 运维复核输入层

用途：

- 补跑复核建议
- 运维 checklist 摘要
- release evidence 阅读辅助

允许输入字段：

### 3.3.1 动作身份字段

- `operation_type`
- `operation_level`
- `operation_scope`
- `operation_started_at`
- `operation_finished_at`
- `operator`

### 3.3.2 主链关联字段

- `result_id`
- `run_id`
- `lifecycle_id`
- `as_of_date`
- `ts_code`

### 3.3.3 受控治理状态字段

- `pointer_integrity_status`
- `entry_guard_status`
- `release_gate_status`
- `promotion_gate_status`
- `artifact_pollution_status`
- `review_queue_status`
- `open_review_items_count`
- `open_high_severity_review_items_count`

### 3.3.4 允许的受控摘要字段

- `blocking_checks`
- `recommended_actions`
- `summary_note`
- `governance_note`

禁止输入字段：

- 全量 shell 输出
- 全量日志正文
- 未脱敏部署信息
- server 凭据
- 任意 current registry 原始对象全文

## 3.4 I4 报告与展示增强输入层

用途：

- 日报/周报摘要
- 图表说明
- 页面导读

允许输入字段：

- 已确认只读的 L2 摘要字段
- 已确认只读的 L3 图表标签、指标标题、摘要数值
- 不含动作权和治理判断的展示元数据

禁止输入字段：

- 任意 current 改写类字段
- 任意 deploy / rollback 决策字段
- 任意 pointer / registry / guard 原始全文

---

## 4. 通用禁止输入字段

以下字段或对象，任何场景都不得直接进入 AI 输入：

- `current_result_pointer` 原始 JSON
- `result_registry current` 原始 JSON
- `run_registry current` 原始 JSON
- `artifact_registry current` 原始 JSON
- `stock_entry_guard_latest.json` 原始全文
- `primary_result_lifecycle_evidence_latest.json` 原始全文
- release decision current 原始全文
- baseline current 原始全文
- deploy current 原始全文
- 任意 secret
- 任意 token
- 任意 cookie
- 任意未脱敏系统路径全集
- 任意环境变量全集
- 任意本地临时测试污染路径

说明：

- 允许从上述对象中抽取经制度授权的少量字段副本
- 不允许直接把对象全文喂给 AI

---

## 5. 输出 Schema 分级

## 5.1 O1 解释摘要 Schema

适用：

- A0/A1 输出
- `/stock` 与 `/T12` 的说明层

允许字段：

- `trace_id`
- `output_level`
- `output_role`
- `display_label`
- `summary_title`
- `summary_text`
- `supporting_facts`
- `risk_flags`
- `source_field_refs`

字段要求：

- `summary_text` 必须是说明性文本
- `supporting_facts` 必须只引用输入白名单字段
- `risk_flags` 只能是提示，不得是动作命令

禁止字段：

- `selected_result_id`
- `selected_ts_code`
- `should_promote`
- `should_release`
- `should_rollback`
- `rewrite_pointer`

## 5.2 O2 建议清单 Schema

适用：

- A2 建议级输出
- 运维复核辅助

允许字段：

- `trace_id`
- `output_level`
- `output_role`
- `display_label`
- `summary_title`
- `summary_text`
- `suggested_checks`
- `suggested_read_order`
- `suggested_manual_actions`
- `source_field_refs`
- `needs_manual_review`

字段要求：

- `suggested_manual_actions` 只能是人工动作建议
- 每条建议必须可映射到人工复核步骤
- `needs_manual_review` 必须为 `true`

禁止字段：

- `execute_now`
- `auto_apply`
- `update_current_pointer`
- `approve_release`
- `force_pass_gate`

## 5.3 O3 高敏感审阅 Schema

适用：

- A3 高敏感输出

允许字段：

- `trace_id`
- `output_level`
- `output_role`
- `display_label`
- `summary_title`
- `summary_text`
- `review_focus_items`
- `review_risk_notes`
- `source_field_refs`
- `needs_manual_review`

字段要求：

- 必须进入人工复核状态机
- 不得直接形成系统结论

禁止字段：

- `release_decision`
- `rollback_decision`
- `lifecycle_decision`
- `promotion_decision`
- `current_pointer_patch`

## 5.4 O4 展示增强 Schema

适用：

- FAQ
- tooltip
- 图表说明
- 页面导读

允许字段：

- `trace_id`
- `output_level`
- `output_role`
- `display_label`
- `title`
- `body`
- `highlights`

禁止字段：

- 任何动作建议字段
- 任何治理裁决字段

---

## 6. 输出通用禁止字段

以下字段在任何 AI 输出 schema 中都不得出现：

- `current_result_pointer`
- `result_registry_current`
- `run_registry_current`
- `artifact_registry_current`
- `result_id_patch`
- `run_id_patch`
- `lifecycle_id_patch`
- `artifact_ids_patch`
- `as_of_date_patch`
- `result_lifecycle_stage_patch`
- `audit_status_patch`
- `execution_status_patch`
- `terminal_outcome_patch`
- `release_decision`
- `rollback_decision`
- `promotion_decision`
- `deploy_decision`
- `approve_now`
- `auto_execute`
- `skip_guard`
- `skip_fail_closed`

硬话直接说：
这些字段只要一出现在 schema 里，等于默认给 AI 留了越权入口。

---

## 7. 场景映射表

## 7.1 `/stock`

- 允许输入层：`I1`
- 允许输出 schema：`O1`
- 条件允许输出 schema：`O2`
- 默认禁止输出 schema：`O3`

说明：

- `/stock` 只能以说明增强为主
- 任何建议级输出都必须显式标注并进入复核逻辑

## 7.2 `/T12`

- 允许输入层：`I2`
- 允许输出 schema：`O1`
- 默认禁止输出 schema：`O2/O3`

说明：

- `/T12` 只能做只读治理解释
- 不允许在 `/T12` 上输出候选推荐或第二主结论建议

## 7.3 运维与补跑复核

- 允许输入层：`I3`
- 允许输出 schema：`O2`
- 条件允许输出 schema：`O3`

说明：

- 任何高敏感输出都必须复核
- 只能辅助人工，不得自动触发 R3

## 7.4 报告与展示增强

- 允许输入层：`I4`
- 允许输出 schema：`O1/O4`
- 默认禁止输出 schema：`O2/O3`

---

## 8. 白名单与 Schema 校验规则

## 8.1 输入校验

输入校验至少必须检查：

- 是否属于已注册场景
- 是否只包含场景允许字段
- 是否包含该场景必需字段
- 是否包含禁止字段
- 是否包含未脱敏对象

## 8.2 输出校验

输出校验至少必须检查：

- 是否符合该场景允许的 schema 类型
- 是否缺少必需字段
- 是否存在未声明字段
- 是否触及通用禁止字段
- 是否声明了与场景不符的动作权

## 8.3 引用校验

输出中的 `source_field_refs` 必须满足：

- 只能引用本次输入白名单字段
- 不能引用隐式字段
- 不能引用系统未记录来源的字段

---

## 9. 阻断矩阵

出现以下情况必须阻断：

### 9.1 输入侧阻断

- 输入字段超白名单
- 输入缺少必需身份锚点
- 输入包含未授权治理对象全文
- 输入包含 secret / token / cookie

### 9.2 输出侧阻断

- 输出 schema 不匹配
- 输出带禁止字段
- 输出含动作执行意图
- 输出含 current 改写意图

### 9.3 场景侧阻断

- `/T12` 想输出建议级内容
- `/stock` 想输出高敏感治理裁决
- 运维复核想自动触发 R3
- 报告层想输出发布/回滚判断

阻断后允许动作：

- 丢弃输出
- 留痕错误
- 转人工复核
- 页面降级隐藏

阻断后不允许：

- 自动把非法字段删掉后悄悄继续上线
- 自动把高敏感输出降格后继续假装正常展示

---

## 10. 与现有模块的边界关系

## 10.1 与 `primary_result_query_service` 的关系

AI 输入可以消费：

- `primary_result_query_service` 产出的受控说明字段副本

AI 输入不得替代：

- `primary_result_query_service` 的 pointer-first 主事实读取逻辑

## 10.2 与 `t12_governance_summary` 的关系

`/T12` 场景输入白名单必须受：

- [t12_governance_summary.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/t12_governance_summary.py:1)

的允许字段边界约束。

## 10.3 与 `stock_entry_guard` 的关系

AI 可以读取：

- guard 的通过/阻断摘要字段副本

AI 不可以输入或输出：

- guard 原始 current 对象改写意图

---

## 11. 最低测试与校验要求

后续任何 AI 接入至少要补：

- 输入超白名单时阻断
- 输入缺 `result_id` / `run_id` / `trace` 锚点时阻断
- 输出 schema 不匹配时阻断
- 输出含禁止字段时阻断
- `/T12` 尝试输出建议级内容时阻断
- 运维 AI 输出试图带动作执行字段时阻断
- `/stock` AI 输出不能越权形成主结论

---

## 12. 最低执行口径

如果未来要快速接入 AI，最低只允许采用以下保守口径：

1. 先接 `I1 -> O1`
2. 再接 `I2 -> O1`
3. 运维复核只允许 `I3 -> O2`
4. 报告层只允许 `I4 -> O1/O4`
5. 任何场景都先做白名单和 schema 校验，再谈模型效果

做不到这五条，就不应该接生产。

---

## 13. 下一步文档

本文档之后，如果继续补治理文档，下一份建议优先补：

- `AI 接入测试门与阻断清单`

原因：

- 本文档解决“喂什么、吐什么”
- 下一份文档要解决“上线前怎么验收、失败时怎么拦”

---

## 14. 结论

本文档正式冻结以下规则：

- AI 输入必须按场景白名单收窄
- AI 输出必须按 schema 类型收窄
- `/stock`、`/T12`、运维、报告层不得共用一套粗暴输入输出 contract
- 任何触及 pointer、registry current、guard、release/deploy 决策的字段都不得进入输出 schema
- 白名单校验和 schema 校验失败必须阻断

硬话直接说：
如果后面有人想跳过这张表，直接把上下文整包喂给 AI、再把模型自由文本直接挂页面，那不是快速接入，而是在主动给系统制造旁路。

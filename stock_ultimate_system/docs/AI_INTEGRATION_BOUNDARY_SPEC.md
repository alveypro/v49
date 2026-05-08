# AI 接入边界说明

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 中 AI 能接什么、不能接什么、输出能落到哪一层、接入前必须满足什么治理条件。

本文档只回答五件事：

- AI 在本系统里属于什么角色
- AI 输出允许落到哪一层
- AI 输出绝不能碰哪些对象
- AI 接入前必须补哪些治理门
- AI 与主链、latest、部署链冲突时谁优先

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. [OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md:1)
5. 本文档

本文档不是模型选型说明，不是 prompt 手册，不是产品营销稿。

---

## 2. 总原则

### 2.1 AI 不是主结果裁决主体

AI 在本系统中的默认角色只有三类：

- 解释器
- 摘要器
- 辅助审阅器

AI 默认不是：

- 主结果裁决器
- 主链改写器
- 发布批准器
- 回滚批准器

### 2.2 AI 默认不得进入 L1

依据 [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)：

- AI 输出默认只允许落在 `L2 受控说明`
- AI 输出可落在 `L3 展示性上下文`
- AI 输出默认不得进入 `L1 主事实`

除非未来新增更高优先级制度文档明确升级，否则该边界不得放宽。

### 2.3 AI 不得绕开 fail-closed

AI 不允许通过任何方式绕开以下门禁：

- `current_result_pointer` 完整性
- `result_registry / run_registry / artifact_registry` 一致性
- `primary_result_lifecycle_evidence`
- `stock_entry_guard`
- release gate

AI 不能因为“看起来合理”就替代缺失事实链。

### 2.4 AI 不得把 latest 升格成主事实

AI 允许读取 `latest` 类文件做解释、摘要、异常提示。

AI 不允许：

- 把 `latest` 重建结果当作主结果裁决依据
- 把 `latest` 摘要升格成 `L1`
- 用 `latest` 反推并改写 `current_result_pointer`

### 2.5 AI 接入优先服从治理，不服从体验

后续所有 AI 接入必须优先满足：

- 可审计
- 可阻断
- 可回溯
- 可降级

如果“更聪明的体验”与“更严格的治理”冲突，必须优先保治理。

---

## 3. AI 角色分层

## 3.1 允许角色

AI 允许承担以下角色：

- `R-A1 解释摘要`
- `R-A2 风险提示`
- `R-A3 辅助审阅`
- `R-A4 展示增强`

### R-A1 解释摘要

允许做：

- 对 L1 已确定结论生成说明文字
- 对 L2 已有字段做结构化摘要
- 对页面说明区生成更易读文案

禁止做：

- 生成新的主结论对象
- 替换已有 `result_lifecycle_stage`
- 替换已有 `result_id`

### R-A2 风险提示

允许做：

- 对证据链异常给出人工审阅提示
- 对数据缺口、状态冲突给出风险标签
- 对候选篮、观察期、补跑记录给出说明建议

禁止做：

- 自动宣布某对象“可发布”
- 自动宣布某对象“应回滚”
- 自动宣布某对象“应升格 current”

### R-A3 辅助审阅

允许做：

- 对 release 证据包给出摘要
- 对操作日志、补跑记录给出审阅清单
- 对部署后检查结果给出结构化复核建议

禁止做：

- 替代人工或制度 gate 最终签发
- 直接写 release decision
- 直接写 deploy current 或 baseline current

### R-A4 展示增强

允许做：

- 图表说明
- 页面提示文案
- 导航引导
- FAQ 式说明

禁止做：

- 用展示文案反向裁决主结果
- 用可视化结果替代主链证据

## 3.2 禁止角色

AI 严禁承担以下角色：

- `R-F1 主链 current 改写者`
- `R-F2 lifecycle 裁决者`
- `R-F3 release 决策者`
- `R-F4 deploy / rollback 决策者`
- `R-F5 T12 第二主结果生成者`

---

## 4. AI 输出落点分级

## 4.1 允许落到 L2 的输出

AI 允许落到 `L2 受控说明` 的输出类型包括：

- 主结果解释文案
- 阻断原因重述
- 审核说明摘要
- 执行状态摘要
- 风险说明建议
- 候选篮/观察期说明
- 运维复核清单建议

这些输出必须满足：

- 明确标识为 `AI 解释`、`AI 摘要`、`AI 建议`
- 引用来源字段必须可追溯
- 与 L1 冲突时自动失效

## 4.2 允许落到 L3 的输出

AI 允许落到 `L3 展示性上下文` 的输出类型包括：

- 页面导读
- 图表描述
- FAQ 文本
- tooltip 文案
- 报告摘要段落
- 阅读顺序建议

这些输出必须满足：

- 不改变系统状态
- 不参与任何 gate
- 不反推主结果

## 4.3 严禁落到 L1 的输出

AI 严禁直接生成或改写以下对象：

- `current_result_pointer`
- `result_registry current`
- `run_registry current`
- `artifact_registry current`
- `result_id`
- `run_id`
- `lifecycle_id`
- `artifact_ids`
- `as_of_date`
- `result_lifecycle_stage`
- `audit_status`
- `execution_status`
- `terminal_outcome`
- `stock_entry_guard_latest.json`
- release decision current
- baseline current

硬话直接说：
这些对象一旦允许 AI 直写，主链治理就等于失效。

---

## 5. AI 允许场景

以下场景允许 AI 接入：

### 5.1 `/stock` 的说明层增强

允许：

- 对已确定 primary result 生成解释摘要
- 对阻断状态生成更易读说明
- 对运行态生成人工复核提示

前提：

- L1 已完整
- `stock_entry_guard` 已通过
- AI 只消费受控输入白名单

### 5.2 运维与补跑复核辅助

允许：

- 对补跑记录生成 checklist
- 对运维入口级别给出提示
- 对 evidence bundle 给出阅读摘要

前提：

- 不自动触发脚本
- 不自动批准任何 R3 动作

### 5.3 报告层与展示层增强

允许：

- 日报/周报摘要
- 图表说明
- FAQ 与提示文案

前提：

- 只读输入
- 只写 L2/L3

---

## 6. AI 禁止场景

以下场景一律禁止：

### 6.1 直接决定谁是 current

禁止：

- 让 AI 在候选对象中直接选 current
- 让 AI 根据多份 latest 综合判断“谁更像主结果”
- 让 AI 替代 `run_primary_result_lifecycle.py` 做最终推进

### 6.2 直接决定能否发布

禁止：

- 让 AI 审批 release gate
- 让 AI 认为“看起来没问题”即可发布
- 让 AI 在 evidence 缺失时给出豁免

### 6.3 直接决定能否回滚

禁止：

- 让 AI 单独决定 deploy rollback
- 让 AI 单独决定 baseline rollback
- 让 AI 用摘要替代正式回滚证据

### 6.4 生成第二主结论

禁止：

- 让 `/T12` 通过 AI 形成另一个主结论
- 让报告页通过 AI 形成另一个 current 对象
- 让 AI 以“推荐对象”方式暗中替代 `/stock` 主结果

### 6.5 直接驱动执行

禁止：

- AI 自动写 pointer
- AI 自动改 registry current
- AI 自动调用 R3 动作并提交
- AI 自动覆盖 fail-closed 页面

---

## 7. AI 输入边界

## 7.1 必须白名单化

AI 输入不得使用“把整个 context 全喂进去”的粗暴方式。

必须先做字段白名单。

允许白名单优先包含：

- L1 已冻结字段的只读副本
- L2 受控说明字段
- 明确允许的 L3 展示字段

默认不允许直接整包输入：

- 任意 registry 原文全集
- 任意 deploy secret
- 任意 token / credential
- 任意未脱敏日志

## 7.2 输入必须带来源说明

AI 输入 payload 至少应带：

- 来源模块
- 来源时间
- 来源对象标识
- 输入字段清单

否则后续无法审计“AI 是基于什么说出这段话的”。

## 7.3 输入必须可降级

当 AI 服务异常、模型超时、外部依赖失败时，系统必须仍可：

- 正常展示 L1 主事实
- 正常执行 fail-closed
- 正常做 release / deploy gate

也就是说：
AI 挂了，系统应退化成“解释变差”，而不是“主链失真”。

---

## 8. AI 输出治理要求

## 8.1 必须标识身份

所有 AI 输出必须明确标识至少一个标签：

- `AI 解释`
- `AI 摘要`
- `AI 建议`
- `AI 风险提示`

禁止把 AI 输出伪装成人工结论或制度结论。

## 8.2 必须留痕

每次 AI 输出至少应保留：

- 触发时间
- 输入来源摘要
- 输出版本
- 模型标识
- 生成失败原因
- 是否被人工采用

## 8.3 必须可回收

AI 输出必须允许：

- 独立失效
- 独立隐藏
- 独立替换

不得与 L1 事实链形成不可拆耦合。

## 8.4 必须有人类可复核路径

如果 AI 输出进入以下高敏感区域：

- 发布说明
- 回滚说明
- 阻断说明
- 运维行动建议

则必须有清晰的人类复核路径。

---

## 9. 冲突优先级

当 AI 输出与系统其他层冲突时，优先级固定如下：

1. `L1 主事实`
2. `stock_entry_guard`
3. release / lifecycle / deploy 正式证据
4. `L2` 人工或制度说明
5. AI 输出
6. `L3` 展示内容

结论：

- AI 永远不能压过 L1
- AI 永远不能压过 guard
- AI 永远不能压过正式证据链

---

## 10. 接入前硬门槛

后续任何 AI 接入，在代码落地前必须先满足以下文档门槛：

### 10.1 先补文档

至少先补齐：

- 接入目标
- 输入字段白名单
- 输出落点分级
- 留痕方式
- 失败降级方式
- 人工复核方式

### 10.2 先补测试

至少补以下测试或校验：

- AI 服务失效时主链仍正常
- AI 输出缺失时 `/stock` 仍正常显示 L1
- AI 输出异常时不改主结论
- AI 输出不能触发 pointer 改写
- AI 输出不能绕过 fail-closed

### 10.3 先补阻断条件

出现以下任一情况，接入必须阻断：

- 需要 AI 直接写 `current_result_pointer`
- 需要 AI 直接写 current registry
- 需要 AI 直接决定 release pass/fail
- 需要 AI 直接决定 deploy rollback
- 需要 AI 替代 `/T12` 形成第二主结论

---

## 11. 与现有模块的边界关系

## 11.1 与主结果 query service 的边界

AI 可以消费：

- [primary_result_query_service.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_query_service.py:1) 产出的解释输入

AI 不可以替代：

- `primary_result_query_service` 对 L1 的读取和约束

## 11.2 与 entry guard 的边界

AI 可以读取：

- [stock_entry_guard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/stock_entry_guard.py:1) 的通过/阻断结果做解释

AI 不可以：

- 改 guard 结果
- 绕过 guard 结果
- 用“智能判断”豁免 guard

## 11.3 与 T12 的边界

AI 可以用于：

- T12 只读治理摘要的解释增强

AI 不可以用于：

- 在 `/T12` 上形成第二套 primary conclusion

来源边界参考：

- [t12_governance_summary.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/t12_governance_summary.py:1)

## 11.4 与执行类 agent 的边界

仓库里的 `agent` 命名不等于允许 AI 改写主链。

例如：

- [execution_agent.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/agents/execution_agent.py:1)

它是执行协调组件，不构成“AI 可以直接接管主结果治理”的授权依据。

硬话直接说：
命名叫 `agent`，不代表它拥有主链制度权限。

---

## 12. 反例清单

以下做法看起来聪明，实际上必须禁止：

### 12.1 “让 AI 综合所有 latest 选一个最像 current 的对象”

禁止原因：

- 这会绕开 lifecycle 正式主线
- 这会让 latest 重新扩权

### 12.2 “当 evidence 缺失时，让 AI 给出临时发布意见”

禁止原因：

- 这会绕开 fail-closed
- 这会把 AI 变成 release 决策者

### 12.3 “让 AI 自动决定是否回滚服务器”

禁止原因：

- 这会把部署链治理交给不具制度责任的组件

### 12.4 “让 AI 在 `/T12` 再生成一个推荐对象”

禁止原因：

- 这会形成第二主结论
- 直接破坏 `/stock` 唯一主结论原则

---

## 13. 最低执行口径

如果未来必须快速接入 AI，最低只允许采用以下保守口径：

1. AI 只读
2. AI 只写 L2/L3
3. AI 输出默认带显式标记
4. AI 不参与 pointer / registry / guard / release / deploy current
5. AI 故障时系统自动降级，不影响主链

达不到这五条，就不应该上线 AI 接入。

---

## 14. 下一步文档

本文档之后，如果继续补治理文档，下一份建议优先补：

- `AI 输出留痕与人工复核规范`

原因：

- 边界说明解决“能不能接”
- 留痕与复核规范解决“接了之后怎么管”

---

## 15. 结论

本文档正式冻结以下边界：

- AI 默认不是主结果裁决主体
- AI 默认不得进入 `L1`
- AI 只能做解释、摘要、风险提示、辅助审阅、展示增强
- AI 不得改 pointer，不得改 current registry，不得替代 lifecycle / release / deploy 正式治理链
- AI 故障只能降低体验，不能破坏主链真实性

硬话直接说：
后续谁如果想让 AI 直接选 current、直接批发布、直接判回滚，本质上不是“智能化”，而是在拆主链治理。

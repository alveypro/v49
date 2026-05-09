# 主结果事实源分级表

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 主结果相关字段的事实源分级。

本文档只回答四件事：

- 哪些字段属于 `L1 主事实`
- 哪些字段属于 `L2 受控说明`
- 哪些字段属于 `L3 展示性上下文`
- `/stock`、`/T12`、AI、`latest` 分别只能消费到哪一层

本文档不是代码说明，不是页面设计稿，不是策略解释文档。

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. 本文档
4. 其他字段说明、页面文案、口头判断

---

## 2. 总原则

### 2.1 只有 L1 允许裁决主结论

`/stock` 首页的 primary conclusion，只允许由 `L1 主事实` 决定。

禁止：

- 用 `L2` 说明性字段改写主结论
- 用 `L3` 展示字段反推主结论
- 用页面层逻辑综合多个弱字段“协商”出主结论

### 2.2 L2 只能解释，不能裁决

`L2` 字段允许做：

- 状态解释
- 证据说明
- 阻断说明
- 边界提示

`L2` 字段不允许做：

- 替代 `result_id`
- 替代 lifecycle stage
- 越权决定是否可发布
- 越权决定是否可推进

### 2.3 L3 只服务展示

`L3` 字段允许做：

- 阅读体验增强
- 图形与摘要展示
- 导航、跳转、布局、标签

`L3` 字段不允许做：

- 主结果裁决
- 主结果门禁
- 发布/回滚/补跑准入判断

### 2.4 latest 默认不能进入 L1

`latest` 只允许作为：

- 快捷访问入口
- 索引入口
- convenience alias

`latest` 默认不得直接作为：

- L1 主事实
- 主结果唯一真相源
- 发布最高依据

### 2.5 AI 默认不能进入 L1

AI 输出默认只能落在：

- `L2 受控说明`
- `L3 展示性上下文`

除非未来有单独制度升级文档明确授权，否则：

- AI 不得直接生成 L1 主事实
- AI 不得直接改变 `/stock` 主结论
- AI 不得替代 lifecycle evidence / entry guard / release decision

---

## 3. 分级定义

## 3.1 L1 主事实

定义：

- 决定 primary conclusion 的唯一业务真相层
- 必须可追溯到 pointer / registry / artifact chain
- 必须可被 fail-closed 校验

L1 字段必须满足：

- 有明确主链来源
- 可定位 `result_id / run_id / lifecycle_id / artifact_ids / as_of_date`
- 不依赖页面层推断

## 3.2 L2 受控说明

定义：

- 用于说明 L1 的上下文、阻断、边界和辅助状态
- 可以支持人工理解
- 不能越权替代 L1

L2 字段必须满足：

- 来源清楚
- 语义只解释、不裁决
- 与 L1 冲突时必须让位于 L1

## 3.3 L3 展示性上下文

定义：

- 页面结构、阅读体验、图表、链接、视觉表达
- 允许来自聚合和格式化
- 不得影响业务裁决

---

## 4. L1 主事实清单

以下字段属于 `L1 主事实`：

### 4.1 主结果身份字段

- `result_id`
- `run_id`
- `lifecycle_id`
- `artifact_ids`
- `as_of_date`

### 4.2 主结果主体字段

- `ts_code`
- `stock_name`
- `result_lifecycle_stage`
- `result_type`

### 4.3 制度约束字段

- `disabled_reason`
- `invalid_reason`
- `audit_status`
- `execution_status`
- `terminal_outcome`

### 4.4 主链校验字段

- `current_result_pointer` 全量身份字段
- `result_registry` 当前登记记录
- `run_registry` 当前登记记录
- `artifact_registry` 对应 artifact chain
- `primary_result_lifecycle_evidence_latest.json`
- `stock_entry_guard_latest.json`

### 4.5 L1 正式来源

L1 只允许来自以下来源组合：

1. [current_result_pointer.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/current_result_pointer.py:1)
2. [unified_result_builder.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/unified_result_builder.py:1)
3. `result_registry`
4. `run_registry`
5. `artifact_registry`
6. `primary_result_lifecycle_evidence`
7. `stock_entry_guard`

### 4.6 L1 禁止来源

以下来源不得直接进入 L1：

- `*_latest.json` 便利文件
- `candidate_cards`
- `market_snapshot`
- `backtest_diagnosis`
- `/T12` 展示摘要
- AI 生成摘要
- 页面层拼接后的字符串

---

## 5. L2 受控说明清单

以下字段属于 `L2 受控说明`：

### 5.1 主结果解释层

- `headline_tone`
- `headline_detail`
- `summary_lines`
- `data_sync_note`
- `history_source_file`
- `history_generation_mode`

来源参考：

- [primary_result_query_service.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_query_service.py:1)

### 5.2 阻断与执行解释层

- `decision_semantics`
- `blocker_semantics`
- `execution_semantics`
- `evidence_semantics`
- `governance_semantics`

说明：

- 这些字段可用于说明“为什么当前是这个结论”
- 不能替代 L1 决定“当前主结论是什么”

### 5.3 候选篮与观察层说明

- `current_basket_pointer_status`
- `current_basket_pointer_basket_id`
- `current_basket_pointer_updated_at`
- `latest_basket_attempt_status`
- `latest_basket_attempt_generated_at`
- `latest_basket_attempt_blocking_reason`
- `observation_wait_status`
- `daily_closure_latest`

说明：

- 这些字段可解释候选篮和观察窗口状态
- 不能替代主结果 pointer

### 5.4 T12 治理摘要允许读取的字段

依据 [t12_governance_summary.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/t12_governance_summary.py:1)，T12 只允许读取以下治理摘要事实：

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

说明：

- 这些字段在 `/T12` 中只能做治理摘要
- `/T12` 不得基于这些字段生成第二主结论

### 5.5 L2 禁止越权动作

- 不得覆盖 `result_lifecycle_stage`
- 不得覆盖 `disabled_reason / invalid_reason`
- 不得覆盖 entry guard 结果
- 不得独立决定 release / rollback / promotion

---

## 6. L3 展示性上下文清单

以下字段属于 `L3 展示性上下文`：

### 6.1 页面与视觉层

- `topbar_pills`
- `nav_items`
- `jump strip`
- `view_banner`
- `hero_side`
- `spotlight`
- `command deck`
- `architecture_steps`

### 6.2 图形与摘要层

- `health_chart_html`
- `backtest_equity_html`
- `backtest_drawdown_html`
- `candidate_chart_html`
- `candidate_map_chart_html`
- `market_snapshot_section`
- `research_visuals_section`

### 6.3 阅读与导出层

- `links_section`
- `guide_section`
- `reports` 导航与导出按钮
- `table export` 相关按钮与容器

### 6.4 说明

L3 允许：

- 格式化
- 聚合
- 排版
- 图形展示

L3 不允许：

- 决定主结果是否通过
- 决定是否 fail-closed
- 反推主结果阶段

---

## 7. `/stock` 消费边界

`/stock` 允许消费：

- `L1`：用于唯一主结论
- `L2`：用于解释、边界、阻断、证据说明
- `L3`：用于展示、导航、图表

但 `/stock` 必须遵守：

1. 主结论只能来自 `L1`
2. `L2` 与 `L1` 冲突时，以 `L1` 为准
3. `L3` 永远不能参与裁决

`/stock` 当前正式读路径应理解为：

- pointer-first 主事实：来自 [unified_result_builder.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/unified_result_builder.py:1)
- query service 解释层：来自 [primary_result_query_service.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_query_service.py:1)
- 页面 render/composer：只消费结果，不新增事实

---

## 8. `/T12` 消费边界

`/T12` 只允许消费：

- `L2` 的治理摘要子集
- 少量 `L1` 状态字段的只读镜像

`/T12` 不允许消费或形成：

- 第二主结论
- 主结果替代解释权
- 推进行为
- 写回行为

硬边界：

- `/T12` 是只读治理镜像
- `/T12` 不是第二业务工作台
- `/T12` 不得通过聚合字段反推主结果裁决

---

## 9. `latest` 落位边界

`latest` 文件允许落位：

- `L2`：当它只是解释性便利入口时
- `L3`：当它只是展示性快捷访问入口时

`latest` 文件默认不得落位：

- `L1`

例外条件：

- 只有当某 `latest` 文件本身就是由正式主链产物生成的只读 alias，并且其身份字段已被 pointer/registry 完整约束时，才允许在实现层被读取。
- 即便如此，它也不能拥有比 pointer 更高的裁决权。

硬规则：

- `latest` 可以指向事实
- `latest` 不可以变成事实本身

---

## 10. AI 落位边界

AI 输出只允许进入：

- `L2 受控说明`
- `L3 展示性上下文`

AI 可以做：

- 文本摘要
- 风险提示
- 异常提示
- 研究辅助说明
- 人工复核建议

AI 不可以做：

- 直接生成 `result_id`
- 直接决定 `result_lifecycle_stage`
- 直接决定 `disabled_reason / invalid_reason`
- 直接决定 release / rollback / promotion
- 直接替代 entry guard
- 直接替代 lifecycle evidence

AI 接入前必须先补：

- AI 接入边界说明
- AI 输出落位表
- AI 输出字段级白名单

---

## 11. 字段冲突处理规则

### 11.1 L1 vs L2 冲突

处理规则：

- `L1` 胜出
- `L2` 降级为解释性文本
- 页面必须显式保留阻断/差异说明，不得静默覆盖

### 11.2 L1 vs L3 冲突

处理规则：

- `L3` 必须完全让位
- 必要时直接隐藏冲突展示块

### 11.3 L2 vs L3 冲突

处理规则：

- `L2` 胜出
- `L3` 只能重排展示，不能改语义

---

## 12. 阻断规则

以下情况必须阻断继续推进相关改动：

- 计划把 `latest` 提升到 L1
- 计划让 AI 输出进入 L1
- 计划让 `/T12` 生成第二主结论
- 计划让页面层聚合逻辑重新裁决主结果
- 计划让 L2/L3 字段直接驱动 release / promotion / execution

默认动作：

- 停止代码变更
- 先改文档
- 先补边界说明
- 进入人工复核

---

## 13. 下一步衔接

本文档冻结后，后续高风险变更必须先回答：

1. 这个字段属于 `L1 / L2 / L3` 哪一层
2. 它的正式来源是什么
3. 它是否会影响 `/stock` 主结论
4. 它是否会影响 `/T12` 边界
5. 它是否允许 AI 生成或放大

如果答不清，禁止继续改代码。

下一份建议补的文档是：

- `运维入口地图 / 补跑分级说明`

---

## 14. 阶段结论

当前结论：

- 主结果事实源必须按 `L1 / L2 / L3` 分级管理
- `/stock` 只能用 `L1` 裁决主结论
- `/T12` 只能读取受限治理摘要
- `latest` 不得重新扩权为 L1
- AI 默认不得进入 L1

后续改公式、接 AI、补跑 latest、扩展页面前，必须先对照本分级表判断是否越界。

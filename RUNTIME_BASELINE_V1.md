# RUNTIME_BASELINE_V1.md

## Airivo Runtime Baseline v1

版本：v1.0  
状态：冻结候选稿  
性质：运行制度基线文件  
适用范围：Airivo 运行制度文档、结果对象制度、状态字段制度、分类字段制度、终局处置制度、显示映射制度、运行字段主从关系

---

## 1. 文档目的

本文件用于将 Airivo 当前已完成的运行制度文件冻结为 Runtime Baseline v1，作为后续运行制度讨论、页面表达校验、字段落地和实现准入的统一基线。

本文件不新增运行制度，不重写既有制度。  
本文件只回答以下问题：

- Runtime Baseline v1 包含哪些文件
- 本轮基线解决了哪些混层问题
- 哪些规则已正式冻结
- 后续哪些修改必须走变更流程

---

## 2. 基线包含文件

Runtime Baseline v1 包含以下文件：

- `RESULT_LAYER_POLICY.md`
- `STATUS_ENUMS.md`
- `LIFECYCLE_DISPLAY_MAPPING.md`
- `TERMINAL_OUTCOME_POLICY.md`
- `RUNTIME_SOURCE_OF_TRUTH.md`

上述文件共同构成 Airivo 的运行制度最小闭环。

---

## 3. 文件职责分工

### 3.1 `RESULT_LAYER_POLICY.md`

职责：定义结果生命周期制度。  
回答：结果处于哪一制度阶段，五层结果制度如何进入、退出和流转。

### 3.2 `STATUS_ENUMS.md`

职责：定义状态集合与状态口径。  
回答：对象当前处于什么状态，各状态字段的可选值、解释、使用场景和流转约束。

### 3.3 `LIFECYCLE_DISPLAY_MAPPING.md`

职责：定义制度层与显示层映射。  
回答：内部生命周期制度名称如何映射为 `/stock` 与 `/T12` 的显示表达。

### 3.4 `TERMINAL_OUTCOME_POLICY.md`

职责：定义终局处置制度。  
回答：对象离开主生命周期推进路径后，最终如何结案。

### 3.5 `RUNTIME_SOURCE_OF_TRUTH.md`

职责：定义运行字段主从关系。  
回答：谁是主字段，谁是状态字段，谁是分类字段，谁是辅助字段，以及冲突时以谁为准。

---

## 4. 本轮基线解决的混层问题

Runtime Baseline v1 已解决以下核心混层问题：

### 4.1 生命周期层级、状态枚举、终局处置的三维混写问题

本轮基线已明确分离：

- 生命周期层级：回答“结果处于哪一制度阶段”
- 状态枚举：回答“对象当前处于什么状态”
- 终局处置：回答“对象最终如何结案”

三者不得混写，不得互相替代。

### 4.2 `/stock` 显示名与状态字段混层问题

本轮基线已修订 `/stock` 的 L3、L4 显示表达：

- L3 使用“审核阶段结果 / 待审核结果”
- L4 使用“执行阶段结果 / 待执行结果”

并允许使用双轨写法：

- 审核阶段结果（审核中）
- 执行阶段结果（运行中）

从而避免生命周期显示名直接占用 `audit_status` 和 `execution_status` 的状态语义。

### 4.3 生命周期主字段不明确问题

本轮基线已正式冻结生命周期主字段名称：

- `result_lifecycle_stage`

并明确其为运行制度中的一级主字段。

### 4.4 `result_type` 与生命周期层级混层问题

本轮基线已明确：

- `result_type` 保留
- `result_type` 只属于分类字段
- `result_type` 不属于状态字段
- `result_type` 仅用于分类、检索、分组、统计
- `result_type` 不参与生命周期判断
- `result_type` 不参与终局判断
- `result_type` 不参与流程推进判断

### 4.5 `promotion_status` 被误升格为主字段的问题

本轮基线已明确：

- `promotion_status` 是辅助状态字段
- `promotion_status=promoted` 不自动触发主层级切换
- 主层级切换只能由 `result_lifecycle_stage` 记录

### 4.6 L5 与 `archived` 混同问题

本轮基线已明确：

- L5 是生命周期层级
- `archived` 是终局处置
- L5 不等于 `archived`
- `archived` 不是生命周期层级值

---

## 5. 已正式冻结的规则

以下规则在 Runtime Baseline v1 中视为正式冻结：

### 5.1 结果生命周期五层制度

正式冻结如下：

- L1 研究层结果
- L2 候选层结果
- L3 审核层结果
- L4 执行层结果
- L5 沉淀层结果

### 5.2 生命周期主字段名称

正式冻结如下：

- `result_lifecycle_stage`

### 5.3 状态字段制度

以下字段作为正式状态字段制度冻结：

- `research_status`
- `candidate_status`
- `signal_level`
- `risk_level`
- `audit_status`
- `promotion_status`
- `execution_status`
- `rollback_status`

说明：  
状态字段负责表达运行状态，不负责表达生命周期主层级。

### 5.4 分类字段制度

以下字段作为正式分类字段制度冻结：

- `result_type`

说明：  
`result_type` 只属于分类字段，不属于状态字段。  
`result_type` 仅用于分类、检索、分组、统计，不参与生命周期判断、终局判断、流程推进判断。

### 5.5 终局处置标准类型

正式冻结如下：

- `rejected`
- `expired`
- `failed`
- `cancelled`
- `archived`
- `superseded`

### 5.6 主从关系规则

正式冻结如下：

- `result_lifecycle_stage` 是主字段
- 状态字段是从字段
- `result_type` 是分类字段
- `promotion_status` 是辅助状态字段
- 冲突时以 `result_lifecycle_stage` 为最高事实来源

### 5.7 显示映射规则

正式冻结如下：

- `/stock` 优先采用业务可理解表达
- `/T12` 优先采用制度可审查表达
- `/stock` 允许采用“生命周期显示名 + 状态附加显示”的双轨写法
- 显示层不得反向重写制度层

---

## 6. 后续必须走变更流程的事项

以下修改不得视为普通文案调整，必须走正式变更流程：

### 6.1 生命周期制度变更

- 增减生命周期层级数量
- 修改 L1-L5 的正式名称
- 修改生命周期进入条件或退出条件
- 修改生命周期标准流转逻辑

### 6.2 主字段变更

- 修改 `result_lifecycle_stage` 的字段名称
- 允许其他字段替代 `result_lifecycle_stage`
- 允许多个字段同时承担主层级定义权

### 6.3 分类字段边界变更

- 让 `result_type` 参与生命周期判断
- 让 `result_type` 参与终局判断
- 让 `result_type` 参与流程推进判断

### 6.4 辅助字段边界变更

- 让 `promotion_status` 具备主层级定义作用
- 让 `promotion_status=promoted` 自动等于主层级切换完成

### 6.5 终局处置制度变更

- 增减标准终局处置类型
- 修改 `rejected`、`expired`、`failed`、`cancelled`、`archived`、`superseded` 的正式定义
- 修改各生命周期层级到终局处置的标准映射

### 6.6 显示映射规则变更

- 将 `/stock` 的显示名直接改回状态字段语义
- 取消 `/stock` 的双轨写法能力
- 用前台显示名替代治理制度正式名称

---

## 7. 基线执行原则

Runtime Baseline v1 执行时，应遵循以下原则：

- 先看制度层，再看显示层
- 先看主字段，再看状态字段
- 先看生命周期层级，再看终局处置
- 先看冻结规则，再讨论实现便利

不得为了实现简化而破坏制度分层。

---

## 8. 结论

Airivo Runtime Baseline v1 已经完成运行制度层的最小冻结。  
其核心成果不是增加了更多字段，而是明确了：

- 哪些概念属于生命周期
- 哪些概念属于状态
- 哪些概念属于终局
- 哪些字段是主字段
- 哪些字段只是辅助字段
- 哪些显示表达可以使用
- 哪些混层做法必须禁止

后续任何进入实现层的工作，都应以本基线为运行制度依据。

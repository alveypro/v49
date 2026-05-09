# RUNTIME_SOURCE_OF_TRUTH.md

## Airivo 运行字段主从关系规范

版本：v1.1  
状态：冻结前修订稿  
性质：运行制度字段主从关系文件  
适用范围：Airivo 结果对象、候选对象、审核对象、执行对象及相关运行元数据

---

## 1. 文档目的

本文件用于明确 Airivo 运行制度中各类字段的主从关系，回答以下问题：

- 哪些字段是主字段
- 哪些字段是派生字段
- 哪些字段属于分类字段
- 哪些字段属于辅助字段
- 生命周期字段、状态字段、分类字段、辅助字段之间如何避免冲突

本文件不定义平台职责边界，不定义生命周期制度本身，不定义状态集合本身。  
本文件只定义**运行字段的主从关系与使用边界**。

---

## 2. 基本原则

### 2.1 单一主语义原则

每个对象在每一类制度语义上，应存在唯一主字段。  
不得用多个字段同时表达同一主语义。

### 2.2 主字段优先原则

当主字段与派生字段冲突时，以主字段为准。  
派生字段必须服从主字段，不得反向重写主字段。

### 2.3 分类与流程分离原则

分类字段用于说明“是什么类型”。  
流程字段用于说明“处于什么阶段/状态”。  
两者不得互相替代。

### 2.4 辅助状态不升格原则

辅助字段可以帮助判断和显示，但不得与主字段竞争解释权。

---

## 3. 字段分类

Airivo 运行字段分为四类：

1. 生命周期主字段
2. 状态字段
3. 分类字段
4. 辅助字段

---

## 4. 生命周期主字段

### 4.1 主字段定义

生命周期主字段用于表达对象当前所属的**结果生命周期层级**。

### 4.2 主字段正式命名

生命周期主字段正式命名为：

- `result_lifecycle_stage`

该字段是运行制度中的一级主字段。

### 4.3 主字段地位

`result_lifecycle_stage` 负责回答：

- 对象当前处于哪一制度阶段

### 4.4 使用约束

`result_lifecycle_stage` 不得由以下字段替代：

- `result_type`
- `promotion_status`
- `audit_status`
- `execution_status`
- `rollback_status`

这些字段可以辅助说明过程，但不能重写对象当前主生命周期层级。

主层级切换只能由 `result_lifecycle_stage` 明确记录。

---

## 5. 状态字段

### 5.1 定义

状态字段用于表达对象当前所处的离散运行状态。

### 5.2 范围

状态字段包括但不限于：

- `research_status`
- `candidate_status`
- `signal_level`
- `risk_level`
- `audit_status`
- `promotion_status`
- `execution_status`
- `rollback_status`

### 5.3 地位

状态字段是二级运行字段。  
它们服务于判断、控制、显示和留痕，但不决定主生命周期层级。

### 5.4 使用约束

状态字段之间可以互相参考，但不得联合重写 `result_lifecycle_stage`。

例如：

- `audit_status=passed` 不自动等于对象已进入 L4
- `promotion_status=promoted` 不自动等于对象主层级已经更新
- `execution_status=completed` 不自动等于对象已进入 L5

主层级变更必须由 `result_lifecycle_stage` 明确表达。

---

## 6. 分类字段

### 6.1 `result_type` 是否保留

建议保留 `result_type`，但严格限定用途。

### 6.2 `result_type` 的用途

`result_type` 只用于以下用途：

- 分类
- 检索
- 分组
- 统计

它用于表达结果对象的分类语义，例如：

- 研究类结果
- 候选类结果
- 审核类结果
- 执行类结果
- 归档类结果

### 6.3 `result_type` 的限制

`result_type` 不得用于表达：

- 当前生命周期层级
- 当前审核状态
- 当前执行状态
- 当前是否已晋升
- 当前终局处置
- 当前流程是否推进

换言之：

- `result_type` 是分类字段
- 不是流程字段
- 不是主字段
- 不参与生命周期判断
- 不参与终局判断
- 不参与流程推进判断

### 6.4 主从关系

若 `result_type` 与 `result_lifecycle_stage` 冲突，以 `result_lifecycle_stage` 为准。  
必要时应修正 `result_type`，但不得由 `result_type` 反向裁定主层级。

---

## 7. 辅助字段

### 7.1 辅助字段定义

辅助字段用于补充说明对象当前是否具备某种条件、资格或控制要求。

### 7.2 `promotion_status` 的定位

`promotion_status` 应明确定位为**辅助状态字段**，而不是生命周期主字段。

它只回答：

- 是否具备晋升资格
- 晋升判断是否处理中
- 晋升是否完成或被驳回

它不回答：

- 当前对象主层级已经是什么

### 7.3 `promotion_status` 的使用约束

以下做法原则上禁止：

- 用 `promotion_status=promoted` 直接替代 `result_lifecycle_stage` 更新
- 用 `promotion_status=eligible` 表示对象已经进入下一层
- 用 `promotion_status` 单独定义对象制度位置

`promotion_status=promoted` 不自动触发主层级切换。  
若对象已晋升，必须由 `result_lifecycle_stage` 同步明确表达新层级。  
否则，`promotion_status` 只能表示辅助判断，不表示主层级事实。

---

## 8. 主从关系总表

| 字段类别 | 字段名 | 作用 | 是否主字段 | 是否可派生他字段 | 冲突时以谁为准 |
|------|------|------|------|------|------|
| 生命周期主字段 | `result_lifecycle_stage` | 定义结果当前制度阶段 | 是 | 可派生部分显示字段 | `result_lifecycle_stage` |
| 状态字段 | `research_status` / `candidate_status` / `signal_level` / `risk_level` / `audit_status` / `promotion_status` / `execution_status` / `rollback_status` | 定义当前运行状态 | 否 | 可派生显示与控制信号 | `result_lifecycle_stage` |
| 分类字段 | `result_type` | 定义结果分类 | 否 | 可派生检索与过滤标签 | `result_lifecycle_stage` |
| 辅助字段 | `promotion_status` 等 | 定义资格、条件、辅助控制 | 否 | 可派生提醒与辅助判断 | `result_lifecycle_stage` |

---

## 9. 派生关系说明

### 9.1 可由主字段派生的内容

`result_lifecycle_stage` 可以派生：

- 前台显示名称
- 某些默认流程提示
- 某些审核入口建议
- 某些展示标签

### 9.2 不可反向派生的内容

以下字段不得反向定义主生命周期层级：

- `result_type`
- `promotion_status`
- `audit_status`
- `execution_status`
- `rollback_status`

### 9.3 派生冲突处理

若出现以下冲突：

- `result_lifecycle_stage` 与 `promotion_status` 冲突
- `result_lifecycle_stage` 与 `result_type` 冲突
- `result_lifecycle_stage` 与显示名冲突

统一以 `result_lifecycle_stage` 为最高事实来源。

---

## 10. 推荐标准口径

建议统一使用以下表达：

- `result_lifecycle_stage` 是生命周期主字段
- 状态字段是运行状态字段
- `result_type` 是分类字段，不是流程字段
- `promotion_status` 是辅助状态字段，不是生命周期主层级
- 显示字段应从主字段和制度字段派生，而不应反向定义制度事实

---

## 11. 禁止混用规则

以下做法原则上禁止：

- 用 `result_type` 代替 `result_lifecycle_stage`
- 用 `promotion_status` 代替 `result_lifecycle_stage`
- 用状态字段决定平台职责边界
- 用显示标签反向定义制度事实
- 让多个字段同时承担“主层级定义权”

### 明确禁止的错误表达

- “因为 `result_type=audit`，所以对象当前一定在 L3”
- “因为 `promotion_status=promoted`，所以对象已经完成主层级迁移”
- “因为 `execution_status=completed`，所以对象已经自动沉淀为 L5”

这些推断都不应自动成立，除非 `result_lifecycle_stage` 已同步更新。

---

## 12. 结论

Airivo 运行制度中的字段关系必须长期稳定如下：

- `result_lifecycle_stage`：定义制度阶段，是主字段
- 状态字段：定义运行状态，是从字段
- `result_type`：定义对象类型，只用于分类、检索、分组、统计
- `promotion_status`：定义辅助晋升状态，不能与主层级冲突

运行制度稳定的前提，不是字段越多越好，而是谁是主字段、谁是派生字段必须长期清晰。

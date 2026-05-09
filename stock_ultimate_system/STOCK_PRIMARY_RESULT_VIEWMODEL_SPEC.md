# STOCK_PRIMARY_RESULT_VIEWMODEL_SPEC.md

## `/stock` 主结果 ViewModel 规范

版本：v1.0  
状态：冻结版  
性质：执行级 ViewModel 规范  
适用范围：`airivo.online/stock` 主结果页面、主结果卡、主结果展示层

---

## 1. 文档目的

本文件用于把 `/stock` 主结果契约推进为可执行的 ViewModel 规范，确保后续代码实现时：

- 字段结构稳定
- 结论层、解释层、边界说明层不混叠
- 空态、异常态、降级态输出统一
- 治理字段和主站字段不会误入主结果中心

---

## 2. 总体原则

`/stock` 主结果 ViewModel 只属于展示层，不替代统一事实层，不反向定义制度事实。

ViewModel 必须遵守三层结构：

1. 结论层
2. 解释层
3. 边界说明层

任何字段都必须先判断其归属层，再决定是否允许进入主结果 ViewModel。

---

## 3. 固定 ViewModel 结构

推荐固定结构如下：

```text
StockPrimaryResultViewModel
- conclusion
- explanation
- boundary
```

其中三层再按固定子字段展开。

---

## 4. 结论层

结论层用于回答：

- 当前对象是谁
- 当前主结果是什么
- 当前主阶段是什么
- 当前核心风险提示是什么

推荐必备字段：

```text
conclusion
- target_code
- target_name
- primary_stage_label
- primary_stage_secondary_label
- primary_result_label
- risk_label
```

说明：

- `primary_stage_label` 是正式主标题
- `primary_stage_secondary_label` 只能是次级说明，不得反向定义主阶段
- `primary_result_label` 用于表达业务主结论
- `risk_label` 只能表达业务风险提示，不得单独升级为制度阻断

---

## 5. 解释层

解释层用于回答：

- 当前主结果为什么成立
- 当前同步情况如何
- 当前历史验证如何
- 当前禁用/失效解释是什么

推荐必备字段：

```text
explanation
- sync_note
- source_timestamp
- history_visible
- history_items
- disabled_visible
- disabled_text
- invalid_visible
- invalid_text
```

说明：

- `history_items` 只服务解释，不得反向定义主结果
- `disabled_text` 与 `invalid_text` 是解释性说明，不是治理操作入口
- 解释层必须服从结论层，不能压过主结论

---

## 6. 边界说明层

边界说明层用于回答：

- 当前页面的边界是什么
- 哪些内容只作辅助理解
- 哪些内容不替代制度判断

推荐必备字段：

```text
boundary
- scope_note
- reference_note
- governance_boundary_note
```

说明：

- 这里只允许做轻量边界提示
- 不允许把 `/T12` 的治理摘要区搬进来
- 不允许在边界说明层中承载治理系统本体

---

## 7. 必须字段

必须字段最少应包括：

- `conclusion.target_code`
- `conclusion.primary_stage_label`
- `conclusion.primary_result_label`
- `conclusion.risk_label`
- `explanation.sync_note`
- `boundary.scope_note`

这些字段缺失时，页面可以降级，但 ViewModel 结构不能失控。

---

## 8. 可选字段

允许作为可选字段存在：

- `conclusion.target_name`
- `conclusion.primary_stage_secondary_label`
- `explanation.source_timestamp`
- `explanation.history_items`
- `explanation.disabled_text`
- `explanation.invalid_text`
- `boundary.reference_note`
- `boundary.governance_boundary_note`

可选字段缺失时，不允许通过自由拼接临时文案补位。

---

## 9. 禁止混入字段

以下字段或能力禁止直接混入 `/stock` 主结果 ViewModel：

- Governance Summary 三元摘要本体
- `/T12` 只读治理摘要区
- 治理动作入口
- 平台入口导航文案
- 主站品牌层主叙事
- 任意控制台能力字段

若字段主要服务治理系统或主站系统，应留在各自系统中表达。

---

## 10. 空态原则

空态指事实存在不足，但页面仍可正常渲染。

要求：

- 保持结构完整
- 不用自由文本替代缺失态
- 缺失文案必须走集中映射

允许的缺失值：

- `暂缺`
- `待补充`
- `降级说明`

---

## 11. 异常态原则

异常态指：

- 数据源读取失败
- 关键字段解析失败
- 解释性字段无法可靠生成

要求：

- 优先保住结论层结构
- 解释层进入降级态
- 不把异常态包装成新的业务结论
- 不把异常态交给模板层做判断

---

## 12. 降级态原则

降级态用于处理：

- 历史验证不足
- 同步字段不完整
- 边界说明只能提供轻量提示

要求：

- 降级只影响解释层与边界说明层
- 降级不能重写结论层
- 降级文案必须可预测、可测试、可回滚

---

## 13. 结论

`/stock` 主结果 ViewModel 必须长期保持：

- 三层结构固定
- 结论层优先
- 解释层辅助
- 边界说明层轻量
- 治理系统与主站系统不混入

这是后续代码落地和长期维护的最低结构约束。

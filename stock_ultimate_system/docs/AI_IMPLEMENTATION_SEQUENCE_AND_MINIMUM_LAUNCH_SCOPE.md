# AI 接入实施顺序与最小上线范围

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 的 AI 接入实施顺序、第一批最小上线范围、明确不接入范围、以及每一阶段的停线条件。

本文档只回答五件事：

- 第一批 AI 到底先接哪一小块
- 哪些功能明确不能进第一批
- 实施顺序必须怎么排
- 每个阶段做到什么程度就该停
- 什么时候从“文档治理”转入“实现与测试”

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. [OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md:1)
5. [AI_INTEGRATION_BOUNDARY_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_BOUNDARY_SPEC.md:1)
6. [AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md:1)
7. [AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md:1)
8. [AI_INTEGRATION_TEST_GATES_AND_BLOCKING_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_TEST_GATES_AND_BLOCKING_CHECKLIST.md:1)
9. [AI_FEATURE_FLAGS_DEGRADATION_AND_ROLLBACK_STRATEGY.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_FEATURE_FLAGS_DEGRADATION_AND_ROLLBACK_STRATEGY.md:1)
10. [AI_RUNTIME_MONITORING_AND_AUDIT_DASHBOARD_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_RUNTIME_MONITORING_AND_AUDIT_DASHBOARD_SPEC.md:1)
11. 本文档

本文档不是 roadmap 幻灯片，不是产品愿景，不是长期研究方向表。

---

## 2. 总原则

### 2.1 第一批必须极小

AI 第一批上线范围必须小到满足两件事：

- 一旦出问题可以快速关掉
- 即使全部关掉也不影响主链治理

### 2.2 先解释，后建议，最后才考虑高敏感

正式顺序只能是：

1. 解释层
2. 展示增强层
3. 低风险建议层
4. 高敏感复核辅助层

禁止反过来。

### 2.3 先 `/stock`，后 `/T12`，再考虑运维

第一批不能把页面解释、治理解释、运维建议一起推。

正式顺序必须先从最小、最容易隔离、最容易退回非 AI 的位置开始。

### 2.4 先做“可关可退”，再做“更聪明”

如果某一块还没有：

- 独立开关
- 独立留痕
- 独立测试门
- 独立降级回退

就不该进入当前实施阶段。

---

## 3. 第一批最小上线范围

## 3.1 正式建议的第一批

第一批最小上线范围只允许包含：

- `/stock` 的 A0/A1 解释摘要增强

更具体地说，只允许：

- 对已确定 primary result 生成说明性摘要
- 对阻断原因做更易读重述
- 对运行态做说明性提示

第一批明确不允许：

- 运维建议
- 发布建议
- 回滚建议
- `/T12` AI 解释
- 报告层 AI 摘要
- 候选推荐
- 任何 A2/A3 输出

硬话直接说：
第一批如果不够小，后面出事时你根本分不清是谁惹的祸。

## 3.2 第一批允许的输入与输出

第一批只能使用：

- 输入层：`I1`
- 输出 schema：`O1`

且只允许：

- `F0`
- `F1-stock-explainer`
- `F2-A0-display`
- `F2-A1-explainer`

默认关闭：

- `F1-t12-explainer`
- `F1-ops-review-assistant`
- `F1-report-enhancer`
- `F2-A2-advisor`
- `F2-A3-sensitive-review`

## 3.3 第一批停线条件

第一批做到以下程度就必须先停，不再扩范围：

- `/stock` A0/A1 已完成
- 输入白名单门通过
- 输出 schema 门通过
- fail-closed 保真门通过
- 可独立开关、可独立降级、可独立回退
- 已能看到运行期监控与审计数据

到这一步就该暂停扩张，先观察稳定性。

---

## 4. 第二批允许范围

## 4.1 第二批前提

只有在第一批满足以下条件后，才允许进入第二批：

- 第一批连续稳定
- 无治理越界
- 无 A-Critical 告警
- 无连续人工误导标记
- 降级/回退链路已实跑验证

## 4.2 第二批建议范围

第二批只允许新增一项：

- `/T12` 的 A0/A1 治理摘要解释

第二批仍不允许：

- A2/A3
- 运维复核建议
- 发布/回滚说明建议
- 报告增强

## 4.3 第二批停线条件

做到以下程度就该停：

- `/T12` 输入边界稳定
- 不产生第二主结论
- 不越界到动作建议
- `/stock + /T12` 双场景可独立回退

---

## 5. 第三批允许范围

## 5.1 第三批前提

只有在第二批稳定后，才允许考虑第三批。

最低前提：

- `/stock` 与 `/T12` 已连续稳定
- review backlog 受控
- 运行期看板可证明没有边界持续被撞

## 5.2 第三批建议范围

第三批才允许考虑：

- 运维复核辅助中的 A2 建议级输出

但必须满足：

- 只进运维复核辅助
- 必须 `needs_manual_review=true`
- 不得自动触发 R3
- 不得自动影响 release gate

## 5.3 第三批仍明确禁止

即使进入第三批，仍不允许：

- A3 高敏感输出上线
- 发布建议自动展示
- 回滚建议自动展示
- 候选推荐
- 第二主结论类输出

---

## 6. 明确不接入范围

以下范围在当前阶段明确不接：

### 6.1 不接主链裁决

不接：

- current 选择
- pointer 改写
- registry current 改写
- lifecycle 决策

### 6.2 不接发布与回滚决策

不接：

- release pass/fail 建议作为正式结论
- rollback 建议作为正式结论
- baseline promote 建议

### 6.3 不接高敏感自动建议

不接：

- A3 自动展示
- 未复核高敏感输出
- 任何带动作执行语义的建议

### 6.4 不接跨场景大一统 AI 层

不接：

- 一个总 prompt 覆盖 `/stock + /T12 + 运维 + 报告`
- 一个总输出 schema 覆盖所有场景

硬话直接说：
这些东西不是“以后再优化”，而是当前阶段明确禁止做。

---

## 7. 实施顺序

正式实施顺序只能是：

1. 第一批 `/stock` A0/A1
2. 观察稳定
3. 第二批 `/T12` A0/A1
4. 再观察稳定
5. 第三批 运维 A2

禁止顺序：

- 先做运维建议
- 先做 A3
- 先做报告层
- 先做候选推荐

---

## 8. 每阶段完成标准

## 8.1 第一批完成标准

- 功能只在 `/stock`
- 只有 A0/A1
- 只读 L1/L2 副本
- 可独立关闭
- 出问题可一键回到非 AI 说明

## 8.2 第二批完成标准

- `/T12` 仍严格只读
- 不形成第二主结论
- 与 `/stock` 场景完全独立

## 8.3 第三批完成标准

- 运维建议全部进入人工复核
- 无自动执行
- 无 R3 触发

---

## 9. 从文档转入实现的门槛

到这里要不要继续写文档，判断标准只有一个：

- 当前缺的是治理规则，还是缺实现与测试

如果以下内容都已经齐：

- 边界
- 留痕
- 白名单
- 测试门
- 开关回退
- 运行期监控
- 实施顺序

那就不该继续堆文档，而该转入：

- 最小实现
- 最小测试
- 最小灰度验证

硬话直接说：
再往下写文档，如果没有立刻转实现，大概率就是文档堆文档。

---

## 10. 上线前最低检查清单

第一批实现前至少确认：

- [ ] 只做 `/stock`
- [ ] 只做 A0/A1
- [ ] 不触碰 `/T12`
- [ ] 不触碰运维建议
- [ ] 不触碰 A2/A3
- [ ] 已定义独立开关
- [ ] 已定义独立降级与回退
- [ ] 已定义最小监控项
- [ ] 已定义最小自动化门

第二批实现前至少确认：

- [ ] 第一批已稳定
- [ ] 无治理越界
- [ ] `/T12` 只读边界测试已准备

第三批实现前至少确认：

- [ ] `/stock + /T12` 已稳定
- [ ] review 链路已可控
- [ ] A2 建议输出不会诱导越权动作

---

## 11. 最低执行口径

如果现在开始真正做 AI 实现，最低只允许采用以下口径：

1. 先实现第一批 `/stock` A0/A1
2. 做完就停
3. 跑测试门
4. 看运行期监控
5. 稳定后再决定要不要开第二批

做不到这五条，就不应该进入生产实现。

---

## 12. 收口结论

本文档写完后，AI 治理基线到这里可以阶段性收口。

原因很简单：

- 能不能接，已经写了
- 接了怎么管，已经写了
- 喂什么吐什么，已经写了
- 不过门怎么拦，已经写了
- 出问题怎么降怎么退，已经写了
- 上线后怎么盯，已经写了
- 现在连第一批到底接哪一小块，也已经写了

再往下如果还不转实现，就会开始明显进入低收益区。

---

## 13. 结论

本文档正式冻结以下规则：

- 第一批只允许 `/stock` 的 A0/A1 解释摘要增强
- 第二批才允许 `/T12` 的 A0/A1 治理解释
- 第三批才允许运维复核中的 A2 建议级输出
- 当前阶段明确不接 A3、高敏感建议、候选推荐、发布/回滚决策类输出
- 本文档完成后，AI 这条线应阶段性停笔，转入最小实现与最小测试

硬话直接说：
如果到这里还不收，还继续写更多 AI 文档，那大概率已经不是在补治理基线，而是在用文档拖延真正的实现与验证。

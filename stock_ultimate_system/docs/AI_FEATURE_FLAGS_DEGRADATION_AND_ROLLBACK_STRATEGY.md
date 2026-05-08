# AI 接入开关、降级与回退策略

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 中 AI 接入功能的开关分级、降级策略、回退策略与恢复条件。

本文档只回答五件事：

- AI 功能应该按什么层级开关
- 哪些异常应触发降级
- 哪些异常应触发回退
- 降级和回退的正式顺序是什么
- 恢复 AI 能力前必须重新满足什么条件

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. [OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md:1)
5. [AI_INTEGRATION_BOUNDARY_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_BOUNDARY_SPEC.md:1)
6. [AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md:1)
7. [AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md:1)
8. [AI_INTEGRATION_TEST_GATES_AND_BLOCKING_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_TEST_GATES_AND_BLOCKING_CHECKLIST.md:1)
9. 本文档

本文档不是开关实现说明，不是配置样例表，不是事故复盘。

---

## 2. 总原则

### 2.1 AI 必须被开关包住

后续任何 AI 能力都不得“默认裸接入”。

每项 AI 能力都必须满足：

- 有独立开关
- 有独立降级路径
- 有独立回退路径
- 有独立恢复条件

禁止：

- 用一个总开关覆盖所有 AI 场景
- 没有开关就把 AI 逻辑直接写死到页面或流程里

### 2.2 降级优先于硬失败

如果 AI 功能失败，但主链事实仍完整，系统优先执行：

- 隐藏 AI 输出
- 退回非 AI 说明
- 保留 L1/L2 非 AI 事实展示

而不是让整个 `/stock` 或 `/T12` 因 AI 失败崩掉。

### 2.3 回退优先于“硬撑着继续”

如果 AI 输出出现治理越界、误导、未复核高敏感输出、或反复异常，必须立即回退到更保守模式，而不是继续观望。

### 2.4 AI 降级不得影响 fail-closed

AI 功能不管开、关、降级、回退，都不能改变以下事实：

- `stock_entry_guard` 仍是正式阻断依据
- fail-closed 仍是正式兜底行为
- `/stock` 主结果仍只来自 L1
- `/T12` 仍只读治理摘要

---

## 3. 开关分级

## 3.1 F0 全局主开关

定义：

- 控制系统是否允许任何 AI 输出进入生产可见链路

允许控制：

- 所有 AI 页面输出
- 所有 AI 报告输出
- 所有 AI 运维复核辅助输出

禁止承担：

- 主链事实开关
- fail-closed 开关
- guard 开关

硬边界：

- `F0=off` 时，所有 AI 功能必须整体失效为“不可见”
- 但系统主链与非 AI 页面必须继续可用

## 3.2 F1 场景开关

每个场景必须有独立开关：

- `F1-stock-explainer`
- `F1-t12-explainer`
- `F1-ops-review-assistant`
- `F1-report-enhancer`

用途：

- 允许逐场景开启
- 允许逐场景回退
- 避免一处出问题拖掉全局

## 3.3 F2 输出级开关

每个场景内至少再按输出等级分开：

- `F2-A0-display`
- `F2-A1-explainer`
- `F2-A2-advisor`
- `F2-A3-sensitive-review`

默认策略：

- `A0/A1` 可以先开
- `A2` 需更严格 gate
- `A3` 默认关闭

## 3.4 F3 人工采纳开关

对于 A2/A3 输出，必须存在单独的人工采纳开关或等价状态门。

原则：

- 生成不等于采纳
- 展示不等于执行

---

## 4. 正式开关策略

## 4.1 默认初始状态

默认应为：

- `F0=off`
- `F1-stock-explainer=off`
- `F1-t12-explainer=off`
- `F1-ops-review-assistant=off`
- `F1-report-enhancer=off`
- `F2-A3-sensitive-review=off`

硬话直接说：
AI 不应以“默认开启”进入生产。

## 4.2 最保守开启顺序

正式建议开启顺序：

1. 打开 `F0`
2. 只打开 `F1-stock-explainer`
3. 只允许 `F2-A0/A1`
4. 通过一轮稳定观察后，再开 `F1-t12-explainer`
5. 运维复核辅助 `F1-ops-review-assistant` 最后开
6. `A3` 默认仍关闭

## 4.3 禁止开启顺序

以下方式一律禁止：

- 直接一次性打开所有 AI 场景
- 先开 A2/A3，再补复核和测试门
- 在没有稳定 `/stock` 解释层验证时先开运维建议层

---

## 5. 降级策略

## 5.1 D1 输出隐藏降级

适用：

- AI 服务短时超时
- AI 输出为空
- AI 输出 schema 校验失败
- 留痕失败

动作：

- 隐藏本次 AI 输出
- 页面退回非 AI 说明
- 写错误留痕

特点：

- 不关闭整个场景
- 不影响主链事实展示

## 5.2 D2 场景级降级

适用：

- 某一场景连续失败
- 某一场景连续命中阻断
- 某一场景出现文案误导风险

动作：

- 关闭对应 `F1` 场景开关
- 保留其他场景运行
- 页面显式回退为非 AI 说明

示例：

- `/T12` AI 解释越界时，只关闭 `F1-t12-explainer`

## 5.3 D3 输出级降级

适用：

- A2/A3 输出问题多发
- 复核链路不稳定
- 高敏感输出误导风险升高

动作：

- 关闭对应 `F2-A2` 或 `F2-A3`
- 保留 `A0/A1`

原则：

- 先保留安全解释层
- 再砍掉高风险建议层

## 5.4 D4 全局降级

适用：

- AI 服务整体异常
- 留痕系统异常
- 白名单或 schema 校验系统异常
- 无法确认 AI 输出安全性

动作：

- `F0=off`
- 全部 AI 输出隐藏
- 系统回到纯非 AI 模式

硬话直接说：
一旦连校验系统本身都不可信，就不该让任何 AI 输出继续可见。

---

## 6. 回退策略

## 6.1 R-A 轻量回退

适用：

- 某次发布后发现单场景解释质量差
- 没有治理越界，只是体验不稳

正式顺序：

1. 关闭相关 `F1`
2. 保留留痕与错误记录
3. 恢复非 AI 文案
4. 进入人工复盘

## 6.2 R-B 治理回退

适用：

- AI 输出触及 pointer / guard / release / rollback 边界
- AI 输出出现第二主结论倾向
- AI 输出绕开复核状态机

正式顺序：

1. 立即关闭相关 `F1/F2`
2. 必要时关闭 `F0`
3. 标记所有相关输出为 `reviewed_rejected` 或等价失效状态
4. 保留原始 AI 输出与阻断留痕
5. 回到非 AI 显示
6. 补复盘与补 gate

## 6.3 R-C 全局回退

适用：

- AI 接入造成系统级误导风险
- 无法判断哪些输出受污染
- 留痕链路本身异常

正式顺序：

1. `F0=off`
2. 清空当前可见 AI 输出
3. 页面全部回退非 AI 模式
4. 人工核查最近一段 AI 输出历史
5. 未完成核查前不得重新开启

---

## 7. 触发条件矩阵

## 7.1 触发降级

以下情况至少触发降级：

- 单次 schema 校验失败
- 单次留痕失败
- 单次输出为空但状态成功
- AI 服务超时
- AI 输出被人工判定误导

## 7.2 触发场景回退

以下情况至少触发场景级回退：

- 同一场景连续多次阻断
- 同一场景连续多次人工拒绝
- 同一场景输出越界到不允许的 schema

## 7.3 触发全局回退

以下情况必须触发全局回退：

- AI 输出触及 pointer current 改写意图
- AI 输出触及 guard 豁免意图
- AI 输出触及 release / rollback 决策意图
- `/T12` 形成第二主结论
- 白名单系统或 schema 校验系统异常失效

---

## 8. 恢复条件

## 8.1 场景恢复条件

单场景从降级或回退恢复前，至少必须满足：

- 相关 bug 已修复
- 相关阻断原因已消失
- 重跑该场景测试门全部通过
- 完成一轮人工误导风险复核

## 8.2 输出级恢复条件

A2/A3 恢复前，至少必须满足：

- 复核状态机测试通过
- 被拒绝输出隐藏测试通过
- 采纳链路留痕测试通过

## 8.3 全局恢复条件

从 `F0=off` 恢复前，至少必须满足：

- 输入白名单门恢复正常
- 输出 schema 门恢复正常
- 主链隔离门恢复正常
- fail-closed 保真门恢复正常
- 完成一轮最近故障窗口人工审计

硬话直接说：
没有重新过门，就不该恢复开关。

---

## 9. 与现有治理链的关系

## 9.1 与 `stock_entry_guard` 的关系

AI 开关、降级、回退都不得改变：

- `stock_entry_guard` 的阻断语义
- `/stock` 与 `/api/primary-result` 的 fail-closed 语义

换句话说：
AI 只能自己退，不得把主链一起拖走。

## 9.2 与 release gate 的关系

AI 开关关闭后：

- release gate 仍按原制度运行
- 不允许因为 AI 关闭就放宽 release 条件

## 9.3 与 review queue 的关系

AI 建议层关闭后：

- review queue 仍按原制度处理
- 不允许因为 AI 辅助缺席就跳过 review

---

## 10. 上线前最低检查清单

AI 功能开关打开前，至少确认：

- [ ] 已定义 `F0/F1/F2` 开关层级
- [ ] 已定义每个场景的降级动作
- [ ] 已定义每个场景的回退动作
- [ ] 已定义全局回退动作
- [ ] 已定义恢复前重测要求
- [ ] 已补开关关闭后的页面退化检查
- [ ] 已补 AI 服务不可用时的退化检查
- [ ] 已补 A2/A3 被关停时的显示退化检查
- [ ] 已确认关闭 AI 不影响主链与 guard

---

## 11. 最低执行口径

如果未来第一次真正上线 AI，最低只允许以下保守口径：

1. 先启 `F0 + F1-stock-explainer + F2-A0/A1`
2. `/T12` 解释层后开
3. 运维建议层最后开
4. `A3` 默认持续关闭
5. 任一治理越界信号出现即回退

做不到这五条，就不应该在生产打开 AI。

---

## 12. 下一步文档

本文档之后，如果继续补治理文档，下一份建议优先补：

- `AI 接入运行期监控与审计看板说明`

原因：

- 本文档解决“怎么开、怎么降、怎么退”
- 下一份文档要解决“开了以后怎么持续盯住”

---

## 13. 结论

本文档正式冻结以下规则：

- AI 能力必须被分层开关包住
- AI 失败默认先降级到非 AI 说明，不影响主链事实
- AI 越界或校验系统失效时必须回退
- 回退后不得无条件重开，必须重新过门
- 关闭 AI 不得改变 fail-closed、guard、release gate 的正式治理语义

硬话直接说：
如果后面谁想把 AI 接进来却不给它开关、不给它降级、不给它回退，那不是工程化接入，而是在给生产系统塞一个拔不掉的风险源。

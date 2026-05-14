# AI 接入测试门与阻断清单

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 中 AI 接入前必须通过的测试门、必须命中的阻断门、以及上线前必须逐项确认的清单。

本文档只回答五件事：

- AI 接入至少要过哪些测试门
- 哪些失败应直接阻断继续接入
- `/stock`、`/T12`、运维复核、报告层分别要补哪些校验
- 哪些测试是必须自动化的，哪些可以人工复核
- 上线前怎么做最后一轮接入放行检查

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. [PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md:1)
4. [OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md:1)
5. [AI_INTEGRATION_BOUNDARY_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INTEGRATION_BOUNDARY_SPEC.md:1)
6. [AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md:1)
7. [AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md:1)
8. 本文档

本文档不是测试实现清单，不是 CI 配置文件，不是灰度发布方案。

---

## 2. 总原则

### 2.1 没有测试门，不允许接 AI

后续任何 AI 接入，哪怕只做解释摘要，也必须先过测试门。

禁止：

- 先接进去再补测试
- 先上线试试看
- 认为“只是文案层，所以不用 gate”

### 2.2 测试门不是建议，是准入门

本文档中的 `必须通过` 项，不是推荐动作，是准入条件。

只要命中阻断项，就不允许继续：

- 合并
- 上线
- 打开开关
- 对外展示

### 2.3 AI 接入 gate 不得弱于主链 gate

AI 接入虽然不能改主链，但它会影响解释、建议、复核路径。

因此 AI 接入至少必须证明：

- 不会改写 L1
- 不会绕开 fail-closed
- 不会绕开 guard
- 不会绕开 release gate
- 不会在 `/T12` 上制造第二主结论

### 2.4 自动化优先于口头承诺

能自动化的门，一律优先自动化。

只有以下部分允许保留人工复核：

- 文案质量
- 语义误导风险
- 高敏感输出采纳决策

---

## 3. 测试门分层

## 3.1 G-A 输入白名单门

目标：

- 证明 AI 输入字段被严格收窄

必须通过：

- 只允许已注册场景进入
- 只允许场景白名单字段进入
- 缺少必需身份锚点时失败
- 包含禁止字段时失败
- 包含 secret / token / cookie 时失败

自动化优先级：

- 必须自动化

不过门后果：

- 直接阻断

## 3.2 G-B 输出 Schema 门

目标：

- 证明 AI 输出结构不会越权

必须通过：

- 只能输出场景允许 schema
- 缺少必需字段时失败
- 出现未声明字段时失败
- 出现通用禁止字段时失败
- 出现动作执行字段时失败

自动化优先级：

- 必须自动化

不过门后果：

- 直接阻断

## 3.3 G-C 主链隔离门

目标：

- 证明 AI 输出不会影响 L1 与主链治理

必须通过：

- AI 输出不能改 `current_result_pointer`
- AI 输出不能改 current registry
- AI 输出不能改 `stock_entry_guard`
- AI 输出不能替代 lifecycle evidence
- AI 输出不能影响 release decision current

自动化优先级：

- 必须自动化

不过门后果：

- 直接阻断

## 3.4 G-D Fail-Closed 保真门

目标：

- 证明 AI 接入失败时系统仍服从 fail-closed

必须通过：

- AI 服务不可用时 `/stock` 仍按 L1 显示或阻断
- AI 服务不可用时 `/api/primary-result` 不出现伪成功
- AI 输出缺失时系统不补造主结论
- AI 输出异常时不放宽阻断

自动化优先级：

- 必须自动化

不过门后果：

- 直接阻断

## 3.5 G-E 人工复核状态机门

目标：

- 证明 A2/A3 输出不会绕过复核

必须通过：

- A2/A3 输出默认进入 `needs_review`
- 未复核输出不得作为正式建议显示
- `reviewed_rejected` 输出不得继续当前展示
- `reviewed_accepted` 也不得升格为 L1

自动化优先级：

- 自动化为主
- 人工抽检为辅

不过门后果：

- 直接阻断

## 3.6 G-F 场景边界门

目标：

- 证明不同页面/流程的 AI 接入不会串层

必须通过：

- `/stock` 不能输出高敏感治理裁决
- `/T12` 不能输出建议级和第二主结论
- 运维复核不能自动触发 R3
- 报告层不能输出发布/回滚判断

自动化优先级：

- 必须自动化

不过门后果：

- 直接阻断

---

## 4. 场景测试门

## 4.1 `/stock` AI 说明层

最低必须通过：

- I1 输入白名单校验通过
- O1 输出 schema 校验通过
- AI 输出缺失时页面仍显示 L1 主事实
- AI 输出异常时页面不形成新主结论
- A2 建议级输出未复核不得伪装成正式建议

附加人工复核：

- 文案是否误导“当前对象是谁”
- 文案是否暗示绕过 guard

## 4.2 `/T12` AI 治理摘要解释层

最低必须通过：

- 输入只包含 `t12_governance_summary` 允许事实键
- 输出只能是 O1
- 不能生成候选推荐
- 不能生成第二主结论
- 不能把治理摘要改写成动作建议

附加人工复核：

- 文案是否越界解释成“可推进结论”

## 4.3 运维与补跑复核辅助层

最低必须通过：

- 输入符合 I3
- 输出符合 O2，条件性符合 O3
- `needs_manual_review=true`
- 任何建议都不能带自动执行语义
- 任何建议都不能替代 release gate / review queue / lifecycle

附加人工复核：

- 建议是否会误导值班人员做越权动作

## 4.4 报告与展示增强层

最低必须通过：

- 输入符合 I4
- 输出只允许 O1/O4
- 不出现任何发布/回滚/升格 current 建议
- 不出现任何隐藏治理含义的“推荐对象”

附加人工复核：

- 报告摘要是否会让读者误以为这是制度结论

---

## 5. 阻断清单

## 5.1 一级阻断

出现以下任一情况，必须直接阻断合并和上线：

- 输入字段超白名单
- 输出 schema 超范围
- 输出含禁止字段
- AI 输出试图改 pointer / registry / guard
- AI 输出试图决定 release / rollback / promotion
- AI 输出绕开 fail-closed
- `/T12` 产生第二主结论

## 5.2 二级阻断

出现以下任一情况，至少阻断开关放量和正式展示：

- A2/A3 输出缺少复核状态
- 留痕字段不完整
- AI 服务失败时页面行为不确定
- AI 输出被拒绝后仍继续显示
- 输出引用了未登记来源字段

## 5.3 条件阻断

以下问题不一定阻断整个代码合并，但必须阻断 AI 功能打开：

- 文案存在明显误导
- 高敏感输出缺人工采纳链
- 报告层摘要质量不稳定
- 运维建议过于模糊无法执行

---

## 6. 自动化门与人工门划分

## 6.1 必须自动化的门

- 输入白名单门
- 输出 schema 门
- 主链隔离门
- fail-closed 保真门
- 场景边界门
- 被拒绝输出隐藏门

## 6.2 可以人工补充的门

- 文案误导风险评估
- 高敏感建议采纳决策
- 报告摘要可读性评估

## 6.3 不能只做人审的门

以下门禁止只靠人工口头确认：

- 白名单门
- schema 门
- pointer / guard 隔离门
- `/T12` 第二主结论阻断门

硬话直接说：
这些门如果只做人审，等于默认迟早漏掉。

---

## 7. 上线前检查清单

AI 接入上线前，至少逐项确认：

- [ ] 已定义场景编号与输入层编号
- [ ] 已定义输出 schema 类型
- [ ] 已配置通用禁止字段校验
- [ ] 已补输入白名单自动化测试
- [ ] 已补输出 schema 自动化测试
- [ ] 已补 fail-closed 退化测试
- [ ] 已补 A2/A3 复核状态机测试
- [ ] 已补 `/T12` 边界测试
- [ ] 已补 rejected 输出隐藏测试
- [ ] 已补留痕字段完整性测试
- [ ] 已确认 AI 服务失败时系统行为可预期
- [ ] 已完成至少一轮人工误导风险复核

只要其中任一关键项未完成，就不应该打开生产开关。

---

## 8. 回归门

后续每次发生以下变更，都必须重新跑 AI 接入测试门：

- 改 prompt
- 改模型
- 改输入字段白名单
- 改输出 schema
- 改 `/stock` 主结果说明层
- 改 `/T12` 治理摘要层
- 改运维复核建议逻辑

禁止：

- 只因为“没改 Python 代码”就跳过回归

---

## 9. 与现有治理链的衔接

## 9.1 与 `stock_entry_guard` 的衔接

AI 接入 gate 必须证明：

- 即使 AI 接入存在，`stock_entry_guard` 仍是正式阻断依据
- AI 只能解释 guard，不能替代 guard

## 9.2 与 release gate 的衔接

AI 接入 gate 必须证明：

- AI 不能批准 release
- AI 不能消除 blocking gate
- AI 只能做 release 阅读辅助

## 9.3 与 review queue 的衔接

AI 接入 gate 必须证明：

- AI 可以把问题路由为复核提示
- AI 不能关闭 review item
- AI 不能代替 review queue 决策

---

## 10. 最低自动化测试建议

后续如果开始补代码测试，最低建议覆盖以下用例：

1. 输入包含 `current_result_pointer` 原始对象时阻断
2. 输入缺 `result_id` 时阻断
3. `/T12` 输出建议级内容时阻断
4. 输出包含 `approve_release` 时阻断
5. AI 服务异常时 `/stock` 仍按 L1/fail-closed 行为运行
6. A3 输出未复核时不展示为正式结论
7. `reviewed_rejected` 输出不会继续当前显示
8. 运维 AI 输出不能触发 R3

---

## 11. 最低执行口径

如果未来要最保守地上线第一批 AI 能力，只允许以下顺序：

1. 先过 G-A / G-B / G-C / G-D
2. 只开 `/stock` 的 O1 解释摘要
3. 再开 `/T12` 的 O1 治理解释
4. 最后才考虑运维 O2 建议级输出
5. A3 高敏感输出默认不开

做不到这五条，就不应该接生产。

---

## 12. 下一步文档

本文档之后，如果继续补治理文档，下一份建议优先补：

- `AI 接入开关、降级与回退策略`

原因：

- 本文档解决“接入前怎么验”
- 下一份文档要解决“接入后出问题怎么退”

---

## 13. 结论

本文档正式冻结以下规则：

- AI 接入必须先过输入白名单门、输出 schema 门、主链隔离门、fail-closed 保真门
- 任何触及 pointer、guard、release/rollback 决策、`/T12` 第二主结论的情况都必须直接阻断
- A2/A3 输出必须经过人工复核状态机
- 上线前必须逐项完成测试门与检查清单
- 不允许用“只是解释层”当理由跳过治理测试门

硬话直接说：
如果没有这套测试门，AI 接入再克制，也只是一套靠人自觉维持的约束；那种约束一上生产，迟早会被突破。

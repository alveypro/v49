# AI 接入运行期监控与审计看板说明

## 1. 文档定位

本文档用于冻结 `stock_ultimate_system` 中 AI 接入后的运行期监控项、审计项、告警项、看板分层与处置动作。

本文档只回答五件事：

- AI 接入上线后必须持续盯哪些指标
- 哪些异常要记入审计，哪些异常要触发告警
- 看板至少要分成哪些视图
- 看到什么状态时应该降级、回退、复核
- AI 运行期监控如何与现有主链、guard、review queue、operations scoreboard 衔接

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
10. 本文档

本文档不是具体监控实现代码，不是 Grafana 配置文件，不是报警平台接入教程。

---

## 2. 总原则

### 2.1 AI 上线后必须可观测

后续任何 AI 能力一旦进生产，就必须做到：

- 看得见是否在运行
- 看得见是否在降级
- 看得见是否在被阻断
- 看得见是否在被人工拒绝
- 看得见是否影响了页面与流程

### 2.2 看板不是展示面子，是治理面板

AI 运行期看板不是为了“看起来很智能”，而是为了回答：

- 现在有没有越界风险
- 现在有没有误导风险
- 现在有没有失效但没被发现
- 现在是不是该降级或回退

### 2.3 审计优先于效果

AI 输出质量高不高可以慢慢优化。

但以下问题必须第一时间可见：

- 留痕缺失
- 白名单越界
- schema 越界
- 未复核高敏感输出
- `/T12` 越界
- fail-closed 受影响

### 2.4 AI 看板不得替代主链看板

AI 运行期看板只能补充：

- AI 自身健康
- AI 自身风险
- AI 自身复核状态

它不能替代：

- `stock_entry_guard`
- release gate
- lifecycle evidence
- daily operations scoreboard

---

## 3. 监控对象分层

## 3.1 M1 开关状态层

必须监控：

- `F0` 全局主开关状态
- 各 `F1` 场景开关状态
- 各 `F2` 输出级开关状态
- `A3` 是否仍保持默认关闭

要回答的问题：

- 哪些 AI 能力现在实际打开了
- 是否有人越过规定顺序直接打开高风险能力

## 3.2 M2 运行健康层

必须监控：

- AI 请求总数
- 成功数
- 失败数
- 超时数
- 空输出数
- schema 校验失败数
- 留痕失败数

要回答的问题：

- AI 当前是不是稳定在跑
- 是否已经进入需要降级的状态

## 3.3 M3 治理合规层

必须监控：

- 输入白名单拦截数
- 输出 schema 拦截数
- 禁止字段命中数
- A2/A3 未复核拦截数
- rejected 输出隐藏命中数
- `/T12` 第二主结论拦截数

要回答的问题：

- AI 有没有反复撞治理边界
- 边界是否正在被某个场景持续试探

## 3.4 M4 人工复核层

必须监控：

- `needs_review` 总数
- `reviewed_accepted` 总数
- `reviewed_rejected` 总数
- 超时未复核数
- A3 待复核数
- 同对象重复被拒绝次数

要回答的问题：

- 人工复核链有没有积压
- 哪些 AI 输出长期不被信任

## 3.5 M5 业务影响层

必须监控：

- AI 输出被展示次数
- AI 输出被隐藏次数
- AI 场景级降级次数
- AI 输出级降级次数
- AI 场景回退次数
- AI 全局回退次数

要回答的问题：

- AI 是否正在频繁失效
- 当前运行是在稳定模式还是在凑合运行

---

## 4. 核心指标清单

## 4.1 基础计数指标

至少统计：

- `ai_requests_total`
- `ai_success_total`
- `ai_failure_total`
- `ai_timeout_total`
- `ai_empty_output_total`
- `ai_trace_written_total`
- `ai_trace_write_failed_total`

## 4.2 合规阻断指标

至少统计：

- `ai_input_whitelist_block_total`
- `ai_output_schema_block_total`
- `ai_forbidden_field_block_total`
- `ai_review_gate_block_total`
- `ai_t12_boundary_block_total`
- `ai_fail_closed_protection_block_total`

## 4.3 复核状态指标

至少统计：

- `ai_review_pending_total`
- `ai_review_accepted_total`
- `ai_review_rejected_total`
- `ai_review_expired_total`
- `ai_sensitive_pending_total`

## 4.4 降级与回退指标

至少统计：

- `ai_degrade_output_hide_total`
- `ai_degrade_scene_total`
- `ai_degrade_output_level_total`
- `ai_rollback_scene_total`
- `ai_rollback_global_total`

## 4.5 质量与误导风险指标

至少统计：

- `ai_manual_misleading_flag_total`
- `ai_same_object_rejected_repeat_total`
- `ai_unadopted_advice_total`
- `ai_post_release_disable_total`

硬话直接说：
如果连这些最基本的计数都没有，后面根本谈不上“运行期治理”。

---

## 5. 状态语义

AI 运行期状态至少统一成以下几类：

- `healthy`
- `degraded`
- `blocked`
- `review_backlog`
- `rollback_required`

## 5.1 healthy

说明：

- 请求稳定
- 白名单与 schema 拦截低
- 复核积压可控
- 无高优先级越界

## 5.2 degraded

说明：

- 请求可运行
- 但连续超时、空输出、trace 失败、文案误导开始上升

动作：

- 优先输出隐藏降级
- 必要时关闭单场景或单输出等级

## 5.3 blocked

说明：

- 合规门正在频繁拦截
- 某场景输出当前不可信

动作：

- 场景级降级
- 强制只保留非 AI 显示

## 5.4 review_backlog

说明：

- A2/A3 复核积压过多
- 超时未复核输出上升

动作：

- 暂停新的高敏感输出展示
- 优先清 backlog

## 5.5 rollback_required

说明：

- 命中治理越界
- 命中第二主结论风险
- 命中主链边界触碰

动作：

- 立即按回退策略执行

---

## 6. 告警分级

## 6.1 A-Info 信息告警

适用：

- 单次输出隐藏降级
- 单次空输出
- 单次人工误导标记

动作：

- 记录
- 不触发回退

## 6.2 A-Warn 警告告警

适用：

- 连续超时
- 连续 schema 失败
- `needs_review` backlog 上升
- 场景级降级触发

动作：

- 值班人关注
- 评估是否关闭对应 `F1/F2`

## 6.3 A-High 高优先级告警

适用：

- A3 未复核却被展示
- rejected 输出仍在当前显示
- `/T12` 边界被多次触发
- 同一对象连续误导

动作：

- 立即人工介入
- 必要时关闭场景

## 6.4 A-Critical 致命告警

适用：

- AI 输出触及 pointer / registry / guard 改写意图
- AI 输出触及 release / rollback 决策意图
- 白名单系统失效
- schema 校验系统失效
- AI 输出影响 fail-closed

动作：

- 立即全局回退
- `F0=off`
- 进入审计核查

---

## 7. 看板视图分层

## 7.1 总览视图

必须展示：

- F0/F1/F2 当前开关状态
- 当前总状态：`healthy/degraded/blocked/review_backlog/rollback_required`
- 最近 24h 请求、失败、阻断、回退总览

用途：

- 一眼看当前 AI 是否处于可控状态

## 7.2 合规视图

必须展示：

- 白名单拦截趋势
- schema 拦截趋势
- 禁止字段命中趋势
- `/T12` 越界命中趋势
- fail-closed 保护拦截趋势

用途：

- 看 AI 有没有越界倾向

## 7.3 复核视图

必须展示：

- `needs_review` backlog
- A3 待复核清单
- 最近被拒绝输出清单
- 同对象重复被拒绝排行

用途：

- 看人工复核链是否堵住

## 7.4 降级与回退视图

必须展示：

- 最近降级事件
- 最近场景关闭事件
- 最近输出级关闭事件
- 最近全局回退事件
- 当前仍处于关闭状态的开关清单

用途：

- 看 AI 当前是不是靠降级在撑

## 7.5 审计明细视图

必须展示单条输出的最小审计信息：

- `trace_id`
- 场景
- 输出等级
- 输入白名单版本
- 输出 schema 版本
- `needs_manual_review`
- `review_status`
- `adoption_status`
- 是否被隐藏/降级/回退

用途：

- 追一条具体 AI 输出到底发生了什么

---

## 8. 与现有治理链的衔接

## 8.1 与 `stock_entry_guard` 的衔接

AI 看板必须同时展示但不得替代：

- AI 当前状态
- `stock_entry_guard` 当前状态

原则：

- AI 看板只能说明“AI 自己怎么样”
- guard 仍说明“主链能不能发布”

## 8.2 与 daily operations scoreboard 的衔接

AI 看板应作为独立补充面板，挂接到：

- `primary_result_daily_operations_scoreboard`

但不能把 AI 指标混成主链健康分本身。

## 8.3 与 review queue 的衔接

AI 看板必须能看见：

- 当前 AI 触发的复核积压
- 当前 review queue 高优先级未闭项

原因：

- AI 输出不能替代 review queue
- 但必须看见 AI 有没有在持续制造复核压力

---

## 9. 处置动作映射

## 9.1 看见 `degraded`

默认动作：

1. 先看是输出隐藏级还是场景级降级
2. 确认主链与页面非 AI 展示仍正常
3. 决定是否关闭对应 `F1/F2`

## 9.2 看见 `blocked`

默认动作：

1. 检查命中的是白名单门还是 schema 门
2. 暂停该场景继续放量
3. 进入人工复核

## 9.3 看见 `review_backlog`

默认动作：

1. 暂停新增 A2/A3 可见输出
2. 优先清积压
3. 如果 backlog 持续上升，关闭高敏感输出开关

## 9.4 看见 `rollback_required`

默认动作：

1. 立即按回退文档执行
2. `F0=off` 或关闭对应场景
3. 保留审计痕迹
4. 未复盘前不得重开

---

## 10. 最低审计留存要求

AI 运行期监控看板至少要能回看：

- 最近 24h
- 最近 7d
- 最近 30d

每个时间窗至少能回答：

- 开了哪些开关
- 发生了多少阻断
- 发生了多少降级
- 发生了多少回退
- 有多少输出被人工拒绝

---

## 11. 最低上线前检查清单

AI 看板启用前，至少确认：

- [ ] 已定义核心计数指标
- [ ] 已定义合规阻断指标
- [ ] 已定义复核 backlog 指标
- [ ] 已定义降级与回退事件指标
- [ ] 已定义总览/合规/复核/回退/审计五个视图
- [ ] 已定义 A-Critical 致命告警触发条件
- [ ] 已确认看板不替代 guard 与 release gate
- [ ] 已确认关闭 AI 后看板仍能显示历史审计记录

---

## 12. 最低执行口径

如果未来第一次上线 AI 运行期监控，最低只允许采用以下保守口径：

1. 先做总览视图
2. 再做合规视图
3. 再做复核视图
4. 回退视图必须在 A2/A3 开启前完成
5. 没有 A-Critical 告警就不应打开高敏感输出

做不到这五条，就不该把 AI 接入视为“已工程化”。

---

## 13. 下一步文档

本文档之后，如果继续补治理文档，下一份建议优先补：

- `AI 接入实施顺序与最小上线范围`

原因：

- 本文档解决“上线后怎么看”
- 下一份文档要解决“第一批到底先接哪一块、先不接哪一块”

---

## 14. 结论

本文档正式冻结以下规则：

- AI 接入上线后必须有独立运行期监控与审计看板
- 看板必须优先暴露越界、阻断、积压、降级、回退，而不是只展示成功率
- 看板只能补充 AI 自身状态，不能替代主链、guard、release gate 看板
- 看到 `rollback_required` 必须立刻进入回退动作
- 没有运行期监控，就不应认为 AI 已进入受控生产状态

硬话直接说：
如果后面只是把 AI 接上了，却没有持续盯住它什么时候越界、什么时候失效、什么时候在制造复核积压，那等于把风险从“接入前”拖到了“运行中才爆”。 

# 下一阶段执行基线差距清单

## 1. 文档定位

本文档是 `stock_ultimate_system` 在 `P1` 阶段性暂停后的下一阶段执行入口。

本文档不讨论愿景，不做宣传，不重复 `P0/P1` 过程记录，只回答五件事：

- 当前真实工程状态是什么
- 还缺哪些上位治理基线
- 哪些差距会直接影响发布可靠性
- 下一步必须先补什么文档与校验
- 哪些差距在补齐前应视为上线阻断项

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. 本文档
3. 其他说明性设计文档、阶段总结文档、口头判断

---

## 2. 当前总判断

当前系统状态，不宜再继续把主精力放在页面层机械拆分。

当前更值钱的工作重心应从：

- `run_dashboard.py` 继续瘦身

切换到：

- 真实生产链路验证
- 事实源口径治理
- 发布与补跑约束固化
- AI 接入边界管控

硬结论：

- `P1` 已进入低收益尾声，可阶段性暂停
- 下一阶段必须回到“文档先行、治理先行、校验先行”
- 后续任何改表、改口径、改公式、接入 AI、回测发布与补跑，都不应再绕开这份差距清单

---

## 3. 差距清单使用规则

每一项差距只允许按以下字段记录：

- `当前实现`
- `主要风险`
- `必须补的文档`
- `必须补的测试/校验`
- `下一步动作`
- `是否阻断上线`

判断原则：

- 以真实流程为准，不以测试夹具为准
- 以调用链贯通为准，不以“已有脚本” 为准
- 以 fail closed 为准，不以“页面可打开” 为准

---

## 4. 主链真实性差距

### 4.1 当前实现

- 主结果主链已经具备 `pointer -> result -> run -> artifact -> lifecycle_evidence -> entry_guard` 基础结构。
- 代码与测试层面已建立 pointer-first、fail-closed、entry guard、deploy verification。
- 已有相关实现与脚本：
  - [current_result_pointer.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/current_result_pointer.py:1)
  - [unified_result_builder.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/unified_result_builder.py:1)
  - [run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_lifecycle.py:1)
  - [run_stock_entry_guard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_stock_entry_guard.py:1)
  - [run_server_post_deploy_verification.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_post_deploy_verification.py:1)

### 4.2 主要风险

- 当前“链路真实可用”更多是代码层和测试层成立，未形成一份面向生产运维的完整日常验收口径。
- 仓库里存在大量主结果相关脚本，但“真实刷新 / 补跑 / 回滚 / 部署后验证”之间的生产顺序和依赖关系尚未在单一上位文档中冻结。
- 一旦人工补跑、故障恢复、部署切换时跳过某一步，主链仍有可能出现“代码设计正确，但生产动作缺失”的问题。

### 4.3 必须补的文档

- 一份“主链生产动作基线”文档：
  - 正常日更顺序
  - 补跑顺序
  - 回滚顺序
  - 部署后验收顺序
  - 每一步必须产出的 artifact
- 一份“主链真实性核对清单”文档：
  - pointer 完整性
  - result/run/artifact 对齐性
  - lifecycle evidence 对齐性
  - entry guard 状态

当前落地状态：

- [x] [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
- [x] [MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md:1)

### 4.4 必须补的测试/校验

- 至少一条“真实生产链路模拟集成校验”，覆盖：
  - lifecycle 刷新
  - guard 刷新
  - deploy verification
  - `/stock` 与 `/api/primary-result` 同步 fail-closed
- 一条“补跑后链路重建”回归校验
- 一条“回滚后 pointer 与 evidence 一致性”回归校验

当前落地状态：

- [x] `tests/test_main_chain_authenticity_integration.py`
- [x] `tests/test_main_chain_recovery_integration.py`
- [x] 已并入主链真实性正式回归基线与 `stock` scope full readiness
- [x] 已并入 `check_release_gates.py` 的 `runtime_metadata` gate

### 4.5 下一步动作

1. 继续把现有脚本按真实生产顺序归档到基线文档里。
2. 把真实性清单对应到可执行集成校验。
3. 把真实性集成校验并入正式回归基线、scope readiness、release gates。
4. 最后才继续扩展更细粒度的负例与异常恢复回归。

### 4.6 是否阻断上线

- `是`

原因：
- 没有真实生产链路基线，后续发布可靠性无法证明。

---

## 5. Fail-Closed 覆盖差距

### 5.1 当前实现

- `/stock` 入口、`/api/primary-result`、entry guard、post-deploy verification 已具备 fail-closed 约束。
- 已有相关实现：
  - [stock_entry_guard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/stock_entry_guard.py:1)
  - [run_dashboard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/run_dashboard.py:1)
  - [run_server_post_deploy_verification.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_post_deploy_verification.py:1)

### 5.2 主要风险

- fail-closed 现在主要落在页面/API/部署验收，尚未明确覆盖所有人工补跑和异常恢复入口。
- 现有脚本数量很多，未来新增发布或补跑入口时，容易绕开已有阻断逻辑。
- “服务存活”与“主链可发布”虽然已开始分离，但还没被提升为统一运维纪律。

### 5.3 必须补的文档

- 一份“fail-closed 触发条件总表”：
  - pointer 缺失
  - artifact 缺失
  - lifecycle evidence 缺失
  - 状态冲突
  - registry 不一致
- 一份“入口矩阵”：
  - 哪些入口必须消费 entry guard
  - 哪些入口只允许只读
  - 哪些入口可以补跑但不能绕过阻断

### 5.4 必须补的测试/校验

- 对所有正式入口做 fail-closed 覆盖盘点
- 增加“人工补跑入口未接 guard” 的负例测试
- 增加“部署已通过但 guard blocked” 的强制失败校验

### 5.5 下一步动作

1. 文档化所有入口。
2. 给每个入口标注是否消费同一套 guard。
3. 对未消费 guard 的入口明确整改优先级。

### 5.6 是否阻断上线

- `是`

原因：
- 任一补跑或发布入口绕开 fail-closed，都会直接破坏主结果治理可信度。

---

## 6. 事实源口径差距

### 6.1 当前实现

- `P0/P1` 已基本完成 `/stock` 主结果唯一 pointer 化。
- `run_dashboard.py` 已转向 `query service / view model / render / composer / assets` 结构。
- `/T12` 保持只读定位，没有获得主结论替代权。
- 相关实现：
  - [dashboard_context.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/dashboard_context.py:1)
  - [primary_result_query_service.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_query_service.py:1)
  - [run_dashboard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/run_dashboard.py:1)
  - [t12_governance_summary.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/t12_governance_summary.py:1)

### 6.2 主要风险

- 虽然主结果层已经收口，但“哪些字段能裁决、哪些字段只能说明”的正式字段级口径还未冻结成上位文档。
- 未来一旦改表、改公式、接 AI，很容易重新出现“辅助字段越权参与主结论”的回潮。
- 现在更多依赖开发约束和测试习惯，不足以支撑后续多人或长期维护。

### 6.3 必须补的文档

- 一份“主结果事实源分级表”：
  - `L1 主事实`
  - `L2 受控说明`
  - `L3 展示性上下文`
- 一份“字段裁决权清单”：
  - 哪些字段可决定 `/stock` 主结论
  - 哪些字段绝不能决定 `/stock` 主结论
  - `/T12` 只能读什么

### 6.4 必须补的测试/校验

- 一条“top1 与 pointer 冲突时仍以 pointer 为准”的回归约束已经有了，但还应继续扩展：
  - regime 冲突
  - backtest 摘要冲突
  - candidate 辅助卡冲突
- 增加 “说明性上下文字段不得覆盖主事实” 的单元测试矩阵

### 6.5 下一步动作

1. 先产出字段级事实源分级表。
2. 再对照 `dashboard_context / query_service / T12` 标注字段职责。
3. 最后才允许继续改主结果口径。

### 6.6 是否阻断上线

- `是`

原因：
- 事实源口径不冻结，后续所有新增逻辑都有可能重新污染主结论。

---

## 7. 文档先行差距

### 7.1 当前实现

- 仓库已有大量阶段文档、验收文档、runbook 与规划文档：
  - [P0_ACCEPTANCE_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/P0_ACCEPTANCE_CHECKLIST.md:1)
  - [P0_COMPLETION_REVIEW.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/P0_COMPLETION_REVIEW.md:1)
  - [PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md:1)
  - [TOP_TIER_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/TOP_TIER_EXECUTION_STANDARD.md:1)

### 7.2 主要风险

- 文档很多，但缺少一份把“下一阶段所有高风险动作”统一收口的执行基线。
- 现有文档更偏阶段回顾、局部 runbook、阶段规划，不足以约束后续“改表、改口径、接 AI、补跑发布”的统一入口。
- 如果不先补上位文档，很容易回到“代码先走，文档补票”的旧路径。

### 7.3 必须补的文档

- 一份“下一阶段上位基线文档”：
  - 变更前必须更新的字段定义
  - 变更前必须更新的 runbook
  - 变更前必须更新的发布验收项
- 一份“文档先行变更流程”：
  - 哪些改动必须先改文档
  - 文档改到什么程度才允许改代码

### 7.4 必须补的测试/校验

- 对高风险目录做变更前自检：
  - `src/primary_result_*`
  - `src/dashboard_*`
  - `scripts/run_*`
  - `deploy/aliyun/*`
- 至少建立一条 CI 或本地 gate：
  - 高风险目录有代码改动但没有对应文档变更时，必须人工复核

### 7.5 下一步动作

1. 先把本差距清单作为过渡入口。
2. 再抽象成真正的下一阶段上位基线。
3. 之后所有高风险改动统一以“先文档、后代码”执行。

### 7.6 是否阻断上线

- `条件阻断`

说明：
- 对一般页面微调不阻断。
- 对主结果、发布、补跑、口径、AI 接入类变更，应视为阻断项。

---

## 8. 运维与补跑治理差距

### 8.1 当前实现

- 已有大量脚本、deploy 资产、systemd timer/service、发布校验与 rebuild runbook。
- 相关资产分散在：
  - [deploy/aliyun](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/deploy/aliyun)
  - [scripts](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts)

### 8.2 主要风险

- 资产很多，但“谁是日常入口、谁是补跑入口、谁是故障恢复入口、谁是只读检查入口”没有统一目录化。
- 对新接手的人来说，脚本与服务很多，但实际生产操作边界不够清晰。
- 容易出现“知道能跑，但不知道何时跑、是否会覆盖 pointer、是否会触发 release chain”的问题。

### 8.3 必须补的文档

- 一份“生产运维入口地图”：
  - 日更
  - 候选刷新
  - lifecycle
  - observation / daily closure
  - guard
  - release verification
- 一份“补跑分级说明”：
  - 只读检查
  - 允许重算但不改 pointer
  - 允许重建 latest
  - 允许正式重写主链

### 8.4 必须补的测试/校验

- 对所有正式 service/timer 与对应脚本建立映射校验
- 增加“误用补跑脚本覆盖正式主链”的防呆校验
- 增加“重建 latest 但未重建 guard / verification” 的一致性校验

### 8.5 下一步动作

1. 先做运维入口地图文档。
2. 再把 service/timer 与脚本逐一归档。
3. 然后补“允许做什么 / 禁止做什么”的补跑分级。

### 8.6 是否阻断上线

- `是`

原因：
- 没有补跑治理基线，生产稳定性更多靠熟悉度而不是制度。

---

## 9. AI 接入边界差距

### 9.1 当前实现

- 当前仓库已具备治理、主链、发布与看板基础，但尚未看到一份正式的 AI 接入治理边界文档。
- 仓库中已存在执行代理、策略相关自动化与大量主结果脚本：
  - [src/agents/execution_agent.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/agents/execution_agent.py:1)
  - [src/primary_result_query_service.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/src/primary_result_query_service.py:1)

### 9.2 主要风险

- 如果没有明确边界，AI 很容易被误接成：
  - 主事实生成器
  - 发布门禁绕路器
  - 解释层与裁决层混合器
- 这类风险不是模型质量问题，而是工程治理越界问题。

### 9.3 必须补的文档

- 一份“AI 接入边界说明”：
  - AI 可做建议
  - AI 可做摘要
  - AI 可做异常提示
  - AI 不可直接生成主结果事实
  - AI 不可绕过 `pointer / lifecycle / guard / release`
- 一份“AI 输出落位表”：
  - 可进入 `L3 展示层`
  - 可进入 `L2 受控说明`
  - 不可进入 `L1 主事实`

### 9.4 必须补的测试/校验

- 对所有未来 AI 接入点增加字段级白名单
- 增加“AI 输出不得直接改写 primary conclusion” 的回归校验
- 增加“AI 输出不得替代 lifecycle evidence / entry guard” 的负例测试

### 9.5 下一步动作

1. 先冻结 AI 边界文档。
2. 再决定 AI 接入点。
3. 最后才允许接代码。

### 9.6 是否阻断上线

- `条件阻断`

说明：
- 不接 AI 时不阻断现有上线。
- 一旦涉及 AI 接入、AI 解释、AI 决策辅助，就应视为阻断项。

---

## 10. 下一步执行顺序

下一阶段只允许按以下顺序推进：

1. 完成本差距清单。
2. 先写“主链生产动作基线”文档。
3. 再写“事实源分级表”文档。
4. 再写“运维入口地图 / 补跑分级”文档。
5. 再写“AI 接入边界说明”文档。
6. 文档冻结后，再决定下一批代码改动。

禁止顺序：

- 先改 AI 再补边界文档
- 先加补跑脚本再补入口地图
- 先改口径再补字段分级表
- 先改发布逻辑再补主链生产动作基线

---

## 11. 阶段结论

当前结论：

- `P1` 可以阶段性暂停
- 下一阶段不应再以页面层重构为主线
- 下一阶段主线应切到：
  - `主链真实性`
  - `fail-closed 全覆盖`
  - `事实源分级`
  - `文档先行`
  - `运维与补跑治理`
  - `AI 接入边界`

只有当这些基线开始成型后，后续改表、改公式、接 AI、回测发布与补跑，才算进入受控工程状态。

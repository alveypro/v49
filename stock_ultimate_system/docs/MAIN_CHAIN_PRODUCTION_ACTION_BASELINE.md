# 主链生产动作基线

## 1. 文档定位

本文档是 `stock_ultimate_system` 下一阶段的主链生产动作上位基线。

用途只有一个：

`冻结主链相关生产动作的正式顺序、入口、产物与验收口径。`

本文档不讨论页面，不讨论愿景，不讨论策略优劣，只约束以下四类高风险动作：

- 正常日更
- 补跑重建
- 服务器部署
- 服务器回滚

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. 本文档
3. [MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md:1)
4. [NEXT_PHASE_EXECUTION_BASELINE_GAP_CHECKLIST.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/NEXT_PHASE_EXECUTION_BASELINE_GAP_CHECKLIST.md:1)
5. 其他 runbook、口头说明、临时习惯

---

## 2. 适用范围

本文档约束的对象包括：

- 主结果主链：
  - `current_result_pointer`
  - `result_registry`
  - `run_registry`
  - `artifact_registry`
  - `primary_result_lifecycle_evidence`
  - `stock_entry_guard`
- 发布与回滚链：
  - activation plan
  - post-deploy verification
  - server deploy evidence bundle
  - server deploy registry

不在本文档直接管理范围内，但受其约束的动作包括：

- 改表
- 改口径
- 改公式
- 补跑 latest
- 手工重建主链
- 部署后验收

---

## 3. 总原则

### 3.1 主链优先于页面

只有当主链产物完整时，页面才允许展示主结果。

禁止：

- 先看页面是否能打开，再倒推主链是否健康
- 用页面显示“还行”代替主链验收通过

### 3.2 先产物，后指针

任何会改动 `current_result_pointer` 的动作，必须满足：

1. 先生成完整链路产物
2. 再登记 registry
3. 最后才允许更新 pointer

禁止：

- 先写 pointer 再补 artifact
- 先改 current 再补 history

### 3.3 先 guard，后发布

任何对外可见主结果都必须经过：

- lifecycle evidence
- entry guard
- deploy/post-deploy verification

禁止：

- 只要 HTTP 200 就视为发布成功
- 只看页面可打开就视为主链可发布

### 3.4 补跑不是免检通道

补跑、重建、清洗 latest、故障恢复，都必须继续服从：

- pointer 完整性
- artifact 完整性
- fail-closed
- 验收留痕

禁止：

- 以“只是补跑”为理由绕过 guard
- 以“只是修 latest”为理由不补验证产物

### 3.5 回滚不是只改指针

服务器回滚与主链回滚都必须是：

- 执行动作
- 验收动作
- 留痕动作

禁止：

- 只改 registry current pointer
- 只改 current.json 不做 post-rollback verification

---

## 4. 正常日更主线

### 4.1 目标

让当天主结果主链从研究输入推进到：

- 可追溯 lifecycle
- 可校验 entry guard
- 可被 `/stock` 与 `/api/primary-result` 消费

### 4.2 前置条件

- 研究输入已准备完成
- 候选结果来自可信生产输入
- 运行目录不是 pytest/tmp 污染路径
- 当前动作不是人工应急补写

### 4.3 正式顺序

#### 步骤 1：运行主结果 lifecycle

正式入口：

- [run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_lifecycle.py:1)

作用：

- 运行 `audit / execution / rollback / observation / terminal`
- 生成 lifecycle evidence
- 写入 `artifact_registry / run_registry / result_registry`
- 在成功条件满足时更新 `current_result_pointer`

最低要求：

- 不允许跳过 lifecycle 直接手写 pointer

#### 步骤 2：刷新 stock entry guard

正式入口：

- [run_stock_entry_guard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_stock_entry_guard.py:1)

作用：

- 对当前主链做统一完整性判断
- 落盘 `stock_entry_guard_latest.json`
- 为 `/stock` 与 `/api/primary-result` 提供统一阻断依据

最低要求：

- 不允许在 lifecycle 后直接视为“可发布”，必须刷新 guard

#### 步骤 3：校验 `/stock` 入口与 API 入口

正式入口：

- `/stock`
- `/api/primary-result`

要求：

- 当 guard 通过时，两个入口必须一致消费当前主链
- 当 guard 失败时，两个入口必须一致 fail-closed

### 4.4 必须产物

- `current_result_pointer/current.json`
- `result_registry/history/<result_id>.json`
- `run_registry/history/<run_id>.json`
- `artifact_registry.jsonl`
- `primary_result_lifecycle_evidence`
- `stock_entry_guard_latest.json`

### 4.5 验收标准

- pointer 指向唯一主结果
- lifecycle evidence 与 pointer 对齐
- guard 状态与主链一致
- `/stock` 与 `/api/primary-result` 不出现主链冲突

### 4.6 禁止项

- 直接编辑 `current.json`
- 用 `latest` 直接替代 pointer
- 只刷新页面不刷新 guard
- 只做脚本执行不留 registry/artifact 证据

### 4.7 正式回归基线

正常日更、主链修复、部署前回归，至少必须跑通以下真实性回归组：

```bash
pytest \
  stock_ultimate_system/tests/test_main_chain_authenticity_integration.py \
  stock_ultimate_system/tests/test_main_chain_recovery_integration.py \
  stock_ultimate_system/tests/test_run_primary_result_lifecycle.py \
  stock_ultimate_system/tests/test_run_server_post_deploy_verification.py \
  stock_ultimate_system/tests/test_unified_result_builder.py \
  stock_ultimate_system/tests/test_dashboard_context.py \
  stock_ultimate_system/tests/test_run_dashboard_primary_result_api.py
```

执行要求：

- 这组回归不再只是开发建议，而是主链真实性正式基线
- 任一失败，视为主链真实性未通过，禁止把 `/stock` 当前结果当作可发布主链
- `scope full readiness` 与 `release gates` 后续都应包含这条真实性集成校验
- 其中 `test_main_chain_recovery_integration.py` 专门覆盖：
  - 补跑后主链重建
  - deploy rollback 后 pointer / evidence 一致性

---

## 5. 补跑重建主线

### 5.1 目标

在主链或 latest 污染、缺失、失配时，按制度顺序重建，而不是临时打补丁。

### 5.2 适用场景

- latest 被 pytest/tmp 污染
- 主结果 evidence 与 latest 不一致
- 候选篮 feedback latest 污染
- handoff gate latest 指向错误来源

### 5.3 前置条件

- 先明确是“latest 污染”还是“主链本身损坏”
- 先审计再重建，不能直接覆盖

### 5.4 正式顺序

#### 步骤 0：先做污染审计

正式入口：

- [inspect_artifact_source_pollution.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/inspect_artifact_source_pollution.py:1)

#### 步骤 1：隔离当前脏 latest

要求：

- 污染 latest 不能继续留在生产路径被消费

#### 步骤 2：重建 current daily closure / performance evidence / operations latest

正式依据：

- [PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md:1)

最低顺序：

1. `run_current_primary_result_daily_closure.py`
2. `build_primary_result_performance_evidence.py`
3. `refresh_primary_result_operations_artifacts.py`

#### 步骤 3：重建候选篮反馈 latest

正式入口：

- `run_current_candidate_basket_observation.py`

#### 步骤 4：重建 handoff gate latest

正式入口：

- `build_primary_result_candidate_handoff_gate.py`

#### 步骤 5：再次执行污染审计

要求：

- 二次审计必须为零污染，才能结束补跑

### 5.5 必须产物

- 污染审计结果 JSON
- 重建后的 latest artifacts
- 二次污染审计结果 JSON

### 5.6 验收标准

- latest 不再引用 pytest/tmp 路径
- rebuilt latest 来自可信生产输入
- handoff gate / feedback / performance evidence 相互一致
- 页面与 API 不继续消费脏 latest

### 5.7 禁止项

- 不审计直接覆盖
- 不隔离直接重写
- 手工伪造 ledger entry
- 把测试样本保留为生产 latest

---

## 6. 服务器部署主线

### 6.1 目标

让一次部署从本地预检推进到：

- 可激活
- 可验证
- 可登记
- 可回滚

### 6.2 前置条件

- 本地 preflight 通过
- staging 目录可用
- 域名占用预检通过
- activation plan 状态为 passed

### 6.3 正式顺序

#### 步骤 1：本地 sync preflight

正式依据：

- [deploy/aliyun/DEPLOY.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/deploy/aliyun/DEPLOY.md:1)

要求：

- 不得用未过滤全量目录直接覆盖生产

#### 步骤 2：服务器域名预检

要求：

- 不得在域名归属不清时继续激活

#### 步骤 3：生成 activation plan

正式入口：

- `build_server_activation_plan.py`

#### 步骤 4：先 dry-run，再正式执行 activation

正式入口：

- `run_server_activation_plan.py`

#### 步骤 5：执行 post-deploy verification

正式入口：

- [run_server_post_deploy_verification.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_post_deploy_verification.py:1)

要求：

- 必须检查：
  - systemd 服务/定时器
  - nginx 配置
  - `/`
  - `/stock/`
  - `/stock/api/primary-result`
  - `/T12/`
  - 关键路径
  - error log
- 若 `stock entry guard blocked primary result publication.`，视为部署失败

#### 步骤 6：生成 deploy evidence bundle

正式入口：

- `build_server_deploy_evidence_bundle.py`

#### 步骤 7：登记当前服务器部署

正式入口：

- `register_server_deploy.py`

### 6.4 必须产物

- sync preflight JSON
- sync file list JSON
- activation plan JSON
- activation execution JSON
- post-deploy verification JSON
- server deploy evidence bundle JSON
- server deploy registry history/current

### 6.5 验收标准

- activation passed
- post-deploy verification passed
- deploy evidence bundle 完整
- deploy registry current 指向本次部署
- `/stock` 主链状态与 guard 状态一致

### 6.6 禁止项

- 跳过 dry-run 直接激活
- 不做 post-deploy verification
- 用 deploy 成功替代主链成功
- 没有 evidence bundle 就标记上线完成

---

## 7. 服务器回滚主线

### 7.1 目标

让服务器回滚成为正式执行路径，而不是人工改指针。

### 7.2 前置条件

- 明确目标 deployment id
- `confirm_deployment_id` 与目标一致
- rollback 先 dry-run

### 7.3 正式顺序

#### 步骤 1：读取历史 deployment snapshot

正式入口：

- [run_server_deploy_rollback.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_server_deploy_rollback.py:1)

#### 步骤 2：先执行 rollback dry-run

要求：

- 不允许跳过 dry-run 直接正式回滚

#### 步骤 3：正式执行 rollback activation

要求：

- 必须通过 activation plan 里的 rollback commands

#### 步骤 4：执行 post-deploy verification

要求：

- 回滚后也必须重新验收 `/`、`/stock/`、`/api/primary-result`、`/T12/`

#### 步骤 5：更新 deploy registry current pointer

要求：

- 只有 activation 与 verification 都通过后，才允许更新 current

### 7.4 必须产物

- rollback dry-run JSON
- rollback activation execution JSON
- rollback post-deploy verification JSON
- rollback summary JSON
- deploy registry current/history 更新记录

### 7.5 验收标准

- rollback activation passed
- rollback post-deploy verification passed
- deploy registry current 已更新
- 回滚后入口状态与目标 deployment 一致

### 7.6 禁止项

- 只改 registry pointer
- 不做 post-rollback verification
- 只恢复页面，不恢复正式激活状态

---

## 8. 主链动作与产物矩阵

| 动作 | 正式入口 | 必须产物 | 是否允许改 pointer |
|---|---|---|---|
| 正常日更 | `run_primary_result_lifecycle.py` | lifecycle evidence / registries / pointer | 是 |
| guard 刷新 | `run_stock_entry_guard.py` | guard artifact | 否 |
| latest 重建 | rebuild runbook 相关脚本 | rebuilt latest / pollution audit | 原则上否 |
| 服务器部署 | activation + post-deploy verification + deploy registry | deploy evidence bundle | 否，部署链不改主结果 pointer |
| 服务器回滚 | `run_server_deploy_rollback.py` | rollback execution + verification | 否，部署链只改 deploy registry |

说明：

- 只有主结果 lifecycle 正式流程允许改 `current_result_pointer`
- latest rebuild 默认是修复性动作，不是主结果重新裁决动作
- 服务器部署/回滚只处理服务态与部署态，不自动替代主结果治理

---

## 9. 阻断判定

以下任一项成立，必须阻断继续推进：

- lifecycle evidence 缺失
- pointer 不完整
- guard blocked
- post-deploy verification failed
- rebuilt latest 仍含 pytest/tmp 污染
- rollback 未经过 verification

阻断后的默认动作：

- fail closed
- 保留现场产物
- 进入人工复核

禁止默认动作：

- 自动补脑为成功
- 自动跳过失败步骤
- 自动宣称“只是小问题不影响主结果”

---

## 10. 下一步衔接

本文档落地后，下一份必须补的上位文档是：

- `主结果事实源分级表`

在该文档完成前：

- 不建议继续大规模改主结果口径
- 不建议继续引入 AI 解释层
- 不建议新增新的补跑/发布捷径入口

---

## 11. 阶段结论

当前结论：

- 主链相关高风险动作，后续必须统一服从本基线
- 任何偏离本文档顺序的动作，都应视为治理风险
- 后续改代码前，先对照本文档判断：属于哪条正式主线、会产出什么、如何验收、是否会碰 pointer

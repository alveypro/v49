# 第一次真实 `stock-scoped` Activation 准入门

## 1. 文档定位

本文档不是执行手册，也不是发布提案。

它只回答三件事：

- 第一次真实 `stock-scoped activation` 在什么条件下才允许发生
- 哪些条件未满足时必须继续保持阻断
- 一旦准入通过，唯一允许的单次执行顺序是什么

本文档用于约束以下高风险动作：

- 第一次 non-dry-run `stock-scoped activation`
- 第一次正式 `server_deploy_evidence_bundle`
- 第一次正式 deploy registry `current` snapshot 写入
- 第一次真实 `stock-scoped readiness recheck`

配套执行清单：

- `docs/first_real_stock_scoped_activation_remaining_gap_checklist.md`
- `docs/first_real_stock_scoped_activation_responsibility_freeze.md`
- `docs/first_real_stock_scoped_activation_ownership_sufficiency_decision.md`

如本文档与任何“先执行再说”的口头判断冲突，以本文档为准。

---

## 2. 当前冻结结论

第一次真实 `stock-scoped activation` 已按本准入门合法完成。

当前阶段的真实状态必须写成：

`第一次真实 stock-scoped activation 已执行成功，正式 evidence、registry、readiness 已建立；full-domain activation 仍未打开。`

当前文档保留的作用不再是继续阻断第一次真实 activation，而是作为：

- 本次真实 activation 的治理回放依据
- 后续不得误写成 full-domain convergence 的边界证据

---

## 3. 准入条件

以下条件已全部满足，并已支撑第一次真实 `stock-scoped activation` 的正式批准与执行：

### 3.1 正式边界条件

1. `airivo.online/stock` 继续保持唯一正式股票主链
2. `apex` 继续只承担内部验证 / 预发布 / 旁路角色
3. `airivo.online` 的 formal hosting boundary 已经书面冻结
4. 当前 `stock-scoped` rollout 不会接管 `/`、`/app/`、`/auth/*`、`/health`、`/chat`
5. `/T12/` 继续保持本阶段 `legacy-preserved` 结论，不进入本次切换范围

### 3.2 拓扑与计划条件

6. `build_server_activation_plan.py` 生成的 plan 明确为 `scope=stock-scoped`
7. plan 中不包含整份 live nginx 替换
8. plan 中不包含 `stock-ultimate-main-site.service` 重启
9. plan 中不包含 `stock-ultimate-t12.service` 重启
10. plan 中不要求 `8764` / `8766` 在线
11. plan 的 `scope_constraints` 与当前阶段冻结边界一致

### 3.3 远端验证条件

12. 正式机 `stock-scoped` activation plan dry-run 已通过
13. 正式机 `stock-scoped` activation execution dry-run 已通过
14. 正式机 post-deploy verification 已通过
15. verification 输出中：
   - `rollout_scope = stock-scoped`
   - `rollback_hint.scope = stock-scoped`
16. `/stock` 与 `/apex/stock` 的关键字段一致性已通过
17. canonical env 检查已通过

### 3.4 产物诚实性条件

18. `build_server_deploy_evidence_bundle.py` 已按 `scope` 生成真实 topology
19. `register_server_deploy.py` / `server_deploy_registry.py` 已按 `scope` 记录 snapshot 与 current pointer
20. `run_server_deploy_rollback.py` 已明确回滚对象是 `stock-scoped rollback`
21. `build_server_go_live_readiness.py` 在没有真实 current snapshot 时会失败且不冒充 full-domain readiness

### 3.5 责任与窗口条件

22. 本次真实 activation 的批准人明确
23. 本次真实 activation 的执行人明确
24. rollback 决策责任人明确
25. 执行窗口明确
26. 若失败，允许回滚的时间窗口明确

缺任何一项，均不得进入第一次真实 `stock-scoped activation`。

---

## 4. 明确阻断项

对第一次真实 `stock-scoped activation` 而言，以下阻断项已被解除；但对 full-domain activation 仍继续有效：

1. ownership 仍有未冻结项却试图提前执行 full-domain activation
2. `scoped_activation_rollout_spec.md` 被绕开
3. activation plan 再次出现 full-domain 语义
4. dry-run 与 verification 结果未落盘，只有口头“通过”
5. readiness 仍无真实 current snapshot，却有人要求补绿灯
6. evidence bundle、registry、rollback、readiness 任一产物仍在暗示 full-domain formal convergence
7. 任何人试图把第一次真实 activation 作为“先执行再补文档”的动作

命中任一项时，只允许：

- 继续 dry-run
- 继续 verification
- 继续补 ownership / topology / scoped rollout 边界
- 继续维持阻断

不允许：

- full-domain 真实 activation
- 正式 evidence registration
- current snapshot 更新
- 正式 readiness 通过宣告

---

## 5. 单次执行顺序

本次真实 `stock-scoped activation` 已按下面顺序完成：

### Step 1

生成并冻结正式 `stock-scoped activation plan`

要求：

- `scope=stock-scoped`
- 输出路径固定
- release id 固定
- plan `status=passed`

### Step 2

执行一次 non-dry-run `stock-scoped activation`

要求：

- 仅执行已批准 plan
- 不允许现场改 plan
- 产出正式 `activation_execution.json`

### Step 3

立刻执行 post-deploy verification

要求：

- verification 必须落盘
- `rollout_scope` 必须为 `stock-scoped`
- `/stock` 与 `/apex/stock` 一致性必须仍为 passed

### Step 4

生成正式 `server_deploy_evidence_bundle`

要求：

- 该 bundle 必须是第一次真实 `stock-scoped` formal deploy evidence
- 必须带 `scope=stock-scoped`
- 不得包含 full-domain route topology

### Step 5

把 evidence bundle 注册进 deploy registry

要求：

- 只允许写入 `stock-scoped` snapshot
- 只允许生成 `stock-scoped current pointer`
- 不得把本次注册描述成 full-domain current formal deployment

### Step 6

立即执行 readiness recheck

要求：

- readiness 必须基于刚注册的真实 current snapshot
- 输出必须继续带 `scope=stock-scoped`
- 如果失败，立即按 rollback 门限处理

### Step 7

如果 Step 2-6 任一步失败，立即进入 rollback 决策

要求：

- 不得继续补写 success 产物
- 不得保留伪 green current pointer
- rollback 结果也必须落盘

---

## 6. 单次执行后的允许结论

由于 Step 1-6 已全部通过，现在允许写出以下结论：

`第一次真实 stock-scoped activation 已完成，正式 deploy evidence、current snapshot、go-live readiness 已建立。`

在此之前，禁止写：

- “正式上线完成”
- “formal deployment 已闭环”
- “airivo.online 全域 readiness passed”
- “当前 production snapshot 已完整代表 formal domain”

---

## 7. 与其他文档的关系

执行本准入门时，必须同时服从：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/scoped_activation_rollout_spec.md`

如果这些文档中任一仍处于阻断状态，则本准入门自动判定为未满足。

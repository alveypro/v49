# 正式托管边界与 Activation 阻断规范

## 1. 文档定位

本文档用于冻结 `airivo.online` 当前阶段的正式托管边界，并把非 dry-run activation 的阻断条件写成制度。

它只回答三件事：

- 当前 `airivo.online` 哪些路由由谁正式托管
- 什么情况下禁止执行 non-dry-run activation
- 什么情况下才允许进入真实 activation

配套执行产物：

- `docs/airivo.online_route_ownership_matrix.md`
- `docs/formal_nginx_active_vs_repo_current_gap_checklist.md`
- `docs/airivo.online_route_ownership_freeze_decision.md`
- `docs/t12_formal_topology_decision.md`
- `docs/scoped_activation_rollout_spec.md`
- `docs/first_real_stock_scoped_activation_admission_gate.md`
- `docs/first_real_stock_scoped_activation_remaining_gap_checklist.md`

本文档是当前阶段的阻断级文档。凡涉及：

- `/etc/nginx/conf.d/airivo.online.conf`
- `build_server_activation_plan.py`
- `run_server_activation_plan.py`
- formal nginx reload
- formal deployment registry

必须同时服从本文档。

如本文档与说明性部署步骤冲突，以本文档为准。

---

## 2. 当前硬结论

当前仍不允许做 full-domain non-dry-run activation。

但第一次真实 `stock-scoped activation` 已在受限边界内合法完成。

继续阻断的原因不是执行力不够，而是正式入口归属还没收口成 full-domain 单一真相。

在这一前提下，任何会覆盖整份 live nginx 或重启未确认正式归属 service 的 activation，都属于越权改生产，不属于合规推进。

当前冻结口径：

`先把 airivo.online 的正式托管边界收口成 full-domain 单一真相，再谈 full-domain 真实 activation。`

---

## 3. 当前正式托管边界

当前阶段正式身份继续保持：

- `airivo.online/stock`：唯一正式股票主链
- `airivo.online/apex`：内部验证 / 预发布 / 旁路复核环境

但 `airivo.online` 整体托管边界尚未完全收口。当前必须按以下方式理解：

### 3.1 已明确正式主链

- `/stock/`
- `/stock/api/primary-result`

以上路径已经进入当前阶段正式主链治理范围。

### 3.2 已明确为内部验证路径

- `/apex/`
- `/apex/stock/`
- `/apex/T12/`

以上路径只允许承担内部验证职责，不参与并列正式产品叙事。

### 3.3 尚未完成托管归属冻结的正式路径

以下路径当前仍必须视为“托管归属未冻结”，不得被 `stock_ultimate_system` activation 直接接管：

- `airivo.online/`
- `airivo.online/app/`
- `airivo.online/auth/*`
- `airivo.online/health`
- `airivo.online/chat`
- 任何当前 live nginx 中仍由既有线上系统托管、但 repo current 未等价承接的路径

结论：

- `/stock` 的正式身份已冻结
- `airivo.online` 全域托管权尚未冻结
- 在全域托管权未冻结前，禁止执行会覆盖整份 live nginx 的 activation

---

## 4. 当前 non-dry-run Activation 禁止条件

只要命中以下任一项，就禁止执行 full-domain non-dry-run activation：

1. live nginx 与 repo current 的 formal topology 不一致
2. live `airivo.online.conf` 仍承载 repo current 未声明接管的路由
3. activation plan 需要覆盖整份 `/etc/nginx/conf.d/airivo.online.conf`
4. formal target topology 声称需要 `8764` 或 `8766`，但线上未实际监听或未正式收口
5. `stock-ultimate-main-site.service` 或 `stock-ultimate-t12.service` 未真实上线，却被 activation plan 视为必重启服务
6. live 主站、`/app`、认证、`/health`、`/chat` 的正式归属未书面冻结
7. domain preflight 只能证明“域名未完全被外部占用”，但不能证明“当前 repo current 可以安全等价替换 live nginx”

命中任一项时，唯一允许的动作是：

- dry-run activation
- post-deploy verification
- active vs repo current 差异盘点
- 路由归属冻结

不允许：

- full-domain 正式 activation
- 正式 rollback pointer 更新
- 生成 passed 的正式 deploy evidence bundle
- 登记 current formal deployment snapshot

---

## 5. 允许真实 Activation 的前置条件

只有以下条件全部成立，才允许进入 full-domain non-dry-run activation：

1. `airivo.online` 路由托管归属表已经冻结
2. live nginx 与目标 repo current 已完成逐段收口
3. activation plan 不再越权覆盖未冻结归属的 live 路由
4. formal topology 中声明的 service 与端口全部真实存在并已验证
5. `stock-ultimate-main-site.service`、`stock-ultimate-dashboard.service`、`stock-ultimate-t12.service` 的正式托管责任已经明确
6. domain preflight 与 hosting boundary precheck 同时 passed
7. dry-run activation、dry-run rollback、post-deploy verification 全部通过
8. 真实 activation 影响面、回滚窗口、回滚责任人已明确记录

说明：

- 第一次真实 `stock-scoped activation` 已于 `2026-05-01` 完成，不再受本条阻断
- 本文档当前继续阻断的对象是 full-domain activation，而不是已经完成的 `stock-scoped` formal deploy

缺任何一项，都只能继续停留在 dry-run 阶段。

---

## 6. `airivo.online` 路由托管归属表

当前阶段先按下面的治理状态执行：

| 路由 | 当前角色 | 当前托管状态 | 当前规则 |
| --- | --- | --- | --- |
| `/stock/` | 唯一正式股票主链 | 已冻结 | 允许继续强化 |
| `/stock/api/primary-result` | 正式主结果契约 | 已冻结 | 允许继续强化 |
| `/apex/` | 内部验证入口 | 已冻结 | 禁止产品化 |
| `/apex/stock/` | 内部验证股票入口 | 已冻结 | 禁止并列正式化 |
| `/apex/T12/` | 内部验证治理入口 | 已冻结 | 禁止并列正式化 |
| `/` | 主站 | 未冻结 | 禁止被当前 activation 越权接管 |
| `/app/` | 既有线上工作台/应用入口 | 未冻结 | 禁止被当前 activation 越权接管 |
| `/auth/*` | 认证链 | 未冻结 | 禁止被当前 activation 越权接管 |
| `/health` | 线上探针入口 | 未冻结 | 禁止被当前 activation 越权接管 |
| `/chat` | 线上业务入口 | 未冻结 | 禁止被当前 activation 越权接管 |

当且仅当该表中的“未冻结”项被正式收口后，才允许更新 activation 执行策略。

---

## 7. 当前阶段允许做的推进

当前阶段围绕 activation 只允许做以下工作：

- 补 formal hosting boundary 文档
- 盘点 live nginx 与 repo current 差异
- 补 route ownership matrix
- 补 topology precheck
- 跑 dry-run activation
- 跑 dry-run rollback
- 跑 post-deploy verification
- 修 activation plan 的越权范围
- 冻结第一次真实 `stock-scoped activation` 的准入门

禁止把 dry-run 阶段伪装成“只差按下执行按钮”。

---

## 8. 阶段性关闭标准

只有以下事实同时成立，本文档对应的 full-domain 阻断才允许解除：

1. `airivo.online` 路由托管归属表已冻结
2. live nginx 与 repo current 已不再存在越权覆盖风险
3. formal topology 与真实监听端口一致
4. full-domain non-dry-run activation 已被书面允许

在此之前，所有正式 deployment 相关结论都必须写明：

`stock-scoped activation completed; full-domain non-dry-run activation remains blocked by unresolved formal hosting boundary`

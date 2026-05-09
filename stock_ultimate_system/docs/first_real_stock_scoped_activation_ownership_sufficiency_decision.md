# 第一次真实 `stock-scoped` Activation Ownership 充分性判断

## 1. 文档定位

本文档用于回答一个比“ownership 是否全域闭合”更精确的问题：

`在当前全域 ownership 尚未完全闭合的前提下，第一次真实 stock-scoped activation 是否已经具备“充分合法性”。`

它不要求把 `airivo.online` 全域都接管进来，而是要明确：

- 哪些 live 路由不在本次切换范围内
- 为什么本次真实 `stock-scoped activation` 不会越权
- 现有 ownership 冻结是否已经足以支撑第一次真实 activation

改判路径见：

- `docs/first_real_stock_scoped_activation_ownership_sufficiency_upgrade_path.md`

---

## 2. 当前已冻结的 ownership 事实

### 2.1 已冻结为正式主链

- `/stock/`
- `/stock/api/primary-result`

### 2.2 已冻结为内部验证环境

- `/apex/`
- `/apex/stock/`
- `/apex/T12/`

### 2.3 已冻结为本阶段不接管范围

- `/`
- `/app/`
- `/auth/*`
- `/health`
- `/chat`
- `/api/ai/chat`

### 2.4 已冻结为 legacy-preserved 保留项

- `/T12/`
- `/T12/api/stock-ai-runner`
- `/T12/ops/stock-ai-runner`

---

## 3. 充分性判断标准

第一次真实 `stock-scoped activation` 只有在下面三条同时成立时，ownership 才能被判断为“充分”：

1. 本次 activation 只触碰 `/stock` 主链所需资产
2. 本次 activation 不会改变未冻结 live 路由的托管责任
3. 本次 activation 不会把 `/T12/` 从 `legacy-preserved` 状态拉进正式切换范围

如果三条里任何一条不成立，则 ownership 充分性判断失败。

---

## 4. 当前阶段的判断

### 4.1 已满足的部分

- `stock-scoped` activation plan 已经不再整份替换 live nginx
- 计划中已明确排除 `stock-ultimate-main-site.service`
- 计划中已明确排除 `stock-ultimate-t12.service`
- 计划中已不再要求 `8764` / `8766` 在线
- dry-run、verification、scope-aware readiness 已经证明当前 rollout model 不再宣称 full-domain 收口
- 第一次真实 `stock-scoped activation` 已于 `2026-05-01` 在正式机成功执行
- 正式 `server_deploy_evidence_bundle` 已生成并通过
- `deployments/current.json` 已登记为真实 `stock-scoped` current snapshot
- `server_go_live_readiness.stock_scoped.real.final.json` 已正式 passed

### 4.2 可复核事实依据

当前正式可复核的依据包括：

1. `stock-scoped` activation plan 只围绕 `/stock` 主链资产与下列 service 展开：
   - `stock-ultimate-dashboard.service`
   - `stock-ultimate-entry-guard.service`
   - `stock-ultimate-entry-guard.timer`
2. `stock-scoped` activation plan 明确不包含：
   - 整份 live nginx 替换
   - `stock-ultimate-main-site.service` 重启
   - `stock-ultimate-t12.service` 重启
   - `8764` / `8766` listener 要求
3. post-deploy verification 已经证明：
   - `rollout_scope = stock-scoped`
   - `/stock` 与 `/apex/stock` 关键字段一致
   - canonical env 检查通过
4. scope-aware readiness 已经证明：
   - 没有真实 current snapshot 时会诚实失败
   - 在真实 current snapshot 建立后，只会按 `stock-scoped` scope 给出正式 passed
   - 不会再把当前状态伪装成 full-domain readiness
5. 当前 live 路由治理事实仍然是：
   - `/`、`/app/`、`/auth/*`、`/health`、`/chat`、`/api/ai/chat` 不属于本阶段正式接管范围
   - `/T12/` 继续是 `legacy-preserved`
6. 与第一次真实 activation 直接对应的正式证据已存在：
   - `server_activation_plan.stock_scoped.real.final.json`
   - `server_activation_execution.stock_scoped.real.final.json`
   - `server_post_deploy_verification.stock_scoped.real.final.json`
   - `server_deploy_evidence_bundle.stock_scoped.real.final.json`
   - `deployments/current.json`
   - `server_go_live_readiness.stock_scoped.real.final.json`

### 4.3 仍未完成的部分

当前不再存在阻断第一次真实 `stock-scoped activation` 的 ownership 缺口。

仍然需要保持的边界只有两条：

- 这次通过的是 `stock-scoped` ownership sufficiency，不是 full-domain ownership closure
- `/`、`/app/`、`/auth/*`、`/health`、`/chat`、`/T12/` 的既有托管边界仍需在后续阶段继续维持，不得被回写成“全域已收口”

---

## 5. 当前阶段结论

当前对此项的正式结论是：

`在当前全域 ownership 尚未完全闭合的前提下，现有边界已经足以合法支撑第一次真实 stock-scoped activation，并且该 activation 已成功执行且未影响未接管 live 路由。`

因此，本项当前状态为：

`已满足`

正式理由：

- 本次 activation 的 `touch-set` 只触碰 `/stock` 主链相关 app、systemd、drop-in 与 deploy registry 资产
- `non-impact matrix` 已确认 `/`、`/app/`、`/auth/*`、`/health`、`/chat`、`/api/ai/chat`、`/T12/*` 的 route ownership、nginx 行为、upstream 依赖均不变化
- pre-activation baseline 与 post-deploy verification 已共同证明：非 `/stock` 路由没有被纳入本次正式切换对象
- approver 已正式批准 `ownership sufficiency = satisfied`，且真实 activation、evidence registration、readiness recheck 全部通过

正式正结论：

`ownership sufficiency has been formally reviewed and is currently judged satisfied for the first real stock-scoped activation.`

这不是口头放行，而是已经形成可复核判断：

- 当前控制系统已经足以支撑真实 `stock-scoped activation`
- 真实 activation 已成功执行且证据链已正式闭合
- 因此，对第一次真实 `stock-scoped activation` 而言，ownership 充分性阻断已被解除

---

## 6. 允许改判为“已满足”的前置条件

本次改判为 `已满足` 所依据的三条条件已经全部被明确写实并复核通过：

1. 明确列出本次真实 activation 触碰的资产范围，并证明只限 `/stock` 主链
2. 明确列出未触碰的 live 路由，并证明其托管责任不变
3. 明确列出 `/T12/` 的继续保留结论，并证明本次真实 activation 不会影响其当前 live 形态

因此，后续禁止再写：

- “ownership 仍然未满足”
- “现在还只差执行”
- “第一次真实 stock-scoped activation 仍未准入”

---

## 7. 当前冻结结论

当前对此项的冻结结论是：

`ownership sufficiency = satisfied`

因此：

- 第一次真实 `stock-scoped activation` 已合法完成
- 本阶段不再因 ownership sufficiency 阻断继续冻结
- 后续开发可回到 `STRICT_CONTINUATION_EXECUTION_STANDARD.md` 进入下一阶段

当前文档状态：

`已形成可复核正式结论，且该结论为 satisfied。`

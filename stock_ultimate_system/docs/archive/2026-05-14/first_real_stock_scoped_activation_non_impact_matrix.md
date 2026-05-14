# 第一次真实 `stock-scoped` Activation Non-Impact Matrix

## 1. 文档定位

本文档用于证明第一次真实 `stock-scoped activation` 不会影响未冻结的 live 路由。

它不是 activation plan，也不是执行结果，而是 ownership sufficiency 改判所需的第二组证据。

只有当本文件被填实并复核通过，才允许进入后续：

- `pre/post non-stock invariance proof`
- `ownership sufficiency = satisfied` 改判审批

---

## 2. 证明目标

本文件必须证明下面这句话是真的：

`本次真实 stock-scoped activation 对未冻结 live 路由的 route ownership、nginx 行为、upstream 依赖、外部可见行为均无变化。`

如果本文件做不到这一点，则 ownership sufficiency 不允许改判为 `satisfied`。

---

## 3. 填写规则

矩阵中的每一行都必须写实，不能留空，也不能用“理论上”“应该”“预计”这种主观词。

每个对象至少要回答：

- 当前 live 托管者是谁
- 本次 activation 是否改变其 route ownership
- 本次 activation 是否改变其 nginx 行为
- 本次 activation 是否改变其 upstream / service 依赖
- 结论是否为 `no change`
- 证明依据是什么

只要有一行不能被证明为 `no change`，本矩阵就不能作为改判证据。

---

## 4. Non-Impact Matrix

| 路由 / 对象 | 当前 live 托管者 | route ownership 是否变化 | nginx 行为是否变化 | upstream / service 依赖是否变化 | 结论 | 证明依据 |
| --- | --- | --- | --- | --- | --- | --- |
| `/` | 既有 live 主站 | `否` | `否` | `否` | `no change` | `stock-scoped` activation plan 不切换 main-site、不要求 `8764`、不替换整份 live nginx |
| `/app/` | 既有 live 应用入口 | `否` | `否` | `否` | `no change` | activation commands 只触碰 `/stock` 主链 app/config/systemd 对象，不包含 `/app` 路由或其 upstream 变更 |
| `/auth/*` | 既有 live 认证链 | `否` | `否` | `否` | `no change` | activation commands 无 auth 路由相关 nginx/service 变更，ownership freeze 已明确排除当前阶段接管 |
| `/health` | 既有 live 探针入口 | `否` | `否` | `否` | `no change` | activation commands 无 health 路由相关变更，formal hosting boundary 文档已明确其不在当前接管范围 |
| `/chat` | 既有 live 业务入口 | `否` | `否` | `否` | `no change` | activation commands 无 chat 路由相关变更，formal hosting boundary 文档已明确其不在当前接管范围 |
| `/api/ai/chat` | 既有 live API 入口 | `否` | `否` | `否` | `no change` | activation commands 无该 API 路由变更，ownership freeze 已将其列为本阶段不接管范围 |
| `/T12/` | legacy live `T12` 形态 | `否` | `否` | `否` | `no change` | `T12` 已冻结为 `legacy-preserved`；activation plan 不复制/重启 `stock-ultimate-t12.service`，不要求 `8766` |
| `/T12/api/stock-ai-runner` | legacy live `T12` API + nginx hotfix 404 阻断 | `否` | `否` | `否` | `no change` | post-deploy verification 仍以 `404 expected` 通过；当前 plan 不改变 `T12` nginx/service 边界 |
| `/T12/ops/stock-ai-runner` | legacy live `T12` ops + nginx hotfix 404 阻断 | `否` | `否` | `否` | `no change` | post-deploy verification 仍以 `404 expected` 通过；当前 plan 不改变 `T12` nginx/service 边界 |

---

## 5. 必须明确回答的 6 个问题

本文件必须明确回答以下问题：

1. 本次真实 activation 是否会改变 `/` 的 route ownership？
   - `不会。` 当前 `stock-scoped` plan 明确不切换 main-site，也不替换整份 live nginx
2. 本次真实 activation 是否会改变 `/app/` 与 `/auth/*` 的 nginx 行为？
   - `不会。` 当前 activation commands 不包含 `/app/` 或 `/auth/*` 路由相关 nginx / upstream 变更
3. 本次真实 activation 是否会改变 `/health` 与 `/chat` 的 upstream / service 依赖？
   - `不会。` 当前 activation 只重启 dashboard 与 entry-guard，不修改 `/health` 与 `/chat` 的既有 live 依赖链
4. 本次真实 activation 是否会改变 `/T12/` 当前 `legacy-preserved` 托管事实？
   - `不会。` 当前 plan 不引入 `stock-ultimate-t12.service`，也不要求 `8766`，与 `legacy-preserved` 冻结结论一致
5. 是否存在任何一个非 `/stock` 路由需要靠“应该没事”来判断不受影响？
   - `当前第一版证据稿判断为不存在。` 现有判断基于 activation commands、scope constraints、ownership freeze、T12 topology decision 和 verification 结果；最终仍需 invariance proof 实测复核
6. 上述所有非 `/stock` 对象能否全部写成 `no change`？
   - `当前第一版证据稿可以。` 但最终成立仍依赖后续 `pre/post non-stock invariance proof` 的实测等价性验证

只要其中任一问题回答不清，本文件就不能通过。

---

## 6. 复核结论

### 6.1 执行人自检结论

- 结论：`基于当前 stock-scoped activation plan、ownership freeze、T12 legacy-preserved 冻结与 verification 结果，non-impact matrix 第一版事实稿已形成；仍待正式 activation 前最后一次逐项复核签字`
- 签名角色：`当班运维 / 发布执行人`
- 时间：`待正式 activation 前最后一次复核时填实`

### 6.2 Approver 复核结论

- 结论：`当前仅确认 non-impact matrix 第一版事实稿与既有 stock-scoped plan 一致；是否据此批准 ownership sufficiency 改判，仍待后续 invariance proof 一并复核`
- 签名角色：`/stock` 唯一正式主链系统 owner
- 时间：`待 ownership sufficiency 最终审批时填实`

### 6.3 当前状态

- 当前状态：`第一版事实稿已形成，最终签字结论未填实`

---

## 7. 当前阶段结论

当前本文件的唯一合法结论是：

`non-impact matrix first-pass evidence drafted from the current stock-scoped activation plan and frozen ownership boundary; final signed evidence not yet completed.`

在最终签字与配套 `pre/post non-stock invariance proof` 完成前，禁止把本文件单独视为 ownership sufficiency 已通过的证据。

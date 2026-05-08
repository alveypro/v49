# `airivo.online` Route Ownership Freeze Decision

## 1. 文档定位

本文档用于把 `airivo.online` 当前阶段的 route ownership 从“盘点结果”推进成“冻结决策”。

它只回答三件事：

- 当前阶段哪些路由已经完成正式 ownership 冻结
- 哪些路由明确不属于当前阶段接管范围
- 在 ownership 未冻结前，哪些动作被制度性阻断

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/airivo.online_route_ownership_matrix.md`

---

## 2. 冻结结论

当前阶段 route ownership 冻结结论如下：

### 2.1 已正式冻结

- `airivo.online/stock/`
- `airivo.online/stock/api/primary-result`

冻结语义：

- 它们属于 `stock_ultimate_system` 当前阶段唯一正式股票主链
- 允许继续强化
- 允许围绕它们推进事实源、验证链、部署边界治理

### 2.2 已冻结为内部验证环境

- `airivo.online/apex/`
- `airivo.online/apex/stock/`
- `airivo.online/apex/T12/`

冻结语义：

- 它们只承担内部验证 / 预发布 / 旁路复核职责
- 不拥有并列正式产品身份
- 不允许被重新包装成第二正式面

### 2.3 明确不属于当前阶段正式接管范围

以下路由当前阶段明确不由 `stock_ultimate_system` 正式接管：

- `airivo.online/`
- `airivo.online/app/`
- `airivo.online/auth/*`
- `airivo.online/health`
- `airivo.online/chat`
- `airivo.online/api/ai/chat`

冻结语义：

- 这些路径当前 live 已承载独立线上职责
- repo current 尚未完成等价接管声明
- 在正式 ownership 冻结前，禁止被当前 activation 越权覆盖

### 2.4 特别保留项

- `airivo.online/T12/`
- `airivo.online/T12/api/stock-ai-runner`
- `airivo.online/T12/ops/stock-ai-runner`

冻结语义：

- 当前不纳入正式 ownership 完成项
- 其最终形态必须由独立的 `t12_formal_topology_decision.md` 冻结
- 在该决策完成前，视为“不得通过当前 activation 强切”的保留路径

---

## 3. 当前阶段的硬决策

### 决策 1

`/stock` 是当前阶段唯一正式股票主链。

### 决策 2

`apex` 继续只是内部验证环境，不拥有并列正式产品身份。

### 决策 3

`/`、`/app/`、`/auth/*`、`/health`、`/chat`、`/api/ai/chat` 当前全部冻结为“非本阶段正式接管范围”。

### 决策 4

在上述范围未重新评审并书面冻结前，禁止任何会整体覆盖 live `airivo.online.conf` 的 activation。

---

## 4. 当前阶段允许动作

只允许以下动作：

- 强化 `/stock` 主链
- 强化 `/stock/api/primary-result` 契约
- 强化 `apex` 的内部验证身份
- 盘点 `/`、`/app/`、`/auth/*`、`/health`、`/chat` 的 live 归属事实
- 为后续 scoped rollout 缩小可变更范围

---

## 5. 当前阶段禁止动作

禁止：

- 把 `/` 默认视为由当前 repo current 可正式接管
- 把 `/app/`、`/auth/*`、`/health`、`/chat` 默认视为“顺手一起迁”
- 以 `/stock` 已稳定为理由，整份替换 live nginx
- 把 `apex` 重新拉回并列正式面

---

## 6. 与 activation 的直接关系

基于本冻结决策，当前 activation 只允许有两类行为：

1. `dry-run`
2. `stock-scoped planning`

当前明确不允许：

1. `full-domain non-dry-run activation`
2. 对未冻结 ownership 路由的正式接管

---

## 7. 阶段关闭条件

只有同时满足下面三项，本文档才允许升级：

1. `/`、`/app/`、`/auth/*`、`/health`、`/chat` 的正式 ownership 被重新书面冻结
2. `T12` 形态决策完成
3. `scoped_activation_rollout_spec.md` 能基于已冻结边界设计，而不是反向定义边界

在此之前，唯一合法结论是：

`/stock ownership frozen; full-domain ownership intentionally not frozen in this phase.`


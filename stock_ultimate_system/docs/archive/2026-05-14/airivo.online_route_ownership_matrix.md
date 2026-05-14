# `airivo.online` Route Ownership Matrix

## 1. 文档定位

本文档用于冻结 `airivo.online` 当前阶段的 route ownership matrix。

它不是产品介绍，不是 nginx 教程，也不是理想拓扑图。

它只回答四件事：

- 当前对外路由有哪些
- 当前 live 由谁实际托管
- `stock_ultimate_system` 是否已声明正式接管
- 当前是否允许 activation 变更

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`

如本文档与口头认知冲突，以本文档和 live runtime 事实为准。

---

## 2. 当前硬结论

当前阶段必须按下面这条结论执行：

`/stock` 的正式身份已冻结，但 `airivo.online` 全域托管权尚未冻结。

因此：

- `/stock` 可以继续强化
- `apex` 只能继续承担内部验证职责
- 其余 live 路由在正式归属冻结前，不允许被 `stock_ultimate_system` activation 越权接管

---

## 3. 路由归属矩阵

| 路由 | 用户可见角色 | 当前 live 托管者 | `stock_ultimate_system` 是否已正式接管 | 当前状态 | 当前 activation 规则 | 阻断原因 |
| --- | --- | --- | --- | --- | --- | --- |
| `/stock/` | 唯一正式股票主链 | `stock-ultimate-dashboard.service` -> `127.0.0.1:8765` | 是 | 已冻结 | 允许继续强化 | 无 |
| `/stock/api/primary-result` | 正式主结果契约 | `stock-ultimate-dashboard.service` -> `127.0.0.1:8765` | 是 | 已冻结 | 允许继续强化 | 无 |
| `/apex/` | 内部验证主入口 | `airivo-apex-main-site.service` -> `127.0.0.1:18764` | 否，且不应转正 | 已冻结为内部环境 | 仅允许内部验证变更 | 禁止产品化 |
| `/apex/stock/` | 内部验证股票入口 | `airivo-apex-stock.service` -> `127.0.0.1:18765` | 否，且不应转正 | 已冻结为内部环境 | 仅允许内部验证变更 | 禁止并列正式化 |
| `/apex/T12/` | 内部验证治理入口 | `airivo-apex-t12.service` -> `127.0.0.1:18766` | 否，且不应转正 | 已冻结为内部环境 | 仅允许内部验证变更 | 禁止并列正式化 |
| `/` | 主站 | live nginx productized config 托管 | 否 | 未冻结 | 禁止被当前 activation 覆盖 | repo current 不等价承接 |
| `/app/` | 既有线上应用入口 | live nginx + upstream `127.0.0.1:8501` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/auth/login` | 认证入口 | live nginx + upstream `127.0.0.1:18601` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/auth/logout` | 认证入口 | live nginx + upstream `127.0.0.1:18601` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/auth/health` | 认证健康检查 | live nginx + upstream `127.0.0.1:18601` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/auth/session` | 会话检查 | live nginx + upstream `127.0.0.1:18601` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/health` | 线上探针入口 | live nginx + upstream `127.0.0.1:5101` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/chat` | 线上业务入口 | live nginx + upstream `127.0.0.1:5101` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/api/ai/chat` | AI bridge 入口 | live nginx + upstream `127.0.0.1:3443` | 否 | 未冻结 | 禁止被当前 activation 覆盖 | 当前 repo current 未声明承接 |
| `/T12/` | 对外治理入口 | 当前 live nginx legacy 静态/文档/API 组合托管 | 否，repo current 宣称接管但 live 未完成 | 未冻结 | 禁止被当前 activation 直接切换 | `8766` 未监听，live 与 repo current 不一致 |
| `/T12/api/stock-ai-runner` | 禁止暴露的治理侧 AI runner 入口 | live nginx 热补 `404` | 否 | 未冻结 | 禁止通过 activation 误改 | 当前靠热补维持 |
| `/T12/ops/stock-ai-runner` | 禁止暴露的治理侧运维入口 | live nginx 热补 `404` | 否 | 未冻结 | 禁止通过 activation 误改 | 当前靠热补维持 |

---

## 4. route ownership 解释规则

### 4.1 已冻结

含义：

- 正式角色已明确
- 允许继续强化
- 不需要再讨论“它到底归谁”

当前只有：

- `/stock/`
- `/stock/api/primary-result`
- `apex` 全部入口的“内部验证身份”

### 4.2 未冻结

含义：

- 当前 live 已存在用户可见行为
- 但 `stock_ultimate_system` 尚未完成正式接管条件
- 任何会整体覆盖 live 规则的 activation 都必须被阻断

当前必须明确视为未冻结的包括：

- `/`
- `/app/`
- `/auth/*`
- `/health`
- `/chat`
- `/T12/`

---

## 5. 当前执行规则

基于这张矩阵，当前阶段只允许做以下事情：

- 强化 `/stock` 的正式主链治理
- 强化 `/apex` 的内部验证身份
- 盘点并收口未冻结路由的正式托管归属
- 修 activation plan 的越权范围

不允许做的事：

- 用 non-dry-run activation 覆盖整份 live `airivo.online.conf`
- 把 `/apex` 重新拉回并列正式面
- 把 `/T12` 误写成已完成 `8766` 服务化接管
- 把 `/app`、认证、`/health`、`/chat` 默认视为可被当前 repo current 接管

---

## 6. 当前必须补齐的动作

只有下面这些动作完成，route ownership 才有资格从“盘点结果”进入“正式冻结结果”：

1. 明确主站 `/` 的正式托管责任人和目标实现
2. 明确 `/app/` 是否继续保留在当前域名下
3. 明确 `/auth/*` 是否继续归既有认证链托管
4. 明确 `/health`、`/chat`、`/api/ai/chat` 是否属于当前阶段范围
5. 明确 `/T12/` 是继续保留 legacy live 形态，还是正式切到 `8766` 服务

在这五项未完成前，任何“full-domain activation”都必须视为越权。

---

## 7. 阶段验收语句

在当前阶段，只允许使用下面这句对 route ownership 现状做结论：

`/stock formal ownership frozen; full-domain airivo.online ownership not yet frozen.`

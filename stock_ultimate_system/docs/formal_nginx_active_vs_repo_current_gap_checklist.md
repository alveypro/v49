# Formal Nginx Active vs Repo Current Gap Checklist

## 1. 文档定位

本文档用于把 formal active nginx 与 repo current 的差异，压成可执行清单。

它不追求“最终长什么样”，只记录：

- 当前 live nginx 在做什么
- repo current 想做什么
- 差异属于哪一类
- 是否阻断 non-dry-run activation
- 下一步该怎么处理

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/FORMAL_RUNTIME_CONVERGENCE_FINDINGS_2026-04-30.md`

---

## 2. 当前硬结论

当前 formal active nginx 与 repo current 不是同一张图。

因此：

- dry-run activation 可以继续作为演练
- non-dry-run activation 必须阻断

阻断原因不是语法错误，也不是单点服务挂掉，而是：

`repo current 试图覆盖 live 里仍由其他线上职责承载的路由与行为。`

---

## 3. 差异分类标准

本文档中的每条差异只允许归入以下三类之一：

- `A: 可接受差异`
  当前保留，不直接阻断 `/stock` 主链

- `B: 必须收口差异`
  不收口就不能进入 non-dry-run activation

- `C: 越权差异`
  repo current 当前无权覆盖 live 行为，必须先冻结 ownership 再处理

---

## 4. 差异清单

| 项目 | live nginx 当前行为 | repo current 当前行为 | 差异类型 | 是否阻断 non-dry-run activation | 建议处理 |
| --- | --- | --- | --- | --- | --- |
| TLS server scope | 承载 productized `airivo.online` 全域入口 | 视为 `stock_ultimate_system` formal 三入口专属 conf | C | 是 | 先冻结 full-domain ownership，再决定是否允许整份替换 |
| `/stock/` | 反代 `127.0.0.1:8765` | 反代 `127.0.0.1:8765` | A | 否 | 保持，继续围绕 `/stock` 主链强化 |
| `/apex/*` include | live conf 已 include `/etc/nginx/snippets/airivo-apex.locations.conf` | repo current formal nginx 未内置 productized 其余路由，只保留三入口骨架 | A | 否 | apex 继续以 snippet 形式托管，不转正 |
| `/` | live 提供 productized 主站和静态资源 | repo current 直接反代 `127.0.0.1:8764` | C | 是 | 在 `/` 归属冻结前，禁止整体替换 |
| `/app/` | live 反代 `127.0.0.1:8501` 并挂认证链 | repo current 不承接 `/app/` | C | 是 | 必须先决定 `/app/` 是否继续在当前域名存活 |
| `/auth/*` | live 反代 `127.0.0.1:18601` | repo current 不承接 `/auth/*` | C | 是 | 必须先冻结认证链托管归属 |
| `/health` | live 反代 `127.0.0.1:5101` | repo current 不承接 `/health` | C | 是 | 必须先冻结探针入口归属 |
| `/chat` | live 反代 `127.0.0.1:5101` | repo current 不承接 `/chat` | C | 是 | 必须先冻结业务入口归属 |
| `/api/ai/chat` | live 反代 `127.0.0.1:3443` | repo current 不承接 | C | 是 | 必须先冻结 AI bridge 入口归属 |
| `/T12/` 主入口 | live 仍是 legacy 静态/docs/API 组合 | repo current 反代 `127.0.0.1:8766` | B | 是 | 先决定是收敛到 `8766`，还是承认当前 legacy 形态并改 repo current |
| `/T12/api/stock-ai-runner` | live 靠热补 `404` 阻断 | repo current 假设 `8766` 统一承载，再由应用层/路径层处理 | B | 是 | 把阻断规则正式回收入仓，不能只靠热补 |
| `/T12/ops/stock-ai-runner` | live 靠热补 `404` 阻断 | repo current 假设 `8766` 统一承载，再由应用层/路径层处理 | B | 是 | 把阻断规则正式回收入仓，不能只靠热补 |
| `8764` formal main-site port | live 未监听 | repo current 要求存在 | B | 是 | 若继续 formal 三服务拓扑，必须先真正上线 `8764` |
| `8766` formal T12 port | live 未监听 | repo current 要求存在 | B | 是 | 若继续 formal 三服务拓扑，必须先真正上线 `8766` |
| nginx overall replacement strategy | live 是 productized conf | activation plan 直接 `cp nginx.airivo.online.conf /etc/nginx/conf.d/airivo.online.conf` | C | 是 | 改成 scoped strategy，禁止当前阶段整份覆盖 |

---

## 5. 当前可执行动作

基于上面这张表，当前只允许做以下收口动作：

1. 保持 `/stock/ -> 8765` 这条正式主链稳定
2. 保持 `apex` snippet 模式，不升格为正式面
3. 明确 `/`、`/app/`、`/auth/*`、`/health`、`/chat` 的 ownership
4. 明确 `/T12/` 是保留 legacy live 还是切到 `8766`
5. 把 activation plan 从“整体替换 nginx”改成“按冻结边界 scoped rollout”

---

## 6. 当前禁止动作

在这张 checklist 未清零前，禁止：

- 执行 non-dry-run activation
- 生成 passed 的正式 activation execution
- 生成正式 server deploy evidence bundle
- 登记 current formal deployment snapshot
- 声称 formal nginx 已经与 repo current 收敛

---

## 7. 收口优先级

当前应该按下面顺序处理差异：

1. 先处理 `C: 越权差异`
2. 再处理 `B: 必须收口差异`
3. 最后才考虑 `A: 可接受差异` 是否继续长期保留

原因很简单：

- 越权差异不解决，真实 activation 永远不能做
- 必须收口差异不解决，formal topology 永远说不清
- 可接受差异只影响最终整洁度，不决定当前阶段是否越权

---

## 8. 当前阶段结论

当前 formal nginx 差异结论必须固定写成：

`active nginx supports formal /stock, but full-domain formal nginx ownership has not converged to repo current.`

对 activation 的对应结论固定为：

`dry-run completed; non-dry-run activation blocked by unresolved ownership and topology gaps.`


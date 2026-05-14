# `T12` Formal Topology Decision

## 1. 文档定位

本文档用于冻结 `airivo.online/T12/` 当前阶段的 formal topology 决策。

它不讨论长期愿景，只回答：

- 当前阶段是否承认 live legacy `T12` 形态
- 当前阶段是否允许强切到 `127.0.0.1:8766`
- 这条决策对 activation 边界意味着什么

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/formal_nginx_active_vs_repo_current_gap_checklist.md`
- `docs/FORMAL_RUNTIME_CONVERGENCE_FINDINGS_2026-04-30.md`

---

## 2. 当前已核实事实

当前线上已核实事实如下：

1. live `airivo.online/T12/` 仍由 legacy 静态 / docs / API 组合托管
2. `127.0.0.1:8766` 当前未监听
3. `stock-ultimate-t12.service` 当前未真实上线
4. `/T12/api/stock-ai-runner` 与 `/T12/ops/stock-ai-runner` 当前依赖 live nginx 热补 `404`
5. repo current 里的 formal nginx 则假设 `/T12/ -> 127.0.0.1:8766`

结论：

`repo current 对 T12 的 formal topology 描述，当前并不是 live reality。`

---

## 3. 当前阶段决策

当前阶段冻结决策为：

`承认 T12 当前仍是 legacy live 形态，不允许在本阶段通过 activation 强切到 8766。`

这是当前阶段唯一合法决策。

原因不是不想收口，而是当前条件不满足：

- `8766` 未监听
- `stock-ultimate-t12.service` 未真实上线
- 热补边界尚未正式回收入仓
- 一旦强切，会把当前“边界虽然靠热补但对外正确”的状态替换成未经验证的新失败面

---

## 4. 当前阶段禁止项

在本决策生效期间，禁止：

1. 把 `/T12/` 口头宣布为“已完成 formal service 化”
2. 把 repo current 中的 `/T12/ -> 8766` 当成当前 live 事实
3. 在 non-dry-run activation 中切换 `/T12/` 到 `127.0.0.1:8766`
4. 以“顺手一起收口”名义把 `T12` 混进 `/stock` 主链 activation

---

## 5. 当前阶段允许项

允许：

1. 继续维持当前 live `T12` 正确边界
2. 盘点并文档化 hotfix 规则
3. 把 `/T12/api/stock-ai-runner` 和 `/T12/ops/stock-ai-runner` 的阻断规则正式回收入仓
4. 单独准备未来 `8766` 服务化的条件，而不是现在切换

---

## 6. 对 scoped rollout 的直接影响

基于当前决策，后续 `scoped_activation_rollout_spec.md` 必须遵守：

1. scoped rollout 当前不能把 `/T12/` 纳入“已可正式切换”范围
2. `/T12/` 只能被标记为：
   - `legacy-preserved`
   - `not-in-current-rollout-scope`
3. 任何把 `/T12/` 放进当前阶段 non-dry-run activation 的方案，都视为越权

---

## 7. 未来允许改判的前置条件

只有以下条件全部成立，才允许把当前决策从“承认 legacy live”改成“正式切到 8766”：

1. `stock-ultimate-t12.service` 已真实上线
2. `127.0.0.1:8766` 已稳定监听
3. `/T12` 边界不再依赖 live nginx 热补
4. `T12` 的 formal runtime、route、verification 已可重复验证
5. 该切换不会越权影响当前未冻结的 `airivo.online` 其他路由

缺任何一项，都不允许改判。

---

## 8. 当前阶段结论

当前阶段对 `T12` 的唯一合法结论是：

`T12 remains legacy live in this phase; 8766 cutover is explicitly blocked.`


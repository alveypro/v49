# 第一次真实 `stock-scoped` Activation Ownership 充分性改判路径

## 1. 文档定位

本文档只回答一个问题：

`当前 ownership sufficiency = not satisfied，未来要怎样才允许正式改判为 satisfied。`

它不是愿景文档，也不是执行日志，而是把改判路径压成一组必须补齐的证据要求。

---

## 2. 改判原则

只有当“不会越权”和“不会误伤”这两件事被书面证明，而不是口头相信时，`ownership sufficiency` 才允许从 `not satisfied` 改判为 `satisfied`。

换句话说，后续不是要证明：

- `/stock` 很重要
- dry-run 很稳定
- verification 很全

而是要证明：

- 第一次真实 `stock-scoped activation` 只会触碰被授权的 `/stock` 主链资产
- 第一次真实 `stock-scoped activation` 不会改变未冻结 live 路由的托管责任与对外行为

---

## 3. 改判为 `satisfied` 的必备证据

必须同时具备下面 4 组证据，缺一不可。

### 3.1 触碰范围证据

必须形成一份 `touch-set` 证明，明确列出第一次真实 `stock-scoped activation` 会触碰什么。

对应文档：

- `docs/first_real_stock_scoped_activation_touch_set.md`

至少要包含：

- 变更的应用文件范围
- 变更的 systemd unit 范围
- 变更的 drop-in 范围
- 明确不触碰的 nginx 全域配置范围

必须写实成一句：

`本次真实 activation 的触碰范围仅限 /stock 主链所需资产，不包含任何未冻结 live route ownership 的接管动作。`

### 3.2 非触碰范围证据

必须形成一份 `non-impact matrix`，明确列出本次真实 activation 不会触碰的 live 路由：

对应文档：

- `docs/first_real_stock_scoped_activation_non_impact_matrix.md`

- `/`
- `/app/`
- `/auth/*`
- `/health`
- `/chat`
- `/api/ai/chat`
- `/T12/`
- `/T12/api/stock-ai-runner`
- `/T12/ops/stock-ai-runner`

每个路由至少要写清：

- 当前 live 托管者
- 本次 activation 是否变更其 route ownership
- 本次 activation 是否变更其 nginx 行为
- 本次 activation 是否变更其 upstream/service 依赖

只有全部结论都是 `no change`，这组证据才算通过。

### 3.3 变更前后等价性证据

必须形成一份 `pre/post non-stock invariance proof`，证明本次真实 activation 前后，未冻结 live 路由的外部行为没有变化。

对应文档：

- `docs/first_real_stock_scoped_activation_pre_post_non_stock_invariance_proof.md`

至少覆盖：

- `/`
- `/app/`
- `/health`
- `/chat`
- `/T12/`

至少记录：

- HTTP status
- 关键响应行为
- 关键路由仍由原 live 形态承接

如果任一路由需要依赖“应该没事”的主观判断，而不是实测对比，则证据不通过。

### 3.4 书面批准证据

必须有一份明确的批准语句，至少同时满足：

- activation approver 明确确认“ownership sufficiency = satisfied”
- rollback authority owner 明确接受若证明失效则立即回滚
- execution window 与 rollback window 已经冻结

没有这份书面批准，就算技术证据齐全，也不能改判。

---

## 4. 改判动作顺序

要把 `ownership sufficiency` 从 `not satisfied` 推进成 `satisfied`，只能按下面顺序做：

1. 先补 `touch-set` 证据
2. 再补 `non-impact matrix`
3. 再补 `pre/post non-stock invariance proof`
4. 最后拿书面批准，把结论改判为 `satisfied`

不允许跳过前 3 步直接改结论。

---

## 5. 当前还没满足什么

当前最核心的缺口是：

- 还没有一份被批准的 `touch-set` 证明
- 还没有一份可复核的 `non-impact matrix`
- 还没有一份 `pre/post non-stock invariance proof`

因此当前只能继续维持：

`ownership sufficiency = not satisfied`

---

## 6. 一旦满足后的允许结论

只有当第 3 节的 4 组证据全部齐备并被批准后，才允许在正式文档里写：

`ownership sufficiency = satisfied`

并且只有在那之后，才允许推进：

- 第一次真实 `stock-scoped activation`
- 第一次正式 `server_deploy_evidence_bundle`
- 第一次正式 deploy registry current snapshot
- 第一次真实 readiness recheck

---

## 7. 当前阶段结论

当前阶段对此项的唯一正确推进方式是：

`先补 ownership sufficiency 改判证据，再谈改判；证据不齐，继续冻结。`

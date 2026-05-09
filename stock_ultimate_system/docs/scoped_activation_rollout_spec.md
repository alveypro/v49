# Scoped Activation Rollout Spec

## 1. 文档定位

本文档不是“能不能执行”的讨论稿，而是当前阶段受冻结边界约束的有限 rollout 设计规范。

它只回答四件事：

- 当前阶段 activation 允许覆盖的范围是什么
- 当前阶段 activation 明确不能覆盖什么
- `stock-scoped rollout` 应该长成什么样
- 这份 spec 对后续代码与部署改造提出什么约束

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/airivo.online_route_ownership_freeze_decision.md`
- `docs/t12_formal_topology_decision.md`

如本文档与任何“整份替换 live nginx”的旧流程冲突，以本文档为准。

---

## 2. 当前硬结论

当前阶段只允许设计和准备：

`stock-scoped rollout`

当前阶段明确不允许：

`full-domain non-dry-run activation`

原因已经冻结，不再重复争论：

- 未冻结 ownership 的全域路由仍存在
- `T12` 已冻结为 legacy-preserved
- 当前 activation plan 仍是整体替换 live nginx

所以这份 spec 的性质是：

`受冻结边界约束的有限 rollout 设计`

不是：

- 全域 cutover 方案
- 当前即可执行的正式 activation 授权书
- 对既有 live nginx 的整体接管许可

---

## 3. 当前阶段 rollout scope

### 3.1 允许纳入当前 rollout scope 的对象

当前阶段只允许把以下对象纳入 scoped rollout 设计范围：

1. `/stock/`
2. `/stock/api/primary-result`
3. `stock-ultimate-dashboard.service`
4. `stock-ultimate-entry-guard.service`
5. `stock-ultimate-entry-guard.timer`
6. `stock` 主链所需 canonical env
7. `/stock` 必需的 verification / rollback / evidence 产物链

### 3.2 明确排除在当前 rollout scope 外的对象

当前阶段必须显式排除：

1. `/`
2. `/app/`
3. `/auth/*`
4. `/health`
5. `/chat`
6. `/api/ai/chat`
7. `/T12/`
8. `/T12/api/stock-ai-runner`
9. `/T12/ops/stock-ai-runner`
10. `/apex/*` 的正式化切换

这些对象在当前阶段只能被：

- 盘点
- 标注
- 保留

不能被：

- 正式 cutover
- activation 覆盖
- 作为“顺手一并迁移”的附带范围

---

## 4. 当前阶段 rollout model

### 4.1 总体模型

当前阶段的 rollout model 必须从：

`replace live airivo.online.conf`

改成：

`apply stock-scoped changes without taking ownership of unfrozen live routes`

### 4.2 模型目标

目标只有三个：

1. 强化 `/stock` 唯一正式主链
2. 不越权覆盖未冻结 ownership 路由
3. 不把 `T12` 拉进当前 cutover 范围

### 4.3 模型约束

任何 scoped rollout 设计都必须满足：

1. 不要求整体替换 `/etc/nginx/conf.d/airivo.online.conf`
2. 不要求 `/` 由 `127.0.0.1:8764` 承接
3. 不要求 `/T12/` 由 `127.0.0.1:8766` 承接
4. 不修改 live 已承载的 `/app/`、`/auth/*`、`/health`、`/chat` 路由职责
5. 只围绕 `/stock` 必需链路和 formal verification 进行 scoped 变更

---

## 5. 当前阶段推荐的技术形态

### 5.1 nginx 层

当前阶段推荐形态：

- 保留 live productized `airivo.online.conf`
- 不整份替换
- 对 `/stock` 相关规则只做 scoped 对齐
- `apex` 继续保持 snippet include 形态
- `T12` 保持 legacy-preserved，不纳入本轮切换

这意味着后续工程改造应优先考虑：

- `location /stock/` scoped patch
- `/stock` 相关 verification scoped patch
- 而不是新的 full-domain formal nginx 文件直接替换 live conf

### 5.2 service 层

当前阶段只允许把以下 service 视为 rollout 主体：

- `stock-ultimate-dashboard.service`
- `stock-ultimate-entry-guard.service`
- `stock-ultimate-entry-guard.timer`

当前阶段不允许把以下 service 视为“本轮必须一起切换”：

- `stock-ultimate-main-site.service`
- `stock-ultimate-t12.service`

### 5.3 evidence / verification 层

当前阶段 scoped rollout 必须继续保留：

- dry-run activation
- post-deploy verification
- rollback hint
- `/stock` 与 `/apex/stock` 一致性校验

但不得生成以下误导性结论：

- “formal full-domain activation ready”
- “T12 cutover ready”
- “airivo.online fully converged to repo current”

---

## 6. 对 activation plan 改造的直接要求

后续若改 `build_server_activation_plan.py`，必须遵守下面这些要求：

### 6.1 必须新增 scope 概念

activation plan 后续必须支持至少两种 scope：

1. `stock-scoped`
2. `full-domain`

当前阶段只允许设计和验证 `stock-scoped`。

### 6.2 `stock-scoped` 模式必须满足

1. 不复制整份 formal nginx 到 live path
2. 不重启未冻结 ownership 的 service
3. 不要求 `8764` / `8766` 在线
4. 不把 `/T12/` 纳入切换对象
5. 只处理 `/stock` 所需 deployment assets

### 6.3 `full-domain` 模式当前必须保持阻断

除非 ownership 与 T12 topology 被重新冻结，否则：

- `full-domain` 只能存在于未来设计层
- 不得在当前阶段生成可执行 non-dry-run plan

---

## 7. 当前阶段 rollout 输出物要求

基于本 spec，当前阶段允许产生的 rollout 相关输出物只有：

1. `stock-scoped planning` 文档与命令草案
2. `stock-scoped dry-run` 结果
3. `/stock` 相关 verification artifact
4. ownership / topology / gap checklist

当前阶段不允许产生并宣称有效的输出物：

1. full-domain activation execution
2. full-domain deploy evidence bundle
3. full-domain deployment registry current pointer

---

## 8. 当前阶段禁止项

禁止：

1. 把 scoped rollout 写成 full-domain activation 的委婉说法
2. 在 spec 中偷偷把 `/T12/` 加回本轮 scope
3. 在 spec 中把 `/`、`/app/`、`/auth/*`、`/health`、`/chat` 视为默认一起迁
4. 用“反正 nginx 能 reload”替代 ownership freeze

---

## 9. 当前阶段结论

当前阶段对 rollout 的唯一合法结论是：

`Only stock-scoped rollout design is allowed in this phase; full-domain activation remains blocked.`

以及：

`T12 is legacy-preserved and explicitly outside the current activation scope.`


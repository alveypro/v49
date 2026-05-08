# 第一次真实 `stock-scoped` Activation Pre/Post Non-Stock Invariance Proof

## 1. 文档定位

本文档用于证明第一次真实 `stock-scoped activation` 前后，非 `/stock` 路由的外部行为保持不变。

它不是 activation plan，也不是 post-deploy verification 的替代品，而是 ownership sufficiency 改判所需的第三组证据。

只有当本文件被填实并复核通过，才允许进入：

- ownership sufficiency 最终改判审批

---

## 2. 证明目标

本文件必须证明下面这句话是真的：

`第一次真实 stock-scoped activation 前后，所有未冻结 non-stock live 路由的外部行为与托管形态保持等价，没有被本次 activation 误伤。`

如果做不到这一点，则 ownership sufficiency 不允许改判为 `satisfied`。

---

## 3. 适用范围

本证明至少覆盖以下对象：

- `/`
- `/app/`
- `/health`
- `/chat`
- `/T12/`

如有需要，可补充：

- `/auth/*`
- `/api/ai/chat`
- `/T12/api/stock-ai-runner`
- `/T12/ops/stock-ai-runner`

---

## 4. 采集规则

每个对象都必须记录两组证据：

1. activation 前
2. activation 后

每组证据至少包含：

- HTTP status
- 关键响应行为
- 是否仍由原 live 形态承接
- 采集时间
- 采集人

禁止：

- 用“感觉一样”代替对比
- 只记录状态码，不记录关键响应行为
- 只记录 activation 后结果，不保留 activation 前基线

---

## 5. Pre/Post Invariance Matrix

| 路由 / 对象 | activation 前 status | activation 前关键行为 | activation 前 live 托管形态 | activation 后 status | activation 后关键行为 | activation 后 live 托管形态 | 是否等价 | 证明依据 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `/` | `200` | `公开首页正常返回；标题为 "Airivo | A股研究与决策工作台"` | `既有 live 主站` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后判断` | `2026-05-01 公开 GET 基线；curl 返回 200 与首页标题` |
| `/app/` | `302` | `跳转到 /login?next=/app/` | `既有 live 应用入口` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后判断` | `2026-05-01 HEAD 基线；Location=https://airivo.online/login?next=/app/` |
| `/health` | `200` | `返回 application/json` | `既有 live 探针入口` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后判断` | `2026-05-01 HEAD 基线；content-type=application/json` |
| `/chat` | `405` | `Method Not Allowed；Allow=OPTIONS, POST` | `既有 live 业务入口` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后判断` | `2026-05-01 GET 基线；405 且 allow=OPTIONS, POST` |
| `/T12/` | `401` | `Basic Auth gate；realm=\"T12 Validation Console\"` | `legacy live T12 形态` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后采集` | `待真实 activation 后判断` | `2026-05-01 HEAD 基线；401 且 www-authenticate=Basic realm=\"T12 Validation Console\"` |

---

## 6. 必须明确回答的 6 个问题

本文件必须明确回答以下问题：

1. activation 前后 `/` 的外部行为是否完全等价？
   - `当前只能确认 activation 前基线。` activation 后尚未发生，必须待第一次真实 activation 后对比
2. activation 前后 `/app/` 的外部行为是否完全等价？
   - `当前只能确认 activation 前基线。` 当前已记录其 302 登录跳转行为，activation 后必须复采验证
3. activation 前后 `/health` 与 `/chat` 的外部行为是否完全等价？
   - `当前只能确认 activation 前基线。` `/health` 当前为 200 JSON，`/chat` 当前为 405 且 Allow=OPTIONS, POST
4. activation 前后 `/T12/` 是否继续由原 live 形态承接？
   - `当前只能确认 activation 前基线。` 当前为 401 Basic Auth gate，activation 后必须证明仍保持该 live 形态
5. 是否存在任何一个 non-stock 路由只能证明“看起来差不多”，而不能证明“行为等价”？
   - `当前存在。` 因为还没有真实 activation 后采集结果，所以目前只能证明 pre 基线，不能证明 pre/post 等价
6. 上述所有对象是否都能写成 `equivalent / no change`？
   - `当前不能。` 真实 activation 尚未发生，post 证据缺失，因此还不能把本文件作为等价性通过证据

只要任一问题回答不清，本文件就不能通过。

---

## 7. 采集与复核信息

### 7.1 Baseline 采集

- 采集人：`Codex based on public route sampling`
- 时间：`2026-05-01 Asia/Shanghai`
- 说明：`已采集 /、/app/、/health、/chat、/T12/ 的 pre-activation public behavior baseline`

### 7.2 Post-Activation 采集

- 采集人：`待真实 activation 后填实`
- 时间：`待真实 activation 后填实`
- 说明：`当前尚未发生 non-dry-run stock-scoped activation，不能伪造 post 结果`

### 7.3 Approver 复核

- 结论：`当前仅确认 pre-activation baseline 第一版事实稿已建立；本文件尚不足以形成 invariance passed 结论`
- 角色：`/stock` 唯一正式主链系统 owner
- 时间：`待 ownership sufficiency 最终审批时填实`

### 7.4 当前状态

- 当前状态：`pre-activation baseline 第一版事实稿已建立，post-activation 证据未发生`

---

## 8. 当前阶段结论

当前本文件的唯一合法结论是：

`pre-activation baseline drafted; post-activation invariance evidence not yet available.`

在真实 activation 发生并补齐 post-activation 对照前，禁止把本文件视为 ownership sufficiency 已通过的证据。

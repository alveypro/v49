# 第一次真实 `stock-scoped` Activation Touch-Set 证明

## 1. 文档定位

本文档用于证明第一次真实 `stock-scoped activation` 到底会触碰什么、不会触碰什么。

它不是执行日志，也不是 activation plan 本身，而是 ownership sufficiency 改判所需的第一组证据。

只有当本文件被填实并复核通过，才允许进入后续：

- `non-impact matrix`
- `pre/post non-stock invariance proof`
- `ownership sufficiency = satisfied` 改判审批

---

## 2. 证明目标

本文件必须证明下面这句话是真的：

`本次真实 activation 的触碰范围仅限 /stock 主链所需资产，不包含任何未冻结 live route ownership 的接管动作。`

如果本文件做不到这一点，则不能继续推进 ownership sufficiency 改判。

---

## 3. 本次 Activation 基本信息

- activation 类型：`第一次真实 stock-scoped activation`
- release id：`stock-scoped-20260501`
- activation plan 路径：`/opt/stock-ultimate/server_activation_plan.stock_scoped.json`
- activation approver：`/stock` 唯一正式主链系统 owner
- activation executor：`当班运维 / 发布执行人`
- rollback authority owner：`/stock` 唯一正式主链系统 owner

---

## 4. 触碰范围总表

下面所有条目都必须按“是否触碰”写实，不能空着。

| 类别 | 对象 | 是否触碰 | 触碰方式 | 备注 |
| --- | --- | --- | --- | --- |
| app files | `/opt/stock-ultimate/app` 下本次同步文件集合 | `是` | `rsync -a --delete --exclude data/ --exclude artifacts/ '/opt/stock-ultimate/staging/' '/opt/stock-ultimate/app/'` | `仅限 /stock 主链所需 staged app 文件` |
| config | `/opt/stock-ultimate/app/config/server/settings.yaml` | `是` | `mkdir -p .../config/server` + `cp settings.server.yaml -> settings.yaml` | `仅刷新 formal stock server config` |
| python deps | `/opt/stock-ultimate/.venv` 依赖安装 | `是` | `'/opt/stock-ultimate/.venv/bin/pip' install -r '/opt/stock-ultimate/app/requirements.txt'` | `不替换 venv 路径，仅安装 requirements` |
| systemd unit | `stock-ultimate-dashboard.service` | `是` | `cp unit file` + `systemctl restart` | `formal /stock 主服务` |
| systemd drop-in | `stock-ultimate-dashboard.service.d/canonical-artifacts.conf` | `是` | `mkdir -p` + `cp drop-in` | `保持 canonical artifact/env 指向` |
| systemd unit | `stock-ultimate-entry-guard.service` | `是` | `cp unit file` + `systemctl start` | `formal /stock 入口保护链` |
| systemd timer | `stock-ultimate-entry-guard.timer` | `是` | `cp timer file` + `systemctl enable --now` | `formal /stock 入口保护定时链` |
| live nginx full file | `/etc/nginx/conf.d/airivo.online.conf` | `否` | `无` | `stock-scoped plan 未包含整份替换；scope_constraints.allows_live_nginx_replacement=false` |
| nginx scoped snippet / patch | `/stock` 相关规则 | `否` | `无` | `当前 stock-scoped plan 不执行 live nginx scoped patch，只依赖既有 live stock routing` |
| deployment registry | `/opt/stock-ultimate/deployments/current.json` | `否` | `无` | `activation 本身不触碰；仅在 activation 成功后 evidence registration 才允许触碰` |
| deployment history | `/opt/stock-ultimate/deployments/history/*.json` | `否` | `无` | `activation 本身不触碰；仅在 evidence registration 后才允许触碰` |

---

## 5. 允许触碰对象

这一节只能列“允许触碰”的对象，而且必须能说明为什么属于 `/stock` 主链所需资产。

### 5.1 应用与配置

- `/opt/stock-ultimate/app` 下 `/stock` 主链所需 staged app 文件
- `/opt/stock-ultimate/app/config/server/settings.yaml`
- `/opt/stock-ultimate/.venv` 中由 `requirements.txt` 驱动的依赖安装

### 5.2 systemd 对象

- `stock-ultimate-dashboard.service`
- `stock-ultimate-dashboard.service.d/canonical-artifacts.conf`
- `stock-ultimate-entry-guard.service`
- `stock-ultimate-entry-guard.timer`

### 5.3 release / backup / activation 产物

- `/opt/stock-ultimate/releases/stock-scoped-20260501_previous_app`
- `/opt/stock-ultimate/server_activation_plan.stock_scoped.json`
- activation plan 中声明的 rollback commands 所依赖的 backup app 目录

---

## 6. 明确不触碰对象

这一节必须明确列出“本次真实 activation 不触碰”的对象。

至少包括：

- `/`
- `/app/`
- `/auth/*`
- `/health`
- `/chat`
- `/api/ai/chat`
- `/T12/`
- `stock-ultimate-main-site.service`
- `stock-ultimate-t12.service`
- `127.0.0.1:8764`
- `127.0.0.1:8766`
- 整份 live nginx 全域 ownership 接管

对每一项都要写清：

| 对象 | 不触碰的具体含义 | 证明方式 |
| --- | --- | --- |
| `/` | 不改变 route ownership、不切换到 `8764`、不改其对外行为 | `scope_constraints.allows_main_site_cutover=false`，且 activation commands 无 main-site/nginx replace 指令 |
| `/app/` | 不改变既有 live 托管责任与 nginx 行为 | activation commands 未包含 `/app` 路由或其 upstream 变更 |
| `/auth/*` | 不改变认证链 nginx 行为与托管责任 | activation commands 未包含 auth 路由相关变更 |
| `/health` | 不改变探针入口行为与托管责任 | activation commands 未包含 health 路由相关变更 |
| `/chat` | 不改变既有业务入口行为与托管责任 | activation commands 未包含 chat 路由相关变更 |
| `/api/ai/chat` | 不改变既有 API 路由行为与托管责任 | activation commands 未包含该路由相关变更 |
| `/T12/` | 不切换到 `8766`，继续保持 `legacy-preserved` | `scope_constraints.allows_t12_cutover=false`，且 activation commands 无 t12 service/nginx 变更 |
| `stock-ultimate-main-site.service` | 不复制 unit、不重启服务 | activation commands 不包含 main-site unit 或 restart |
| `stock-ultimate-t12.service` | 不复制 unit、不重启服务 | activation commands 不包含 t12 unit 或 restart |
| `127.0.0.1:8764` | 不要求 listener 在线，不引入正式依赖 | `scope_constraints.requires_8764_listener=false` |
| `127.0.0.1:8766` | 不要求 listener 在线，不引入正式依赖 | `scope_constraints.requires_8766_listener=false` |
| 整份 live nginx 全域 ownership 接管 | 不替换 `/etc/nginx/conf.d/airivo.online.conf`，不重载整域 formal ownership | activation commands 无 nginx file copy / nginx -t / nginx reload |

---

## 7. 必须明确回答的 5 个问题

本文件必须明确回答以下问题，不能留模糊话术：

1. 本次 activation 是否会整份替换 live nginx？
   - `不会。` 当前 `stock-scoped` plan 不包含 `/etc/nginx/conf.d/airivo.online.conf` 的 copy、`nginx -t`、`nginx reload`，且 `scope_constraints.allows_live_nginx_replacement=false`
2. 本次 activation 是否会改变 `/` 的对外行为？
   - `不会。` plan 不切换 main-site，不要求 `8764`，也不修改 live nginx 全域 ownership
3. 本次 activation 是否会改变 `/app/`、`/auth/*`、`/health`、`/chat` 的托管责任？
   - `不会。` plan 只覆盖 `/stock` 主链所需 app/config/systemd 对象，不包含这些路由的 nginx 或 upstream 变更
4. 本次 activation 是否会把 `/T12/` 从 `legacy-preserved` 拉入切换范围？
   - `不会。` plan 不复制或重启 `stock-ultimate-t12.service`，也不要求 `8766`，与 `legacy-preserved` 冻结结论一致
5. 本次 activation 是否只会改变 `/stock` 主链所需对象？
   - `是。` 当前 activation commands 仅触碰 `/stock` 主链所需 app/config/deps/dashboard/entry-guard/release-backup 对象

只要任一问题回答不清，本文件就不能作为 ownership sufficiency 改判证据。

---

## 8. 复核结论

### 8.1 执行人自检结论

- 结论：`基于 stock-scoped activation plan 与 dry-run 结果，本次 touch-set 已形成第一版事实稿；仍待执行人按正式 activation 前最后一次 plan 复核签字`
- 签名角色：`当班运维 / 发布执行人`
- 时间：`待正式 activation 前最后一次复核时填实`

### 8.2 Approver 复核结论

- 结论：`当前仅确认 touch-set 事实稿与既有 stock-scoped plan 一致；是否据此批准 ownership sufficiency 改判，仍待后续 non-impact matrix 与 invariance proof 一并复核`
- 签名角色：`/stock` 唯一正式主链系统 owner
- 时间：`待 ownership sufficiency 最终审批时填实`

### 8.3 当前状态

- 当前状态：`第一版事实稿已形成，最终签字结论未填实`

---

## 9. 当前阶段结论

当前本文件的唯一合法结论是：

`touch-set first-pass evidence drafted from the current stock-scoped activation plan; final signed evidence not yet completed.`

在最终签字与配套 `non-impact matrix`、`pre/post non-stock invariance proof` 完成前，禁止把本文件单独视为 ownership sufficiency 已通过的证据。

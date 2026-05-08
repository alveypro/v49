# Formal 现网收敛盘点（2026-04-30）

## 1. 文档定位

本文档记录 `Gate-1：现网收敛` 的第一轮真实盘点结果。

它不讨论方案优美与否，只记录：

- 当前 formal 现网到底是什么
- 与 repo current 到底差在哪
- 哪些已经收敛
- 哪些仍然是阻断项

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/FOUR_HARD_GATES_EXECUTION_BOARD.md`
- `docs/FORMAL_RUNTIME_CONVERGENCE_EXECUTION_PLAN.md`

---

## 2. 当前结论

当前 Gate-1 结论固定为：

`未过线`

原因不是 formal 站点不可用，而是：

- formal 业务面已经放行
- formal 运维与配置面仍未完全收敛

硬话直接说：

`现在的问题不是“能不能打开 /stock”，而是“当前现网运行形态和 repo current 还不是同一张图”。`

---

## 3. 已确认收敛项

### 3.1 formal stock 主服务在线

已确认：

- `stock-ultimate-dashboard.service` 存在
- 状态为 `enabled + active`
- `ExecStart` 指向：
  - `/opt/stock-ultimate/.venv/bin/python`
  - `/opt/stock-ultimate/app/run_dashboard.py`
  - `--port 8765`
  - `--base-path /stock`

结论：

- formal `/stock` 主服务链在线

### 3.2 entry guard timer 在线

已确认：

- `stock-ultimate-entry-guard.service` 存在
- `stock-ultimate-entry-guard.timer` 存在
- timer 状态为 `enabled + active`
- service 为 oneshot，最近一次执行成功

结论：

- formal `entry guard` 定时刷新链在线

### 3.3 formal 主链产物在线

已确认：

- `/opt/stock-ultimate/app/artifacts/current_result_pointer/current.json` 存在
- `/opt/stock-ultimate/app/artifacts/result_registry/` 存在
- `/opt/stock-ultimate/app/artifacts/run_registry/` 存在
- `/opt/stock-ultimate/app/artifacts/stock_entry_guard_latest.json` 存在

并且：

- `current_result_pointer.current.json` 的 `run_id / lifecycle_id` 与 formal 当前主链一致
- `stock_entry_guard_latest.json` 顶层 `run_id / lifecycle_id` 与当前 pointer 一致

结论：

- formal 当前主链产物并未丢失

### 3.4 formal release verification 脚本存在

已确认：

- `/opt/stock-ultimate/app/scripts/run_server_post_deploy_verification.py` 存在

结论：

- release verification 已有脚本基础

---

## 4. 已确认未收敛项

### 4.1 active nginx 与 repo current 不一致

repo current：

- `deploy/aliyun/nginx.airivo.online.conf`

其 formal 期望拓扑是：

- `/stock/ -> 127.0.0.1:8765`
- `/T12/ -> 127.0.0.1:8766`
- `/ -> 127.0.0.1:8764`

实际 active nginx：

- 使用的是 productized `airivo.online.conf`
- 包含：
  - `root /opt/airivo/public`
  - `include /etc/nginx/snippets/airivo-apex.locations.conf;`
- `/stock/` 走 `8765`
- `/T12` 仍是 legacy 静态 + docs + `/api/t12/`
- `/T12/api/stock-ai-runner` 和 `/T12/ops/stock-ai-runner` 通过热补规则直接 `404`

结论：

- active nginx 不是 repo current 那张 formal topology 图
- formal `/T12` 边界当前仍依赖热补规则

### 4.2 repo current 期望的 formal 三服务拓扑未成立

repo current 对应 formal service 资产包括：

- `stock-ultimate-main-site.service`
- `stock-ultimate-dashboard.service`
- `stock-ultimate-t12.service`

线上实查结果：

- `stock-ultimate-dashboard.service` 存在
- `stock-ultimate-main-site.service` 不存在
- `stock-ultimate-t12.service` 不存在

端口实查结果：

- `8765` 监听中
- `8764` 未监听
- `8766` 未监听

结论：

- formal runtime 当前不是 repo current 定义的三服务运行形态

### 4.3 `/T12` 边界仍是热补维持

已确认 active nginx 中显式存在：

- `location ^~ /T12/api/stock-ai-runner { return 404; }`
- `location ^~ /T12/ops/stock-ai-runner { return 404; }`

结论：

- 当前 `/T12` 边界虽然对外正确
- 但配置治理上仍属于热补债，而不是正式回收入仓后的天然状态

### 4.4 release verification 仍未形成正式产物链

已确认：

- formal verification 脚本存在
- 但当前未确认有固定输出的正式 verification artifact 路径与产物命名

结论：

- formal 目前是“能手工复验”
- 还不是“发布后自动留下正式 verification artifact”

---

## 5. 当前最关键阻断项

当前 Gate-1 最关键阻断项只有三条：

1. active nginx 与 repo current 不一致
2. formal 三服务拓扑未成立
3. `/T12` 边界仍依赖热补，而不是正式配置收口

硬话直接说：

`Gate-1 现在最大的债，不在 Python 服务本身，而在现网拓扑和配置治理。`

---

## 6. 当前最容易误判的点

### 6.1 不能因为 `/stock` 正常就判定 Gate-1 通过

`/stock` 正常只说明业务面可用，不说明现网配置收敛。

### 6.2 不能因为 `/T12` 现在是 404 就判定边界治理已完成

当前 `404` 是对的，但它依赖热补规则仍在线。

### 6.3 不能因为 entry guard timer 在线就判定 formal runtime 全收敛

timer 在线只说明主链刷新链在线，不说明 nginx、service topology、release verification 已收口。

---

## 7. 下一步唯一动作

基于这轮盘点，下一步只该做三件事：

1. 明确 formal canonical runtime topology  
   先决定：
   - 是把 repo current 改成当前 productized active topology
   - 还是把线上 active topology 收敛回 repo current 三服务图

2. 清理 `/T12` 热补债  
   把当前热补边界正式回收入仓配置与受控部署路径。

3. 固化 release verification artifact  
   让 formal 发布后自动留下正式 verification 产物。

---

## 8. 当前执行结论

这轮盘点后，Gate-1 状态应正式记为：

- `formal business runtime: passed`
- `formal entry guard timer chain: passed`
- `formal active nginx convergence: failed`
- `formal topology convergence: failed`
- `formal release verification artifactization: pending`

一句话收口：

`Gate-1 已经从“泛泛要收敛”进入“差异项已列实”；现在该开始清 active nginx、formal topology 和 release verification 这三笔核心账。`

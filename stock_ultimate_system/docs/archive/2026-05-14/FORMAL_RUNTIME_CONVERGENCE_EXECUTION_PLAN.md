# Formal 现网收敛执行方案

## 1. 文档定位

本文档只服务第一道硬门：

`Gate-1：现网收敛`

它不讨论 AI，不讨论样本闭环，不讨论新的页面重构。

它只回答一个问题：

`怎样把当前 formal 现网从“能跑”收成“配置、服务、路由、定时器、发布校验都一致”。`

上位约束：

- `docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `docs/TOP_TIER_GAP_MATRIX.md`
- `docs/FOUR_HARD_GATES_EXECUTION_BOARD.md`

---

## 2. 当前问题定义

当前 formal 发布已经成功，但现网收敛还没完成。

最关键的现状不是功能失败，而是：

- formal active nginx 仍不是完全按 repo current 在跑
- `/T12` 的部分阻断规则靠线上热补存在
- 当前“业务面已放行”不等于“运维面已完全收口”

硬结论：

`Gate-1 当前未过线。`

---

## 3. Gate-1 完成标准

只有同时满足下面五条，才允许把 Gate-1 标为完成：

1. formal active nginx 与 repo current 收敛
2. `/T12` 边界规则完全回收入仓配置
3. formal service / timer / route / artifact path 有一致性盘点结果
4. formal release verification 可重复执行
5. 机器重启后仍不靠人工热补保持当前边界

---

## 4. 现网收敛拆解

### 4.1 nginx 配置收敛

目标：

- 把 active nginx 中的正式 `/stock`、`/T12` 相关规则与 repo current 对齐

当前差距：

- active nginx 中仍存在热补逻辑
- repo current 与 active state 还不是单一事实源

最小动作：

- 导出当前 active formal nginx
- 与 `deploy/aliyun/nginx.airivo.online.conf` 做逐段 diff
- 明确：
  - 哪些规则属于必要保留
  - 哪些规则只属于热补债
  - 哪些规则必须正式回收入仓

验收命令：

```bash
nginx -t
grep -n "T12/api/stock-ai-runner\\|T12/ops/stock-ai-runner\\|location /stock/" /etc/nginx/conf.d/airivo.online.conf
```

放行标准：

- active config 中不再存在“只在线上有、仓库里没有”的关键边界规则

阻断标准：

- `/T12` 关键边界仍只能靠人工热补维持

### 4.2 service / timer 收敛

目标：

- formal 运行链不依赖人工记忆

最小动作：

- 盘点并确认以下正式资产：
  - `stock-ultimate-dashboard.service`
  - `stock-ultimate-entry-guard.service`
  - `stock-ultimate-entry-guard.timer`
- 明确它们的：
  - `WorkingDirectory`
  - `ExecStart`
  - `enabled / active`

验收命令：

```bash
systemctl status stock-ultimate-dashboard.service --no-pager
systemctl status stock-ultimate-entry-guard.service --no-pager
systemctl status stock-ultimate-entry-guard.timer --no-pager
systemctl is-enabled stock-ultimate-dashboard.service
systemctl is-enabled stock-ultimate-entry-guard.timer
```

放行标准：

- 关键 service / timer 都是 `enabled + active`

阻断标准：

- 机器重启后存在明显恢复风险

### 4.3 路由与边界收敛

目标：

- `/stock` 和 `/T12` 的边界由正式配置天然表达，而不是靠事后补丁

最小动作：

- 重新核 formal 的：
  - `/stock`
  - `/stock/api/primary-result`
  - `/T12/api/stock-ai-runner`
  - `/T12/ops/stock-ai-runner`

验收命令：

```bash
curl -sk --resolve airivo.online:443:127.0.0.1 https://airivo.online/stock/
curl -sk --resolve airivo.online:443:127.0.0.1 https://airivo.online/stock/api/primary-result
curl -sk --resolve airivo.online:443:127.0.0.1 -i https://airivo.online/T12/api/stock-ai-runner
curl -sk --resolve airivo.online:443:127.0.0.1 -i https://airivo.online/T12/ops/stock-ai-runner
```

放行标准：

- `/stock` 正常
- `/stock/api/primary-result` 正常
- `/T12` 下 AI runner 入口稳定 `404`

阻断标准：

- `/T12` 边界一旦重载 nginx 就漂移

### 4.4 release verification 收敛

目标：

- 让正式发布后的核验不再依赖人工口头结论

最小动作：

- 固定 formal release verify 最小命令集
- 统一输出 release verification artifact
- 把验证结果记成受控产物

建议最小核验项：

- dashboard service
- entry guard timer
- `/stock`
- `/stock/api/primary-result`
- `/T12` 边界
- guard `ok:true`

放行标准：

- 发布后有正式 verification 产物

阻断标准：

- 仍靠人工记忆“应该已经发好了”

---

## 5. 实施顺序

### 第一步

先做 nginx active vs repo current 的差异盘点。

### 第二步

把必须保留的边界规则正式回收入仓配置。

### 第三步

重装或重核 formal service / timer。

### 第四步

固化 formal release verification 命令与产物。

### 第五步

做一次完整重启级验证，确认不靠热补仍成立。

---

## 6. 当前禁止项

Gate-1 未过线前，禁止把重心切到：

- 真实 AI provider 扩场景
- 新页面扩张
- 长期样本闭环大规模建设
- 新入口、新 namespace、新工作台

硬话直接说：

`Gate-1 未过线前，再做别的，都是在不稳的底板上继续加层。`

---

## 7. 当前唯一执行结论

下一步不该平均开工四条线。

下一步只该执行：

`formal nginx/config/service/timer/release verification 收敛`

做到这一步之后，才有资格进入：

`Gate-2：长期稳定`

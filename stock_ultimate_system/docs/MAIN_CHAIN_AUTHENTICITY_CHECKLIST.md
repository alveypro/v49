# 主链真实性核对清单

## 1. 文档定位

本文档用于回答一个严格问题：

`当前 /stock 主链是否真实、完整、可发布，而不是只是页面还能打开。`

它只核对五件事：

- `current_result_pointer` 是否真实
- `result / run / artifact` 是否对齐
- `lifecycle evidence` 是否闭合
- `stock_entry_guard` 是否与主链一致
- `/stock` 与 `/api/primary-result` 是否共同消费同一条主链

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
3. 本文档
4. 其他 runbook、口头判断、页面观察结果

---

## 2. 适用场景

以下场景必须执行本清单：

- 正常日更后的主链验收
- latest 重建后的主链复核
- pointer / registry 修复后的复核
- formal deploy 后主链复核
- 补跑、回滚、故障恢复后的主链复核

不适用场景：

- 纯文案调整
- 与主结果主链无关的静态资源调整

---

## 3. 总原则

### 3.1 主链真实性高于页面可见性

下面这些现象都不能单独证明真实性通过：

- `/stock` 能打开
- `/api/primary-result` 返回 `200`
- 页面上显示了股票代码
- 某个 `latest` 文件存在

真实性只以链路事实为准。

### 3.2 先核 pointer，再核消费面

核对顺序必须固定：

1. `current_result_pointer`
2. `result_registry`
3. `run_registry`
4. `artifact_registry`
5. `lifecycle evidence`
6. `stock_entry_guard`
7. `/stock` 与 `/api/primary-result`

不允许倒过来。

### 3.3 冲突立即 fail closed

任一项冲突时，唯一合法结论是：

- 主链真实性未通过
- 当前不得补写“可发布”
- 必须进入修复、补跑或回滚流程

---

## 4. Blocking 项

以下任一项未通过，本次核对直接失败：

- `current_result_pointer/current.json` 缺失
- pointer 缺少 `result_id / run_id / artifact_ids / lifecycle_id / as_of_date`
- `result_registry` 找不到 pointer 对应 `result_id`
- `run_registry` 找不到 pointer 对应 `run_id`
- `artifact_registry` 缺少 pointer 声明的任一 `artifact_id`
- lifecycle evidence 缺失或与 pointer 不一致
- `stock_entry_guard_latest.json` 缺失
- `stock_entry_guard` 与 pointer 结论冲突
- `/stock` 与 `/api/primary-result` 未共同消费同一 `result_id / run_id / lifecycle_id`

---

## 5. 核对顺序

### Step 1：核对 pointer 完整性

正式入口：

- [check_current_result_pointer_integrity.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/check_current_result_pointer_integrity.py:1)

必须确认：

- `current_result_pointer/current.json` 存在
- `result_id` 非空
- `run_id` 非空
- `artifact_ids` 为非空列表
- `lifecycle_id` 非空
- `as_of_date` 非空
- `snapshot_path` 存在且可追溯

失败结论：

`pointer integrity failed`

### Step 2：核对 result / run / artifact 对齐性

必须确认：

- `result_registry` 中存在 pointer 指向的 `result_id`
- `run_registry` 中存在 pointer 指向的 `run_id`
- `artifact_registry.jsonl` 中存在 pointer 列出的全部 `artifact_id`
- 全部 artifact 的 `run_id` 与 pointer 一致
- 全部 artifact 的 `result_id` 与 pointer 一致

失败结论：

`registry alignment failed`

### Step 3：核对 lifecycle evidence

正式入口：

- [run_primary_result_lifecycle.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_primary_result_lifecycle.py:1)

必须确认：

- lifecycle evidence 已生成
- lifecycle evidence 的 `run_id / lifecycle_id / result_id` 与 pointer 一致
- lifecycle evidence 不来自 pytest/tmp 路径

失败结论：

`lifecycle evidence alignment failed`

### Step 4：核对 entry guard

正式入口：

- [run_stock_entry_guard.py](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/scripts/run_stock_entry_guard.py:1)

必须确认：

- `artifacts/stock_entry_guard_latest.json` 存在
- `ok=true` 时，pointer 与 artifact 链闭合
- `ok=false` 时，页面/API 必须 fail-closed
- `run_id / lifecycle_id` 与 pointer 一致

失败结论：

`entry guard failed or drifted`

### Step 5：核对 `/stock` 与 `/api/primary-result`

必须确认：

- `/stock` 当前主结论对象与 pointer 一致
- `/api/primary-result` 返回的 `result_id / run_id / lifecycle_id / as_of_date` 与 pointer 一致
- 若 guard blocked，两者都必须 fail-closed，不允许一边放行一边阻断

失败结论：

`formal consumer mismatch`

---

## 6. 最低命令集

### 6.1 pointer 完整性

```bash
python stock_ultimate_system/scripts/check_current_result_pointer_integrity.py
```

### 6.2 guard 刷新

```bash
python stock_ultimate_system/scripts/run_stock_entry_guard.py
```

### 6.3 formal deploy 后主链复核

```bash
python stock_ultimate_system/scripts/run_server_post_deploy_verification.py \
  --activation-plan /opt/stock-ultimate/server_activation_plan.stock_scoped.real.final.json \
  --output /opt/stock-ultimate/server_post_deploy_verification.stock_scoped.real.final.json \
  --json
```

### 6.4 `/stock` 与 `/api/primary-result` 抽查

```bash
curl -fsSL https://airivo.online/stock/
curl -fsSL https://airivo.online/stock/api/primary-result
```

说明：

- `curl` 只能作为消费面抽查
- 不能替代 pointer / registry / guard 核对

---

## 7. 通过标准

只有以下条件同时成立，本清单才允许判定：

`main chain authenticity passed`

通过条件：

- pointer 完整
- `result / run / artifact` 对齐
- lifecycle evidence 对齐
- entry guard 对齐
- `/stock` 与 `/api/primary-result` 共用同一主链

---

## 8. 失败后的唯一结论

如果任一 blocking 项未通过，只允许写：

`main chain authenticity failed`

并附失败类型：

- `pointer integrity failed`
- `registry alignment failed`
- `lifecycle evidence alignment failed`
- `entry guard failed or drifted`
- `formal consumer mismatch`

禁止写：

- “页面还可以”
- “大致没问题”
- “先放行，后面再补”

---

## 9. 与其他文档的关系

如果真实性失败：

- 正常日更、补跑、部署、回滚顺序回到 [MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md:1)
- 主结果事实源边界回到 [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
- formal deploy 后消费面核对回到 `server_post_deploy_verification`

本文档不负责决定怎么修，只负责决定：

`主链当前是否真实可信。`

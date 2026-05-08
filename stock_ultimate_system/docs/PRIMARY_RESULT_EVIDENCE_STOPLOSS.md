# Primary Result Evidence Stoploss

目的：停止把“主结果证据卡在 `1/20`”当成抽象问题讨论。  
本文件只处理一件事：为什么 `primary_result.entry_total` 没有持续增长，以及如何把这条证据流恢复成干净、稳定、可审计的生产闭环。

## 核心判断

当前问题不是一句“样本不够”就能解释完。  
更准确的说法是：

- 主结果证据流没有持续闭环入账。
- 现有 `1` 条样本高度疑似测试遗留。
- 当前指标既薄、又可能不干净。

因此本项优先级定义为 `P0`。  
在它处理干净之前，不应对外强化“策略证据强度”或“可放大性”的叙事。

## 目标状态

达到以下状态后，才可认为主结果证据流已止血：

- `primary_result_performance/ledger.jsonl` 的新增样本全部来自生产路径。
- `primary_result_performance/summary.json.entry_total` 能解释为什么增长或为什么不增长。
- `primary_result_performance_evidence_latest.json` 与 ledger / summary 一致。
- 任意一天如果没有新增样本，系统能明确指出断点在 observation、terminal、ledger 还是 evidence rebuild。
- 测试 artifacts 不再进入生产默认 evidence 路径。

## P0 排查链

主链路：

`current -> observation -> wait_status -> closure_preflight -> metrics -> terminal -> performance_ledger -> performance_evidence -> page`

只要其中任何一环不成立，`主结果证据` 就不会从 `1` 继续增长。

## 闭环断点排查表

### 1. 当前主结果对象

**应该存在的 artifact**
- `artifacts/primary_result_lifecycle/current.json`

**检查点**
- 当前 `result_id / ts_code` 是否与 observation、terminal 使用的是同一个对象。

**不存在或不一致会导致什么**
- observation、terminal、ledger 可能针对旧对象写入。
- 页面显示对象和实际入账对象脱节。

**修复后应看到的变化**
- 当前对象与 observation / terminal 的 `result_id` 一致。
- 这是后续增长的前提，不直接让 `1 -> 2`。

### 2. Observation Artifact

**应该存在的 artifact**
- `artifacts/primary_result_observation_latest.json`

**检查点**
- 文件是否真实存在于生产默认路径。
- `observation_status` 是否已经进入 `completed` 或 `failed`。
- `observation_window.started_at / ended_at` 是否完整。

**不存在会导致什么**
- performance ledger 无法 append。
- 闭环停在观察层。

**修复后应看到的变化**
- observation artifact 每天稳定生成。
- 后续 terminal / ledger 才有资格进入。

### 3. Observation Wait Status

**应该存在的 artifact**
- `artifacts/primary_result_observation_wait_status_latest.json`

**检查点**
- `status` 当前是 `pending_window`、`ready_for_data_check` 还是 `blocked`。
- `next_actions` 是否明确。

**不存在会导致什么**
- 团队无法知道当前是窗口未开始，还是已允许做 closure。
- 页面只能显示“待复核”，不能解释为什么不增长。

**修复后应看到的变化**
- 状态至少从 `pending_window` 推进到 `ready_for_data_check`。
- 这一步仍不直接让 `1 -> 2`，但决定后面能否 closure。

### 4. Closure Preflight

**应该存在的 artifact**
- `artifacts/primary_result_observation_closure_preflight_latest.json`

**检查点**
- `closure_outcome` 是否为 `completed` 或 `failed`。
- 是否存在 `metrics`。
- 是否列出 `blocking_reasons`。

**不存在会导致什么**
- orchestrator 不进入 metrics / terminal / ledger。
- 主结果证据不会增长。

**修复后应看到的变化**
- closure preflight 给出明确 outcome。
- 当天才有机会形成新 ledger entry。

### 5. Observation Metrics

**应该存在的 artifact**
- `artifacts/primary_result_observation_metrics_latest.json`

**检查点**
- `observed_return`
- `benchmark_return`
- `excess_return`
- `max_drawdown`
- `completion_criteria`

**不存在会导致什么**
- terminal 无法严谨生成。
- ledger 无法构造有效样本。

**修复后应看到的变化**
- metrics artifact 稳定落地。
- terminal 可以进入 `success` 或 `failed`。

### 6. Terminal Artifact

**应该存在的 artifact**
- `artifacts/primary_result_terminal_latest.json`

**检查点**
- `terminal_outcome` 是否仍为 `null`。
- `reason` 是否存在。
- 对应 `result_id` 是否和当前 observation 一致。

**不存在会导致什么**
- 主结果处于“未真正闭环”状态。
- 页面能展示对象，但不能形成可记账 closed sample。

**修复后应看到的变化**
- `terminal_outcome` 不再为 `null`。
- terminal 变为 `success` 或 `failed`。

### 7. Performance Ledger

**应该存在的 artifact**
- `artifacts/primary_result_performance/ledger.jsonl`
- `artifacts/primary_result_performance/summary.json`

**检查点**
- ledger 最新 entry 的 `source_observation_path` 是否来自生产路径。
- ledger 最新 entry 的 `recorded_at / result_id / window_ended_at` 是否合理。
- `summary.entry_total` 是否与 ledger 行数一致。

**不存在或异常会导致什么**
- evidence 永远不会增长。
- 或增长的是污染样本。

**修复后应看到的变化**
- `ledger.jsonl` 行数增长。
- `summary.json.entry_total` 从 `1` 增长到 `2`。
- 最新样本不再引用 pytest 临时目录。

### 8. Performance Evidence

**应该存在的 artifact**
- `artifacts/primary_result_performance_evidence_latest.json`

**检查点**
- `streams[].entry_total`
- `windows[].checks`
- `blocking_reasons`
- 与 ledger / summary 是否一致

**不存在会导致什么**
- 页面可能继续显示旧值。
- 团队误判为“ledger 没加”，实际上只是 evidence 没重建。

**修复后应看到的变化**
- `primary_result.entry_total` 与 ledger 同步增长。
- 页面从 `1/20` 变为 `2/20`。

### 9. 页面显示层

**应该存在的来源**
- evidence cockpit
- primary result latest facts

**检查点**
- 页面显示值是否和 `primary_result_performance_evidence_latest.json` 一致。

**不存在会导致什么**
- 用户看到旧值或错值。

**修复后应看到的变化**
- 前面 artifacts 更新后，页面值同步变化。
- 页面不是入账源，只是展示层。

## 当前最可能的真实断点排序

按当前已知事实，优先怀疑顺序如下：

1. 生产 observation artifact 没有稳定落在默认路径。
2. terminal 没有形成有效终局。
3. daily closure orchestrator 没有稳定跑到 append ledger。
4. 现有 ledger 被 pytest 样本污染。
5. evidence 只是诚实显示后端停在 `1`，并非前端 bug。

## 立即检查的 5 个问题

1. 今天的 `primary_result_observation_latest.json` 是否真实存在于生产默认路径？
2. 当前 observation 的 `observation_status` 是 `observing`、`completed` 还是 `failed`？
3. 今天的 `primary_result_terminal_latest.json` 里 `terminal_outcome` 是否仍然为 `null`？
4. `ledger.jsonl` 最新一条是否仍然来自 pytest 临时目录？
5. today close 之后 `summary.json.entry_total` 是否有新增？

## P0 动作清单

### P0-1 Ledger 溯源审计

目标：
- 分清当前唯一样本是否为测试污染。

动作：
- 审计 `ledger.jsonl` 所有 entry 的 `source_observation_path`。
- 标记哪些来自 pytest 临时目录，哪些来自正式生产路径。
- 输出一份 “可信样本 / 非可信样本” 分类结果。

验收：
- 能明确回答当前 `1` 是否为生产样本。

### P0-2 测试与生产 artifact 隔离

目标：
- 测试运行不再写入生产默认 evidence 路径。

动作：
- 检查测试是否仍有默认路径写入。
- 检查 orchestrator / ledger / evidence 相关测试是否强制使用 tmp_path。
- 对生产路径写入增加更清晰的环境边界。

验收：
- 测试样本不再进入生产 ledger / summary / evidence。

### P0-3 主结果闭环断点日检

目标：
- 每天都能回答“为什么今天没有新增主结果样本”。

动作：
- 固定检查以下产物：
  - `current.json`
  - `primary_result_observation_latest.json`
  - `primary_result_observation_wait_status_latest.json`
  - `primary_result_observation_closure_preflight_latest.json`
  - `primary_result_terminal_latest.json`
  - `ledger.jsonl`
  - `summary.json`
  - `primary_result_performance_evidence_latest.json`

验收：
- 每天能够定位唯一断点。

### P0-4 连续增长报警

目标：
- `entry_total` 连续不增长时，系统自动报出断点位置。

动作：
- 为 `primary_result.entry_total` 增加连续不增长检查。
- 报警时指向 observation、terminal、ledger 或 evidence rebuild。

验收：
- 不再需要靠人工猜测“为什么还是 1”。

## 推荐检查命令

```bash
sed -n '1,200p' stock_ultimate_system/artifacts/primary_result_lifecycle/current.json
sed -n '1,200p' stock_ultimate_system/artifacts/primary_result_observation_wait_status_latest.json
sed -n '1,200p' stock_ultimate_system/artifacts/primary_result_terminal_latest.json
sed -n '1,120p' stock_ultimate_system/artifacts/primary_result_performance/ledger.jsonl
sed -n '1,200p' stock_ultimate_system/artifacts/primary_result_performance/summary.json
sed -n '1,260p' stock_ultimate_system/artifacts/primary_result_performance_evidence_latest.json
```

如果观察工件缺失，再补查：

```bash
rg -n "primary_result_observation_latest.json|primary_result_terminal_latest.json|primary_result_observation_closure_preflight_latest.json" stock_ultimate_system/artifacts stock_ultimate_system/data -g '!**/.venv/**'
```

## 验收标准

在以下条件同时满足前，不应宣称主结果证据流已恢复：

- ledger 最新样本来自正式生产路径。
- `summary.entry_total` 能解释为什么增长。
- `primary_result_performance_evidence_latest.json` 与 ledger 一致。
- 页面 `主结果证据` 能稳定跟随 artifacts 变化。
- 连续数天都能明确知道“今天为什么加样本，或为什么没加样本”。

## 禁止事项

- 禁止为了把 `1` 改成 `2` 而手工补写 ledger。
- 禁止混用测试样本和生产样本。
- 禁止在 `terminal_outcome = null` 时对外强化证据叙事。
- 禁止把页面刷新误判成证据增长。

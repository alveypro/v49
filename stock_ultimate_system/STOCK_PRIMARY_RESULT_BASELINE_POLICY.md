# STOCK_PRIMARY_RESULT_BASELINE_POLICY.md

## /stock 官方 Benchmark Baseline 政策

### 1. 什么是官方 baseline

官方 benchmark baseline 是当前 `/stock` canonical 单轨主结果系统被认可的基准发布结果。

它不是任意一次测试输出，而是用于后续比较、发布判断和持续优化留档的正式比较基线。

### 2. baseline 的组成

官方 baseline 至少绑定以下产物：

- `stock_primary_result_benchmark_report.json`
- `stock_primary_result_benchmark_report.md`
- benchmark diff 对比结果
- `release_evidence_bundle.json`
- `release_gates.json`
- `release_pipeline_manifest.json`
- `primary_result_release_decision.json`
- `artifacts/baselines/current.json` 当前 baseline 指针
- `artifacts/baselines/history/<baseline_id>.json` 不可变历史快照

### 2.1 baseline 生命周期工件

`/stock` baseline 现在不是概念性说明，而是正式生命周期工件：

- `artifacts/baselines/current.json`
  只表示当前生效 baseline 的指针，不承载完整历史快照。
- `artifacts/baselines/history/*.json`
  每次晋升生成一次不可覆盖的 immutable snapshot。
- `src/stock_baseline_registry.py`
  负责 baseline promotion、current pointer 切换、history 查询与 rollback。
- `scripts/promote_stock_baseline.py`
  负责执行正式 baseline promotion / rollback。
- `scripts/run_stock_release_pipeline.py --promote-baseline`
  负责在统一发布流水线内显式执行 baseline promotion，并把结果写入 release summary 与 evidence bundle。该入口必须提供 approved release decision。
- `artifacts/primary_result_release_decisions/current.json`
  只保存当前 release decision 指针。
- `artifacts/primary_result_release_decisions/history/*.json`
  保存不可覆盖的 release decision 历史快照。
- `artifacts/primary_result_production_readiness/current.json`
  保存当前 production readiness 指针。
- `artifacts/primary_result_production_readiness/history/*.json`
  保存不可覆盖的 production readiness 证据结论。
- `artifacts/artifact_registry.jsonl`
  负责登记 release 与 baseline 相关产物路径、sha256、producer、run_id 与 artifact type，支持后续按运行查询。

### 3. baseline 更新条件

只有在以下条件同时满足时，才允许更新 baseline：

- release gates 全部通过
- benchmark report 结构稳定
- benchmark diff 未出现 blocking regression
- release evidence bundle 可稳定生成
- release pipeline manifest 可稳定生成
- release evidence checklist 已完成，且 release decision 已批准
- `/stock` 仍为 canonical 单轨运行

正式 promotion 时还必须满足：

- `primary_result_release_decision.json` 存在，`decision=approved`
- release decision 必须显式允许 `baseline_promotion_allowed=true`
- release decision 必须保持 `do_not_auto_apply=true`
- `release_gates.json` 存在且 `status=passed`
- `release_gates.json` 中无失败项
- `release_evidence_bundle.json` 中 `blocking_status_summary` 不含阻断失败
- `release_pipeline_manifest.json` 中的 benchmark report、benchmark diff、release gates 哈希必须与输入产物一致
- baseline snapshot 的 `run_id`、report、diff、gates、evidence bundle、manifest 必须互相可追溯
- baseline snapshot 必须记录 release decision path 与 hash
- 历史 snapshot 不允许覆盖，current pointer 只允许指向已有 immutable snapshot
- 如通过统一发布流水线请求 promotion，promotion 失败必须使本轮 release summary 状态变为 failed
- 如统一发布流水线复用既有 `release_gates.json`，仍必须把该 gate JSON 复制为本轮 release artifact，并进入 manifest hash、evidence bundle 与 artifact registry

### 4. 允许重设 baseline 的情况

以下情况可以重设 baseline：

- benchmark registry 正式扩容，且属于受控增强
- render contract version 或 runtime observability version 正式升级
- 内容质量规则正式收紧，且未造成阻断级退化

### 5. 不允许覆盖 baseline 的情况

以下情况属于退化，不能覆盖 baseline：

- `has_blocking_regression=true`
- `blocking_total` 增加
- release gate 失败
- `/stock` 混入治理摘要内容
- 主站承接业务主结果判断
- 主站主入口不再指向 `/stock`

### 6. baseline 与 report / diff / evidence bundle 的关系

- benchmark report
  表示当前版本的正式结果摘要
- benchmark diff
  表示当前版本与 baseline 的变化分类
- release evidence bundle
  表示本轮发布的最小证据集合

baseline 的更新应基于这三者共同判断，而不是基于单一测试通过。

### 6.1 production readiness 关系

baseline promotion 完成后，不代表系统已完成生产闭环。只有同时存在 approved release decision、有效 baseline current pointer、terminal success、performance ledger success，才允许生成 `PrimaryResultProductionReadiness`。

production readiness 的 `ready` 只是证据结论，不是自动上线命令；`blocked` 必须保留阻断原因。

### 7. rollback 原则

baseline rollback 允许发生，但只能：

- 回退 current pointer 到已有历史 snapshot
- 保留所有历史 snapshot，不允许删除或重写旧 baseline
- 通过 registry 层执行，保证 rollback 后仍可追溯当前 baseline 指向何处
- CLI 入口为 `scripts/promote_stock_baseline.py --rollback-baseline-id <baseline_id>`

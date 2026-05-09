# Artifact Pollution Cleanup Inventory

目的：记录当前 `artifacts/` 目录内已确认的 pytest / 临时目录污染项，作为清洗和重建的执行清单。

## 已确认污染的 latest / current 工件

### P0 立即清洗

- `artifacts/primary_result_performance/ledger.jsonl`
  - 问题：唯一 entry 的 `source_observation_path` 指向 `pytest-of-mac` 临时目录。
  - 影响：当前 `primary_result 1/20` 不是可信生产样本。

- `artifacts/primary_result_performance/summary.json`
  - 问题：完全由上述污染 ledger 派生。
  - 影响：`entry_total=1`、`success_rate=1.0` 等统计均不可信。

- `artifacts/primary_result_performance_evidence_latest.json`
  - 问题：读取了污染后的 primary ledger。
  - 影响：页面主结果证据 `1/20` 不可作为生产证据口径。

### P1 紧随处理

- `artifacts/primary_result_candidate_baskets/feedback_latest.json`
  - 问题：`source_observation_path` 与 `source_performance_summary_path` 均指向 `pytest-of-mac` 临时目录。
  - 影响：候选篮反馈 latest 不能视为可信生产 latest。

- `artifacts/primary_result_candidate_handoff_gate_latest.json`
  - 问题：`snapshot_path` 指向 `pytest-of-mac` 临时目录下的 lifecycle history。
  - 影响：handoff gate latest 带测试残留，不能视为可信生产 latest。

## 当前未在本轮扫描中发现污染的 latest / current 工件

- `artifacts/primary_result_lifecycle/current.json`
- `artifacts/primary_result_candidate_baskets/current.json`
- `artifacts/primary_result_candidate_baskets/latest_attempt.json`

说明：这不代表它们永久安全，只代表本轮针对 `pytest/tmp` 来源的扫描未发现污染路径。

## 清洗原则

- 不把测试样本和生产样本混用。
- 不保留带 `pytest-*`、`pytest-of-*`、`/tmp/`、`/var/folders/...` 来源的 latest 产物作为生产最新态。
- 清洗后应由可信生产输入重新生成 latest，而不是手工伪造内容。

## 重建顺序

1. 清空或隔离污染的 `primary_result_performance` latest。
2. 从可信 observation / terminal / closure 产物重新生成 ledger、summary、evidence。
3. 清空或隔离污染的 `candidate_basket feedback latest`。
4. 从可信 observation 与 performance summary 重新生成 feedback latest。
5. 清空或隔离污染的 `candidate_handoff_gate latest`。
6. 从当前可信 candidate 与 lifecycle pointer 重新生成 handoff gate latest。

## 验收标准

- 所有 production latest 工件中的 `source_*_path` 和 `snapshot_path` 不再指向 pytest 或临时目录。
- 页面显示值来自清洗后的 latest 工件。
- 主结果证据计数以可信生产样本为准，而不是历史测试残留。

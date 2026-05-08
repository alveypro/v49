# Primary Result Latest Rebuild Runbook

目的：在不引入新功能的前提下，清洗被 pytest / 临时目录污染的 latest artifacts，并从可信生产输入重建主结果和候选篮的 latest 证据层。

## 适用范围

本 runbook 只处理以下对象：

- `artifacts/primary_result_performance/ledger.jsonl`
- `artifacts/primary_result_performance/summary.json`
- `artifacts/primary_result_performance_evidence_latest.json`
- `artifacts/primary_result_candidate_baskets/feedback_latest.json`
- `artifacts/primary_result_candidate_handoff_gate_latest.json`

## 原则

- 不手工伪造 ledger entry。
- 不把测试样本当生产样本保留。
- 清洗后必须由可信生产输入重新生成 latest。
- 先审计、再隔离、后重建、最后验收。

## 第 0 步：先做污染审计

推荐命令：

```bash
python3 stock_ultimate_system/scripts/inspect_artifact_source_pollution.py --json
```

期望结果：

- 明确列出所有 `latest/current/summary/ledger` 文件里命中的污染路径。
- 输出 `polluted_file_total`。

## 第 1 步：隔离当前脏 latest

说明：
- 这一步不要求立即删除历史。
- 建议先把当前已确认污染的 latest 文件移出 latest 路径，改存到隔离目录或备份目录。

最低要求：
- 污染文件不能继续作为生产 latest 被页面或后续脚本消费。

## 第 2 步：重建主结果 latest

前提：
- 当前 `primary_result` 对象可信。
- `primary_result_observation_latest.json`、`primary_result_terminal_latest.json` 来自可信生产输入。

推荐顺序：

1. 运行 current primary result daily closure，尝试重新闭合 observation / terminal / ledger：

```bash
python3 stock_ultimate_system/scripts/run_current_primary_result_daily_closure.py --json
```

2. 如果 ledger 重新生成成功，再重建 performance evidence：

```bash
python3 stock_ultimate_system/scripts/build_primary_result_performance_evidence.py --json
```

3. 再刷新 operations 级 latest 聚合：

```bash
python3 stock_ultimate_system/scripts/refresh_primary_result_operations_artifacts.py --json
```

预期变化：
- `artifacts/primary_result_performance/ledger.jsonl` 不再引用 pytest 路径。
- `artifacts/primary_result_performance/summary.json` 由可信 ledger 重建。
- `artifacts/primary_result_performance_evidence_latest.json` 与 ledger 一致。

## 第 3 步：重建候选篮 latest

前提：
- 当前 basket snapshot 可信。
- 当前 basket observation 能从可信生产输入生成。

推荐命令：

```bash
python3 stock_ultimate_system/scripts/run_current_candidate_basket_observation.py --json
```

预期变化：
- `artifacts/primary_result_candidate_baskets/feedback_latest.json` 不再引用 pytest 路径。
- 对应 performance summary / observation 来源为可信生产路径。

## 第 4 步：重建 handoff gate latest

推荐命令：

```bash
python3 stock_ultimate_system/scripts/build_primary_result_candidate_handoff_gate.py --json
```

预期变化：
- `artifacts/primary_result_candidate_handoff_gate_latest.json` 的 `snapshot_path` 不再指向 pytest 临时目录。

## 第 5 步：做二次污染验收

再次执行：

```bash
python3 stock_ultimate_system/scripts/inspect_artifact_source_pollution.py --json
```

验收标准：
- `polluted_file_total == 0`
- 相关 latest 文件不再出现：
  - `pytest-`
  - `pytest-of-`
  - `/tmp/`
  - `/var/folders/`

## 第 6 步：页面验收

重建完成后再核对页面：

- `/apex/stock` 的主结果证据是否仍显示历史污染值
- 候选篮证据是否来自可信 latest
- handoff gate 与 operations refresh 是否同步更新

## 失败时怎么处理

### 如果 current daily closure 仍无法产出可信 ledger

说明：
- 问题不在 latest rebuild，而在闭环上游。

应该回退检查：
- current candidate / lifecycle pointer
- observation artifact 是否存在
- wait status 是否仍 `pending_window`
- terminal 是否仍 `null`

### 如果 basket feedback 仍无法重建

应该回退检查：
- current basket snapshot
- observation latest
- performance summary

### 如果 handoff gate 仍引用 pytest 路径

说明：
- lifecycle current pointer 仍然带着测试残留。

应该回退检查：
- `artifacts/primary_result_lifecycle/current.json`
- lifecycle history snapshot 来源

## 最终验收口径

只有同时满足以下条件，才算本轮清洗与重建完成：

- latest artifacts 不再含 pytest/tmp 路径。
- 主结果 ledger / summary / evidence 来自可信生产输入。
- 候选篮 feedback latest 来自可信生产输入。
- handoff gate latest 的 snapshot_path 来自可信 lifecycle history。
- 二次审计 `polluted_file_total == 0`。

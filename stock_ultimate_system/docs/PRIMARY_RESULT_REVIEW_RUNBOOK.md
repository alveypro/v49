# /stock 候选质量 Review Runbook

## 1. 文档定位

本 runbook 只解决一件事：

`如何使用 daily planner 与 morning operations brief，驱动 /stock 候选质量评估、失败样本反哺与机制整改的日常执行。`

它不讨论部署，不讨论页面，不讨论临时判断。

执行优先级：

1. [STRICT_CONTINUATION_EXECUTION_STANDARD.md](/Users/mac/Desktop/M-agent2026/stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md:1)
2. [INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md](./archive/2026-05-14/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md)
3. 本 runbook

## 2. 每日唯一入口

每天 review 只允许从以下两个正式产物开始：

- `artifacts/primary_result_daily_planner_latest.json`
- `artifacts/primary_result_morning_operations_brief_latest.md`

长窗厚度推进只允许看这一个正式补充产物：

- `data/experiments/candidate_quality_density_progress.json`

禁止：

- 先翻多个 experiments 文件再自己拼判断
- 跳过 planner 直接看 queue history
- 只看页面，不看 candidate quality / failure evidence

## 3. 每日执行顺序

### Step 1. 读 morning brief

先确认：

- `scoreboard_status`
- `promotion_decision`
- `owner_workload_schedule`
- `benchmark_execution_batches`
- `candidate_iteration_schedule`
- `candidate_quality_sample_density`
- `candidate_quality_density_progress`

### Step 2. 先处理 critical/high review workload

排序规则固定为：

1. `critical_priority_total` 更高的 owner 先处理
2. `high_priority_total` 更高的 owner 次之
3. `open_total` 更多的 owner 再次之

如果 `owner_workload_schedule` 中存在：

- `critical_priority_total > 0`

则当天禁止：

- baseline promotion
- “质量提升已成立”口头结论
- 跳过 benchmark 验证直接推进整改

### Step 3. 再处理 benchmark batch

只按 planner 中的 `benchmark_execution_batches` 顺序执行：

- `batch_01_expedite`
- `batch_02_standard`
- `batch_03_backlog`

禁止人工改序，除非更新 planner 上游产物。

### Step 4. 最后处理 candidate iteration schedule

只按 `candidate_iteration_schedule.sequence` 顺序执行。

优先处理：

- `priority_band=critical`
- 然后 `high`
- 然后 `medium/low`

禁止跳过高优先级 failure field，直接先改自己更熟的模块。

## 4. 长窗样本厚度硬门槛

若 morning brief 中出现：

- `120d: status=blocked`

则当天必须把以下动作放进 `next_actions` 的前列：

- 补厚 validation history
- 检查 `candidate_quality_multiwindow_source`
- 禁止把长窗结论写成“已稳定领先”

一句硬规则：

`120d 样本厚度不足时，只允许说“结构已建立、长期样本未站稳”，不允许说“长期领先已成立”。`

并且必须额外确认：

- `remaining_samples_needed`
- `progress_ratio`
- `latest_validation_date`

如果 `remaining_samples_needed > 0`，当天 review runbook 必须把“补正式 validation history”排进前列动作。

## 5. Promotion 阻断规则

只要任一成立，当天禁止进入 promotion review：

- `promotion_decision != promotion_review_allowed`
- `critical_priority_total > 0`
- `open_high_priority_total > 0`
- `120d sample density is insufficient`
- `candidate_quality_diff.pass_or_fail = blocked`

## 6. 每日收口要求

每日 review 完成后，必须至少产出：

- 更新后的 queue / decision history
- 如有 benchmark 执行，则更新 benchmark execution evidence
- 如有机制整改结论，则更新 candidate quality diff 对应 remediation 结论

禁止只口头说“今天看过了”。没有产物，就算没完成。

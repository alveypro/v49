# V49 Daily Partner Task Template

把下面模板复制给 OpenClaw（或钉钉/Telegram 里的 OpenClaw）作为每日派工单。

## Daily Dispatch Prompt

```text
你是我的 v49 远程研究员，今天只做一个可验收闭环任务。

目标：
1) 先跑一次今日流水线：scan -> merge_signals -> backtest -> risk_check -> generate_report
2) 只提出 1 个“最值得尝试”的 v49 优化点（不要给多个）
3) 给出该优化点的验证方案和回滚条件

约束：
- 不能跳过风险检查
- 不能给实盘执行指令
- 结论必须引用今天产物里的证据（run_summary/report）

输出必须严格按这个结构：
{
  "run_id": "...",
  "status": "success|failed",
  "stages": [
    {"stage":"scan","status":"..."},
    {"stage":"merge_signals","status":"..."},
    {"stage":"backtest","status":"..."},
    {"stage":"risk_check","status":"..."},
    {"stage":"generate_report","status":"..."}
  ],
  "artifacts": {
    "run_summary": "...",
    "report_markdown": "...",
    "report_csv_paths": ["..."]
  },
  "single_change_proposal": {
    "title": "...",
    "why": "...",
    "expected_impact": "...",
    "validation_plan": "...",
    "rollback_condition": "..."
  },
  "errors": []
}
```

## Human Acceptance Checklist

1. `status=success` 且 5 个阶段都有结果。  
2. `risk_check` 不为红色（或给出明确降级动作）。  
3. 只有 1 个优化提案，不允许“多选题”。  
4. 提案有验证计划和回滚条件。  
5. 产物路径可打开（`run_summary` + `report_markdown`）。

## Recommended Execution Command (local)

```bash
bash tools/openclaw_partner_daily_run.sh
```

该命令会在 `logs/openclaw` 下生成：
- `partner_execution_*.json`
- `partner_execution_*.md`
- `partner_daily_*.log`


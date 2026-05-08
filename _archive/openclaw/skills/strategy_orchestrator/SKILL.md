---
name: strategy-orchestrator
description: Orchestrate v49 strategy runs (scan, backtest, report) through a controlled workflow with traceable run IDs and deterministic task states.
---

# Strategy Orchestrator

## Use this skill when
- User asks to run multi-step tasks: scan -> rank -> report -> notify.
- User needs reproducible execution with logs and status tracking.

## Workflow
1. Create `run_id` and persist request metadata.
2. Validate strategy ID and parameter ranges.
3. Execute task stages in order:
   - `scan`
   - `backtest` (optional)
   - `merge_signals`
   - `risk_check`
   - `generate_report`
4. Save outputs and stage status.
5. Return compact execution summary.

## Output contract
Return:
- `run_id`
- `status` (`success` or `failed`)
- `stages`
- `artifacts`
- `errors`

## Guardrails
- Reject out-of-policy actions (for example, live trade commands in read-only mode).
- Do not swallow exceptions; return normalized error payload.

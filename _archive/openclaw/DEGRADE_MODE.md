# OpenClaw Risk Degrade Mode

`openclaw/run_daily.py` now emits a `fallback_plan` in each `run_summary_*.json`.

## Behavior

- `risk_level=green`
  - `execution_mode=normal`
  - Keep current params for next run.

- `risk_level=orange`
  - `execution_mode=degraded`
  - Returns conservative `next_run_params` for next run.
  - If `signal_density_collapse` is triggered, profile is adjusted to keep diagnostic signal flow.

- `risk_level=red`
  - `execution_mode=halted`
  - `auto_execute_next_run=false`
  - `next_run_params={}`
  - Report opportunities are cleared by safety gate (no actionable picks output).

## Example fields

```json
{
  "risk": {"risk_level": "red"},
  "fallback_plan": {
    "execution_mode": "halted",
    "auto_execute_next_run": false,
    "next_run_params": {}
  }
}
```

## Ops recommendation

When `execution_mode=halted`, require manual review before next scheduled run.

## Scheduler integration

`openclaw/scripts_run_daily.sh` supports fallback parameters automatically.

- `OPENCLAW_USE_LAST_FALLBACK=1` (default): read latest `run_summary_*.json` in `OPENCLAW_OUTPUT_DIR`.
- if latest `fallback_plan.execution_mode=degraded`: override next run params automatically.
- if latest `fallback_plan.execution_mode=halted`: skip the scheduled run safely.

### DB freshness gate

`openclaw/scripts_run_daily.sh` also validates local market DB freshness before execution.

- `OPENCLAW_REQUIRE_FRESH_DB=1` (default): enable freshness check.
- `OPENCLAW_MAX_DB_STALE_DAYS=3` (default): max allowed staleness.
- if stale days exceed threshold, run is skipped safely.

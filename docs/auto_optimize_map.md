# Auto Optimize Map

## 1) System Overview

This system has two optimization loops running in parallel:

- Offline strategy optimization loop (`auto_evolve.py`): grid-searches strategy params and writes best snapshots.
- Online assistant tuning loop (`trading_assistant.py`): adjusts assistant config from recent learning outcomes.

Both loops are fed by daily workflow outputs (`openclaw/run_daily.py`) and market DB updates.

## 2) Three Lanes

### Data Lane

1. Data sources: Tushare (+ AkShare fallback in some updates).
2. `auto_evolve.py` updates local DB tables (daily, market cap, moneyflow, northbound, margin, top list, fund portfolio).
3. `openclaw/run_daily.py` generates `run_summary_*.json`, tracking artifacts, scoreboard.
4. Assistant learning store (`logs/openclaw/assistant_learning.db`) accumulates learning cards and outcomes.

### Param Lane

1. `auto_evolve.py` grid searches:
   - v4: threshold / holding / stop-loss / take-profit.
   - v5/v6/v7/v8/v9/ai/stable: per-strategy threshold/holding style.
2. Hard filters in score function:
   - reject if `win_rate < 45%`.
   - reject if `max_drawdown < -12%`.
3. Best params persisted into:
   - `evolution/*.json` (for UI/runtime consumption).
   - `evolution_*` tables in DB (history/audit).
4. `trading_assistant.py` micro-tunes assistant config from:
   - `learning_cards.outcome_json.horizons.d5/d20` first.
   - `trade_history` fallback if samples are sparse.

### Execution Lane

1. Schedulers:
   - launchd/systemd timers invoke `auto_evolve.py` and/or daily workflow.
2. Daily workflow (`openclaw/run_daily.py`) does:
   - scan -> backtest -> risk -> report -> publish -> tracking.
3. Post-run hooks:
   - `run_self_learning_cycle()`
   - `TradingAssistant.apply_auto_tuning()`
4. UI (`v49_app.py`) reads best snapshots and exposes "generate/apply tuning" controls.

## 3) Key Files

- Core optimize:
  - `auto_evolve.py`
  - `evolution/last_run.json`
  - `evolution/health_report.json`
- Assistant tune:
  - `trading_assistant.py`
  - `trading_assistant.db` (config/trades)
  - `logs/openclaw/assistant_learning.db` (learning cards/outcomes)
- Daily pipeline:
  - `openclaw/run_daily.py`
  - `logs/openclaw/run_summary_*.json`
- UI:
  - `终极量价暴涨系统_v49.0_长期稳健版.py` (or deployed `v49_app.py`)

## 4) What "Auto Optimize Is Working" Means

All of these should hold:

1. `evolution/last_run.json` is recent (not stale).
2. `evolution/health_report.json` exists and is not warning-heavy.
3. Learning DB has recent cards and non-empty D5/D20 outcome samples.
4. `trading_assistant.py` includes both methods:
   - `get_auto_tuning_recommendation`
   - `apply_auto_tuning`
5. Daily flow still calls `ta.apply_auto_tuning()` after run completion.

## 5) Typical Failure Modes

1. Version drift:
   - UI expects new assistant methods but deployed assistant file is old.
2. Sample starvation:
   - learning cards exist but `horizons.d5/d20` rarely populated.
3. Data freshness gating:
   - optimize phase exits early when data is stale in enforced window.
4. Runtime mismatch:
   - service Python differs from shell Python, causing false syntax alarms.

## 6) Operator Checklist

Run:

```bash
bash tools/auto_optimize_doctor.sh
```

Interpretation:

- `GREEN`: healthy.
- `YELLOW`: degraded but running.
- `RED`: broken path needing immediate action.

## 7) Recommended Next Improvements

1. Emit daily KPI JSON:
   - sample_count, tuned_params_count, last_tuned_at, stale_days.
2. Add explicit "auto_tuning_applied_at" in assistant config table.
3. Expose one combined status card in UI:
   - data freshness, optimize freshness, learning sample sufficiency, tuning apply status.

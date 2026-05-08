# v49 Modular Architecture (Phase 1)

This repository now has a migration-friendly modular skeleton:

- `ui/` UI entry and composition
- `strategies/` strategy registry/profiles
- `data/` DAO and migrations
- `backtest/` backtest engine wrappers
- `risk/` dual-channel risk engine (`market_risk` + `system_risk`)
- `assistant/` assistant service wrapper

## Thin Entrypoint

- `main.py` is the thin entrypoint.
- During transition, it forwards to root legacy app through `ui/app.py`.

## DB Migrations

- SQL files: `data/migrations/*.sql`
- Runner: `python3 -m data.migrations.runner`
- Tracks applied versions in `schema_version` table.

## Regression

- Smoke: `bash scripts/regression/run_smoke.sh`
- Integration: `bash scripts/regression/run_integration.sh`
- Overnight: `bash scripts/regression/run_overnight.sh`

## Production Script Control

- Allowlist: `tools/production/ALLOWED_SCRIPTS.json`
- Runner: `python3 tools/production/run_allowed.py <script>`

## Observability

- `openclaw/run_daily.py` appends JSON lines to `metrics_daily_YYYYMMDD.jsonl`.
- Rollup: `python3 scripts/metrics/daily_rollup.py --input <jsonl>`

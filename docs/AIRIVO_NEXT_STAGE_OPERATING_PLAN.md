# Airivo Next Stage Operating Plan

Current system identity:

`v9 canary driven Top5 trading candidate system with hard evidence gates.`

It is not an automatic trading system and does not make an accuracy promise.

## Stage Goals

For the next 20-60 trading days, do not expand the strategy set. Improve
evidence quality, execution observability, and daily repeatability.

## Daily Evidence Loop

Required order:

1. Data health check.
2. `v9,stable` evidence run.
3. Top5 audit and forward gate.
4. Trader brief manifest rebuild.
5. Page or manifest health check.

Command:

```bash
python3 tools/run_daily_v9_evidence_pipeline.py \
  --db-path /opt/openclaw/permanent_stock_database.db \
  --site-health-url https://airivo.online/stock/
```

Failure rule:

- If any step fails, the current Top5 must be treated as `not executable`.
- Do not fall back to old artifacts to restore a green page.

## Execution Evidence Loop

Every trading day, record the Top5 candidate state at minimum as `planned`:

```bash
python3 tools/record_top5_execution_observation.py \
  --manifest exports/top5_trader_brief_latest_manifest.json \
  --status planned \
  --operator daily_ops
```

For each symbol after market or after broker feedback, append one observation
with actual status:

```bash
python3 tools/record_top5_execution_observation.py \
  --ts-code 600522.SH \
  --status filled \
  --actual-entry-price 12.34 \
  --filled-qty 1000 \
  --slippage-bp 18 \
  --operator daily_ops
```

Required fields to collect:

- candidate and manifest lineage
- planned entry, exit, stop
- actual entry and exit
- fill status and quantity
- slippage
- stop/take-profit trigger
- unfilled or skipped reason
- failure attribution

Daily completeness check:

```bash
python3 tools/check_top5_execution_observation_completeness.py \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --open-output-csv exports/top5_execution_open_observations.csv
```

The CSV `exports/top5_execution_open_observations.csv` is the daily operations
work queue. Each row must be closed by appending a new observation with one of:
`filled`, `partial_fill`, `not_filled`, `cancelled`, `stopped`, `take_profit`,
or `manual_skip`.

After operations fills the CSV, import the updates:

```bash
python3 tools/import_top5_execution_observation_updates.py \
  --input-csv exports/top5_execution_open_observations.csv \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --operator daily_ops \
  --output-dir logs/openclaw/top5_execution_imports
```

Run with `--dry-run` before the real import. Every import writes an immutable
batch manifest and reject CSV under `logs/openclaw/top5_execution_imports/`.
The manifest records input CSV SHA256, ledger SHA256 before/after, imported
keys, and rejected rows. Treat this as the audit trail; do not edit the JSONL
ledger by hand.

Preferred daily close command:

```bash
python3 tools/close_top5_execution_day.py \
  --input-csv exports/top5_execution_open_observations.csv \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --operator daily_ops
```

This dry-runs the import, rebuilds completeness, risk summary, and v9 promotion
readiness. After the dry-run is clean, commit:

```bash
python3 tools/close_top5_execution_day.py \
  --input-csv exports/top5_execution_open_observations.csv \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --operator daily_ops \
  --commit
```

The command refuses commit when rows are skipped unless `--allow-skipped` is
explicitly set. Use `--allow-skipped` only with a written reason in the batch
report.

Required update fields:

- `filled` / `partial_fill`: `actual_entry_price`, `filled_qty`, `slippage_bp`
- `stopped` / `take_profit`: `actual_exit_price` and reason or note
- `not_filled` / `cancelled` / `manual_skip`: `miss_reason` or `failure_attribution`

Daily risk summary:

```bash
python3 tools/build_top5_execution_evidence_summary.py \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --output-json exports/top5_execution_evidence_summary.json \
  --output-md exports/top5_execution_evidence_summary.md
```

Use the summary risk level operationally:

- `green`: evidence closed; eligible for review.
- `yellow`: minor open items; do not promote strategy status.
- `orange`: closure below operating threshold; keep execution closure as audit-only.
- `red`: parse errors, no planned evidence, or material evidence gaps; stop any production promotion claim.

Daily operations SLA:

```bash
python3 tools/check_top5_execution_ops_sla.py \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --output-json exports/top5_execution_ops_sla.json \
  --output-md exports/top5_execution_ops_sla.md
```

Interpretation:

- `current_trade_date_open_count > 0`: same-day work queue remains open.
- `stale_open_observation_count > 0`: historical execution debt; Top5 must not be described as operationally executable.
- Set `AIRIVO_REQUIRE_NO_STALE_OPEN=1` only after daily close discipline is stable; then stale historical opens become a hard pipeline failure.

V9 promotion readiness:

```bash
python3 tools/evaluate_v9_canary_promotion_readiness.py \
  --ledger logs/openclaw/top5_execution_observations.jsonl \
  --import-dir logs/openclaw/top5_execution_imports \
  --output-json exports/v9_canary_promotion_readiness.json \
  --output-md exports/v9_canary_promotion_readiness.md
```

Promotion interpretation:

- `<20` closed evidence trading days: blocked.
- `20-59` closed evidence trading days with clean evidence: reviewable, not promotable.
- `>=60` closed evidence trading days with clean evidence: eligible only for separate human approval.
- Any ledger parse error, quality gap, missing import audit trail, or closure rate below 95% keeps v9 blocked.

Court-of-record evidence bundle:

```bash
python3 tools/build_top5_execution_court_record.py \
  --root . \
  --output-json exports/top5_execution_court_record.json \
  --output-md exports/top5_execution_court_record.md
```

This is the single audit entry point for review. It hashes the Top5 manifest,
open-observation CSV, execution ledger, SLA report, evidence summary, v9
promotion readiness, and latest import manifest. Use it for post-mortem,
promotion review, and operator accountability.

The production pipeline currently records this as an audit check. Promote it to
a hard gate only after the manual or broker-feedback update process is reliable:

```bash
AIRIVO_REQUIRE_EXECUTION_CLOSURE=1 \
python3 tools/run_daily_v9_evidence_pipeline.py \
  --db-path /opt/openclaw/permanent_stock_database.db \
  --site-health-url https://airivo.online/
```

## Frozen Strategy Admission

Current registry policy:

- `v9`: canary; may contribute Top5 only after evidence gate passes.
- `stable`: shadow; defensive allocator candidate, no direct Top5 contribution.
- `v8`, `combo`: shadow; comparison and research only.
- `v5`: baseline; benchmark only.
- `v6`: research; no production admission.
- `v7`: retired; no production admission.
- `v4`: research/diagnostic.

Admission rule:

No strategy enters Top5 unless it has the same walk-forward contract, sufficient
sample count, traceable execution evidence, and explicit registry promotion.

## Promotion Criteria

After 20 trading days:

- no broken daily evidence loop
- manifest lineage complete
- execution observation ledger complete
- no hidden fallback or rescue

After 60 trading days:

- evaluate net return distribution, not only win rate
- review drawdown and slippage
- classify failure attribution
- decide whether v9 remains canary, moves to production, or is demoted

Production promotion requires a separate human approval artifact.

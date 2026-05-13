# Airivo Tools Boundary

This directory should contain stable daily operations entrypoints, not every
historical review, adjudication, or experiment script.

## Stable Daily Entrypoints

Keep these commands visible as the normal operator surface:

- `run_daily_v9_evidence_pipeline.py`
- `record_top5_execution_observation.py`
- `check_top5_execution_observation_completeness.py`
- `import_top5_execution_observation_updates.py`
- `close_top5_execution_day.py`
- `build_top5_execution_evidence_summary.py`
- `check_top5_execution_ops_sla.py`
- `evaluate_v9_canary_promotion_readiness.py`
- `build_top5_execution_court_record.py`
- `rebuild_top5_trader_brief_exports.py`
- `evaluate_top5_forward_returns.py`
- `top5_audit_evidence_gate.py`
- `run_top5_competition_audit_then_gate.sh`
- `tool_boundary_audit.py`

## Candidates For Consolidation Or Archive

Scripts with names starting with these prefixes should not stay as permanent
top-level operator commands unless they are promoted through the runtime boundary
map:

- `build_strategy_competition_*`
- `review_strategy_competition_*`
- `adjudicate_strategy_competition_*`
- `reconcile_strategy_competition_*`
- `ensemble_alpha_*`
- `ensemble_rebuilt_*`
- `archive_failed_*`
- `predeclare_*`
- `backfill_*`

Current archive namespace:

- `tools/archive/strategy_competition/`
- `tools/archive/research/`
- `tools/archive/maintenance/`
- `tools/archive/governance/`

Current intentional top-level manual review exceptions: none.

Do not promote manual-review scripts without adding them to the allowlist in
`tool_boundary_audit.py` and documenting why they belong in the daily operator
surface.

CI boundary command:

```bash
python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2
```

Preferred end state:

- durable logic lives in `openclaw/services/`;
- one stable CLI calls it with explicit modes;
- retired one-off scripts move under an archive namespace with their evidence
  manifests.

Do not delete or move these scripts without first generating a manifest and
checking references from docs, CI, scheduler units, and tests.

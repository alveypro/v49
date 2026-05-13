# Airivo Mainline PR Gate

## Mainline Authority

- Delivery standard: `docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md`
- 30-day execution plan: `docs/AIRIVO_30_DAY_EXECUTION_PLAN.md`
- Adjudication checklist: `_archive/docs/AIRIVO_MAINLINE_DELIVERY_ADJUDICATION_CHECKLIST.md`
- Review rejection standard: `docs/AIRIVO_CODE_REVIEW_REJECTION_STANDARD.md`

## Scope Declaration

- Formal scope:
  - [ ] `/stock` candidate chain
  - [ ] `/T12` fixed five-ticket governance chain
  - [ ] `execution feedback` closed loop
  - [ ] `trading kernel`
  - [ ] unified release / rollback chain
- Why this change belongs to exactly one formal scope:

## Runtime Boundary Review

- Runtime boundary map checked: `docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md`
  - [ ] Yes
- Review packet boundary touched:
  - [ ] `tools/` boundary
  - [ ] `/stock` dashboard shell
  - [ ] archived governance downgrade
  - [ ] documentation / CI anti-regression gates
  - [ ] not applicable
- Why this change does not expand the production operating surface:

## Unique Truth Source

- Canonical module / table / file / service:
- What old or parallel source is explicitly *not* authoritative:
- Does this change introduce or preserve a second truth source?
  - [ ] No
  - [ ] Yes, and this PR must be rejected

## Rollback Path

- Rollback command, script, or procedure:
- Data / artifact impact during rollback:
- How to verify rollback success:

## Daily Outputs Impact

- Does this PR change existing `daily outputs` behavior, format, timing, or paths?
  - [ ] No
  - [ ] Yes, with explicit compatibility and rollback notes below
- Notes:

## Drift / Rejection Self-Check

- [ ] No parallel entry added
- [ ] No second mainline introduced
- [ ] No second truth source introduced
- [ ] No root-level runtime artifact, ad hoc script, or unmanaged entry added
- [ ] No cross-scope responsibility mixing without explicit boundary ownership
- [ ] Change can be explained by one formal scope, one truth source, one rollback path
- [ ] Required docs were updated if governance, release, or mainline behavior changed
- [ ] Required tests or gates were updated if kernel, migration, or execution behavior changed

## Validation

- [ ] `python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2`
- [ ] Stock dashboard boundary tests were run when `/stock`, `run_dashboard.py`, or CI routing changed
- [ ] Relevant tests or smoke checks were run
- [ ] Release / rollback impact was reviewed

## Archived Governance Gate Checklist

- Runbook followed when archived governance evidence is relevant: `docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md`
  - [ ] Yes
- Archived gate execution mode:
  - [ ] local full scan (`bash _archive/tools/run_governance_gate_ci.sh`)
  - [ ] diff scan (`GOVERNANCE_BASE_SHA/GOVERNANCE_HEAD_SHA`)
- Blocking mode:
  - [ ] informational only
  - [ ] blocking with `AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`
- Governance artifact (if produced by gate tooling):
- Execution attribution hygiene dry-run artifact (`execution_attribution_backfill_dry_run_*.json`):
- If hygiene dry-run found gaps (`patched_count > 0`), remediation evidence:
  - [ ] `python tools/archive/strategy_competition/backfill_execution_attribution.py --apply ...` executed in controlled step
  - [ ] post-remediation gate rerun passed
- Required gate env values used in this PR context:
  - `AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH`:
  - `AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STATUSES`:
  - `AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STALE_MINUTES`:
  - `AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_MAX_ORDERS`:

## Strategy Optimization Evidence

- Is this PR changing strategy optimization, backtest selection, combo/v8/v6 logic, or promotion governance?
  - [ ] No
  - [ ] Yes, evidence below is required
- Evidence manifest (`AIRIVO_STRATEGY_PR_EVIDENCE_FILE`):
- Backtest sweep artifact path:
- `backtest_credibility` confirmed in sweep artifact:
  - [ ] Yes
- `strategy_backtest_diagnostics` confirmed in sweep artifact:
  - [ ] Yes
- `strategy_optimization_stage_audit` JSON path:
- `strategy_optimization_stage_audit` Markdown path:
- Rejected artifact ledger path:
- Gate tests run (`full` or `specified`):
- Gate test result artifact path(s):
- If strategy moves from `observation` to `candidate`, linked evidence must include:
  - [ ] `decision_id`
  - [ ] `order / fill / attribution`
  - [ ] `slippage`
  - [ ] `miss reason`
  - [ ] `manual override` record
  - [ ] linked `run_id`

## Reviewer Focus

- Highest-risk area:
- What would make this PR an immediate reject:

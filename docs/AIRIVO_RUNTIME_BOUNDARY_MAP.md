# Airivo Runtime Boundary Map

Status: frozen baseline for slimming work
Date: 2026-05-14

This document is the single operating map for the current Airivo/OpenClaw
repository. It defines which directories may own production responsibilities,
which paths are compatibility surfaces, and what must not grow.

## System Identity

Current identity:

`v9 canary driven Top5 trading candidate system with hard evidence gates`

The system is not an automatic trading system. It does not make an accuracy or
return promise. For the next 20-60 trading days, the system must improve daily
evidence quality, execution observability, and repeatability instead of adding
new strategies.

## Five Boundaries

| Boundary | Owner | Allowed responsibilities | Forbidden responsibilities |
| --- | --- | --- | --- |
| `stock_ultimate_system/` | Public `/stock` product surface | `/stock` page, primary-result API, static/public rendering, stock-domain read models, release-visible artifacts | Private Streamlit operations, direct trading action console, broad governance adjudication, strategy experiments |
| `openclaw/runtime/` | Private operator runtime | Streamlit page modules, local runtime orchestration, UI dependency adapters, task state views | Public product contract, strategy scoring ownership, long-term business logic that belongs in services |
| `openclaw/services/` | Testable service layer | Durable business services, evidence builders, governance gates, artifact and lineage services | One-off court/review/submission scripts as permanent APIs, UI rendering, direct route ownership |
| `strategies/` | Strategy implementation | Strategy registry, evaluators, scan pipeline, explicit production/canary/shadow/research tiers | UI orchestration, governance release authority, filesystem cleanup |
| `tools/` | Daily operations entrypoints | Stable CLI commands for daily evidence, cleanup dry-run, scheduler install, verification, deployment handoff | Long-lived sprawl of one-off review/build/adjudicate scripts, duplicate service logic |

## Entrypoint Rules

- `stock_ultimate_system/run_dashboard.py` may remain the public HTTP launcher
  during transition, but route-specific responsibilities must continue moving
  into focused modules under `stock_ultimate_system/src/`.
- `v49_app.py` is a compatibility and private Streamlit launch shell. New
  production business logic must not be added to it.
- `start_v49_full.sh` may launch the private console, but it must not become
  the public `/stock` product contract.
- `/stock` is the only formal stock-business public surface currently allowed
  to strengthen.
- `/T12` is read-only governance status. It may consume shared facts but must
  not become a business console or writeback system.
- `/` main site is platform entry and product matrix only. It must not output
  stock primary judgments.

## Strategy Admission

The only strategy line allowed to improve production relevance in the next
stage is `v9`, and only behind hard evidence gates.

Current tier interpretation:

- `v9`: canary, candidate for Top5 only after evidence gates.
- `stable`: shadow defensive allocator candidate.
- `v8`, `combo`: shadow and comparison only.
- `v5`: historical baseline.
- `v6`, `v4`, `ai`, `ensemble_core`: research only.
- `v7`: retired.

No strategy may enter Top5 unless it has a walk-forward contract, sufficient
sample count, traceable execution evidence, and explicit registry promotion.

## Data And Runtime Assets

Runtime data is not code. The local repository currently keeps a large
compatibility database at:

- `permanent_stock_database.db`

The latest full rollback database is:

- `permanent_stock_database.backup.db`

Both are ignored by Git and require manual review before any move or deletion.
Long term, local and production runs should resolve the active database through
`PERMANENT_DB_PATH` or a documented config path outside the source tree.

Generated CSV, logs, cache, and backup files are not durable product code. They
must be controlled by cleanup manifests and artifact manifests, not committed as
source.

## Cleanup Rule

Cleanup must follow this order:

1. Generate dry-run manifest.
2. Review delete/manual-review candidates.
3. Apply only manifest-supported safe deletes.
4. Verify service/page/manifest health.
5. Archive the cleanup manifest under `logs/openclaw/cleanup_audit/`.

Current dry-run manifest:

- `logs/openclaw/cleanup_audit/openclaw_cleanup_audit_20260513_062352.json`

Summary:

- Safe delete candidates: 468, about 22 MB.
- Manual review candidates: 2, about 5.2 GB.
- Files deleted: 0.

Previous tool boundary audit:

- `logs/openclaw/tool_boundary_audit/tool_boundary_audit_20260513_01.json`

Summary:

- Stable operator entrypoints: 16.
- Top5 support review scripts: 9.
- Archive candidates: 48.
- Manual review scripts: 31.

Current tool boundary audit after archive move:

- `logs/openclaw/tool_boundary_audit/tool_boundary_audit_20260513_02.json`

Summary:

- Stable operator entrypoints: 16.
- Top5 support review scripts: 9.
- Archive candidates remaining at top level: 0.
- Strategy competition archived scripts: 48.
- Manual review scripts: 31.

Current tool boundary audit after manual-review triage:

- `logs/openclaw/tool_boundary_audit/tool_boundary_audit_20260513_03.json`

Summary:

- Stable operator entrypoints: 14.
- Support review scripts: 2.
- Archive candidates remaining at top level: 0.
- Manual review scripts remaining at top level: 0.
- Archive namespaces: `strategy_competition`, `research`, `maintenance`, `governance`.

Current enforceable tool boundary gate:

- `logs/openclaw/tool_boundary_audit/tool_boundary_audit_20260513_11.json`
- Command: `python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2`

Gate rule:

- top-level `archive_candidate` must be 0;
- top-level `manual_review` must be 0;
- top-level `support_review` must be <= 2;
- missing stable entrypoints must be 0.

Legacy governance decision:

- Top-level `tools/run_governance_gate_ci.sh` has been moved to
  `tools/archive/governance/`.
- Historical `_archive/tools/governance_gate.py` remains available for forensic
  review and legacy CI references, but it no longer defines the current top-level
  operator surface.
- CI may still execute `_archive/tools/run_governance_gate_ci.sh` for PR comment
  and artifact continuity. It is informational by default and only blocks when
  `AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`.
- New daily gates should be explicit Top5/v9 evidence commands, not broad legacy
  governance wrappers that force one-off scripts back into top-level `tools/`.

## Test Gate Policy

Default tests must protect the formal production surface and the current
evidence chain. Experimental, legacy, and one-off strategy competition tests
should move to explicit nightly/archive invocations instead of expanding the
default local gate.

Do not mix root `tests/` and `stock_ultimate_system/tests/` in the same direct
pytest command until their `conftest.py` package names are reconciled. Run those
suites separately or through the configured default gate.

Production default gate should prioritize:

- `/stock` primary-result contract.
- Airivo route/scope registry.
- T12 read-only contract.
- Top5 evidence and trader brief freshness.
- v9 evidence pipeline and execution observation closure.
- `v49_app.py` compatibility shell import/route smoke tests.

Current CI hard boundary gates:

- `Tool boundary audit` must run `python tools/tool_boundary_audit.py` with
  `--fail-on-archive-candidates`, `--max-manual-review 0`, and
  `--max-support-review 2`.
- `Stock dashboard boundary tests` must include
  `stock_ultimate_system/tests/test_ci_dashboard_boundary_gate.py` plus the
  `/stock` dashboard shell, render-input, HTTP route, and primary-result API
  tests.
- `Governance boundary documentation tests` must protect README, PR template,
  runbook, release-gate workflow text, and archived-governance semantics.
- CI pytest commands must not mix root `tests/` and
  `stock_ultimate_system/tests/` in one command until the duplicate `conftest.py`
  package-name issue is reconciled.

## Review Packet

Review this slimming batch as four separate boundaries:

1. `tools/` boundary: stable entrypoints, archive namespaces, and
   `tools/tool_boundary_audit.py` budget enforcement.
2. `/stock` dashboard shell: `stock_ultimate_system/run_dashboard.py` remains a
   launch shell, while route/page/render responsibilities stay in focused
   `stock_ultimate_system/src/` modules.
3. Archived governance downgrade: `_archive/tools/run_governance_gate_ci.sh`
   remains available for PR comments and forensic artifacts, but it is not the
   current default blocking gate unless
   `AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`.
4. Documentation and CI anti-regression gates: README, PR template, runbook,
   runtime boundary map, and CI self-tests must agree on the current hard gates.

Do not review this batch as a feature expansion. The acceptance criterion is
that the system produces a smaller, clearer, and enforceable operating surface
for daily Top5 evidence work.

## Slimming Backlog

P0:

- Keep `v49_app.py` as shell only; move new logic into runtime/services.
- Stop adding new strategy lines for 20-60 trading days.
- Keep cleanup dry-runs mandatory before deleting generated assets.

P1:

- Split `stock_ultimate_system/run_dashboard.py` into route-specific modules:
  main site, `/stock`, `/T12`, read-only ops/API.
- Reduce `tools/` to stable daily commands. First archive namespace:
  `tools/archive/strategy_competition/`.
- Classify root `tests/` into production gate, nightly research, or archive.

P2:

- Move local DB assets outside the source tree behind `PERMANENT_DB_PATH`.
- Remove root-level evaluator shims after all imports use
  `strategies.evaluators.*`.
- Replace duplicate review/adjudication service variants with one versioned
  evidence service plus explicit modes.

# AIRIVO Slimming Cleanup Manifest 2026-05-14

Status: dry-run manifest only
Owner boundary: Top5 evidence closure, runtime boundary, production operations

## Non-Actions In This Pass

This manifest does not delete files, move files, rewrite CI, or change runtime behavior.

Explicit exclusions:

- `.git/` and `.venv/` are excluded from cleanup counting.
- DB, CSV archives, evidence exports, and historical strategy records are not direct-delete targets.
- `settings.yaml`, `requirements.txt`, runtime split work, and strategy implementation files are out of scope for this cleanup pass.
- Archived maintenance tools are not reactivated as default workflows.

## Decision Standard

Keep code and documents only when they serve one of these current production needs:

- Produce the daily Top5 result.
- Prove the Top5 result has a complete and uncontaminated evidence chain.
- Close execution feedback and post-trade review loops.
- Operate the public `/stock` dashboard or the private Streamlit runtime console.
- Preserve a minimal, auditable operations path.

Everything else should move toward one of three outcomes: safe delete, archive/nightly, or historical documentation.

## Dry-Run Summary

| Area | Observed Scope | Recommendation | Risk |
| --- | ---: | --- | --- |
| Runtime cache artifacts | 506 files excluding `.git` and `.venv` | Batch 1 safe delete after review | Low |
| Generated Top5 rebuild intermediates | 2 files | Batch 2 delete after reference check | Low/medium |
| `ensemble_*`, `post_rerun_*`, `formal_rerun_*`, release/promotion services | 42 service files | Batch 3 downgrade from default path to archive/nightly | Medium |
| Matching research/governance tests | 41 test files | Batch 3 move out of default gate or mark nightly | Medium |
| `stock_ultimate_system/docs` markdown | 55 files | Batch 4 keep 10-15 current docs, archive the rest | Low/medium |
| `docs/reports` markdown | 252 files | Archive as historical reports unless linked by current runbooks | Low |
| `stock_ultimate_system/*.md` root docs | 37 files | Consolidate into current boundary/runbook set | Low/medium |
| DB, CSV, evidence exports | Multiple runtime/data folders | Retention policy only, no direct delete | High |

## Batch 1: Safe Runtime Artifact Delete Candidates

Scope:

- `__pycache__/`
- `*.pyc`
- `.pytest_cache/`
- temporary cache artifacts that are reproducible from source

Observed examples:

- `.pytest_cache/`
- `__pycache__/v49_app.cpython-313.pyc`
- `_archive/openclaw/__pycache__/`
- `backtest/__pycache__/`
- `data/__pycache__/`
- `deploy/tools/__pycache__/`
- `openclaw/__pycache__/`
- `openclaw/runtime/__pycache__/`
- `tools/__pycache__/`

Recommendation:

Delete this batch first, but only after a reviewed cleanup command or script is approved. This is the only batch that should be treated as mechanically safe.

Execution note 2026-05-14:

- Batch 1 cleanup was executed for `.git/.venv` excluded `__pycache__`, `.pyc`, and `.pytest_cache` artifacts.
- Immediate post-cleanup verification found those patterns cleared.
- Subsequent compile/test verification regenerated ignored cache directories, which is expected and not a tracked repository change.
- `git ls-files '*__pycache__*' '*.pyc' '.pytest_cache*'` found no tracked cache artifacts.

## Batch 2: Generated Intermediate File Candidates

Delete only after confirming no production import or test dependency remains:

- `openclaw/services/_top5_rebuild_body.py`
- `openclaw/services/_top5_rebuild_body.transformed.py`

Known context:

- These look like generated assembly intermediates for the Top5 rebuild service.
- The archived maintenance assembler references `_top5_rebuild_body.transformed.py`, so this batch should include an explicit `rg` reference check before deletion.
- Current dry-run reference check found no production reference outside this manifest and `tools/archive/maintenance/assemble_top5_rebuild_service.py`.

Recommendation:

If references are limited to archived maintenance or historical docs, delete from active service namespace and document the assembler as historical.

Execution note 2026-05-14:

- Final reference check found no production reference outside this manifest and `tools/archive/maintenance/assemble_top5_rebuild_service.py`.
- The archived assembler was documented as historical migration context only.
- `_top5_rebuild_body*` files were approved for removal from the active service namespace.

## Batch 3: Research/Governance Downgrade Candidates

These files are not first-class delete targets yet. The right move is downgrade from default production gates into archive/nightly because they represent research/governance branches rather than daily Top5 production operations.

Representative service candidates:

- `openclaw/services/ensemble_allocator_throttle_attribution_service.py`
- `openclaw/services/ensemble_alpha_component_failure_diagnostic_service.py`
- `openclaw/services/ensemble_alpha_failure_attribution_service.py`
- `openclaw/services/ensemble_alpha_gate_contrast_service.py`
- `openclaw/services/ensemble_alpha_predeclared_gate_walk_forward_service.py`
- `openclaw/services/ensemble_alpha_rebuild_lab_service.py`
- `openclaw/services/ensemble_alpha_sleeve_service.py`
- `openclaw/services/ensemble_core_contract_service.py`
- `openclaw/services/ensemble_execution_cost_service.py`
- `openclaw/services/ensemble_observation_gate_service.py`
- `openclaw/services/ensemble_observation_monitor_service.py`
- `openclaw/services/ensemble_observation_promotion_apply_service.py`
- `openclaw/services/ensemble_observation_promotion_decision_service.py`
- `openclaw/services/ensemble_rebuilt_candidate_rule_freeze_service.py`
- `openclaw/services/ensemble_risk_off_alpha_repair_review_service.py`
- `openclaw/services/ensemble_shadow_portfolio_service.py`
- `openclaw/services/ensemble_sleeve_policy_audit_service.py`
- `openclaw/services/ensemble_walk_forward_benchmark_service.py`
- `openclaw/services/strategy_competition_formal_rerun_output_submission_service.py`
- `openclaw/services/strategy_competition_formal_rerun_plan_service.py`
- `openclaw/services/strategy_competition_formal_rerun_result_review_service.py`
- `openclaw/services/strategy_competition_post_rerun_evidence_chain_manifest_service.py`
- `openclaw/services/strategy_competition_post_rerun_release_readiness_service.py`
- `openclaw/services/strategy_competition_post_rerun_trade_lifecycle_adjudication_service.py`

Matching default-gate test candidates include:

- `tests/test_ensemble_*.py`
- `tests/test_strategy_competition_formal_rerun_*.py`
- `tests/test_strategy_competition_post_rerun_*.py`
- `tests/test_release_*.py`
- `tests/test_promotion_decision_artifact_service.py`

Recommendation:

Do not delete this group immediately. First remove it from default CI or move it behind a nightly/archive marker. After 20-60 trading days with no production dependency, reassess for archive-only retention or deletion.

## Batch 4: Documentation Reduction Candidates

Current documentation surface is too large for a production system:

- `stock_ultimate_system/docs`: 55 markdown files
- `stock_ultimate_system/*.md`: 37 markdown files
- `docs/reports`: 252 markdown files

Recommended keep set, capped at 10-15 current operational documents:

- `docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md`
- `docs/AIRIVO_CLEANUP_RETENTION_POLICY.md`
- `docs/AIRIVO_NEXT_STAGE_OPERATING_PLAN.md`
- `docs/auth_loop_hardening_runbook.md`
- `stock_ultimate_system/README.md`
- `stock_ultimate_system/AIRIVO_RELEASE_GATES.md`
- `stock_ultimate_system/STOCK_PRIMARY_RESULT_CONTRACT.md`
- `stock_ultimate_system/STOCK_PRIMARY_RESULT_RUNTIME_OBSERVABILITY_SPEC.md`
- `stock_ultimate_system/T12_GOVERNANCE_SUMMARY_INVARIANTS.md`
- `stock_ultimate_system/docs/API_REFERENCE.md`
- `stock_ultimate_system/docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `stock_ultimate_system/docs/MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md`
- `stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md`
- `stock_ultimate_system/docs/PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md`
- `stock_ultimate_system/docs/PRIMARY_RESULT_REVIEW_RUNBOOK.md`

Recommendation:

Archive historical docs instead of rewriting them in place. Many old docs contain historical paths and governance language; blind replacement would create semantic drift.

## Do-Not-Delete Areas

These areas should stay active unless a separate production review proves otherwise:

- Stable Top5 operations commands under `tools/`
- `tools/tool_boundary_audit.py`
- `tools/run_top5_competition_audit_then_gate.sh`
- Current Top5 evidence and execution services
- Public `/stock` dashboard modules under `stock_ultimate_system/`
- Private Streamlit runtime modules under `openclaw/runtime/`
- Strategy registry and active v9 Top5 evidence path
- DBs, CSV archives, and current evidence exports

## Recommended Next Execution Order

1. Review this manifest.
2. Run an explicit reference check for `_top5_rebuild_body*`.
3. Delete only runtime cache artifacts in Batch 1.
4. Commit Batch 1 as a pure cleanup commit.
5. Downgrade Batch 3 tests/services from default CI to archive/nightly in a separate commit.
6. Archive documentation in Batch 4 with a keep-list review.
7. Reassess old strategy pages and research entrypoints only after the production Top5 chain stays stable for 20-60 trading days.

## Hard Stop Rule

Do not continue deleting once the change stops improving the daily Top5 evidence closure. The goal is not fewer files by count; the goal is a smaller system that produces a stable, reviewable Top5 with no evidence pollution and a complete execution feedback loop.

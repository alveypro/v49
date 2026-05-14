# AIRIVO Stock Docs Archive Manifest 2026-05-14

Status: Batch 4 dry-run manifest only
Scope: `stock_ultimate_system/docs/*.md`

## Non-Actions In This Pass

This pass does not move, delete, rename, or rewrite documentation.

It only defines the review boundary for a later archive commit. This matters because many files in `stock_ultimate_system/docs/` cross-link to historical plans and some links still point to old absolute paths. Moving files before classification would create broken context and false confidence.

## Decision Standard

Keep only documents that directly support the current production objective:

- operate the public `/stock` page and its primary result API;
- prove the Top5 / primary-result evidence chain is uncontaminated;
- run, rebuild, review, or recover the current primary result;
- preserve current operator entry maps and blocker boundaries.

Archive documents that are historical plans, past P0/P1 execution boards, scoped activation decisions, AI launch blueprints, or duplicated standard documents that no longer drive daily operation.

## Current Surface

Observed files in `stock_ultimate_system/docs`: 55 markdown files.

Target active set: 10-15 files.

Recommended active keep count in this manifest: 12 files.

## Keep: Current Operating Documents

These should remain in the active docs namespace unless a later review proves they are superseded.

- `stock_ultimate_system/docs/API_REFERENCE.md`
- `stock_ultimate_system/docs/ARTIFACT_POLLUTION_CLEANUP_INVENTORY.md`
- `stock_ultimate_system/docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `stock_ultimate_system/docs/MAIN_CHAIN_AUTHENTICITY_CHECKLIST.md`
- `stock_ultimate_system/docs/MAIN_CHAIN_PRODUCTION_ACTION_BASELINE.md`
- `stock_ultimate_system/docs/OPERATIONS_ENTRY_MAP_AND_REPLAY_GRADING.md`
- `stock_ultimate_system/docs/PRIMARY_RESULT_EVIDENCE_STOPLOSS.md`
- `stock_ultimate_system/docs/PRIMARY_RESULT_FACT_SOURCE_CLASSIFICATION.md`
- `stock_ultimate_system/docs/PRIMARY_RESULT_LATEST_REBUILD_RUNBOOK.md`
- `stock_ultimate_system/docs/PRIMARY_RESULT_REVIEW_RUNBOOK.md`
- `stock_ultimate_system/docs/STRICT_CONTINUATION_EXECUTION_STANDARD.md`
- `stock_ultimate_system/docs/FORMAL_RUNTIME_CONVERGENCE_FINDINGS_2026-04-30.md`

Rationale:

- The primary-result and main-chain documents define the current evidence and operation loop.
- `FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md` is dirty in the current worktree and must be reviewed before any move.
- `FORMAL_RUNTIME_CONVERGENCE_FINDINGS_2026-04-30.md` is retained for now because it appears to explain current hosting/runtime gap context. It can be downgraded after hosting blockers are resolved.

## Archive Candidates: AI Launch / Integration Planning

These look like planning or launch-scope documents, not daily Top5 production controls.

- `stock_ultimate_system/docs/AI_FEATURE_FLAGS_DEGRADATION_AND_ROLLBACK_STRATEGY.md`
- `stock_ultimate_system/docs/AI_IMPLEMENTATION_SEQUENCE_AND_MINIMUM_LAUNCH_SCOPE.md`
- `stock_ultimate_system/docs/AI_INPUT_WHITELIST_AND_OUTPUT_SCHEMA_CONTRACT.md`
- `stock_ultimate_system/docs/AI_INTEGRATION_BOUNDARY_SPEC.md`
- `stock_ultimate_system/docs/AI_INTEGRATION_TEST_GATES_AND_BLOCKING_CHECKLIST.md`
- `stock_ultimate_system/docs/AI_OUTPUT_AUDIT_AND_MANUAL_REVIEW_SPEC.md`
- `stock_ultimate_system/docs/AI_RUNTIME_MONITORING_AND_AUDIT_DASHBOARD_SPEC.md`

Action: archive as historical AI integration planning unless an active `/stock` runtime route or CI gate still depends on a specific contract.

## Archive Candidates: P0/P1, Execution Boards, and Old Plans

- `stock_ultimate_system/docs/P0_ACCEPTANCE_CHECKLIST.md`
- `stock_ultimate_system/docs/P0_COMPLETION_REVIEW.md`
- `stock_ultimate_system/docs/P0_EXECUTION_PLAN.md`
- `stock_ultimate_system/docs/P0_TASK_BACKLOG.md`
- `stock_ultimate_system/docs/P1_TEST_ACCELERATION_PLAN.md`
- `stock_ultimate_system/docs/FORMAL_RUNTIME_CONVERGENCE_EXECUTION_PLAN.md`
- `stock_ultimate_system/docs/FOUR_HARD_GATES_EXECUTION_BOARD.md`
- `stock_ultimate_system/docs/NEXT_PHASE_EXECUTION_BASELINE_GAP_CHECKLIST.md`
- `stock_ultimate_system/docs/STOCK_UI_SECOND_STAGE_RECONSTRUCTION_CHECKLIST.md`
- `stock_ultimate_system/docs/SYSTEM_PUSH_PLAN_30D.md`
- `stock_ultimate_system/docs/STOCK_TWO_WEEK_EXECUTION_CHECKLIST.md`

Action: archive as historical execution plans. They should not remain in the active operator namespace unless referenced by a current runbook.

## Archive Candidates: Candidate Quality / Benchmark / Tier Standards

- `stock_ultimate_system/docs/CANDIDATE_QUALITY_EVALUATION_AND_BENCHMARK_SUITE.md`
- `stock_ultimate_system/docs/CANDIDATE_QUALITY_PROOF_EXECUTION_TABLE.md`
- `stock_ultimate_system/docs/EXPERIMENT_REDESIGN_BLUEPRINT.md`
- `stock_ultimate_system/docs/GRID_BACKTEST_PRESETS.md`
- `stock_ultimate_system/docs/INDUSTRY_FIRST_CANDIDATE_SYSTEM_EXECUTION_STANDARD.md`
- `stock_ultimate_system/docs/SCORE_8_GATE.md`
- `stock_ultimate_system/docs/STOCK_SINGLE_FORMAL_CHAIN_EXECUTION_STANDARD.md`
- `stock_ultimate_system/docs/TOP_TIER_EXECUTION_STANDARD.md`
- `stock_ultimate_system/docs/TOP_TIER_GAP_MATRIX.md`
- `stock_ultimate_system/docs/DEVELOPMENT_GUIDE.md`

Action: archive or consolidate. These documents are broad standards or research-quality gates. They may be useful historically, but they dilute the active `/stock` operating surface.

## Archive Candidates: Scoped Activation / Route Ownership

- `stock_ultimate_system/docs/airivo.online_route_ownership_freeze_decision.md`
- `stock_ultimate_system/docs/airivo.online_route_ownership_matrix.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_admission_gate.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_non_impact_matrix.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_ownership_sufficiency_decision.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_ownership_sufficiency_upgrade_path.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_pre_post_non_stock_invariance_proof.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_remaining_gap_checklist.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_responsibility_freeze.md`
- `stock_ultimate_system/docs/first_real_stock_scoped_activation_touch_set.md`
- `stock_ultimate_system/docs/formal_nginx_active_vs_repo_current_gap_checklist.md`
- `stock_ultimate_system/docs/scoped_activation_rollout_spec.md`
- `stock_ultimate_system/docs/t12_formal_topology_decision.md`

Action: archive as scoped activation history after checking whether `FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md` still imports any unique current blocker from these files.

## Manual Review Before Archive

These are not strong active-keep candidates, but they may contain current facts that should be extracted before moving.

- `stock_ultimate_system/docs/APEX_ONLINE_REMEDIATION_EXECUTION_SHEET.md`
- `stock_ultimate_system/docs/APEX_SYNC_PREFLIGHT_CHECKLIST.md`
- `stock_ultimate_system/docs/FORMAL_RUNTIME_CONVERGENCE_EXECUTION_PLAN.md`
- `stock_ultimate_system/docs/formal_nginx_active_vs_repo_current_gap_checklist.md`

Action: review for current hosting/deploy blockers. If the current facts are already represented in `FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`, archive them.

## Batch 4 Execution Plan

1. Review this manifest.
2. Confirm the 12-file keep set.
3. Add a docs boundary test that asserts the active docs allowlist.
4. Move archive candidates to a controlled archive namespace in one commit.
5. Update only active docs links that would otherwise break.
6. Do not rewrite historical docs just to modernize language.

## Hard Stop Rule

If a document is only useful for explaining why a past phase happened, archive it. If it is needed to operate or review today’s `/stock` primary result, keep it active.

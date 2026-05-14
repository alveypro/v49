# AIRIVO Worktree Isolation Audit 2026-05-14

Status: working-tree isolation audit
Scope: uncommitted local changes after slimming Batch 1/2/3

## Immediate Safety Action

The previously staged research draft was removed from the index:

- `openclaw/research/strategy_optimization_stage_audit.py`

No file content was reverted or deleted. The staging area is now empty.

Execution note:

- `auth/deploy` changes were isolated into stash entry `isolate auth deploy worktree 2026-05-14`.
- `openclaw/services` changes were isolated into stash entry `isolate openclaw services worktree 2026-05-14`.
- Dirty worktree entries dropped from 89 to 33 after those two isolations.
- The staging area remained empty after isolation.
- `research/strategies` changes were isolated into stash entry `isolate research strategies worktree 2026-05-14`.
- `data/exports/artifacts` changes were isolated into stash entry `isolate data exports artifacts worktree 2026-05-14`.
- Misc runtime/test changes were isolated into stash entry `isolate misc runtime test worktree 2026-05-14`.
- Dirty worktree entries dropped from 33 to 4 after the second isolation pass.

## Current Dirty Scope

Observed uncommitted entries: 89

Top-level distribution:

| Area | Count | Isolation Decision |
| --- | ---: | --- |
| `openclaw/` | 46 | Separate review branch; do not mix with docs cleanup |
| `deploy/` | 9 | Split auth/deploy from Top5 scheduler work |
| `tests/` | 6 | Review with the service files they validate |
| `strategies/` | 5 | Strategy freeze applies; no production promotion |
| `tools/` | 3 | Do not reopen tools slimming unless explicitly scoped |
| `stock_ultimate_system/` | 3 | Keep separate from runtime and docs archive work |
| `docs/` | 3 | Candidate for Batch 4 review, but not blind rewrite |
| `_archive/` | 2 | Historical context; avoid semantic rewriting |
| Other single-entry areas | 12 | Manual review only |

## Thematic Buckets

### Auth / Deploy

Keep out of Batch 4 documentation slimming:

- `.env.example`
- `deploy/airivo-auth-decision-alert.service`
- `deploy/tools/auth_decision_alert.py`
- `deploy/tools/deploy_auth_to_release.sh`
- `deploy/tools/deploy_auth_to_server.sh`
- `deploy/tools/install_auth_alert_timer.sh`
- `tools/auth_decision_alert.py`
- `tools/install_auth_alert_timer.sh`
- `create_auth_db.py`
- `create_auth_db_on_server.py`
- `stock_ultimate_system/config/settings.yaml`
- `stock_ultimate_system/requirements.txt`

Decision: isolate as an auth/deploy branch. These changes touch credentials, service activation, or dependencies and must not be mixed with slimming commits.

### OpenClaw Services

High-risk service surface:

- `openclaw/services/airivo_*`
- `openclaw/services/execution_*`
- `openclaw/services/release_*`
- `openclaw/services/data_*`
- `openclaw/services/strategy_*`
- `openclaw/services/unified_strategy_recommendation_service.py`

Decision: separate service-contract review. These files can affect execution evidence, strategy recommendations, release gates, and CI failures. Do not include in docs cleanup.

### Research

Research-only or staged-candidate work:

- `openclaw/research/all_strategy_evidence_run.py`
- `openclaw/research/backtest_param_sweep.py`
- `openclaw/research/strategy_optimization_stage_audit.py`
- `openclaw/research/v4_factor_research.py`
- `openclaw/research/v8_controlled_experiment.py`
- `_archive/docs/AIRIVO_ENSEMBLE_CORE_SHADOW_PORTFOLIO_DEVELOPMENT_PLAN.md`
- `_archive/docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md`

Decision: keep outside production slimming. This belongs to research/nightly governance, not default Top5 production closure.

### Docs / Batch 4 Candidates

Potentially relevant to documentation slimming:

- `docs/AIRIVO_CLEANUP_RETENTION_POLICY.md`
- `docs/AIRIVO_NEXT_STAGE_OPERATING_PLAN.md`
- `docs/auth_loop_hardening_runbook.md`
- `stock_ultimate_system/docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`

Decision: review only after worktree isolation is complete. Batch 4 should keep 10-15 current operating docs and archive the rest without rewriting historical meaning.

### Data / Exports / Artifacts

Manual review only:

- `artifacts/`
- `data/`
- `exports/`
- `kad_template/`

Decision: no delete in slimming commits. Apply retention policy separately.

## Hard Gate Before Batch 4

Do not start documentation archive work until all of the following are true:

1. `git diff --cached --name-status` is empty.
2. Auth/deploy changes are either stashed or moved to a separate review branch.
3. OpenClaw service changes are either stashed or moved to a separate review branch.
4. Data/export/artifact directories are excluded from docs cleanup.
5. The next commit scope contains only documentation archive changes and their tests.

## Recommended Next Action

Create a temporary holding branch or stash for non-Batch-4 changes, then start a clean Batch 4 branch from `origin/main`. The current working tree is too broad to safely perform documentation slimming in place.

After the second isolation pass, the remaining dirty worktree scope is documentation-only:

- `docs/auth_loop_hardening_runbook.md`
- `stock_ultimate_system/docs/FORMAL_HOSTING_BOUNDARY_AND_ACTIVATION_BLOCKERS.md`
- `docs/AIRIVO_CLEANUP_RETENTION_POLICY.md`
- `docs/AIRIVO_NEXT_STAGE_OPERATING_PLAN.md`

This is the first point where Batch 4 documentation slimming can proceed without mixing auth/deploy, services, research, strategies, runtime, tests, data, exports, or artifacts.

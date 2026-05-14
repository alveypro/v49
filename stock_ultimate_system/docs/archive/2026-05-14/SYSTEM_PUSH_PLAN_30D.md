# SYSTEM_PUSH_PLAN_30D

## 1. Document Intent

This document turns the current system maturity assessment into a 30-day execution plan.

It is not a product vision memo. It is a delivery and governance document used to push the system from:

- advanced architecture and governance skeleton

to:

- stable green trunk
- continuous daily closure
- continuously growing evidence
- explicit failure learning
- auditable operational discipline

This plan targets an "8-point ready" state, not a forced claim that every dimension has already reached 8/10.

---

## 2. Current Reality

### 2.1 What is already strong

- Product boundary is relatively clear across main site, `/stock`, and `/T12`.
- Governance and gating structure is real, not cosmetic.
- Artifact chain already exists across candidate, lifecycle, observation, terminal, evidence, release, and baseline.
- The system is beyond a signal page; it is already evolving into a governed research production system.

### 2.2 What is still weak

- Long-horizon evidence is still thin.
- Daily closure exists structurally, but not yet fully proven as a stable routine.
- Production reliability is not yet strong enough to justify an 8/10 claim.
- Some page layers are heavier than the underlying continuously validated evidence.

### 2.3 Operating conclusion

For the next 30 days, only work that directly improves one of the following should be prioritized:

- trunk green rate
- closure continuity
- evidence thickness
- failure learning
- operational traceability

Any work that does not directly improve one of these five should be deprioritized.

---

## 3. 30-Day Success Definition

This plan is considered successful only if all of the following become true together:

- `pytest -q` is continuously green, not just green once.
- The homepage has a single primary conclusion and a single primary blocker.
- `candidate -> lifecycle -> observation -> terminal -> ledger` runs as a daily governed chain.
- `primary_result` and `candidate_basket` evidence streams both grow continuously.
- Failed terminal outcomes automatically route into attribution, feedback, and review queue.
- Operational health can be reviewed across a time range, not just via latest snapshots.
- Promotion, release, and baseline artifacts are linked by governed decisions.
- "Research usable" and "scalable for real capital" are no longer conflated.

---

## 4. Priority Model

### 4.1 P0 definition

`P0` means the system should not claim 8-point maturity without it.

### 4.2 P1 definition

`P1` means the system may run without it, but cannot become stable, defensible, and scalable without it.

---

## 5. P0 Task List

### P0-1 Trunk Reliability Reset

#### Objective

Restore the trunk from "strong test coverage with current red lights" to "credible and continuously green".

#### Actions

- Fix dashboard first-screen contract drift.
- Fix basket risk pressure score normalization or upper-bound logic.
- Fix `run_server_sync_preflight.py --json` exit semantics.
- Fix missing-column robustness in basket capacity pressure logic.
- Add regression coverage for each of the above failure classes.

#### Acceptance Commands

```bash
pytest -q
pytest -q stock_ultimate_system/tests/test_first_place_evidence_cockpit.py
pytest -q stock_ultimate_system/tests/test_model_capability_upgrade.py
pytest -q stock_ultimate_system/tests/test_run_server_sync_preflight.py
pytest -q stock_ultimate_system/tests/test_top_candidates_universe.py
```

#### Primary Paths

- `stock_ultimate_system/tests/test_first_place_evidence_cockpit.py`
- `stock_ultimate_system/tests/test_model_capability_upgrade.py`
- `stock_ultimate_system/tests/test_run_server_sync_preflight.py`
- `stock_ultimate_system/tests/test_top_candidates_universe.py`
- `stock_ultimate_system/run_top_candidates.py`

#### Risks

- Fixing assertions without fixing the underlying semantics will cause repeated failures.
- Fixing only the current fixture set without adding edge-case tests will leave the trunk unstable.

---

### P0-2 Homepage Decision Convergence

#### Objective

Reduce the homepage from an information-heavy board into a decision-first operating surface.

#### Actions

- Converge the homepage into six zones only:
  - today status
  - current object
  - promotion or progression decision
  - evidence floor progress
  - ops health
  - detail entry points
- Push down long explanations, repeated summaries, history blocks, and fine-grained checklists.
- Require every homepage card to expose:
  - `timestamp`
  - `source artifact`
  - `status`
  - `next action`
- Enforce one primary conclusion and one primary blocker only.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_dashboard_context.py
pytest -q stock_ultimate_system/tests/test_first_place_evidence_cockpit.py
pytest -q stock_ultimate_system/tests/test_main_site_home.py
```

#### Primary Paths

- `stock_ultimate_system/src/dashboard_context.py`
- `stock_ultimate_system/src/dashboard_reports.py`
- `stock_ultimate_system/src/first_place_evidence_cockpit.py`
- `stock_ultimate_system/tests/test_dashboard_context.py`

#### Risks

- Rearranging UI without reducing decision ambiguity is not considered completion.
- Multiple co-equal summary cards on the first screen mean the convergence failed.

---

### P0-3 Candidate to Lifecycle Alignment Gate

#### Objective

Make candidate and lifecycle alignment a hard gate rather than a soft display warning.

#### Actions

- Freeze one authoritative current candidate source.
- Freeze one authoritative lifecycle current pointer source.
- Block daily closure when candidate and lifecycle pointer do not match.
- Emit one primary blocker and one next action when misalignment occurs.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_primary_result_candidate_handoff_gate.py
pytest -q stock_ultimate_system/tests/test_primary_result_candidate_handoff_runner.py
pytest -q stock_ultimate_system/tests/test_run_current_primary_result_daily_closure_guard.py
```

#### Primary Paths

- `stock_ultimate_system/artifacts/primary_result_candidate_handoff_gate_latest.json`
- `stock_ultimate_system/artifacts/primary_result_lifecycle/current.json`
- `stock_ultimate_system/src/primary_result_candidate_basket_feedback.py`

#### Risks

- Hidden multi-source writes will keep this gate unstable.
- Manual operator ordering is not an acceptable substitute for governed alignment.

---

### P0-4 Observation, Terminal, and Ledger Closure Hardening

#### Objective

Ensure ledger entries only come from valid observation and terminal states.

#### Actions

- Define the exact observation entry conditions.
- Ensure terminal only consumes valid observation states.
- Ensure ledger only consumes explicit terminal outcomes.
- Persist `blocked`, `failed`, and `success` as auditable outcomes.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_primary_result_observation_metrics.py
pytest -q stock_ultimate_system/tests/test_primary_result_observation_closure_preflight.py
pytest -q stock_ultimate_system/tests/test_primary_result_execution.py
pytest -q stock_ultimate_system/tests/test_primary_result_performance_ledger.py
```

#### Primary Paths

- `stock_ultimate_system/artifacts/primary_result_observation_wait_status_latest.json`
- `stock_ultimate_system/artifacts/primary_result_performance/ledger.jsonl`
- `stock_ultimate_system/artifacts/primary_result_performance/summary.json`

#### Risks

- Silent skipping when upstream artifacts are missing will create false health signals.
- Treating `blocked` like `success` will poison long-term evidence quality.

---

### P0-5 Daily Evidence Growth

#### Objective

Move evidence from "schema exists" to "daily samples are growing".

#### Actions

- Freeze separate evidence schemas for `primary_result` and `candidate_basket`.
- Recompute evidence summary daily.
- Derive 20, 60, and 120 floor status from ledger only.
- Standardize insufficient-sample output as `accumulating`.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_primary_result_benchmark_plan.py
pytest -q stock_ultimate_system/tests/test_primary_result_benchmark_plan_execution.py
pytest -q stock_ultimate_system/tests/test_stock_primary_result_benchmark_report.py
pytest -q stock_ultimate_system/tests/test_evidence_bundle_consumes_existing_gate_json.py
```

#### Primary Paths

- `stock_ultimate_system/artifacts/primary_result_performance_evidence_latest.json`
- `stock_ultimate_system/artifacts/primary_result_performance/ledger.jsonl`
- `stock_ultimate_system/artifacts/primary_result_performance/summary.json`

#### Risks

- If the basket stream stays near zero, the evidence program remains incomplete.
- Any manual floor overrides will invalidate evidence credibility.

---

### P0-6 Promotion, Release, and Baseline Decision Unification

#### Objective

Ensure that promotion, release, and baseline are actually governed by evidence and review state.

#### Actions

- Make promotion gate consume evidence status and review queue severity.
- Define explicit release decision prerequisites.
- Allow baseline current pointer to reference governed immutable snapshots only.
- Require baseline snapshot binding to approved release decision hash.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_primary_result_promotion_readiness_gate.py
pytest -q stock_ultimate_system/tests/test_primary_result_release_decision.py
pytest -q stock_ultimate_system/tests/test_primary_result_production_readiness_preflight.py
pytest -q stock_ultimate_system/tests/test_baseline_registry_current_pointer.py
```

#### Primary Paths

- `stock_ultimate_system/artifacts/primary_result_promotion_readiness_gate_latest.json`
- `stock_ultimate_system/artifacts/primary_result_release_decisions/current.json`
- `stock_ultimate_system/artifacts/baselines/current.json`
- `stock_ultimate_system/src/primary_result_production_readiness_preflight.py`

#### Risks

- Gates without real baseline movement will leave the release chain ceremonial.
- Page-level "ready" labels must never outrun artifact-level governed state.

---

## 6. P1 Task List

### P1-1 Failed Terminal to Attribution, Feedback, and Queue

#### Objective

Turn failed outcomes into structured learning inputs rather than dead-end logs.

#### Actions

- Generate attribution automatically on failed terminal outcome.
- Generate learning feedback from attribution.
- Push feedback into review queue.
- Make promotion gate consume high-severity unresolved review items.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_primary_result_feedback_loop.py
pytest -q stock_ultimate_system/tests/test_primary_result_feedback_review_queue.py
pytest -q stock_ultimate_system/tests/test_primary_result_learning_feedback.py
pytest -q stock_ultimate_system/tests/test_governance_audit.py
```

#### Primary Paths

- `stock_ultimate_system/artifacts/primary_result_feedback_review_queue/summary.json`
- `stock_ultimate_system/src/primary_result_feedback_loop.py`
- `stock_ultimate_system/src/primary_result_audit.py`

#### Risks

- Free-text logging is not enough; attribution must be structured.
- Learning feedback must not auto-modify live production strategy state.

---

### P1-2 Operational History, Not Latest-Only Health

#### Objective

Upgrade operational health from point-in-time snapshots to continuous observability.

#### Actions

- Add history tracking for automation health.
- Emit a daily operations report covering:
  - success
  - blocked
  - failed
  - degraded
  - next repair priorities
- Add a "continuous healthy days" indicator.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_run_update_database.py
pytest -q stock_ultimate_system/tests/test_run_server_post_deploy_verification.py
pytest -q stock_ultimate_system/tests/test_run_server_sync_preflight.py
bash stock_ultimate_system/tools/check_automation_health.sh
```

#### Primary Paths

- `stock_ultimate_system/tools/check_automation_health.sh`
- `stock_ultimate_system/artifacts/primary_result_daily_operations_scoreboard_latest.json`
- `stock_ultimate_system/artifacts/primary_result_operations_refresh_latest.json`

#### Risks

- Latest-only health creates false confidence during intermittent degradation.
- If `degraded` is not tracked explicitly, slow failure will hide behind nominal success.

---

### P1-3 Data Readiness and Capacity Constraints as Real Gates

#### Objective

Separate "research usable" from "scalable for larger capital" with explicit governed constraints.

#### Actions

- Define market data readiness minimum conditions.
- Standardize stress outputs for capacity, liquidity, and concentration.
- Split final language into:
  - research usable
  - not yet scalable
- Disallow strong scale-up conclusions without passing capacity and liquidity gates.

#### Acceptance Commands

```bash
pytest -q stock_ultimate_system/tests/test_primary_result_market_data_readiness.py
pytest -q stock_ultimate_system/tests/test_market_rule_engine.py
pytest -q stock_ultimate_system/tests/test_top_candidates_universe.py
pytest -q stock_ultimate_system/tests/test_model_capability_upgrade.py
```

#### Primary Paths

- `stock_ultimate_system/artifacts/primary_result_competitive_gap_assessment_latest.json`
- `stock_ultimate_system/run_top_candidates.py`

#### Risks

- Conflating research validity with capital scalability will create false optimism.
- Inconsistent stress semantics will make scale decisions untrustworthy.

---

### P1-4 Weekly Maturity Review

#### Objective

Shift from ad hoc repair to maturity-managed execution.

#### Actions

- Publish a weekly maturity review that answers:
  - which P0 tasks passed
  - which P1 tasks remain open
  - which new failure classes appeared
  - how far evidence floors progressed
  - which blocker was most common
- Compare weekly results month over month.

#### Acceptance Commands

```bash
pytest -q
```

The weekly review also requires manual verification that core artifacts were generated continuously.

#### Primary Paths

- `data/experiments/backtest_weekly_brief_latest.md`
- `data/experiments/backtest_comparison_latest.md`
- `stock_ultimate_system/artifacts/primary_result_performance_evidence_latest.json`
- `stock_ultimate_system/artifacts/primary_result_daily_operations_scoreboard_latest.json`

#### Risks

- Without weekly review, the system will revert to local bug-fixing without maturity tracking.
- Without explicit comparison, perceived progress will diverge from actual progress.

---

## 7. Artifact Map

The following artifacts are central to this plan:

- Candidate handoff gate:
  - `stock_ultimate_system/artifacts/primary_result_candidate_handoff_gate_latest.json`
- Lifecycle pointer:
  - `stock_ultimate_system/artifacts/primary_result_lifecycle/current.json`
- Observation status:
  - `stock_ultimate_system/artifacts/primary_result_observation_wait_status_latest.json`
- Performance ledger and summary:
  - `stock_ultimate_system/artifacts/primary_result_performance/ledger.jsonl`
  - `stock_ultimate_system/artifacts/primary_result_performance/summary.json`
- Performance evidence:
  - `stock_ultimate_system/artifacts/primary_result_performance_evidence_latest.json`
- Promotion gate:
  - `stock_ultimate_system/artifacts/primary_result_promotion_readiness_gate_latest.json`
- Release decision pointer:
  - `stock_ultimate_system/artifacts/primary_result_release_decisions/current.json`
- Baseline pointer:
  - `stock_ultimate_system/artifacts/baselines/current.json`
- Feedback review queue:
  - `stock_ultimate_system/artifacts/primary_result_feedback_review_queue/summary.json`
- Daily operations scoreboard:
  - `stock_ultimate_system/artifacts/primary_result_daily_operations_scoreboard_latest.json`

---

## 8. 30-Day Weekly Cadence

### Week 1

- Complete `P0-1`.
- Start `P0-2`.
- Freeze state dictionary and evidence schemas required by `P0-3` through `P0-6`.

### Week 2

- Complete `P0-2`.
- Run `P0-3` and `P0-4` to stable governed behavior.
- Start `P1-1`.

### Week 3

- Complete `P0-5`.
- Complete `P0-6`.
- Start `P1-2` and `P1-3`.

### Week 4

- Stabilize `P1-1`, `P1-2`, and `P1-3`.
- Produce `P1-4`.
- Run one full-chain maturity review.

---

## 9. Operating Rules for the 30-Day Push

- No new major module should be added unless it directly improves trunk reliability, closure continuity, evidence growth, failure learning, or operational traceability.
- Page-level improvements must be justified by decision clarity, not by visual completeness.
- No strong claim about scale-up readiness may be made before capacity and liquidity constraints are explicitly passed.
- No strong claim about evidence maturity may be made before floor status comes from real ledger accumulation.
- A `blocked` state is acceptable. Silent omission is not acceptable.

---

## 10. Final Acceptance Standard

At the end of the 30-day window, the system is considered to have entered an "8-point ready" state only if all of the following hold together:

- `pytest -q` is continuously green.
- The homepage has one primary conclusion.
- Current candidate and lifecycle pointer remain aligned.
- Observation, terminal, and ledger artifacts are continuously generated.
- Primary and basket evidence streams both grow.
- Failed terminal outcomes can be traced into attribution, feedback, and queue.
- Promotion, release, and baseline decisions are materially linked.
- Operational health can be reviewed over time rather than via latest-only status.

If any one of these is still missing, the system should be described as improving toward 8/10, not yet at 8/10.

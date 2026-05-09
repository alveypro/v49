# FIRST PLACE 20 OBSERVATION SPRINT

## Status

Version: v1.0  
Status: active sprint plan  
Scope: `/stock` primary result and candidate basket evidence accumulation  
Linked protocol: `FIRST_PLACE_CHALLENGE_PROTOCOL.md`

## Objective

This sprint has one objective:

Reach the first 20-sample evidence floor for both streams:

- `primary_result`
- `candidate_basket`

No new strategic surface should be added during this sprint unless it directly removes a blocker in this evidence chain.

## Current Reality

Latest local evidence shows:

- operations scoreboard status: `yellow`
- operations score: `79`
- primary performance ledger entries: `1`
- candidate basket performance ledger entries: `0`
- primary result gap to first floor: `19`
- candidate basket gap to first floor: `20`
- promotion readiness: `blocked`
- competitive positioning: `credible challenger`
- candidate basket current pointer: `approved`
- observation window start: `2026-04-20T01:30:00Z`
- observation wait status: `pending_window`
- primary result daily closure: `blocked by future window`
- candidate basket observation: `pending_window`
- current blocker: observation window has not started yet

This is not a failure. It is the correct early-stage state for a system that has built governance before claiming market edge.

The system is not ready for baseline promotion, production superiority claims, or live trading automation.

## Single Battlefield

The sprint battlefield is:

`A-share daily Top N candidate quality under closed observation`

Everything else is secondary.

Allowed work:

- make daily data readiness green
- register or refresh the current candidate basket
- align lifecycle evidence to the current top candidate
- inspect controlled wait status before the observation window starts
- close primary result observation windows
- close candidate basket observation windows
- append both streams to ledgers
- rebuild performance evidence
- route failures into attribution and review queue
- refresh operations artifacts

Disallowed work:

- adding new strategy families
- changing the platform boundary
- adding new UI surfaces
- claiming first-place capability before the 20-sample floor
- bypassing failed gates for promotion
- treating a green engineering gate as market proof
- converting a waiting state into a closed sample before real market data exists

## Daily Operating Sequence

Run one controlled daily cycle per trading day.

### 1. Confirm Daily Data And Candidate Update

Evidence expected:

- `data/experiments/update_status_latest.json`
- `data/experiments/primary_result_market_data_readiness_latest.json`

Gate:

- daily update must be `healthy`, `completed`, `success`, or a documented non-blocking equivalent
- market data readiness must be `ready`

If data freshness cannot be proven, stop the cycle. Do not record a clean operating day.

### 2. Align Lifecycle To Current Top Candidate

Evidence expected:

- `artifacts/primary_result_candidate_handoff_gate_latest.json`
- `artifacts/primary_result_lifecycle/current.json`
- `data/experiments/primary_result_observation_latest.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/run_primary_result_lifecycle.py
```

Gate:

- current lifecycle pointer must match the current top candidate
- observation artifact must not be stale
- handoff gate should not report stale lifecycle artifacts

If the handoff gate is blocked, fix this before closure.

### 3. Register Current Candidate Basket

Evidence expected:

- `artifacts/primary_result_candidate_baskets/current.json`
- immutable snapshot under `artifacts/primary_result_candidate_baskets/history/`

Command pattern:

```bash
python stock_ultimate_system/scripts/register_primary_result_candidate_basket.py
```

Gate:

- basket status must be `approved`
- basket items and weights must be fixed before the observation window closes
- no after-the-fact item replacement is allowed

### 4. Inspect Controlled Wait Status

Evidence expected:

- `artifacts/primary_result_observation_wait_status_latest.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/inspect_primary_result_observation_wait_status.py --json
```

Gate:

- `pending_window` means wait; do not close observations
- `ready_for_data_check` only authorizes market data readiness and closure checks
- this artifact must not append to either performance ledger

### 5. Close Primary Result Observation

Evidence expected:

- `data/experiments/primary_result_daily_closure_latest.json`
- `artifacts/primary_result_performance/ledger.jsonl`
- `artifacts/primary_result_performance/summary.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/run_current_primary_result_daily_closure.py --json
```

Gate:

- closure must use the current primary result
- benchmark comparison must be present
- terminal outcome must be recorded
- blocked closure is evidence of an operational problem, not a clean sample

### 6. Close Candidate Basket Observation

Evidence expected:

- `artifacts/primary_result_candidate_baskets/observation_latest.json`
- `artifacts/primary_result_candidate_baskets/performance_ledger.jsonl`
- `artifacts/primary_result_candidate_baskets/performance_summary.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/run_current_candidate_basket_observation.py --json
```

Gate:

- basket snapshot must be approved
- benchmark index must be included
- each basket code and benchmark must have at least two price points in the window
- duplicate ledger entries must be treated as duplicate evidence, not new samples

### 7. Rebuild Performance Evidence

Evidence expected:

- `artifacts/primary_result_performance_evidence_latest.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/build_primary_result_performance_evidence.py --json
```

Gate:

- first floor remains 20 samples
- `accumulating` is acceptable before 20
- `failed` requires review before any promotion discussion
- `ready` only means promotion review may begin

### 8. Route Failures Into Feedback

Evidence expected:

- `data/experiments/primary_result_failure_attribution_latest.json`
- `data/experiments/primary_result_feedback_loop_latest.json`
- `artifacts/primary_result_feedback_review_queue/summary.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/run_primary_result_feedback_loop.py
```

Gate:

- failed or weak observations must not be ignored
- unknown attribution must remain visible
- repeated unknown causes block promotion

### 9. Refresh Operations Artifacts

Evidence expected:

- `artifacts/primary_result_operations_refresh_latest.json`
- `artifacts/primary_result_daily_operations_scoreboard_latest.json`
- `artifacts/primary_result_promotion_readiness_gate_latest.json`
- `artifacts/primary_result_competitive_gap_assessment_latest.json`

Command pattern:

```bash
python stock_ultimate_system/scripts/refresh_primary_result_operations_artifacts.py --json
```

Gate:

- red scoreboard means the day is not clean
- yellow scoreboard means evidence is incomplete or accumulating
- green scoreboard means no operational blocker was detected for the current evidence state

## Weekly Review

Every five trading cycles, review:

- primary ledger entry count
- candidate basket ledger entry count
- average excess return
- worst max drawdown
- failed observations
- unknown attribution count
- open high-severity review items
- current promotion readiness decision
- competitive gap assessment status

One weekly improvement may be selected only if it addresses a measured blocker.

Examples of valid weekly improvements:

- fix stale lifecycle pointer generation
- improve price history extraction reliability
- reduce unknown attribution
- tighten rejected-candidate evidence
- improve benchmark completeness

Examples of invalid weekly improvements:

- add another model without a measured blocker
- create another dashboard page
- change promotion thresholds after seeing bad results
- manually remove bad samples
- rename fields to make status look better

## Sprint Exit Criteria

The sprint can exit only when all are true:

- primary result ledger has at least 20 closed entries
- candidate basket ledger has at least 20 closed entries
- performance evidence first floor is not `accumulating`
- no missing benchmark comparison
- no stale lifecycle blocker
- no missing candidate basket current pointer
- no open high-severity review item
- release gates still pass

If the first floor status is `ready`, proceed to governed promotion review.

If the first floor status is `failed`, do not promote. Run failure review and define one controlled challenger improvement.

## Promotion Boundary

Passing this sprint does not authorize live trading.

Passing this sprint only authorizes asking:

Should the current challenger become the new `/stock` baseline champion?

The answer still requires:

- approved release decision
- passed release gates
- clean benchmark diff
- release evidence bundle
- baseline promotion policy compliance

## Stop Conditions

Stop the sprint and fix the blocker if any of the following occurs:

- data source is stale or unproven
- lifecycle pointer does not match the current top candidate
- current basket pointer is missing
- candidate basket observation cannot produce benchmark comparison
- performance evidence is failed before sample floor because criteria are broken
- feedback queue accumulates unresolved high-severity items
- operations scoreboard remains red for repeated cycles for the same cause
- release gates fail

## Expert Conclusion

The system does not need broader ambition right now. It needs narrower proof.

The first-place move is to make every trading day produce defensible evidence, including losses. A system that cannot close 20 honest observations has no basis to claim market leadership. A system that can close them, explain failures, and still beat its champion has earned the next fight.

# Airivo Mainline Delivery Adjudication Checklist

This checklist is the operational gate for `2026Qlin`, the only Airivo production mainline repo.

Authoritative references:

- `docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md`
- `docs/AIRIVO_30_DAY_EXECUTION_PLAN.md`
- `docs/AIRIVO_PROFESSIONAL_SYSTEM_BLUEPRINT.md`
- `docs/AIRIVO_CURRENT_STAGE_STRATEGY_PRODUCTION_READINESS_PLAN.md`
- `docs/AIRIVO_CODE_REVIEW_REJECTION_STANDARD.md`

Any change that cannot pass this checklist must not enter the production mainline.

## 1. Demand Intake

- The request belongs to exactly one formal scope:
  - `/stock` candidate chain
  - `/T12` fixed five-ticket governance chain
  - `execution feedback` closed loop
  - `trading kernel`
  - unified release / rollback chain
- The request directly strengthens the mainline closed loop instead of extending parallel capability.
- The request names one unique source of truth.
- The request names one rollback path.
- The request does not require a second entry, second mainline, or second truth source.
- The request does not break existing `daily outputs`.
- If the request concerns production strategy, experimental strategy, execution evidence, or release readiness, it follows `docs/AIRIVO_CURRENT_STAGE_STRATEGY_PRODUCTION_READINESS_PLAN.md`.

Reject at intake if any item above is unclear.

## 2. Development

- New code lands only in canonical mainline locations such as `openclaw/runtime`, `openclaw/services`, `trading_kernel`, `tools`, or governed docs.
- No new unmanaged root-level production script, runtime artifact, or historical copy is introduced.
- No business logic is pushed back into `v49_app.py` except thin orchestration wrappers.
- The change reduces or at least does not increase system ambiguity.
- Compatibility shims are only tolerated when they have a clear retirement boundary.
- If release, governance, or mainline behavior changes, the relevant docs are updated in the same change.

## 3. Review

- Reviewer can state the single formal scope in one sentence.
- Reviewer can point to the single truth source without ambiguity.
- Reviewer can explain how the change rolls back.
- Reviewer can confirm no parallel entry or second truth source was introduced.
- Reviewer can confirm no cross-scope responsibility mixing was hidden inside the patch.
- Reviewer can confirm `daily outputs` remain compatible or have an explicit migration and rollback note.
- Reviewer can confirm tests and gates match the risk of the change.
- Reviewer can confirm the change does not claim top-tier production readiness without four-chain evidence.

If any answer is uncertain, the review result is `reject`, not `follow up later`.

## 4. Release

- `tools/governance_gate.py` passes.
- Release authority still maps to:
  - `docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md`
  - `docs/AIRIVO_30_DAY_EXECUTION_PLAN.md`
- The change has a release operator and rollback operator.
- The release path does not create a temporary side channel outside the mainline.
- Production data, audit records, and `daily outputs` remain explainable after deployment.

## 5. Rollback

- Rollback can be executed without inventing a new procedure during the incident.
- Rollback restores the previous truth source and release state.
- Rollback does not create orphan artifacts or a shadow operational path.
- Rollback verification is explicit:
  - app health
  - mainline outputs
  - audit continuity
  - execution feedback continuity

## Decision Rule

A change enters the mainline only if it satisfies all of the following:

- one formal scope
- one truth source
- one rollback path
- no drift against existing `daily outputs`
- no second mainline or second truth source

If not, the change is drift and must be rejected.

# Airivo Mainline PR Gate

## Mainline Authority

- Delivery standard: `docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md`
- 30-day execution plan: `docs/AIRIVO_30_DAY_EXECUTION_PLAN.md`
- Adjudication checklist: `docs/AIRIVO_MAINLINE_DELIVERY_ADJUDICATION_CHECKLIST.md`
- Review rejection standard: `docs/AIRIVO_CODE_REVIEW_REJECTION_STANDARD.md`

## Scope Declaration

- Formal scope:
  - [ ] `/stock` candidate chain
  - [ ] `/T12` fixed five-ticket governance chain
  - [ ] `execution feedback` closed loop
  - [ ] `trading kernel`
  - [ ] unified release / rollback chain
- Why this change belongs to exactly one formal scope:

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

- [ ] `python3 tools/governance_gate.py`
- [ ] Relevant tests or smoke checks were run
- [ ] Release / rollback impact was reviewed

## Reviewer Focus

- Highest-risk area:
- What would make this PR an immediate reject:

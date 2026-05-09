# Airivo Code Review Rejection Standard

This document defines mandatory rejection conditions for the Airivo production mainline.

Authoritative references:

- `docs/AIRIVO_UNIQUE_MAINLINE_DELIVERY_STANDARD.md`
- `docs/AIRIVO_30_DAY_EXECUTION_PLAN.md`
- `docs/AIRIVO_MAINLINE_DELIVERY_ADJUDICATION_CHECKLIST.md`

If any condition below is met, the change must be rejected. No "merge first, clean later" exception is allowed.

## Immediate Reject Conditions

- The change cannot clearly declare one formal scope.
- The change introduces or preserves a second truth source.
- The change introduces a parallel entry, parallel workflow, or second production mainline.
- The change mixes UI, service, state, and release responsibilities without explicit boundaries.
- The change writes new production logic back into `v49_app.py` instead of canonical runtime or service modules.
- The change adds unmanaged root-level scripts, runtime artifacts, or historical copies.
- The change modifies release or governance behavior without updating the governing docs.
- The change impacts `daily outputs` without an explicit compatibility note and rollback path.
- The change has no credible rollback path.
- The reviewer cannot explain where the truth source moved or why it stayed authoritative.

## High-Risk Reject Conditions

- The patch expands compatibility shims without a retirement boundary.
- The patch hides cross-scope behavior inside a large mixed diff.
- The patch increases system ambiguity even if functionality appears to work.
- The patch depends on manual operator memory instead of declared process.
- The patch bypasses governance, release, or audit gates.

## Reviewer Decision Rule

Reviewers must answer all of the following before approval:

- What is the exact formal scope?
- What is the exact truth source?
- What is the exact rollback path?
- Does this patch reduce or at least not increase ambiguity?
- Does this patch preserve the mainline and `daily outputs`?

If any answer is unclear, the correct decision is `reject`.

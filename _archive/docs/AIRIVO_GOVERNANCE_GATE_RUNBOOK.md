# Airivo Governance Gate Runbook

This runbook standardizes how developers and CI execute governance gates, so all paths use the same checks and produce the same failure semantics.

Quick map: `_archive/docs/AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md`

## 1. Scope

Use this runbook for:

- local pre-PR governance checks
- CI governance checks
- release-diff governance checks before pushing `main`

Do not bypass this runbook with ad-hoc `governance_gate.py` calls unless troubleshooting.

## 2. Required Inputs

Minimum required:

- Python runtime that can run `tools/governance_gate.py`
- a readable DB path for execution attribution hygiene dry-run

Common DB choices:

- local: `data/openclaw.db`
- release preflight: `${AIRIVO_RELEASE_DB_PATH}`

## 3. Local Quick Start

```bash
# optional but recommended
export AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="data/openclaw.db"
export AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE=1

bash _archive/tools/run_governance_gate_ci.sh
```

Expected behavior:

- exits `0` when clean
- exits non-zero when governance blockers exist
- prints a deterministic list of blockers

## 4. Local Diff-Only Check

Use when you only want checks for a specific commit range:

```bash
BASE_SHA="$(git merge-base HEAD origin/main)"
HEAD_SHA="$(git rev-parse HEAD)"

GOVERNANCE_BASE_SHA="$BASE_SHA" \
GOVERNANCE_HEAD_SHA="$HEAD_SHA" \
AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="data/openclaw.db" \
bash _archive/tools/run_governance_gate_ci.sh
```

## 5. CI Standard Template

Use this environment contract in CI:

```bash
export GOVERNANCE_BASE_SHA="${GITHUB_BASE_SHA:-${GITHUB_EVENT_BEFORE:-}}"
export GOVERNANCE_HEAD_SHA="${GITHUB_SHA:-$(git rev-parse HEAD)}"

export AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE=1
export AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="${AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH:-data/openclaw.db}"
export AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STATUSES="${AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STATUSES:-created,submitted}"
export AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STALE_MINUTES="${AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_STALE_MINUTES:-30}"
export AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_MAX_ORDERS="${AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_MAX_ORDERS:-500}"

bash _archive/tools/run_governance_gate_ci.sh
```

Policy:

- hygiene gate is dry-run only in CI
- if dry-run finds missing attribution rows (`patched_count > 0`), fail the pipeline
- fix data hygiene first, then rerun

## 6. Release Diff Gate

Before pushing mainline release, run governance on the release diff:

```bash
REMOTE_BEFORE="$(git rev-parse origin/main)"
LOCAL_HEAD="$(git rev-parse HEAD)"

GOVERNANCE_BASE_SHA="$REMOTE_BEFORE" \
GOVERNANCE_HEAD_SHA="$LOCAL_HEAD" \
AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="${AIRIVO_RELEASE_DB_PATH:-data/openclaw.db}" \
bash _archive/tools/run_governance_gate_ci.sh
```

If this fails, do not push.

## 7. Failure Triage

1. Read first blocker, do not batch-guess fixes.
2. If blocker is evidence-env related, provide the missing env or artifact path explicitly.
3. If blocker is execution attribution hygiene, run:

```bash
python tools/backfill_execution_attribution.py \
  --db-path "${AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH:-data/openclaw.db}" \
  --statuses created,submitted \
  --stale-minutes 30 \
  --max-orders 500
```

Then decide whether to run `--apply` in a controlled remediation step.

## 8. Non-Negotiable Boundaries

- no `--non-blocking` semantics for governance gate
- no merge when gate fails
- no replacing gate checks with screenshots, chat logs, or manual claims


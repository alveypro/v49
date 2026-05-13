# Airivo Governance Gate Runbook

This runbook documents the archived governance gate. It is retained for forensic
review, PR comments, and migration continuity; it is not the current default
hard production gate.

Quick map: `docs/AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md`

## 1. Scope

Use this runbook for:

- troubleshooting historical governance blockers
- generating archived governance PR comment artifacts
- explicitly opted-in release-diff checks

Current hard gates live in the runtime boundary map:

- `python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2`
- stock dashboard boundary tests in `.github/workflows/ci.yml`

Do not promote this archived runner back to default blocking CI unless it is
fully migrated to current `tools/`, `openclaw/services/`, and DB path semantics.

## 2. Required Inputs

Minimum required when running the archived gate intentionally:

- Python runtime that can run `_archive/tools/governance_gate.py`
- a readable DB path for execution attribution hygiene dry-run

Common DB choices:

- local: `data/openclaw.db`
- release preflight: `${AIRIVO_RELEASE_DB_PATH}`

## 3. Local Quick Start

```bash
# optional but recommended
export AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="data/openclaw.db"
export AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE=1

bash tools/archive/governance/run_governance_gate_ci.sh
```

Expected behavior:

- exits `0` when clean
- exits non-zero when governance blockers exist
- prints a deterministic list of blockers

In CI, this archived gate is informational by default. It blocks only when
`AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`.

## 4. Local Diff-Only Check

Use when you only want checks for a specific commit range:

```bash
BASE_SHA="$(git merge-base HEAD origin/main)"
HEAD_SHA="$(git rev-parse HEAD)"

GOVERNANCE_BASE_SHA="$BASE_SHA" \
GOVERNANCE_HEAD_SHA="$HEAD_SHA" \
AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="data/openclaw.db" \
bash tools/archive/governance/run_governance_gate_ci.sh
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

bash tools/archive/governance/run_governance_gate_ci.sh
```

Policy:

- hygiene gate is dry-run only in CI
- if dry-run finds missing attribution rows (`patched_count > 0`), treat it as an
  audit finding unless `AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`
- fix data hygiene first before using this archived gate as a release blocker

## 6. Release Diff Gate

Before pushing mainline release, run governance on the release diff:

```bash
REMOTE_BEFORE="$(git rev-parse origin/main)"
LOCAL_HEAD="$(git rev-parse HEAD)"

GOVERNANCE_BASE_SHA="$REMOTE_BEFORE" \
GOVERNANCE_HEAD_SHA="$LOCAL_HEAD" \
AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH="${AIRIVO_RELEASE_DB_PATH:-data/openclaw.db}" \
bash tools/archive/governance/run_governance_gate_ci.sh
```

If this fails, do not push only when the release process has explicitly opted
into archived governance blocking with `AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`.

## 7. Failure Triage

1. Read first blocker, do not batch-guess fixes.
2. If blocker is evidence-env related, provide the missing env or artifact path explicitly.
3. If blocker is execution attribution hygiene, run:

```bash
python tools/archive/strategy_competition/backfill_execution_attribution.py \
  --db-path "${AIRIVO_EXECUTION_ATTRIBUTION_HYGIENE_DB_PATH:-data/openclaw.db}" \
  --statuses created,submitted \
  --stale-minutes 30 \
  --max-orders 500
```

Then decide whether to run `--apply` in a controlled remediation step.

## 8. Non-Negotiable Boundaries

- archived governance failure must be visible in PR comments or artifacts
- archived governance failure blocks only under explicit opt-in
- current hard gates must remain the tool boundary audit and stock dashboard
  boundary tests until a new governance runner replaces the archived one
- no replacing gate findings with screenshots, chat logs, or manual claims

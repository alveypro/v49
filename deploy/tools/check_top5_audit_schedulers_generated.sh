#!/usr/bin/env bash
# CI / pre-commit: fail if generated deploy artifacts drift from deploy/top5_audit_schedule.env.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if ! command -v git >/dev/null 2>&1; then
  echo "[check-top5-schedulers] WARNING: git not found; skipping drift check" >&2
  exit 0
fi

bash "$ROOT/deploy/tools/gen_top5_audit_schedulers.sh"

if git diff --quiet deploy/airivo-top5-competition-audit.service deploy/airivo-top5-competition-audit.timer deploy/airivo-top5-competition-audit.launchd.plist.example 2>/dev/null; then
  echo "[check-top5-schedulers] OK: generated files match single source"
  exit 0
fi

echo "[check-top5-schedulers] ERROR: generated deploy/* files drift from deploy/top5_audit_schedule.env.sh" >&2
echo "  Fix: bash deploy/tools/gen_top5_audit_schedulers.sh && git add deploy/ && git commit" >&2
git --no-pager diff deploy/airivo-top5-competition-audit.service deploy/airivo-top5-competition-audit.timer deploy/airivo-top5-competition-audit.launchd.plist.example || true
exit 1

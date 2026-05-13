#!/usr/bin/env bash
# Aggregate Top5 audit evidence gates; see tools/top5_audit_evidence_gate.py for exit codes.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PY_BIN="${TOP5_AUDIT_PYTHON:-}"
if [[ -z "$PY_BIN" ]]; then
  for candidate in \
    "$ROOT_DIR/.venv/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "$(command -v python3 2>/dev/null || true)" \
    "/usr/bin/python3"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      PY_BIN="$candidate"
      break
    fi
  done
fi

if [[ -z "$PY_BIN" || ! -x "$PY_BIN" ]]; then
  echo "[top5-audit-evidence-gate] ERROR: no available python interpreter" >&2
  exit 1
fi

exec "$PY_BIN" "$ROOT_DIR/tools/top5_audit_evidence_gate.py" "$@"

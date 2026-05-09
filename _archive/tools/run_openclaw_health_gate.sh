#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${OPENCLAW_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$ROOT_DIR"

resolve_python_bin() {
  local c
  for c in \
    "${OPENCLAW_PYTHON:-}" \
    "${PYTHON_BIN:-}" \
    "$ROOT_DIR/.venv/bin/python" \
    "$ROOT_DIR/venv311/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "/opt/airivo/app/.venv/bin/python" \
    "$(command -v python3 2>/dev/null || true)"; do
    [[ -n "${c}" && -x "${c}" ]] && { echo "${c}"; return 0; }
  done
  echo "python3"
}

assert_python_ge_311() {
  "$1" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

PYTHON_BIN="$(resolve_python_bin)"
if ! assert_python_ge_311 "$PYTHON_BIN"; then
  echo "[health-gate] ERROR: Python>=3.11 required, got: $("$PYTHON_BIN" -V 2>&1)"
  exit 2
fi

exec "$PYTHON_BIN" "$ROOT_DIR/tools/openclaw_health_gate.py"

#!/usr/bin/env bash
set -euo pipefail

cd /Users/mac/2026Qlin
export TZ=Asia/Shanghai
export AUTO_PUSH=1
export UPDATE_DAYS=30
export FUND_PORTFOLIO_FUNDS="001051.OF,005733.OF,110020.OF,007339.OF,160119.OF,004348.OF,110026.OF,004744.OF,011608.OF,011609.OF,022455.OF,022456.OF,460300.OF,006131.OF,161039.OF,110022.OF,110011.OF,009265.OF"
export EVOLVE_FAST=0
export EVOLVE_MAX_SECONDS=0
export EVOLVE_LOG_EVERY=10

resolve_python_bin() {
  local c
  for c in \
    "${PYTHON_BIN:-}" \
    "${OPENCLAW_PYTHON:-}" \
    "/Users/mac/2026Qlin/.venv/bin/python" \
    "/Users/mac/2026Qlin/venv311/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "$(command -v python3 2>/dev/null || true)"; do
    [[ -n "${c}" && -x "${c}" ]] && { echo "${c}"; return 0; }
  done
  echo "python3"
}

PYTHON_BIN="$(resolve_python_bin)"
if ! "$PYTHON_BIN" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Python>=3.11 required, got: $("$PYTHON_BIN" -V 2>&1)" >> /Users/mac/2026Qlin/auto_evolve.launchd.log
  exit 2
fi

"$PYTHON_BIN" /Users/mac/2026Qlin/auto_evolve.py >> /Users/mac/2026Qlin/auto_evolve.launchd.log 2>&1

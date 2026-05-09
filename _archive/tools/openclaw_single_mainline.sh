#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

resolve_python_bin() {
  local c
  for c in \
    "$ROOT_DIR/.venv/bin/python" \
    "$ROOT_DIR/venv311/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "/opt/airivo/app/.venv/bin/python" \
    "${OPENCLAW_PYTHON:-}" \
    "${PYTHON_BIN:-}" \
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
  echo "[single-mainline] ERROR: Python>=3.11 required, got: $("$PYTHON_BIN" -V 2>&1)"
  exit 2
fi

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "[single-mainline] ERROR: TELEGRAM_BOT_TOKEN is missing."
  exit 2
fi

# Single mainline hard rules.
export OPENCLAW_NON_STOCK_ALLOW_STOCK_FALLBACK=0
export OPENCLAW_NON_STOCK_EMERGENCY_FALLBACK=0
export OPENCLAW_STOCK_CONCEPT_CLOUD_FIRST=1
export OPENCLAW_CLOUD_BRAIN_ONLY="${OPENCLAW_CLOUD_BRAIN_ONLY:-0}"

# Keep one telegram bridge process only.
pkill -f "deploy_stock/telegram_bridge_bot.py" >/dev/null 2>&1 || true
pkill -f "deploy_stock/dingtalk_bridge_server.py" >/dev/null 2>&1 || true

echo "[single-mainline] starting telegram bridge only..."
echo "[single-mainline] OPENCLAW_CLOUD_BRAIN_ONLY=${OPENCLAW_CLOUD_BRAIN_ONLY}"
echo "[single-mainline] NON_STOCK fallback disabled"

exec "$PYTHON_BIN" "$ROOT_DIR/deploy_stock/telegram_bridge_bot.py"

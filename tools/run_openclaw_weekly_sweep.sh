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
  echo "[weekly-sweep] ERROR: Python>=3.11 required, got: $("$PYTHON_BIN" -V 2>&1)"
  exit 2
fi

MODULE_PATH="${OPENCLAW_WEEKLY_SWEEP_MODULE_PATH:-$ROOT_DIR/v49_app.py}"
CENTER_CONFIG="${OPENCLAW_STRATEGY_CENTER_CONFIG:-$ROOT_DIR/openclaw/config/strategy_center.yaml}"
OUTPUT_DIR="${OPENCLAW_OUTPUT_DIR:-$ROOT_DIR/logs/openclaw}"
LOOKBACK_DAYS="${OPENCLAW_WEEKLY_SWEEP_LOOKBACK_DAYS:-730}"
MODE="${OPENCLAW_WEEKLY_SWEEP_MODE:-single}"
TIMEOUT_SEC="${OPENCLAW_WEEKLY_SWEEP_TIMEOUT_SEC:-90}"
STRATEGIES="${OPENCLAW_WEEKLY_SWEEP_STRATEGIES:-v5,v8,v9,combo}"

mkdir -p "$OUTPUT_DIR"

exec "$PYTHON_BIN" "$ROOT_DIR/tools/openclaw_weekly_sweep.py" \
  --module-path "$MODULE_PATH" \
  --center-config "$CENTER_CONFIG" \
  --output-dir "$OUTPUT_DIR" \
  --lookback-days "$LOOKBACK_DAYS" \
  --mode "$MODE" \
  --per-run-timeout-sec "$TIMEOUT_SEC" \
  --strategies "$STRATEGIES"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

LOG_DIR="${ROOT_DIR}/logs/openclaw"
mkdir -p "$LOG_DIR"

RUN_TS="$(date +%Y%m%d_%H%M%S)"
OUT_LOG="${LOG_DIR}/auto_backtest_daily.log"
ERR_LOG="${LOG_DIR}/auto_backtest_daily.err"
LOCK_FILE="/tmp/openclaw_auto_backtest_daily.lock"

if [[ -f "$LOCK_FILE" ]]; then
  OLD_PID="$(cat "$LOCK_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "[$(date '+%F %T')] skip: already running pid=${OLD_PID}" >>"$OUT_LOG"
    exit 0
  fi
fi
echo "$$" >"$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

resolve_python_bin() {
  local c
  for c in \
    "${OPENCLAW_PYTHON:-}" \
    "${ROOT_DIR}/venv311/bin/python" \
    "${ROOT_DIR}/.venv/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "$(command -v python3 2>/dev/null || true)"; do
    [[ -n "${c}" && -x "${c}" ]] && { echo "${c}"; return 0; }
  done
  echo "python3"
}

PY_BIN="$(resolve_python_bin)"
STRATEGIES="${OPENCLAW_SWEEP_STRATEGIES:-v5,v8,v9,combo}"
LOOKBACK_DAYS="${OPENCLAW_SWEEP_LOOKBACK_DAYS:-540}"
PER_RUN_TIMEOUT="${OPENCLAW_SWEEP_PER_RUN_TIMEOUT_SEC:-120}"
MODE="${OPENCLAW_SWEEP_MODE:-single}"

{
  echo "[$(date '+%F %T')] auto-backtest start strategies=${STRATEGIES} lookback=${LOOKBACK_DAYS} mode=${MODE}"
  "$PY_BIN" tools/openclaw_weekly_sweep.py \
    --strategies "$STRATEGIES" \
    --lookback-days "$LOOKBACK_DAYS" \
    --mode "$MODE" \
    --per-run-timeout-sec "$PER_RUN_TIMEOUT" \
    --write-center-config
  echo "[$(date '+%F %T')] auto-backtest done"
} >>"$OUT_LOG" 2>>"$ERR_LOG"


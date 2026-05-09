#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${OPENCLAW_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOG_DIR="$ROOT_DIR/logs/openclaw"
LOG_FILE="$LOG_DIR/data_update.launchd.log"
STAMP_FILE="$LOG_DIR/data_update.last_ok"
LOCK_DIR="$LOG_DIR/.data_update.lock"
PYTHON_BIN="${PYTHON_BIN:-python3}"

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

mkdir -p "$LOG_DIR"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip: data_update lock exists" >> "$LOG_FILE"
  exit 0
fi
cleanup() {
  rmdir "$LOCK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT_DIR"
if ! assert_python_ge_311 "$PYTHON_BIN"; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Python>=3.11 required, got: $("$PYTHON_BIN" -V 2>&1)" >> "$LOG_FILE"
  exit 2
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] start data update" >> "$LOG_FILE"

OUT="$("$PYTHON_BIN" openclaw/update_db_calendar.py \
  --close-hour "${OPENCLAW_TRADE_CLOSE_HOUR:-15}" \
  --delay-hours "${OPENCLAW_DATA_READY_DELAY_HOURS:-2}" \
  --max-backfill-days "${OPENCLAW_AUTO_UPDATE_MAX_DAYS:-15}" 2>&1 || true)"

echo "$OUT" >> "$LOG_FILE"
if echo "$OUT" | /usr/bin/grep -q '"ok": true'; then
  date '+%Y-%m-%d %H:%M:%S' > "$STAMP_FILE"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] data update ok" >> "$LOG_FILE"
  exit 0
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] data update failed" >> "$LOG_FILE"
exit 1

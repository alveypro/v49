#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${OPENCLAW_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOG_DIR="$ROOT_DIR/logs/openclaw"
LOG_FILE="$LOG_DIR/data_optimize.launchd.log"
STAMP_FILE="$LOG_DIR/data_update.last_ok"
LOCK_DIR="$LOG_DIR/.data_optimize.lock"
PYTHON_BIN="${PYTHON_BIN:-python3}"
MAX_STAMP_AGE_MIN="${OPENCLAW_OPTIMIZE_MAX_STAMP_AGE_MIN:-720}"

mkdir -p "$LOG_DIR"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip: data_optimize lock exists" >> "$LOG_FILE"
  exit 0
fi
cleanup() {
  rmdir "$LOCK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] start data optimize" >> "$LOG_FILE"

if [[ ! -f "$STAMP_FILE" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip: no update stamp found" >> "$LOG_FILE"
  exit 0
fi

STAMP_AGE_MIN="$("$PYTHON_BIN" - "$STAMP_FILE" <<'PY'
import sys
from pathlib import Path
from datetime import datetime

p = Path(sys.argv[1])
ts = p.read_text(encoding="utf-8").strip()
try:
    t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
except Exception:
    print(999999)
    raise SystemExit(0)
delta = datetime.now() - t
print(int(delta.total_seconds() // 60))
PY
)"
if [[ "$STAMP_AGE_MIN" -gt "$MAX_STAMP_AGE_MIN" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip: update stamp too old (${STAMP_AGE_MIN}min)" >> "$LOG_FILE"
  exit 0
fi

DB_PATH="$("$PYTHON_BIN" - <<'PY'
from data.dao import resolve_db_path
print(resolve_db_path())
PY
)"

sqlite3 "$DB_PATH" "PRAGMA optimize;" >> "$LOG_FILE" 2>&1
sqlite3 "$DB_PATH" "ANALYZE;" >> "$LOG_FILE" 2>&1

"$PYTHON_BIN" openclaw/strategy_tracking_cli.py refresh --lookback-days 360 >> "$LOG_FILE" 2>&1 || true
"$PYTHON_BIN" openclaw/strategy_tracking_cli.py scoreboard --lookback-days 180 --output-dir logs/openclaw >> "$LOG_FILE" 2>&1 || true

echo "[$(date '+%Y-%m-%d %H:%M:%S')] data optimize ok db=$DB_PATH" >> "$LOG_FILE"

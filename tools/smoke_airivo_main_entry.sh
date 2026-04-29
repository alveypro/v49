#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/tools/lib/remote_access.sh"

REMOTE_HOST="${REMOTE_HOST:-$AIRIVO_REMOTE_TARGET}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-$AIRIVO_REMOTE_APP_DIR}"
REMOTE_SERVICE="${REMOTE_SERVICE:-openclaw-streamlit.service}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8501/_stcore/health}"
PUBLIC_URL="${PUBLIC_URL:-https://airivo.online}"
MAIN_FILE="${MAIN_FILE:-openclaw/runtime/airivo_execution_center.py}"
RUN_ASYNC_SCAN_SMOKE="${RUN_ASYNC_SCAN_SMOKE:-1}"
ASYNC_SCAN_SMOKE_STRATEGIES="${ASYNC_SCAN_SMOKE_STRATEGIES:-v5,v8,v9}"
ASYNC_SCAN_SMOKE_TIMEOUT_SEC="${ASYNC_SCAN_SMOKE_TIMEOUT_SEC:-180}"
RUN_UI_ASYNC_TASK_SMOKE="${RUN_UI_ASYNC_TASK_SMOKE:-1}"
REMOTE_PY_BIN="${REMOTE_PY_BIN:-/opt/openclaw/venv311/bin/python}"

EXPECT_TEXTS=(
  "批次概览"
  "为什么是这个结论"
  "技术明细（调试）"
)

msg() { printf "[smoke-airivo-main-entry] %s\n" "$*"; }
die() { printf "[smoke-airivo-main-entry] ERROR: %s\n" "$*" >&2; exit 1; }

run_remote() {
  AIRIVO_REMOTE_TARGET="$REMOTE_HOST" airivo_remote_exec_ssh "$1"
}

msg "check service active"
run_remote "systemctl is-active \"$REMOTE_SERVICE\""

msg "check streamlit health"
HEALTH="$(run_remote "curl -sS --max-time 5 \"$HEALTH_URL\" || true")"
[[ "$HEALTH" == *"ok"* ]] || die "health check failed: $HEALTH"

msg "check public endpoint"
run_remote "curl -I -sS --max-time 8 \"$PUBLIC_URL\" | head -n 1"

msg "check source strings on deployed main file"
for text in "${EXPECT_TEXTS[@]}"; do
  run_remote "grep -q \"$text\" \"$REMOTE_APP_DIR/$MAIN_FILE\""
  msg "found text: $text"
done

if run_remote "command -v chromium-browser >/dev/null 2>&1"; then
  msg "check browser-rendered shell via chromium"
  run_remote "chromium-browser --headless --disable-gpu --no-sandbox --dump-dom \"$PUBLIC_URL\" 2>/dev/null | grep -q streamlit"
else
  msg "skip browser DOM check: chromium-browser not installed"
fi

if [[ "$RUN_ASYNC_SCAN_SMOKE" == "1" ]]; then
  msg "run async scan smoke: strategies=${ASYNC_SCAN_SMOKE_STRATEGIES}"
  run_remote "cd \"$REMOTE_APP_DIR\" && \"$REMOTE_PY_BIN\" tools/async_scan_smoke.py --strategies \"$ASYNC_SCAN_SMOKE_STRATEGIES\" --timeout-sec \"$ASYNC_SCAN_SMOKE_TIMEOUT_SEC\""
else
  msg "skip async scan smoke: RUN_ASYNC_SCAN_SMOKE=${RUN_ASYNC_SCAN_SMOKE}"
fi

if [[ "$RUN_UI_ASYNC_TASK_SMOKE" == "1" ]]; then
  msg "run ui async task smoke: strategies=${ASYNC_SCAN_SMOKE_STRATEGIES}"
  run_remote "cd \"$REMOTE_APP_DIR\" && \"$REMOTE_PY_BIN\" tools/ui_async_task_smoke.py --strategies \"$ASYNC_SCAN_SMOKE_STRATEGIES\""
else
  msg "skip ui async task smoke: RUN_UI_ASYNC_TASK_SMOKE=${RUN_UI_ASYNC_TASK_SMOKE}"
fi

msg "smoke passed"

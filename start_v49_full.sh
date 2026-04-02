#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

PORT="${STREAMLIT_PORT:-8501}"

# Prevent launchd KeepAlive from creating restart loops when port is already occupied.
existing="$(lsof -ti:"$PORT" 2>/dev/null || true)"
if [[ -n "$existing" ]] && kill -0 "$existing" 2>/dev/null; then
  echo "[$(date '+%F %T')] streamlit already running on :$PORT (pid=$existing), keepalive hold"
  while kill -0 "$existing" 2>/dev/null; do
    sleep 30
  done
  exit 0
fi

CANONICAL_APP_FILE="v49_app.py"
APP_FILE="${AIRIVO_APP_FILE:-$CANONICAL_APP_FILE}"
if [[ "$APP_FILE" != "$CANONICAL_APP_FILE" ]]; then
  echo "ERROR: non-canonical app entry requested: $APP_FILE (expected $CANONICAL_APP_FILE)"
  echo "Hint: unset AIRIVO_APP_FILE or set AIRIVO_APP_FILE=$CANONICAL_APP_FILE"
  exit 1
fi
if [[ ! -f "$APP_FILE" ]]; then
  echo "ERROR: app file not found: $APP_FILE"
  exit 1
fi

PY_BIN="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3 2>/dev/null || echo /usr/bin/python3)"
fi

exec "$PY_BIN" -m streamlit run "$APP_FILE" \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --server.runOnSave false \
  --server.fileWatcherType none \
  --server.disconnectedSessionTTL 900 \
  --server.maxMessageSize 500 \
  --server.enableWebsocketCompression true

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Load workspace .env into environment.
if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

if [[ -z "${OPENCLAW_ROOT:-}" ]]; then
  export OPENCLAW_ROOT="$ROOT_DIR"
fi

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="/Users/mac/.pyenv/versions/3.10.12/bin/python3"
fi

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
  echo "[telegram-run] ERROR: TELEGRAM_BOT_TOKEN is missing in .env"
  exit 2
fi

# Ensure single local poller instance for this token/process entrypoint.
pkill -f "deploy_stock/telegram_bridge_bot.py" >/dev/null 2>&1 || true

# Partner mode default: local OpenClaw first, not cloud-only.
export OPENCLAW_CLOUD_BRAIN_ONLY="${OPENCLAW_CLOUD_BRAIN_ONLY:-0}"

echo "[telegram-run] starting telegram bridge..."
echo "[telegram-run] OPENCLAW_CLOUD_BRAIN_ONLY=${OPENCLAW_CLOUD_BRAIN_ONLY}"
if [[ -n "${TELEGRAM_ALLOWED_CHAT_IDS:-}" ]]; then
  echo "[telegram-run] TELEGRAM_ALLOWED_CHAT_IDS=${TELEGRAM_ALLOWED_CHAT_IDS}"
else
  echo "[telegram-run] TELEGRAM_ALLOWED_CHAT_IDS not set (all chats allowed)"
fi

exec "$PYTHON_BIN" "$ROOT_DIR/deploy_stock/telegram_bridge_bot.py"

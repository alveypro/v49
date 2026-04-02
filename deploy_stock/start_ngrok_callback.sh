#!/bin/bash
set -euo pipefail

PORT="${DINGTALK_BRIDGE_PORT:-8601}"
AUTHTOKEN="${NGROK_AUTHTOKEN:-}"
NGROK_CFG="${HOME}/Library/Application Support/ngrok/ngrok.yml"

if [[ ! -f "$NGROK_CFG" ]]; then
  if [[ -n "$AUTHTOKEN" ]]; then
    ngrok config add-authtoken "$AUTHTOKEN"
  else
    echo "[ngrok] missing config and NGROK_AUTHTOKEN is empty."
    echo "[ngrok] run once: ngrok config add-authtoken <YOUR_TOKEN>"
    exit 1
  fi
fi

echo "[ngrok] expose http://127.0.0.1:${PORT}"
exec /usr/local/bin/ngrok http "${PORT}" --log=stdout

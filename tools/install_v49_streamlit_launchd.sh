#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_SRC="$ROOT_DIR/launchd/com.airivo.v49.streamlit.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.airivo.v49.streamlit.plist"

mkdir -p "$ROOT_DIR/logs"
mkdir -p "$(dirname "$PLIST_DST")"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl unload "$PLIST_DST" >/dev/null 2>&1 || true
launchctl load "$PLIST_DST"
launchctl kickstart -k "gui/$(id -u)/com.airivo.v49.streamlit"

echo "Installed and started: com.airivo.v49.streamlit"
echo "Log: $ROOT_DIR/logs/v49.streamlit.launchd.log"
echo "Err: $ROOT_DIR/logs/v49.streamlit.launchd.err.log"

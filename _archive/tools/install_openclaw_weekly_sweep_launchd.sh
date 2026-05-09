#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_SRC="$ROOT_DIR/launchd/com.airivo.openclaw.weekly-sweep.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.airivo.openclaw.weekly-sweep.plist"
LABEL="com.airivo.openclaw.weekly-sweep"

mkdir -p "$ROOT_DIR/logs/openclaw"
mkdir -p "$(dirname "$PLIST_DST")"

chmod +x "$ROOT_DIR/tools/run_openclaw_weekly_sweep.sh"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl unload "$PLIST_DST" >/dev/null 2>&1 || true
launchctl load "$PLIST_DST"
launchctl kickstart -k "gui/$(id -u)/${LABEL}" || true

echo "Installed launchd: ${LABEL}"
echo "plist: ${PLIST_DST}"
echo "stdout: $ROOT_DIR/logs/openclaw/weekly_sweep.launchd.log"
echo "stderr: $ROOT_DIR/logs/openclaw/weekly_sweep.launchd.err.log"

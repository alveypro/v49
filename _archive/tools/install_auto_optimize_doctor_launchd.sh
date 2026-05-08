#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_SRC="$ROOT_DIR/launchd/com.airivo.auto-optimize-doctor.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.airivo.auto-optimize-doctor.plist"
LABEL="com.airivo.auto-optimize-doctor"

mkdir -p "$ROOT_DIR/logs"
mkdir -p "$ROOT_DIR/logs/doctor"
mkdir -p "$(dirname "$PLIST_DST")"

chmod +x "$ROOT_DIR/tools/run_auto_optimize_doctor.sh"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl unload "$PLIST_DST" >/dev/null 2>&1 || true
launchctl load "$PLIST_DST"
launchctl kickstart -k "gui/$(id -u)/${LABEL}" || true

echo "Installed launchd: ${LABEL}"
echo "plist: ${PLIST_DST}"
echo "stdout: $ROOT_DIR/logs/doctor.launchd.log"
echo "stderr: $ROOT_DIR/logs/doctor.launchd.err.log"
echo "latest json: $ROOT_DIR/logs/doctor/latest.json"
echo "latest log: $ROOT_DIR/logs/doctor/latest.log"


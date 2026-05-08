#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LAUNCHD_SRC="$ROOT_DIR/launchd"
LEGACY_LAUNCHD_SRC="$ROOT_DIR/openclaw/launchd"
LAUNCHD_DST="$HOME/Library/LaunchAgents"

mkdir -p "$LAUNCHD_DST" "$ROOT_DIR/logs" "$ROOT_DIR/logs/openclaw" "$ROOT_DIR/logs/doctor"

PLISTS=(
  "com.airivo.auto-evolve.plist"
  "com.airivo.syncdb.plist"
  "com.airivo.openclaw.data-update.plist"
  "com.airivo.openclaw.data-optimize.plist"
  "com.airivo.openclaw.health-gate.plist"
  "com.airivo.auto-optimize-doctor.plist"
  "com.airivo.openclaw.weekly-sweep.plist"
  "com.airivo.openclaw.daily.plist"
)

chmod +x "$ROOT_DIR/scripts/auto_evolve_local.sh" || true
chmod +x "$ROOT_DIR/tools/run_auto_optimize_doctor.sh" || true
chmod +x "$ROOT_DIR/tools/run_openclaw_health_gate.sh" || true
chmod +x "$ROOT_DIR/tools/run_openclaw_weekly_sweep.sh" || true
chmod +x "$ROOT_DIR/scripts/openclaw_data_update.sh" || true
chmod +x "$ROOT_DIR/scripts/openclaw_data_optimize.sh" || true

for plist in "${PLISTS[@]}"; do
  src="$LAUNCHD_SRC/$plist"
  [[ -f "$src" ]] || src="$LEGACY_LAUNCHD_SRC/$plist"
  dst="$LAUNCHD_DST/$plist"
  label="${plist%.plist}"
  [[ -f "$src" ]] || { echo "skip missing plist: $src"; continue; }
  cp "$src" "$dst"
  launchctl unload "$dst" >/dev/null 2>&1 || true
  launchctl load "$dst"
  echo "installed: $label"
done

echo
echo "core launchd plists installed:"
for plist in "${PLISTS[@]}"; do
  echo "  - ${plist%.plist}"
done

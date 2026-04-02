#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

SNAP_ROOT="${OPENCLAW_SNAPSHOT_DIR:-logs/openclaw/stable_snapshots}"
TS="$(date +%Y%m%d_%H%M%S)"
SNAP_DIR="${SNAP_ROOT}/${TS}"
LATEST_LINK="${SNAP_ROOT}/latest"

mkdir -p "$SNAP_DIR"

FILES=(
  "openclaw/scripts_run_daily.sh"
  "openclaw/run_daily.py"
  "openclaw/update_db_calendar.py"
  "auto_evolve.py"
  "v6_data_provider_optimized.py"
  "v6_leader_analyzer.py"
  "tools/openclaw_partner_daily_run.sh"
)

for f in "${FILES[@]}"; do
  if [[ -f "$f" ]]; then
    mkdir -p "${SNAP_DIR}/$(dirname "$f")"
    cp -a "$f" "${SNAP_DIR}/$f"
  fi
done

cat > "${SNAP_DIR}/MANIFEST.txt" <<EOF
snapshot_time=${TS}
root=${ROOT_DIR}
files=${#FILES[@]}
EOF

rm -f "$LATEST_LINK"
ln -s "$SNAP_DIR" "$LATEST_LINK"

echo "snapshot=${SNAP_DIR}"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

SNAP_ROOT="${OPENCLAW_SNAPSHOT_DIR:-logs/openclaw/stable_snapshots}"
LATEST_LINK="${SNAP_ROOT}/latest"

if [[ ! -L "$LATEST_LINK" && ! -d "$LATEST_LINK" ]]; then
  echo "no_snapshot_found:${LATEST_LINK}"
  exit 1
fi

SNAP_DIR="$(cd "$LATEST_LINK" 2>/dev/null && pwd)"
if [[ -z "${SNAP_DIR:-}" || ! -d "$SNAP_DIR" ]]; then
  echo "invalid_snapshot:${LATEST_LINK}"
  exit 1
fi

while IFS= read -r -d '' src; do
  rel="${src#${SNAP_DIR}/}"
  mkdir -p "$(dirname "$rel")"
  cp -a "$src" "$rel"
  echo "restored:${rel}"
done < <(find "$SNAP_DIR" -type f ! -name 'MANIFEST.txt' -print0)

echo "rollback_done:${SNAP_DIR}"

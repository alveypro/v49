#!/usr/bin/env bash
set -euo pipefail

# A server (source) and local target path on B server.
SRC="${SRC:-root@47.90.160.87:/opt/openclaw/permanent_stock_database.db}"
DST="${DST:-/opt/openclaw/permanent_stock_database.db}"
TMP="${DST}.tmp"

mkdir -p "$(dirname "$DST")"

# Use rsync for resumable transfer; fallback to scp if rsync not available.
if command -v rsync >/dev/null 2>&1; then
  rsync -av --partial --inplace "$SRC" "$TMP"
else
  scp "$SRC" "$TMP"
fi

mv -f "$TMP" "$DST"
chmod 644 "$DST"

# Optional: quick sanity check
sqlite3 "$DST" ".tables" | grep -Eq "stock_basic|daily_trading_data"

# Reload qa service to pick newest DB.
systemctl restart openclaw-qa-stock.service


#!/usr/bin/env bash
set -euo pipefail

SRC_DB="${SRC_DB:-/opt/airivo/data/permanent_stock_database.db}"
SRC_LAST_RUN="${SRC_LAST_RUN:-/opt/airivo/app/evolution/last_run.json}"
SRC_SUMMARY_GLOB_A="${SRC_SUMMARY_GLOB_A:-/opt/airivo/app/logs/openclaw/run_summary_*.json}"
SRC_SUMMARY_GLOB_B="${SRC_SUMMARY_GLOB_B:-/opt/airivo/logs/openclaw/run_summary_*.json}"

DST_HOST="${DST_HOST:-root@47.90.160.87}"
DST_DB="${DST_DB:-/opt/openclaw/permanent_stock_database.db}"
DST_LAST_RUN="${DST_LAST_RUN:-/opt/openclaw/evolution/last_run.json}"
DST_SUMMARY_DIR="${DST_SUMMARY_DIR:-/opt/openclaw/logs/openclaw}"
KEY="${KEY:-/root/.ssh/id_db_push}"

SNAP="/tmp/permanent_stock_database.snapshot.db"
TMP_ON_B="${DST_DB}.new"
SSH_OPTS=(-i "$KEY" -o BatchMode=yes -o StrictHostKeyChecking=yes)

mkdir -p /tmp

# Build a consistent SQLite snapshot first; never rsync a live DB directly.
sqlite3 "$SRC_DB" ".backup '$SNAP'"

rsync -az --partial -e "ssh ${SSH_OPTS[*]}" "$SNAP" "${DST_HOST}:${TMP_ON_B}"
ssh "${SSH_OPTS[@]}" "$DST_HOST" "mv -f '$TMP_ON_B' '$DST_DB' && chmod 644 '$DST_DB'"

if [ -f "$SRC_LAST_RUN" ]; then
  ssh "${SSH_OPTS[@]}" "$DST_HOST" "mkdir -p '$(dirname "$DST_LAST_RUN")'"
  scp "${SSH_OPTS[@]}" "$SRC_LAST_RUN" "${DST_HOST}:${DST_LAST_RUN}"
fi

latest_summary="$(ls -1t $SRC_SUMMARY_GLOB_A $SRC_SUMMARY_GLOB_B 2>/dev/null | head -n 1 || true)"
if [ -n "$latest_summary" ] && [ -f "$latest_summary" ]; then
  ssh "${SSH_OPTS[@]}" "$DST_HOST" "mkdir -p '$DST_SUMMARY_DIR'"
  scp "${SSH_OPTS[@]}" "$latest_summary" "${DST_HOST}:${DST_SUMMARY_DIR}/$(basename "$latest_summary")"
fi

ssh "${SSH_OPTS[@]}" "$DST_HOST" "systemctl restart openclaw-qa-stock.service && systemctl is-active openclaw-qa-stock.service"


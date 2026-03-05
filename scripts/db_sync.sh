#!/usr/bin/env bash
set -euo pipefail

OPENCLAW_ROOT="${OPENCLAW_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LOCAL_DB="${LOCAL_DB:-$OPENCLAW_ROOT/permanent_stock_database.db}"
BACKUP_DB="${BACKUP_DB:-$OPENCLAW_ROOT/permanent_stock_database.backup.db}"
REMOTE_DB="${REMOTE_DB:-/opt/openclaw/permanent_stock_database.db}"
REMOTE_HOST="${REMOTE_HOST:-root@47.90.160.87}"
SSH_KEY="${SSH_KEY:-}"
SSH_PASS="${SSH_PASS:-}"
LOG_DIR="$OPENCLAW_ROOT/logs"
LOG_FILE="$LOG_DIR/db_sync.log"
LOCK_DIR="$LOG_DIR/.db_sync.lock"
MAX_RETRIES="${MAX_RETRIES:-3}"

mkdir -p "$LOG_DIR"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] skip: lock exists ($LOCK_DIR)" >> "$LOG_FILE"
  exit 0
fi
cleanup() {
  rmdir "$LOCK_DIR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ ! -f "$LOCAL_DB" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] error: local db not found ($LOCAL_DB)" >> "$LOG_FILE"
  exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] start host=$REMOTE_HOST path=$REMOTE_DB retries=$MAX_RETRIES" >> "$LOG_FILE"

# Hot backup to avoid copying a live DB file
sqlite3 "$LOCAL_DB" ".backup $BACKUP_DB"
sqlite3 "$BACKUP_DB" "PRAGMA quick_check;" >> "$LOG_FILE" 2>&1

# Incremental rsync transfer
SSH_OPTS="-o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new"
if [ -n "$SSH_KEY" ]; then
  SSH_OPTS="$SSH_OPTS -i $SSH_KEY"
fi

REMOTE_TMP="${REMOTE_DB}.sync_tmp"
REMOTE_DIR="$(dirname "$REMOTE_DB")"

run_ssh() {
  if [ -n "$SSH_PASS" ]; then
    sshpass -p "$SSH_PASS" ssh $SSH_OPTS "$REMOTE_HOST" "$1"
  else
    ssh $SSH_OPTS "$REMOTE_HOST" "$1"
  fi
}

run_rsync() {
  if [ -n "$SSH_PASS" ]; then
    sshpass -p "$SSH_PASS" rsync -avz --inplace -e "ssh $SSH_OPTS" "$BACKUP_DB" "$REMOTE_HOST:$REMOTE_TMP" >> "$LOG_FILE" 2>&1
  else
    rsync -avz --inplace -e "ssh $SSH_OPTS" "$BACKUP_DB" "$REMOTE_HOST:$REMOTE_TMP" >> "$LOG_FILE" 2>&1
  fi
}

if [ -n "$SSH_PASS" ]; then
  if ! command -v sshpass >/dev/null 2>&1; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] error: SSH_PASS provided but sshpass not installed" >> "$LOG_FILE"
    exit 1
  fi
fi

run_ssh "mkdir -p '$REMOTE_DIR'"

attempt=1
while true; do
  if run_rsync; then
    break
  fi
  if [[ "$attempt" -ge "$MAX_RETRIES" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] error: rsync failed after $attempt attempts" >> "$LOG_FILE"
    exit 1
  fi
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] warn: rsync attempt $attempt failed, retrying..." >> "$LOG_FILE"
  attempt=$((attempt + 1))
  sleep 2
done

LOCAL_SIZE="$(wc -c < "$BACKUP_DB" | tr -d ' ')"
REMOTE_SIZE="$(run_ssh "wc -c < '$REMOTE_TMP'" | tr -d ' ')"
if [[ "$LOCAL_SIZE" != "$REMOTE_SIZE" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] error: size mismatch local=$LOCAL_SIZE remote=$REMOTE_SIZE" >> "$LOG_FILE"
  exit 1
fi

run_ssh "mv '$REMOTE_TMP' '$REMOTE_DB'"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] done size=$LOCAL_SIZE" >> "$LOG_FILE"

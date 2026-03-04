#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# -------- Config (override via env) --------
REMOTE_NAME="${REMOTE_NAME:-origin}"
REMOTE_BRANCH="${REMOTE_BRANCH:-main}"
DEPLOY_HOST="${DEPLOY_HOST:-root@47.90.160.87}"
DEPLOY_PASS="${DEPLOY_PASS:-Keep@2026}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/openclaw/app}"
REMOTE_SERVICE="${REMOTE_SERVICE:-openclaw-streamlit.service}"
REMOTE_HEALTH_URL="${REMOTE_HEALTH_URL:-http://127.0.0.1:8501/_stcore/health}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"     # 0: block on dirty tree, 1: allow (still deploys committed HEAD only)
RESTART_SERVICE="${RESTART_SERVICE:-1}"

msg() { printf "[sync-dual] %s\n" "$*"; }
die() { printf "[sync-dual] ERROR: %s\n" "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

need_cmd git
need_cmd ssh
need_cmd scp
need_cmd sshpass

if command -v sha1sum >/dev/null 2>&1; then
  HASH_CMD="sha1sum"
elif command -v shasum >/dev/null 2>&1; then
  HASH_CMD="shasum"
else
  die "missing hash command: sha1sum/shasum"
fi

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git repository: $ROOT_DIR"

if [[ "$ALLOW_DIRTY" != "1" ]]; then
  if ! git diff --quiet || ! git diff --cached --quiet; then
    die "working tree is dirty. commit/stash first, or set ALLOW_DIRTY=1"
  fi
else
  if ! git diff --quiet || ! git diff --cached --quiet; then
    msg "working tree is dirty; only committed HEAD content will be deployed"
  fi
fi

LOCAL_HEAD="$(git rev-parse HEAD)"
LOCAL_HEAD_SHORT="$(git rev-parse --short HEAD)"

msg "fetching $REMOTE_NAME/$REMOTE_BRANCH ..."
git fetch "$REMOTE_NAME" "$REMOTE_BRANCH"
REMOTE_BEFORE="$(git rev-parse "$REMOTE_NAME/$REMOTE_BRANCH")"
REMOTE_BEFORE_SHORT="$(git rev-parse --short "$REMOTE_NAME/$REMOTE_BRANCH")"

if ! git merge-base --is-ancestor "$REMOTE_BEFORE" "$LOCAL_HEAD"; then
  die "HEAD is not a fast-forward from $REMOTE_NAME/$REMOTE_BRANCH (before=$REMOTE_BEFORE_SHORT, head=$LOCAL_HEAD_SHORT)"
fi

msg "pushing HEAD -> $REMOTE_NAME/$REMOTE_BRANCH (commit=$LOCAL_HEAD_SHORT)"
git push "$REMOTE_NAME" "$LOCAL_HEAD:$REMOTE_BRANCH"

CHANGED_FILES=()
while IFS= read -r line; do
  [[ -n "$line" ]] && CHANGED_FILES+=("$line")
done < <(git diff --name-only --diff-filter=ACMRT "$REMOTE_BEFORE" "$LOCAL_HEAD")

DELETED_FILES=()
while IFS= read -r line; do
  [[ -n "$line" ]] && DELETED_FILES+=("$line")
done < <(git diff --name-only --diff-filter=D "$REMOTE_BEFORE" "$LOCAL_HEAD")

if [[ "${#CHANGED_FILES[@]}" -eq 0 && "${#DELETED_FILES[@]}" -eq 0 ]]; then
  msg "no file changes in range $REMOTE_BEFORE_SHORT..$LOCAL_HEAD_SHORT"
  exit 0
fi

msg "changed=${#CHANGED_FILES[@]} deleted=${#DELETED_FILES[@]}"

SSH_BASE=(sshpass -p "$DEPLOY_PASS" ssh -o StrictHostKeyChecking=no "$DEPLOY_HOST")
SCP_BASE=(sshpass -p "$DEPLOY_PASS" scp -o StrictHostKeyChecking=no)

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REMOTE_BACKUP_DIR="$REMOTE_APP_DIR/backups/manual_sync_$TIMESTAMP"
msg "creating remote backup dir: $REMOTE_BACKUP_DIR"
"${SSH_BASE[@]}" "set -e; mkdir -p \"$REMOTE_BACKUP_DIR\""

backup_remote_file() {
  local rel="$1"
  local remote_file="$REMOTE_APP_DIR/$rel"
  local backup_file="$REMOTE_BACKUP_DIR/$rel"
  "${SSH_BASE[@]}" "set -e;
    if [ -e \"$remote_file\" ]; then
      mkdir -p \"\$(dirname \"$backup_file\")\";
      cp -a \"$remote_file\" \"$backup_file\";
    fi"
}

deploy_one_file() {
  local rel="$1"
  local tmp_file
  tmp_file="$(mktemp)"
  git show "$LOCAL_HEAD:$rel" > "$tmp_file"

  local remote_file="$REMOTE_APP_DIR/$rel"
  "${SSH_BASE[@]}" "set -e; mkdir -p \"\$(dirname \"$remote_file\")\""
  "${SCP_BASE[@]}" "$tmp_file" "$DEPLOY_HOST:$remote_file"

  # Keep executable bit for scripts.
  local mode
  mode="$(git ls-tree "$LOCAL_HEAD" -- "$rel" | awk '{print $1}')"
  if [[ "$mode" == "100755" ]]; then
    "${SSH_BASE[@]}" "chmod +x \"$remote_file\""
  fi

  rm -f "$tmp_file"
}

verify_one_file() {
  local rel="$1"
  local local_sha remote_sha
  local_sha="$(git show "$LOCAL_HEAD:$rel" | "$HASH_CMD" | awk '{print $1}')"
  remote_sha="$("${SSH_BASE[@]}" "if command -v sha1sum >/dev/null 2>&1; then sha1sum \"$REMOTE_APP_DIR/$rel\"; else shasum \"$REMOTE_APP_DIR/$rel\"; fi | awk '{print \$1}'")"
  [[ "$local_sha" == "$remote_sha" ]] || die "hash mismatch: $rel local=$local_sha remote=$remote_sha"
}

# 1) Backup files that may be changed/deleted
for f in "${CHANGED_FILES[@]}"; do
  backup_remote_file "$f"
done
for f in "${DELETED_FILES[@]}"; do
  backup_remote_file "$f"
done

# 2) Deploy changed files from committed HEAD content
for f in "${CHANGED_FILES[@]}"; do
  msg "deploy $f"
  deploy_one_file "$f"
done

# 3) Delete removed files on server (after backup)
for f in "${DELETED_FILES[@]}"; do
  msg "delete $f"
  "${SSH_BASE[@]}" "rm -f \"$REMOTE_APP_DIR/$f\""
done

# 4) Verify hashes for deployed files
for f in "${CHANGED_FILES[@]}"; do
  verify_one_file "$f"
done

if [[ "$RESTART_SERVICE" == "1" ]]; then
  msg "restarting service: $REMOTE_SERVICE"
  "${SSH_BASE[@]}" "set -e; systemctl restart \"$REMOTE_SERVICE\"; sleep 2; systemctl is-active \"$REMOTE_SERVICE\""

  msg "health check: $REMOTE_HEALTH_URL"
  HEALTH="$("${SSH_BASE[@]}" "curl -sS --max-time 10 \"$REMOTE_HEALTH_URL\" || true")"
  if [[ "$HEALTH" != *"ok"* ]]; then
    die "health check failed: $HEALTH"
  fi
  msg "health=ok"
fi

msg "done"
msg "remote backup: $REMOTE_BACKUP_DIR"
msg "synced commits: $REMOTE_BEFORE_SHORT..$LOCAL_HEAD_SHORT"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

DEPLOY_HOST="${DEPLOY_HOST:-root@47.90.160.87}"
DEPLOY_PASS="${DEPLOY_PASS:-}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/openclaw/app}"

FILES=(
  "v49_app.py"
  "openclaw/run_daily.py"
  "openclaw/strategy_tracking.py"
  "openclaw/scripts_run_daily.sh"
  "tools/openclaw_partner_daily_run.sh"
  "openclaw/tracking_guard.py"
)

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing command: $1" >&2; exit 1; }
}

need_cmd ssh

if [[ -n "$DEPLOY_PASS" ]]; then
  need_cmd sshpass
fi

if command -v sha256sum >/dev/null 2>&1; then
  HASH_CMD="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  HASH_CMD="shasum -a 256"
else
  echo "missing command: sha256sum/shasum" >&2
  exit 1
fi

echo "[consistency] host=${DEPLOY_HOST} app=${REMOTE_APP_DIR}"
echo "[consistency] checking canonical files..."

run_ssh() {
  if [[ -n "$DEPLOY_PASS" ]]; then
    sshpass -p "$DEPLOY_PASS" ssh -o StrictHostKeyChecking=no "$DEPLOY_HOST" "$1"
  else
    ssh -o StrictHostKeyChecking=no "$DEPLOY_HOST" "$1"
  fi
}

fail=0
for f in "${FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "  - MISSING local: $f"
    fail=1
    continue
  fi
  local_sha="$($HASH_CMD "$f" | awk '{print $1}')"
  remote_sha="$(run_ssh "sha256sum \"$REMOTE_APP_DIR/$f\" 2>/dev/null | cut -d' ' -f1" || true)"
  if [[ -z "$remote_sha" ]]; then
    echo "  - MISSING remote: $f"
    fail=1
    continue
  fi
  if [[ "$local_sha" == "$remote_sha" ]]; then
    echo "  - OK $f"
  else
    echo "  - DIFF $f"
    echo "      local : $local_sha"
    echo "      remote: $remote_sha"
    fail=1
  fi
done

echo "[consistency] checking non-canonical duplicate files on remote root..."
dups="$(run_ssh "
  cd \"$REMOTE_APP_DIR\" || exit 1
  for f in strategy_tracking.py scripts_run_daily.sh openclaw_partner_daily_run.sh tracking_guard.py; do
    if [ -f \"\$f\" ]; then
      echo \"\$f\"
    fi
  done
")"
if [[ -n "${dups}" ]]; then
  echo "$dups" | sed 's/^/  - DUPLICATE remote root: /'
  fail=1
else
  echo "  - OK no root duplicates"
fi

if [[ "$fail" -ne 0 ]]; then
  echo "[consistency] FAILED"
  exit 1
fi

echo "[consistency] PASSED"

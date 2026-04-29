#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/tools/lib/remote_access.sh"

REMOTE_HOST="${REMOTE_HOST:-$AIRIVO_REMOTE_TARGET}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-$AIRIVO_REMOTE_APP_DIR}"
REMOTE_SERVICE="${REMOTE_SERVICE:-openclaw-streamlit.service}"
MANIFEST_FILE="${MANIFEST_FILE:-$ROOT_DIR/tools/production/airivo_mainline_files.txt}"

msg() { printf "[audit-airivo-mainline-drift] %s\n" "$*"; }
die() { printf "[audit-airivo-mainline-drift] ERROR: %s\n" "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

need_cmd bash
need_cmd ssh

if [[ -n "${AIRIVO_REMOTE_PASS:-}" ]]; then
  need_cmd sshpass
fi

[[ -f "$MANIFEST_FILE" ]] || die "missing manifest: $MANIFEST_FILE"

if command -v sha256sum >/dev/null 2>&1; then
  HASH_CMD="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
  HASH_CMD="shasum -a 256"
else
  die "missing hash command: sha256sum/shasum"
fi

run_remote() {
  AIRIVO_REMOTE_TARGET="$REMOTE_HOST" \
  AIRIVO_REMOTE_PASS="${AIRIVO_REMOTE_PASS:-}" \
  AIRIVO_REMOTE_KEY="${AIRIVO_REMOTE_KEY:-}" \
  airivo_remote_exec_ssh "$1" </dev/null
}

msg "host=${REMOTE_HOST} app=${REMOTE_APP_DIR}"

service_exec="$(run_remote "systemctl cat \"$REMOTE_SERVICE\" 2>/dev/null | grep -E '^ExecStart=' | tail -n 1" || true)"
if [[ "$service_exec" == *"/opt/openclaw/app/v49_app.py"* ]]; then
  msg "service entry OK: ${REMOTE_SERVICE} -> v49_app.py"
else
  echo "  - SERVICE DRIFT ${REMOTE_SERVICE}"
  echo "      ${service_exec:-missing ExecStart}"
  fail_service=1
fi

fail=0
while IFS= read -r rel || [[ -n "$rel" ]]; do
  [[ -z "$rel" ]] && continue
  [[ "${rel:0:1}" == "#" ]] && continue

  if [[ ! -f "$rel" ]]; then
    echo "  - MISSING local: $rel"
    fail=1
    continue
  fi

  local_sha="$($HASH_CMD "$rel" | awk '{print $1}')"
  remote_sha="$(run_remote "if [ -f \"$REMOTE_APP_DIR/$rel\" ]; then if command -v sha256sum >/dev/null 2>&1; then sha256sum \"$REMOTE_APP_DIR/$rel\"; else shasum -a 256 \"$REMOTE_APP_DIR/$rel\"; fi | awk '{print \$1}'; fi" || true)"
  if [[ -z "$remote_sha" ]]; then
    echo "  - MISSING remote: $rel"
    fail=1
    continue
  fi
  if [[ "$local_sha" == "$remote_sha" ]]; then
    echo "  - OK $rel"
  else
    echo "  - DIFF $rel"
    echo "      local : $local_sha"
    echo "      remote: $remote_sha"
    fail=1
  fi
done < "$MANIFEST_FILE"

dups="$(run_remote "
  cd \"$REMOTE_APP_DIR\" || exit 1
  for f in airivo_execution_center.py airivo_production_dashboard_page.py data_ops_core_page.py data_ops_update_page.py airivo_dashboard_snapshot_service.py; do
    if [ -f \"\$f\" ]; then
      echo \"\$f\"
    fi
  done
" || true)"
if [[ -n "$dups" ]]; then
  echo "$dups" | sed 's/^/  - DUPLICATE remote root: /'
  fail=1
else
  msg "no remote-root duplicates for mainline runtime files"
fi

if [[ "${fail_service:-0}" -ne 0 || "$fail" -ne 0 ]]; then
  msg "FAILED"
  exit 1
fi

msg "PASSED"

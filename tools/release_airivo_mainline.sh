#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/tools/lib/remote_access.sh"

REMOTE_NAME="${REMOTE_NAME:-origin}"
REMOTE_BRANCH="${REMOTE_BRANCH:-main}"
REMOTE_HOST="${REMOTE_HOST:-$AIRIVO_REMOTE_TARGET}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-$AIRIVO_REMOTE_APP_DIR}"
REMOTE_SERVICE="${REMOTE_SERVICE:-openclaw-streamlit.service}"
PUBLIC_URL="${PUBLIC_URL:-https://airivo.online}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8501/_stcore/health}"
RUN_GATE="${RUN_GATE:-1}"
REQUIRE_MAIN="${REQUIRE_MAIN:-1}"
RUN_MAINLINE_AUDIT="${RUN_MAINLINE_AUDIT:-1}"

msg() { printf "[release-airivo-mainline] %s\n" "$*"; }
die() { printf "[release-airivo-mainline] ERROR: %s\n" "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

need_cmd git
need_cmd bash

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git repository: $ROOT_DIR"

BRANCH="$(git branch --show-current)"
if [[ "$REQUIRE_MAIN" == "1" && "$BRANCH" != "main" ]]; then
  die "current branch is '$BRANCH' (expected main)"
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  die "working tree is dirty. commit or stash first"
fi

HEAD_SHORT="$(git rev-parse --short HEAD)"
msg "release start branch=${BRANCH} commit=${HEAD_SHORT}"

if [[ "$RUN_GATE" == "1" ]]; then
  msg "step 1/5: release gate"
  bash tools/release_gate.sh --skip-remote
fi

msg "step 2/5: push mainline to ${REMOTE_NAME}/${REMOTE_BRANCH}"
git fetch "$REMOTE_NAME" "$REMOTE_BRANCH"
REMOTE_BEFORE="$(git rev-parse "$REMOTE_NAME/$REMOTE_BRANCH")"
LOCAL_HEAD="$(git rev-parse HEAD)"
if ! git merge-base --is-ancestor "$REMOTE_BEFORE" "$LOCAL_HEAD"; then
  die "HEAD is not a fast-forward from ${REMOTE_NAME}/${REMOTE_BRANCH}"
fi
git push "$REMOTE_NAME" "$LOCAL_HEAD:$REMOTE_BRANCH"

msg "step 3/5: ensure server git worktree exists"
bash tools/bootstrap_server_git_worktree.sh

if [[ "$RUN_MAINLINE_AUDIT" == "1" ]]; then
  msg "step 4/6: audit canonical mainline drift"
  bash tools/audit_airivo_mainline_drift.sh
fi

msg "step 5/6: server fast-forward + restart ${REMOTE_SERVICE}"
AIRIVO_REMOTE_TARGET="$REMOTE_HOST" airivo_remote_exec_ssh "set -euo pipefail
  cd \"$REMOTE_APP_DIR\"
  git fetch origin \"$REMOTE_BRANCH\"
  git diff --quiet || { echo 'server_worktree_dirty'; git status --short; exit 2; }
  git merge --ff-only \"origin/$REMOTE_BRANCH\"
  systemctl restart \"$REMOTE_SERVICE\"
  systemctl is-active \"$REMOTE_SERVICE\"
"

msg "step 6/6: smoke validation (service + async backend + ui task panel)"
bash tools/smoke_airivo_main_entry.sh

msg "release success commit=${HEAD_SHORT}"

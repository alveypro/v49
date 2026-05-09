#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/tools/lib/remote_access.sh"

REMOTE_HOST="${REMOTE_HOST:-$AIRIVO_REMOTE_TARGET}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-$AIRIVO_REMOTE_APP_DIR}"
REMOTE_BRANCH="${REMOTE_BRANCH:-main}"
REMOTE_URL="${REMOTE_URL:-https://github.com/alveypro/v49}"

msg() { printf "[bootstrap-server-git] %s\n" "$*"; }
die() { printf "[bootstrap-server-git] ERROR: %s\n" "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

need_cmd bash
need_cmd git

run_remote() {
  AIRIVO_REMOTE_TARGET="$REMOTE_HOST" airivo_remote_exec_ssh "$1"
}

msg "host=${REMOTE_HOST} app=${REMOTE_APP_DIR} branch=${REMOTE_BRANCH}"

run_remote "set -euo pipefail
  command -v git >/dev/null 2>&1
  git config --global --add safe.directory \"$REMOTE_APP_DIR\" || true
  cd \"$REMOTE_APP_DIR\"

  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git init -b \"$REMOTE_BRANCH\"
  fi

  if git remote get-url origin >/dev/null 2>&1; then
    git remote set-url origin \"$REMOTE_URL\"
  else
    git remote add origin \"$REMOTE_URL\"
  fi

  git fetch origin \"$REMOTE_BRANCH\"
  git reset --mixed \"origin/$REMOTE_BRANCH\"

  mkdir -p .git/info
  cat > .git/info/exclude <<'EOF'
.env
.env.*
.venv/
.venv_broken_*/
logs/
backups/
cache_v9/
enterprise_cache/
sql_cache/
__pycache__/
*.db
*.sqlite
*.sqlite3
*.log
EOF

  echo bootstrapped
  git status --short --branch
"

msg "done"

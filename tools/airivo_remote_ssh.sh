#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT_DIR/tools/lib/remote_access.sh"

airivo_remote_require_auth

if [[ "$#" -eq 0 ]]; then
  airivo_remote_exec_ssh
  exit $?
fi

airivo_remote_exec_ssh "$@"

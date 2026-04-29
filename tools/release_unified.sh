#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

REQUIRE_MAIN="${REQUIRE_MAIN:-1}"
ALLOW_DIRTY="${ALLOW_DIRTY:-0}"

msg() { printf "[release-unified] %s\n" "$*"; }
die() { printf "[release-unified] ERROR: %s\n" "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing command: $1"
}

need_cmd git
need_cmd bash

git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "not a git repository: $ROOT_DIR"

BRANCH="$(git branch --show-current)"
if [[ "$REQUIRE_MAIN" == "1" && "$BRANCH" != "main" ]]; then
  die "current branch is '$BRANCH' (expected main). set REQUIRE_MAIN=0 to override"
fi

if [[ "$ALLOW_DIRTY" != "1" ]]; then
  if ! git diff --quiet || ! git diff --cached --quiet; then
    die "working tree is dirty. commit/stash first, or set ALLOW_DIRTY=1"
  fi
fi

HEAD_SHORT="$(git rev-parse --short HEAD)"
msg "release start: branch=${BRANCH} commit=${HEAD_SHORT}"
msg "step 1/1: canonical mainline release"
bash tools/release_airivo_mainline.sh

msg "release success: commit=${HEAD_SHORT}"

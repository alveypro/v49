#!/usr/bin/env bash
set -euo pipefail

AIRIVO_REMOTE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AIRIVO_REMOTE_ROOT="$(cd "${AIRIVO_REMOTE_LIB_DIR}/../.." && pwd)"

airivo_remote_load_env() {
  local env_file
  for env_file in \
    "${AIRIVO_REMOTE_ENV_FILE:-}" \
    "${AIRIVO_REMOTE_ROOT}/.airivo-remote.env" \
    "${AIRIVO_REMOTE_ROOT}/.env"; do
    if [[ -n "${env_file}" && -f "${env_file}" ]]; then
      # Keep local overrides, only backfill missing values from env files.
      set -a
      # shellcheck disable=SC1090
      source "${env_file}"
      set +a
      break
    fi
  done

  AIRIVO_REMOTE_HOST="${AIRIVO_REMOTE_HOST:-47.90.160.87}"
  AIRIVO_REMOTE_USER="${AIRIVO_REMOTE_USER:-root}"
  AIRIVO_REMOTE_APP_DIR="${AIRIVO_REMOTE_APP_DIR:-/opt/openclaw/app}"
  AIRIVO_REMOTE_PASS="${AIRIVO_REMOTE_PASS:-}"
  AIRIVO_REMOTE_KEY="${AIRIVO_REMOTE_KEY:-}"
  AIRIVO_REMOTE_TARGET="${AIRIVO_REMOTE_USER}@${AIRIVO_REMOTE_HOST}"
}

airivo_remote_need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[airivo-remote] missing command: $1" >&2
    exit 1
  }
}

airivo_remote_require_auth() {
  airivo_remote_need_cmd ssh
  if [[ -n "${AIRIVO_REMOTE_PASS}" ]]; then
    airivo_remote_need_cmd sshpass
  fi
}

airivo_remote_ssh_opts() {
  local opts=(
    -o ConnectTimeout=15
    -o StrictHostKeyChecking=accept-new
  )
  if [[ -n "${AIRIVO_REMOTE_KEY}" ]]; then
    opts+=(-i "${AIRIVO_REMOTE_KEY}")
  fi
  printf '%s\n' "${opts[@]}"
}

airivo_remote_exec_ssh() {
  local opts=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] && opts+=("${line}")
  done < <(airivo_remote_ssh_opts)

  if [[ -n "${AIRIVO_REMOTE_PASS}" ]]; then
    sshpass -p "${AIRIVO_REMOTE_PASS}" ssh "${opts[@]}" "${AIRIVO_REMOTE_TARGET}" "$@"
  else
    ssh "${opts[@]}" "${AIRIVO_REMOTE_TARGET}" "$@"
  fi
}

airivo_remote_exec_scp() {
  local opts=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] && opts+=("${line}")
  done < <(airivo_remote_ssh_opts)

  if [[ -n "${AIRIVO_REMOTE_PASS}" ]]; then
    sshpass -p "${AIRIVO_REMOTE_PASS}" scp "${opts[@]}" "$@"
  else
    scp "${opts[@]}" "$@"
  fi
}

airivo_remote_exec_rsync() {
  local ssh_cmd=(ssh)
  while IFS= read -r line; do
    [[ -n "${line}" ]] && ssh_cmd+=("${line}")
  done < <(airivo_remote_ssh_opts)

  if [[ -n "${AIRIVO_REMOTE_PASS}" ]]; then
    sshpass -p "${AIRIVO_REMOTE_PASS}" rsync -e "${ssh_cmd[*]}" "$@"
  else
    rsync -e "${ssh_cmd[*]}" "$@"
  fi
}

airivo_remote_load_env

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"

# Single-mainline defaults:
# - Keep OC/WAWA unified in telegram bridge
# - Disable non-stock fallback into stock endpoints
export OPENCLAW_CLOUD_BRAIN_ONLY="${OPENCLAW_CLOUD_BRAIN_ONLY:-0}"
export OPENCLAW_NON_STOCK_ALLOW_STOCK_FALLBACK="${OPENCLAW_NON_STOCK_ALLOW_STOCK_FALLBACK:-0}"
export OPENCLAW_NON_STOCK_EMERGENCY_FALLBACK="${OPENCLAW_NON_STOCK_EMERGENCY_FALLBACK:-0}"

_trim_ws() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf "%s" "$s"
}

_validate_cloud_endpoint() {
  local label="$1"
  local raw="$2"
  local u
  u="$(_trim_ws "$raw")"
  [[ -z "$u" ]] && return 0

  if [[ ! "$u" =~ ^https?:// ]]; then
    echo "[telegram-bridge] ERROR: ${label} must start with http:// or https:// -> $u"
    exit 2
  fi
  if [[ "$u" != *"/api/ai/chat"* ]]; then
    echo "[telegram-bridge] ERROR: ${label} must contain /api/ai/chat -> $u"
    exit 2
  fi
}

_is_https_ip_url() {
  local u="$1"
  [[ "$u" =~ ^https://([0-9]{1,3}\.){3}[0-9]{1,3}(/|$) ]]
}

# Validate cloud API endpoint configuration only when cloud-only mode is enabled.
_has_endpoint=0
if [[ -n "${OPENCLAW_GENERAL_URL:-}" ]]; then
  _validate_cloud_endpoint "OPENCLAW_GENERAL_URL" "${OPENCLAW_GENERAL_URL}"
  _has_endpoint=1
fi
if [[ -n "${OPENCLAW_GENERAL_URLS:-}" ]]; then
  IFS=',' read -r -a _urls <<< "${OPENCLAW_GENERAL_URLS}"
  for _u in "${_urls[@]}"; do
    _u="$(_trim_ws "${_u}")"
    [[ -z "${_u}" ]] && continue
    _validate_cloud_endpoint "OPENCLAW_GENERAL_URLS item" "${_u}"
    _has_endpoint=1
  done
fi
if [[ "${OPENCLAW_CLOUD_BRAIN_ONLY}" == "1" && "${_has_endpoint}" -eq 0 ]]; then
  echo "[telegram-bridge] ERROR: missing cloud brain endpoint."
  echo "Set OPENCLAW_GENERAL_URL or OPENCLAW_GENERAL_URLS."
  exit 2
fi

# If endpoint is https://IP/... then cert CN mismatch is common.
# Auto-enable insecure TLS unless user explicitly configured it.
if [[ -z "${OPENCLAW_TLS_INSECURE:-}" ]]; then
  if [[ -n "${OPENCLAW_GENERAL_URL:-}" ]] && _is_https_ip_url "$(_trim_ws "${OPENCLAW_GENERAL_URL}")"; then
    export OPENCLAW_TLS_INSECURE=1
  elif [[ -n "${OPENCLAW_GENERAL_URLS:-}" ]]; then
    IFS=',' read -r -a _urls_tls <<< "${OPENCLAW_GENERAL_URLS}"
    for _u in "${_urls_tls[@]}"; do
      _u="$(_trim_ws "${_u}")"
      [[ -z "${_u}" ]] && continue
      if _is_https_ip_url "${_u}"; then
        export OPENCLAW_TLS_INSECURE=1
        break
      fi
    done
  fi
fi

# Cloud stock answers can be slower; avoid false timeout fallback.
export OPENCLAW_STOCK_CLOUD_TIMEOUT_SEC="${OPENCLAW_STOCK_CLOUD_TIMEOUT_SEC:-80}"
export OPENCLAW_CLOUD_TIMEOUT_SEC="${OPENCLAW_CLOUD_TIMEOUT_SEC:-80}"
export OPENCLAW_SINGLE_REQUEST_TIMEOUT_SEC="${OPENCLAW_SINGLE_REQUEST_TIMEOUT_SEC:-90}"

exec "$PYTHON_BIN" "$ROOT_DIR/deploy_stock/telegram_bridge_bot.py"

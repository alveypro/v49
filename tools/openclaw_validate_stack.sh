#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

UID_NUM="$(id -u)"
LABEL_STOCK="com.airivo.openclaw.stock-agent"
LABEL_TELEGRAM="com.airivo.openclaw.telegram-bridge"
LABEL_V49="com.airivo.v49.streamlit"

pass() { printf "✅ %s\n" "$1"; }
warn() { printf "⚠️  %s\n" "$1"; }
fail() { printf "❌ %s\n" "$1"; }

check_launchd_label() {
  local label="$1"
  local out state pid last_exit
  out="$(launchctl print "gui/${UID_NUM}/${label}" 2>/dev/null || true)"
  if [[ -z "$out" ]]; then
    fail "launchd: ${label} not loaded"
    return 1
  fi
  state="$(printf "%s\n" "$out" | awk '/state = / {print; exit}' || true)"
  pid="$(printf "%s\n" "$out" | awk '/pid = [0-9]+/ {print; exit}' || true)"
  last_exit="$(printf "%s\n" "$out" | awk '/last exit code = [0-9]+/ {print; exit}' || true)"
  pass "launchd: ${label} loaded (${state:-state unknown}; ${pid:-pid unknown}; ${last_exit:-exit unknown})"
  return 0
}

check_http_json_health() {
  local name="$1"
  local url="$2"
  local body
  body="$(curl -sS --max-time 6 "$url" || true)"
  if [[ -z "$body" ]]; then
    fail "${name}: no response (${url})"
    return 1
  fi
  if [[ "$body" == *'"status":"ok"'* ]] || [[ "$body" == *'"status": "ok"'* ]]; then
    pass "${name}: health ok"
    return 0
  fi
  warn "${name}: responded but not explicit ok -> ${body}"
  return 0
}

mask_token() {
  local t="$1"
  if [[ -z "$t" ]]; then
    printf "<empty>"
    return 0
  fi
  local n="${#t}"
  if [[ "$n" -le 10 ]]; then
    printf "***"
    return 0
  fi
  printf "%s***%s" "${t:0:6}" "${t:n-4:4}"
}

check_telegram_token() {
  if [[ -z "${TELEGRAM_BOT_TOKEN:-}" ]]; then
    fail "TELEGRAM_BOT_TOKEN missing in .env"
    return 1
  fi
  local masked body
  masked="$(mask_token "$TELEGRAM_BOT_TOKEN")"
  body="$(curl -sS --max-time 8 "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe" || true)"
  if [[ -z "$body" ]]; then
    fail "Telegram getMe no response (token=${masked})"
    return 1
  fi
  if [[ "$body" == *'"ok":true'* ]] || [[ "$body" == *'"ok": true'* ]]; then
    pass "Telegram token valid (getMe ok, token=${masked})"
    return 0
  fi
  fail "Telegram token invalid or unauthorized (token=${masked}) -> ${body}"
  return 1
}

probe_stock_chat() {
  local resp
  resp="$(curl -sS --max-time 20 -H 'Content-Type: application/json' \
    -d '{"question":"给我一句当前系统状态总结","session_id":"validate-stack"}' \
    "http://127.0.0.1:5101/chat" || true)"
  if [[ -z "$resp" ]]; then
    fail "stock-agent /chat no response"
    return 1
  fi
  if [[ "$resp" == *'"answer":'* ]]; then
    pass "stock-agent /chat responded with answer"
    return 0
  fi
  warn "stock-agent /chat responded without answer field -> ${resp}"
  return 0
}

echo "== OpenClaw Stack Validation =="
echo "workspace: ${ROOT_DIR}"
echo

echo "[1/5] launchd labels"
check_launchd_label "$LABEL_STOCK" || true
check_launchd_label "$LABEL_TELEGRAM" || true
check_launchd_label "$LABEL_V49" || true
echo

echo "[2/5] Telegram token"
check_telegram_token || true
echo

echo "[3/5] HTTP health"
check_http_json_health "stock-agent" "http://127.0.0.1:5101/health" || true
check_http_json_health "v49 streamlit" "http://127.0.0.1:8501/_stcore/health" || true
echo

echo "[4/5] stock-agent chat probe"
probe_stock_chat || true
echo

echo "[5/5] bridge runtime hint"
echo "If Telegram token is valid but no reply in chat, send '/health' to your bot and check:"
echo "  logs/openclaw/telegram_bridge.launchd.err.log"
echo "  logs/openclaw/telegram_bridge.launchd.log"

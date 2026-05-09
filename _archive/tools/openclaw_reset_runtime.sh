#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

UID_NUM="$(id -u)"
CORE_LABELS=(
  "com.airivo.openclaw.stock-agent"
  "com.airivo.openclaw.dingtalk-bridge"
)

echo "[1/5] Restart core OpenClaw services"
for label in "${CORE_LABELS[@]}"; do
  echo "  - kickstart ${label}"
  launchctl kickstart -k "gui/${UID_NUM}/${label}" || true
done

wait_http() {
  local url="$1"
  local sec="${2:-20}"
  local i=0
  while [ "$i" -lt "$sec" ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

echo
echo "[2/5] launchd status"
for label in "${CORE_LABELS[@]}"; do
  echo "  - ${label}"
  launchctl print "gui/${UID_NUM}/${label}" 2>/dev/null | rg -n "state =|pid =|last exit code" -S || true
done

echo
echo "[3/5] HTTP health"
wait_http "http://127.0.0.1:5101/health" 25 || true
wait_http "http://127.0.0.1:8601/health" 25 || true
stock_health="$(curl -sS http://127.0.0.1:5101/health || true)"
bridge_health="$(curl -sS http://127.0.0.1:8601/health || true)"
llm_debug="$(curl -sS "http://127.0.0.1:5101/debug/llm?probe=1" || true)"
echo "  stock-agent /health: ${stock_health}"
echo "  dingtalk-bridge /health: ${bridge_health}"
echo "  stock-agent /debug/llm?probe=1: ${llm_debug}"

echo
echo "[4/5] chat probe"
python3 - <<'PY'
import json
import time
import urllib.request
import urllib.error

payload = {
    "question": "请给我今天v49优化重点三条",
    "session_id": "openclaw-reset-check",
}
req = urllib.request.Request(
    "http://127.0.0.1:5101/chat",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
)
t0 = time.time()
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    dt = round(time.time() - t0, 3)
    data = json.loads(body)
    print("  latency_sec:", dt)
    print("  mode:", data.get("mode"))
    q = data.get("quality") or {}
    print("  quality_pass:", q.get("pass"), "confidence:", q.get("confidence"))
except urllib.error.URLError as exc:
    print("  chat_probe_error:", type(exc).__name__, exc)
except Exception as exc:  # noqa: BLE001
    print("  chat_probe_error:", type(exc).__name__, exc)
PY

echo
echo "[5/5] Baseline verdict"
python3 - <<'PY'
import json
import urllib.request

def get_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.loads(r.read().decode("utf-8", errors="ignore"))

try:
    llm = get_json("http://127.0.0.1:5101/debug/llm?probe=1")
    probe = llm.get("probe") or {}
    ok = bool(probe.get("openai_ok"))
    if ok:
        print("  LLM status: OK (openai_ok=true)")
    else:
        print("  LLM status: NOT READY (openai_ok=false). Update OPENAI_API_KEY in .env.")
except Exception as exc:  # noqa: BLE001
    print("  LLM status: UNKNOWN", exc)
PY

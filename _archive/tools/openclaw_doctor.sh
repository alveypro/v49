#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "[1/4] Health checks"
stock_health="$(curl -sS http://127.0.0.1:5101/health || true)"
dingtalk_health="$(curl -sS http://127.0.0.1:8601/health || true)"
llm_debug="$(curl -sS "http://127.0.0.1:5101/debug/llm?probe=1" || true)"
echo "stock_health: ${stock_health}"
echo "dingtalk_health: ${dingtalk_health}"
echo "llm_debug: ${llm_debug}"

echo
echo "[2/4] Mode + latency (stock-agent)"
python3 - <<'PY'
import json
import time
import urllib.request

url = "http://127.0.0.1:5101/chat"
payload = json.dumps(
    {"question": "给我今天市场概览和Top3候选", "session_id": "doctor-check"}
).encode("utf-8")
req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
t = time.time()
try:
    with urllib.request.urlopen(req, timeout=35) as r:
        body = r.read().decode("utf-8", errors="ignore")
    dt = time.time() - t
    data = json.loads(body)
    print("latency_sec=", round(dt, 3))
    print("mode=", data.get("mode"))
    q = data.get("quality") or {}
    print("quality_pass=", q.get("pass"), "confidence=", q.get("confidence"))
except Exception as exc:
    print("chat_probe_error=", type(exc).__name__, str(exc))
PY

echo
echo "[3/4] launchd status"
launchctl print "gui/$(id -u)/com.airivo.openclaw.stock-agent" 2>/dev/null | rg -n "state =|pid =|last exit code" -S || true
launchctl print "gui/$(id -u)/com.airivo.openclaw.dingtalk-bridge" 2>/dev/null | rg -n "state =|pid =|last exit code" -S || true

echo
echo "[4/4] Done"
echo "Tip: if mode!=agent_llm, check .env OPENAI_* and /debug/llm?probe=1"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

RED=0
YELLOW=0
GREEN=0

say_green() { printf "GREEN  %s\n" "$1"; GREEN=$((GREEN+1)); }
say_yellow() { printf "YELLOW %s\n" "$1"; YELLOW=$((YELLOW+1)); }
say_red() { printf "RED    %s\n" "$1"; RED=$((RED+1)); }

has_pattern() {
  local pattern="$1"
  local file="$2"
  if command -v rg >/dev/null 2>&1; then
    rg -n "$pattern" "$file" >/dev/null 2>&1
  else
    grep -nE "$pattern" "$file" >/dev/null 2>&1
  fi
}

file_age_hours() {
  local path="$1"
  python3 - "$path" <<'PY'
import os, sys, time
p = sys.argv[1]
if not os.path.exists(p):
    print("-1")
    raise SystemExit(0)
age = (time.time() - os.path.getmtime(p)) / 3600.0
print(f"{age:.2f}")
PY
}

echo "== Auto Optimize Doctor =="
echo "root=${ROOT_DIR}"
echo "time=$(date '+%Y-%m-%d %H:%M:%S')"
echo

# 1) Core artifacts freshness
LAST_RUN_JSON="${ROOT_DIR}/evolution/last_run.json"
HEALTH_JSON="${ROOT_DIR}/evolution/health_report.json"

age_last_run="$(file_age_hours "$LAST_RUN_JSON")"
if [[ "$age_last_run" == "-1" ]]; then
  say_red "missing evolution/last_run.json"
else
  python3 - "$age_last_run" <<'PY'
import sys
age = float(sys.argv[1])
if age <= 30:
    print("GREEN  evolution/last_run.json fresh")
elif age <= 72:
    print("YELLOW evolution/last_run.json mildly stale")
else:
    print("RED    evolution/last_run.json stale")
PY
  case "$(python3 - "$age_last_run" <<'PY'
import sys
age = float(sys.argv[1])
print("GREEN" if age <= 30 else ("YELLOW" if age <= 72 else "RED"))
PY
)" in
    GREEN) GREEN=$((GREEN+1));;
    YELLOW) YELLOW=$((YELLOW+1));;
    RED) RED=$((RED+1));;
  esac
fi

if [[ -f "$HEALTH_JSON" ]]; then
  health_status="$(python3 - "$HEALTH_JSON" <<'PY'
import json, sys
p = sys.argv[1]
try:
    obj = json.load(open(p, "r", encoding="utf-8"))
except Exception:
    print("RED invalid health_report.json")
    raise SystemExit(0)
ok = bool(obj.get("ok", False))
warnings = obj.get("warnings") or []
if ok and len(warnings) == 0:
    print("GREEN health_report ok")
elif ok:
    print("YELLOW health_report ok with warnings")
else:
    print("RED health_report reports issues")
PY
)"
  echo "$health_status"
  case "$health_status" in
    GREEN*) GREEN=$((GREEN+1));;
    YELLOW*) YELLOW=$((YELLOW+1));;
    RED*) RED=$((RED+1));;
  esac
else
  say_yellow "missing evolution/health_report.json"
fi

# 2) Method availability and hook wiring
if has_pattern "def get_auto_tuning_recommendation" trading_assistant.py && \
   has_pattern "def apply_auto_tuning" trading_assistant.py; then
  say_green "trading_assistant auto-tuning methods present"
else
  say_red "trading_assistant missing auto-tuning methods"
fi

if has_pattern "ta\\.apply_auto_tuning\\(\\)" openclaw/run_daily.py; then
  say_green "daily pipeline auto-tuning hook present"
else
  say_red "daily pipeline missing auto-tuning hook"
fi

# 3) Learning DB sufficiency
LEARN_DB="${ROOT_DIR}/logs/openclaw/assistant_learning.db"
if [[ ! -f "$LEARN_DB" ]]; then
  say_red "missing logs/openclaw/assistant_learning.db"
else
  learn_report="$(python3 - "$LEARN_DB" <<'PY'
import sqlite3, sys, json
db = sys.argv[1]
conn = sqlite3.connect(db)
cur = conn.cursor()
def q(sql, p=()):
    try:
        return cur.execute(sql, p).fetchone()[0]
    except Exception:
        return None
total = q("SELECT COUNT(*) FROM learning_cards") or 0
recent = q("SELECT COUNT(*) FROM learning_cards WHERE created_at >= datetime('now','-30 day')") or 0
closed = q("SELECT COUNT(*) FROM learning_cards WHERE status='closed' AND created_at >= datetime('now','-30 day')") or 0
d5 = q("SELECT COUNT(*) FROM learning_cards WHERE created_at >= datetime('now','-30 day') AND outcome_json LIKE '%\"d5\"%'") or 0
d20 = q("SELECT COUNT(*) FROM learning_cards WHERE created_at >= datetime('now','-30 day') AND outcome_json LIKE '%\"d20\"%'") or 0
conn.close()
print(json.dumps({"total": total, "recent": recent, "closed": closed, "d5": d5, "d20": d20}, ensure_ascii=False))
PY
)"
  echo "learning_stats=${learn_report}"
  level="$(python3 - "$learn_report" <<'PY'
import json, sys
d=json.loads(sys.argv[1])
recent=d.get("recent",0); closed=d.get("closed",0); d5=d.get("d5",0)
if recent < 10:
    print("RED")
elif closed < 5 or d5 < 5:
    print("YELLOW")
else:
    print("GREEN")
PY
)"
  if [[ "$level" == "GREEN" ]]; then
    say_green "learning sample sufficiency looks healthy"
  elif [[ "$level" == "YELLOW" ]]; then
    say_yellow "learning samples exist but D5/D20 outcomes are still sparse"
  else
    say_red "learning samples insufficient for stable auto-tuning"
  fi
fi

# 4) Local run summary recency
latest_summary="$(ls -1t logs/openclaw/run_summary_*.json 2>/dev/null | head -n 1 || true)"
if [[ -z "${latest_summary}" ]]; then
  say_yellow "no run_summary found in logs/openclaw"
else
  age_summary="$(file_age_hours "$latest_summary")"
  summary_recent="$(python3 - "$age_summary" <<'PY'
import sys
print("1" if float(sys.argv[1]) <= 36 else "0")
PY
)"
  if [[ "$summary_recent" == "1" ]]; then
    say_green "recent run_summary present ($(basename "$latest_summary"))"
  else
    say_yellow "run_summary exists but is stale ($(basename "$latest_summary"))"
  fi
fi

# 5) Runtime status hint (best-effort, non-fatal)
if command -v launchctl >/dev/null 2>&1; then
  if launchctl print "gui/$(id -u)/com.airivo.auto-evolve" >/dev/null 2>&1; then
    say_green "launchd label com.airivo.auto-evolve is loaded"
  else
    say_yellow "launchd label com.airivo.auto-evolve not loaded on this host"
  fi
fi

echo
echo "== Summary =="
echo "GREEN=${GREEN} YELLOW=${YELLOW} RED=${RED}"

if [[ "$RED" -gt 0 ]]; then
  echo "overall=RED"
  echo "next_action: fix RED items first, then rerun: bash tools/auto_optimize_doctor.sh"
  exit 2
elif [[ "$YELLOW" -gt 0 ]]; then
  echo "overall=YELLOW"
  echo "next_action: system runs but quality is degraded; improve sample freshness/completeness"
  exit 1
else
  echo "overall=GREEN"
  echo "next_action: keep current schedule and monitor daily"
fi

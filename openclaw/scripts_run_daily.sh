#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

resolve_python_bin() {
  local c
  for c in \
    "$ROOT_DIR/.venv/bin/python" \
    "$ROOT_DIR/venv311/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "/opt/airivo/app/.venv/bin/python" \
    "${OPENCLAW_PYTHON:-}" \
    "${PYTHON_BIN:-}" \
    "$(command -v python3 2>/dev/null || true)"; do
    [[ -n "${c}" && -x "${c}" ]] && { echo "${c}"; return 0; }
  done
  echo "python3"
}

assert_python_ge_311() {
  "$1" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

PYTHON_BIN="$(resolve_python_bin)"
if ! assert_python_ge_311 "$PYTHON_BIN"; then
  echo "[openclaw] ERROR: Python>=3.11 required, got: $("$PYTHON_BIN" -V 2>&1)"
  exit 2
fi
export OPENCLAW_ROOT="$ROOT_DIR"
STRATEGY="${OPENCLAW_STRATEGY:-v5}"
SCORE_THRESHOLD="${OPENCLAW_SCORE_THRESHOLD:-}"
SAMPLE_SIZE="${OPENCLAW_SAMPLE_SIZE:-}"
HOLDING_DAYS="${OPENCLAW_HOLDING_DAYS:-}"
OFFLINE_LIMIT="${OPENCLAW_OFFLINE_STOCK_LIMIT:-200}"
# Fallback reuse is opt-in for production. Implicitly replaying old degraded params
# makes the next run hard to reason about and can hide real regressions.
USE_LAST_FALLBACK="${OPENCLAW_USE_LAST_FALLBACK:-0}"
FALLBACK_MAX_AGE_HOURS="${OPENCLAW_FALLBACK_MAX_AGE_HOURS:-12}"
ALLOW_HALTED_ON_NON_TRADING="${OPENCLAW_ALLOW_HALTED_ON_NON_TRADING:-1}"
FALLBACK_SCOPE="${OPENCLAW_FALLBACK_SCOPE:-same}"
REQUIRE_FRESH_DB="${OPENCLAW_REQUIRE_FRESH_DB:-1}"
MAX_DB_STALE_TRADE_DAYS="${OPENCLAW_MAX_DB_STALE_TRADE_DAYS:-1}"
RETRY_ON_NO_PICKS="${OPENCLAW_RETRY_ON_NO_PICKS:-1}"
NO_PICKS_RETRY_MAX="${OPENCLAW_NO_PICKS_RETRY_MAX:-2}"
RETRY_THRESHOLD_STEP="${OPENCLAW_RETRY_THRESHOLD_STEP:-5}"
RETRY_MIN_SCORE="${OPENCLAW_RETRY_MIN_SCORE:-55}"
AUTO_UPDATE_DB="${OPENCLAW_AUTO_UPDATE_DB:-1}"
TRADE_CLOSE_HOUR="${OPENCLAW_TRADE_CLOSE_HOUR:-15}"
DATA_READY_DELAY_HOURS="${OPENCLAW_DATA_READY_DELAY_HOURS:-2}"
AUTO_UPDATE_MAX_DAYS="${OPENCLAW_AUTO_UPDATE_MAX_DAYS:-15}"
BACKTEST_MODE="${OPENCLAW_BACKTEST_MODE:-rolling}"
TRAIN_WINDOW_DAYS="${OPENCLAW_TRAIN_WINDOW_DAYS:-180}"
TEST_WINDOW_DAYS="${OPENCLAW_TEST_WINDOW_DAYS:-60}"
STEP_DAYS="${OPENCLAW_STEP_DAYS:-60}"

POLICY_CONFIG="${OPENCLAW_POLICY_CONFIG:-openclaw/config/policy.yaml}"
RISK_CONFIG="${OPENCLAW_RISK_CONFIG:-openclaw/config/risk_thresholds.yaml}"
STRATEGY_CENTER_CONFIG="${OPENCLAW_STRATEGY_CENTER_CONFIG:-openclaw/config/strategy_center.yaml}"
NOTIFY_CONFIG="${OPENCLAW_NOTIFY_CONFIG:-openclaw/config/notify.yaml}"
OUTPUT_DIR="${OPENCLAW_OUTPUT_DIR:-logs/openclaw}"
APPLY_MIGRATIONS_FLAG=""
if [[ "${OPENCLAW_APPLY_MIGRATIONS:-1}" == "1" ]]; then
  APPLY_MIGRATIONS_FLAG="--apply-migrations"
fi

if [[ -z "${SCORE_THRESHOLD}" || -z "${SAMPLE_SIZE}" || -z "${HOLDING_DAYS}" ]]; then
  RESOLVED_PARAMS="$("$PYTHON_BIN" - "$STRATEGY" "$SCORE_THRESHOLD" "$SAMPLE_SIZE" "$HOLDING_DAYS" "$STRATEGY_CENTER_CONFIG" <<'PY'
import sys
from pathlib import Path

ROOT = Path(".").resolve()
sys.path.insert(0, str(ROOT))

from strategies.center_config import load_center_config, resolve_runtime_params

strategy = sys.argv[1]
raw_score = sys.argv[2].strip()
raw_sample = sys.argv[3].strip()
raw_holding = sys.argv[4].strip()
cfg_path = Path(sys.argv[5])

def to_opt_int(v):
    if not v:
        return None
    try:
        return int(v)
    except Exception:
        return None

cfg = load_center_config(cfg_path)
resolved = resolve_runtime_params(
    strategy=strategy,
    requested_score_threshold=to_opt_int(raw_score),
    requested_sample_size=to_opt_int(raw_sample),
    requested_holding_days=to_opt_int(raw_holding),
    center_config=cfg,
    project_root=ROOT,
)
print(f"{resolved['score_threshold']}|{resolved['sample_size']}|{resolved['holding_days']}")
PY
)"
  IFS='|' read -r SCORE_THRESHOLD SAMPLE_SIZE HOLDING_DAYS <<< "$RESOLVED_PARAMS"
  echo "[openclaw] resolved runtime params from strategy center: score=${SCORE_THRESHOLD}, sample=${SAMPLE_SIZE}, holding=${HOLDING_DAYS}"
fi

if [[ "$AUTO_UPDATE_DB" == "1" ]]; then
  echo "[openclaw] auto update db by trade-calendar rules..."
  if ! UPDATE_JSON="$("$PYTHON_BIN" openclaw/update_db_calendar.py \
    --close-hour "$TRADE_CLOSE_HOUR" \
    --delay-hours "$DATA_READY_DELAY_HOURS" \
    --max-backfill-days "$AUTO_UPDATE_MAX_DAYS" 2>&1)"; then
    echo "[openclaw] db auto-update failed (non-fatal): ${UPDATE_JSON}"
  else
    echo "[openclaw] db auto-update result: ${UPDATE_JSON}"
  fi
fi

if [[ "$REQUIRE_FRESH_DB" == "1" ]]; then
  DB_CHECK="$("$PYTHON_BIN" - "$MAX_DB_STALE_TRADE_DAYS" "$TRADE_CLOSE_HOUR" "$DATA_READY_DELAY_HOURS" <<'PY'
import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path

max_stale_trade_days = int(sys.argv[1])
close_hour = int(sys.argv[2])
delay_hours = int(sys.argv[3])


def parse_date(s: str):
    s = str(s)
    if "-" in s:
        return dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
    return dt.datetime.strptime(s[:8], "%Y%m%d").date()


def cn_now():
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return dt.datetime.now()


def load_token():
    env = (os.getenv("TUSHARE_TOKEN", "") or "").strip()
    if env:
        return env
    tp = Path("tushare_token.txt")
    if tp.exists():
        t = tp.read_text(encoding="utf-8").strip()
        if t:
            return t
    return None


def trade_open_dates(pro, start_date: str, end_date: str):
    df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
    if df is None or df.empty:
        return []
    vals = [str(x) for x in df["cal_date"].tolist()]
    vals.sort()
    return vals


candidates = [
    os.getenv("PERMANENT_DB_PATH", "").strip(),
    str(Path("permanent_stock_database.db").resolve()),
    str(Path("permanent_stock_database.backup.db").resolve()),
]
db = ""
for p in candidates:
    if p and Path(p).exists():
        db = p
        break
if not db:
    print("missing||")
    raise SystemExit(0)

try:
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    if "daily_trading_data" in tables:
        table = "daily_trading_data"
    elif "daily_data" in tables:
        table = "daily_data"
    else:
        print(f"missing_table|{db}|")
        raise SystemExit(0)
    cur.execute(f"SELECT MAX(trade_date) FROM {table}")
    latest = cur.fetchone()[0]
    conn.close()
except Exception:
    print(f"db_error|{db}|")
    raise SystemExit(0)

if not latest:
    print(f"empty|{db}|")
    raise SystemExit(0)

latest_s = str(latest)
try:
    db_date = parse_date(latest_s)
except Exception:
    print(f"bad_date|{db}|{latest_s}")
    raise SystemExit(0)

token = load_token()
if not token:
    # Fallback if token missing: do not block by calendar.
    print(f"ok_no_token|{db}|{latest_s}|0")
    raise SystemExit(0)

try:
    import tushare as ts
    pro = ts.pro_api(token)
    now = cn_now()
    today = now.strftime("%Y%m%d")
    today_cal = pro.trade_cal(exchange="SSE", start_date=today, end_date=today)
    is_today_open = (not today_cal.empty and str(today_cal["is_open"].iloc[0]) == "1")
    ready_time = now.replace(hour=close_hour + delay_hours, minute=0, second=0, microsecond=0)
    open_dates = trade_open_dates(pro, (now - dt.timedelta(days=35)).strftime("%Y%m%d"), today)
    if not open_dates:
        print(f"ok_no_cal|{db}|{latest_s}|0")
        raise SystemExit(0)

    expected = open_dates[-1]
    if is_today_open and now < ready_time and len(open_dates) >= 2:
        expected = open_dates[-2]

    missing_open_days = 0
    for d in open_dates:
        if d > latest_s and d <= expected:
            missing_open_days += 1

    if missing_open_days > max_stale_trade_days:
        print(f"stale_trade_days|{db}|{latest_s}|{missing_open_days}|{expected}")
    else:
        print(f"ok|{db}|{latest_s}|{missing_open_days}|{expected}")
except Exception:
    print(f"ok_no_cal|{db}|{latest_s}|0")
PY
)"

  IFS='|' read -r DB_STATUS DB_PATH DB_LATEST DB_STALE DB_EXPECTED <<< "$DB_CHECK"
  if [[ "${DB_STATUS}" != "ok" ]]; then
    if [[ "${DB_STATUS}" == "ok_no_token" || "${DB_STATUS}" == "ok_no_cal" ]]; then
      echo "[openclaw] db freshness check soft-pass (${DB_STATUS}) db=${DB_PATH} latest=${DB_LATEST}"
    else
      echo "[openclaw] skip run: db freshness gate blocked (${DB_STATUS}) db=${DB_PATH} latest=${DB_LATEST} stale_trade_days=${DB_STALE} expected=${DB_EXPECTED}"
      exit 0
    fi
  else
    echo "[openclaw] db freshness check passed: db=${DB_PATH} latest=${DB_LATEST} stale_trade_days=${DB_STALE} expected=${DB_EXPECTED}"
  fi
fi

if [[ "$USE_LAST_FALLBACK" == "1" ]]; then
  FALLBACK_LINE="$("$PYTHON_BIN" - "$OUTPUT_DIR" <<'PY'
import glob
import json
import os
import sys
import time

out_dir = sys.argv[1]
pattern = os.path.join(out_dir, "run_summary_*.json")
files = sorted(glob.glob(pattern), reverse=True)
if not files:
    print("none|||||||")
    raise SystemExit(0)

try:
    with open(files[0], "r", encoding="utf-8") as f:
        data = json.load(f)
except Exception:
    print("none|||||||")
    raise SystemExit(0)

plan = data.get("fallback_plan") or {}
mode = str(plan.get("execution_mode", "none"))
summary_strategy = str(data.get("strategy", ""))
params = plan.get("next_run_params") or {}
score = str(params.get("score_threshold", ""))
sample = str(params.get("sample_size", ""))
holding = str(params.get("holding_days", ""))
offline = str(params.get("offline_stock_limit", ""))
path = files[0]
try:
    age_hours = max(0.0, (time.time() - os.path.getmtime(path)) / 3600.0)
except Exception:
    age_hours = 999999.0
print(f"{mode}|{score}|{sample}|{holding}|{offline}|{age_hours:.2f}|{os.path.basename(path)}|{summary_strategy}")
PY
)"

  IFS='|' read -r FB_MODE FB_SCORE FB_SAMPLE FB_HOLDING FB_OFFLINE FB_AGE_HOURS FB_FILE FB_STRATEGY <<< "$FALLBACK_LINE"
  if [[ -n "${FB_AGE_HOURS:-}" ]]; then
    # shellcheck disable=SC2072
    if (( $(printf '%.0f' "${FB_AGE_HOURS}") > ${FALLBACK_MAX_AGE_HOURS} )); then
      echo "[openclaw] ignore stale fallback_plan from ${FB_FILE} (age=${FB_AGE_HOURS}h > ${FALLBACK_MAX_AGE_HOURS}h)"
      FB_MODE="none"
    fi
  fi

  if [[ "${FB_MODE}" == "halted" && "${FALLBACK_SCOPE}" == "same" ]]; then
    if [[ -n "${FB_STRATEGY}" && "${FB_STRATEGY}" != "${STRATEGY}" ]]; then
      echo "[openclaw] ignore halted fallback from strategy=${FB_STRATEGY} for current strategy=${STRATEGY}"
      FB_MODE="none"
    fi
  fi

  if [[ "${FB_MODE}" == "halted" ]]; then
    if [[ "${ALLOW_HALTED_ON_NON_TRADING}" == "1" ]]; then
      NON_TRADING="$("$PYTHON_BIN" - <<'PY'
import datetime as dt
import json
import os
from pathlib import Path

def cn_now():
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("Asia/Shanghai"))
    except Exception:
        return dt.datetime.now()

def load_token():
    env = (os.getenv("TUSHARE_TOKEN", "") or "").strip()
    if env:
        return env
    tp = Path("tushare_token.txt")
    if tp.exists():
        t = tp.read_text(encoding="utf-8").strip()
        if t:
            return t
    return ""

token = load_token()
today = cn_now().strftime("%Y%m%d")
is_open = None
if token:
    try:
        import tushare as ts
        pro = ts.pro_api(token)
        d = pro.trade_cal(exchange="SSE", start_date=today, end_date=today)
        if d is not None and not d.empty:
            is_open = str(d["is_open"].iloc[0]) == "1"
    except Exception:
        is_open = None

if is_open is None:
    # Fallback: weekend treated as non-trading.
    is_open = cn_now().weekday() < 5

print("0" if is_open else "1")
PY
)"
      if [[ "${NON_TRADING}" == "1" ]]; then
        echo "[openclaw] fallback_plan=halted but non-trading day, continue in research mode."
      else
        echo "[openclaw] fallback_plan=halted, skip this run for safety."
        exit 0
      fi
    else
      echo "[openclaw] fallback_plan=halted, skip this run for safety."
      exit 0
    fi
  fi

  if [[ "${FB_MODE}" == "degraded" ]]; then
    [[ -n "${FB_SCORE}" ]] && SCORE_THRESHOLD="${FB_SCORE}"
    [[ -n "${FB_SAMPLE}" ]] && SAMPLE_SIZE="${FB_SAMPLE}"
    [[ -n "${FB_HOLDING}" ]] && HOLDING_DAYS="${FB_HOLDING}"
    [[ -n "${FB_OFFLINE}" ]] && OFFLINE_LIMIT="${FB_OFFLINE}"
    echo "[openclaw] apply degraded fallback params: score=${SCORE_THRESHOLD}, sample=${SAMPLE_SIZE}, holding=${HOLDING_DAYS}, offline_limit=${OFFLINE_LIMIT}"
  fi
fi

PUBLISH_FLAG=""
APPROVE_FLAG=""
FORCE_PUBLISH_ON_RISK_FLAG=""
USE_DEMO_FLAG=""
ENABLE_KELLY_FLAG=""
RUN_V4_RESEARCH_FLAG=""
if [[ "${OPENCLAW_PUBLISH:-0}" == "1" ]]; then
  PUBLISH_FLAG="--publish"
fi
if [[ "${OPENCLAW_APPROVE_PUBLISH:-0}" == "1" ]]; then
  APPROVE_FLAG="--approve-publish"
fi
if [[ "${OPENCLAW_FORCE_PUBLISH_ON_RISK:-0}" == "1" ]]; then
  FORCE_PUBLISH_ON_RISK_FLAG="--force-publish-on-risk"
fi
if [[ "${OPENCLAW_USE_DEMO:-0}" == "1" ]]; then
  USE_DEMO_FLAG="--use-demo"
fi
if [[ "${OPENCLAW_ENABLE_KELLY:-1}" == "1" ]]; then
  ENABLE_KELLY_FLAG="--enable-kelly"
fi
if [[ "${OPENCLAW_RUN_V4_RESEARCH:-0}" == "1" ]]; then
  RUN_V4_RESEARCH_FLAG="--run-v4-research"
fi

mkdir -p "$OUTPUT_DIR"

ARGS=(
  openclaw/run_daily.py
  --strategy "$STRATEGY"
  --score-threshold "$SCORE_THRESHOLD"
  --sample-size "$SAMPLE_SIZE"
  --holding-days "$HOLDING_DAYS"
  --offline-stock-limit "$OFFLINE_LIMIT"
  --backtest-mode "$BACKTEST_MODE"
  --train-window-days "$TRAIN_WINDOW_DAYS"
  --test-window-days "$TEST_WINDOW_DAYS"
  --step-days "$STEP_DAYS"
  --policy-config "$POLICY_CONFIG"
  --risk-config "$RISK_CONFIG"
  --strategy-center-config "$STRATEGY_CENTER_CONFIG"
  --notify-config "$NOTIFY_CONFIG"
  --output-dir "$OUTPUT_DIR"
)

if [[ -n "$USE_DEMO_FLAG" ]]; then
  ARGS+=("$USE_DEMO_FLAG")
fi
if [[ -n "$ENABLE_KELLY_FLAG" ]]; then
  ARGS+=("$ENABLE_KELLY_FLAG")
fi
if [[ -n "$RUN_V4_RESEARCH_FLAG" ]]; then
  ARGS+=("$RUN_V4_RESEARCH_FLAG")
fi
if [[ -n "$PUBLISH_FLAG" ]]; then
  ARGS+=("$PUBLISH_FLAG")
fi
if [[ -n "$APPROVE_FLAG" ]]; then
  ARGS+=("$APPROVE_FLAG")
fi
if [[ -n "$FORCE_PUBLISH_ON_RISK_FLAG" ]]; then
  ARGS+=("$FORCE_PUBLISH_ON_RISK_FLAG")
fi
if [[ -n "$APPLY_MIGRATIONS_FLAG" ]]; then
  ARGS+=("$APPLY_MIGRATIONS_FLAG")
fi

run_once() {
  "$PYTHON_BIN" "${ARGS[@]}"
}

latest_summary_probe() {
  "$PYTHON_BIN" - "$OUTPUT_DIR" <<'PY'
import glob
import json
import os
import sys

out_dir = sys.argv[1]
files = sorted(glob.glob(os.path.join(out_dir, "run_summary_*.json")), reverse=True)
if not files:
    print("none|0|0|")
    raise SystemExit(0)
p = files[0]
try:
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
except Exception:
    print(f"bad|0|0|{p}")
    raise SystemExit(0)
scan = (data.get("scan") or {})
scan_status = str(scan.get("status") or "unknown")
picks = (((scan.get("result") or {}).get("picks")) or [])
opps = data.get("opportunities") or []
print(f"{scan_status}|{len(picks)}|{len(opps)}|{p}")
PY
}

attempt=0
while true; do
  if ! run_once; then
    exit 1
  fi

  IFS='|' read -r SCAN_STATUS PICKS_N OPPS_N SUM_PATH <<< "$(latest_summary_probe)"
  PICKS_N="${PICKS_N:-0}"
  OPPS_N="${OPPS_N:-0}"

  if [[ "$RETRY_ON_NO_PICKS" != "1" ]]; then
    break
  fi
  if [[ "$SCAN_STATUS" != "success" ]]; then
    break
  fi
  if [[ "$PICKS_N" -gt 0 || "$OPPS_N" -gt 0 ]]; then
    break
  fi
  if [[ "$attempt" -ge "$NO_PICKS_RETRY_MAX" ]]; then
    echo "[openclaw] no-picks retry reached max attempts=${NO_PICKS_RETRY_MAX}, stop."
    break
  fi

  next_score=$((SCORE_THRESHOLD - RETRY_THRESHOLD_STEP))
  if [[ "$next_score" -lt "$RETRY_MIN_SCORE" ]]; then
    next_score="$RETRY_MIN_SCORE"
  fi
  if [[ "$next_score" -ge "$SCORE_THRESHOLD" ]]; then
    echo "[openclaw] no-picks retry skipped: score cannot decrease further (score=${SCORE_THRESHOLD})."
    break
  fi

  attempt=$((attempt + 1))
  echo "[openclaw] no picks in ${SUM_PATH}; retry#${attempt} with lower threshold ${SCORE_THRESHOLD} -> ${next_score}"
  SCORE_THRESHOLD="$next_score"
  ARGS=(
    openclaw/run_daily.py
    --strategy "$STRATEGY"
    --score-threshold "$SCORE_THRESHOLD"
    --sample-size "$SAMPLE_SIZE"
    --holding-days "$HOLDING_DAYS"
    --offline-stock-limit "$OFFLINE_LIMIT"
    --backtest-mode "$BACKTEST_MODE"
    --train-window-days "$TRAIN_WINDOW_DAYS"
    --test-window-days "$TEST_WINDOW_DAYS"
    --step-days "$STEP_DAYS"
    --policy-config "$POLICY_CONFIG"
    --risk-config "$RISK_CONFIG"
    --strategy-center-config "$STRATEGY_CENTER_CONFIG"
    --notify-config "$NOTIFY_CONFIG"
    --output-dir "$OUTPUT_DIR"
  )
  if [[ -n "$USE_DEMO_FLAG" ]]; then
    ARGS+=("$USE_DEMO_FLAG")
  fi
  if [[ -n "$ENABLE_KELLY_FLAG" ]]; then
    ARGS+=("$ENABLE_KELLY_FLAG")
  fi
  if [[ -n "$RUN_V4_RESEARCH_FLAG" ]]; then
    ARGS+=("$RUN_V4_RESEARCH_FLAG")
  fi
  if [[ -n "$PUBLISH_FLAG" ]]; then
    ARGS+=("$PUBLISH_FLAG")
  fi
  if [[ -n "$APPROVE_FLAG" ]]; then
    ARGS+=("$APPROVE_FLAG")
  fi
  if [[ -n "$FORCE_PUBLISH_ON_RISK_FLAG" ]]; then
    ARGS+=("$FORCE_PUBLISH_ON_RISK_FLAG")
  fi
  if [[ -n "$APPLY_MIGRATIONS_FLAG" ]]; then
    ARGS+=("$APPLY_MIGRATIONS_FLAG")
  fi
done

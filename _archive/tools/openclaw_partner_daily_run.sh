#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUTPUT_DIR="${OPENCLAW_OUTPUT_DIR:-logs/openclaw}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_ID="partner_${RUN_TS}"
LOG_PATH="${OUTPUT_DIR}/partner_daily_${RUN_TS}.log"
EXEC_JSON="${OUTPUT_DIR}/partner_execution_${RUN_TS}.json"
EXEC_MD="${OUTPUT_DIR}/partner_execution_${RUN_TS}.md"
PYTHON_BIN="${OPENCLAW_PYTHON:-python3}"
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
STRATEGY_LIST="${OPENCLAW_STRATEGY_LIST:-v9,v8,v5,combo}"
RUN_STATUS_FILE="${OUTPUT_DIR}/partner_runs_${RUN_TS}.log"
TRACKING_STATUS_FILE="${OUTPUT_DIR}/partner_tracking_${RUN_TS}.log"
STRATEGY_TIMEOUT_SEC="${OPENCLAW_STRATEGY_TIMEOUT_SEC:-900}"
STRICT_ALL_STRATEGIES="${OPENCLAW_STRICT_ALL_STRATEGIES:-0}"
MIN_OK_STRATEGIES="${OPENCLAW_MIN_OK_STRATEGIES:-1}"
POLICY_CONFIG="${OPENCLAW_POLICY_CONFIG:-openclaw/config/policy.yaml}"
STRATEGY_CENTER_CONFIG="${OPENCLAW_STRATEGY_CENTER_CONFIG:-openclaw/config/strategy_center.yaml}"

mkdir -p "$OUTPUT_DIR"
: > "$RUN_STATUS_FILE"
: > "$TRACKING_STATUS_FILE"

resolve_allowed_strategies() {
  "$PYTHON_BIN" - "$POLICY_CONFIG" <<'PY'
import sys
from pathlib import Path

cfg = Path(sys.argv[1])
if not cfg.exists():
    print("")
    raise SystemExit(0)

items = []
txt = cfg.read_text(encoding="utf-8", errors="ignore")

# Prefer production_strategies (生产策略); fall back to allowed_strategies for compat.
target_key = "production_strategies"
fallback_key = "allowed_strategies"

try:
    import yaml  # type: ignore
    data = yaml.safe_load(txt) or {}
    vals = data.get(target_key) or data.get(fallback_key) or []
    if isinstance(vals, list):
        items = [str(x).strip() for x in vals if str(x).strip()]
except Exception:
    found_key = None
    for raw in txt.splitlines():
        line = raw.rstrip("\n")
        s = line.strip()
        if not s:
            continue
        if s.startswith(target_key + ":"):
            found_key = target_key
            continue
        if found_key is None and s.startswith(fallback_key + ":"):
            found_key = fallback_key
            continue
        if found_key:
            if s.startswith("- "):
                v = s[2:].strip()
                if v:
                    items.append(v)
                continue
            if not line.startswith(" "):
                if found_key == target_key:
                    break
                found_key = None

print(",".join(items))
PY
}

ALLOWED_BY_POLICY="$(resolve_allowed_strategies)"
if [[ -n "${ALLOWED_BY_POLICY}" ]]; then
  ORIGINAL_STRATEGY_LIST="${STRATEGY_LIST}"
  IFS=',' read -r -a _reqs <<< "${ORIGINAL_STRATEGY_LIST}"
  IFS=',' read -r -a _allows <<< "${ALLOWED_BY_POLICY}"
  FILTERED=()
  for _r in "${_reqs[@]}"; do
    r="$(echo "${_r}" | xargs)"
    [[ -z "${r}" ]] && continue
    for _a in "${_allows[@]}"; do
      a="$(echo "${_a}" | xargs)"
      [[ -z "${a}" ]] && continue
      if [[ "${r}" == "${a}" ]]; then
        FILTERED+=("${r}")
        break
      fi
    done
  done
  if [[ "${#FILTERED[@]}" -eq 0 ]]; then
    for _a in "${_allows[@]}"; do
      a="$(echo "${_a}" | xargs)"
      [[ -n "${a}" ]] && FILTERED+=("${a}")
    done
  fi
  if [[ "${#FILTERED[@]}" -gt 0 ]]; then
    STRATEGY_LIST="$(IFS=','; echo "${FILTERED[*]}")"
  fi
  if [[ "${STRATEGY_LIST}" != "${ORIGINAL_STRATEGY_LIST}" ]]; then
    echo "[openclaw] strategy list adjusted by policy: ${ORIGINAL_STRATEGY_LIST} -> ${STRATEGY_LIST}" >>"$LOG_PATH"
  fi
fi

RUN_OK=1
OK_COUNT=0
TOTAL_COUNT=0
LATEST_SUMMARY=""
TRACKING_JSON=""
IFS=',' read -r -a STRATEGIES <<< "$STRATEGY_LIST"
for _raw in "${STRATEGIES[@]}"; do
  STRATEGY="$(echo "${_raw}" | xargs)"
  [[ -z "$STRATEGY" ]] && continue
  TOTAL_COUNT=$((TOTAL_COUNT + 1))

  BEFORE_SUMMARY="$(ls -1t "${OUTPUT_DIR}"/run_summary_*.json 2>/dev/null | head -n 1 || true)"
  EXTRA_ENV=()
  STRATEGY_TIMEOUT_THIS="$STRATEGY_TIMEOUT_SEC"
  POLICY_LINE="$("$PYTHON_BIN" - "$STRATEGY" "$STRATEGY_CENTER_CONFIG" "$STRATEGY_TIMEOUT_SEC" <<'PY'
import sys
from pathlib import Path

ROOT = Path(".").resolve()
sys.path.insert(0, str(ROOT))

from strategies.center_config import load_center_config, resolve_run_policy

strategy = sys.argv[1]
cfg_path = Path(sys.argv[2])
default_timeout = int(sys.argv[3])
cfg = load_center_config(cfg_path)
policy = resolve_run_policy(strategy=strategy, center_config=cfg, default_timeout_sec=default_timeout)

def pick(k):
    v = policy.get(k)
    return "" if v is None else str(v)

print("|".join([
    pick("timeout_sec"),
    pick("offline_stock_limit"),
    pick("sample_size"),
    pick("score_threshold"),
    pick("holding_days"),
    pick("retry_on_no_picks"),
    pick("no_picks_retry_max"),
]))
PY
)"
  IFS='|' read -r P_TIMEOUT P_OFFLINE P_SAMPLE P_SCORE P_HOLDING P_RETRY P_RETRY_MAX <<< "$POLICY_LINE"
  [[ -n "$P_TIMEOUT" ]] && STRATEGY_TIMEOUT_THIS="$P_TIMEOUT"
  [[ -n "$P_OFFLINE" ]] && EXTRA_ENV+=("OPENCLAW_OFFLINE_STOCK_LIMIT=${P_OFFLINE}")
  [[ -n "$P_SAMPLE" ]] && EXTRA_ENV+=("OPENCLAW_SAMPLE_SIZE=${P_SAMPLE}")
  [[ -n "$P_SCORE" ]] && EXTRA_ENV+=("OPENCLAW_SCORE_THRESHOLD=${P_SCORE}")
  [[ -n "$P_HOLDING" ]] && EXTRA_ENV+=("OPENCLAW_HOLDING_DAYS=${P_HOLDING}")
  [[ -n "$P_RETRY" ]] && EXTRA_ENV+=("OPENCLAW_RETRY_ON_NO_PICKS=${P_RETRY}")
  [[ -n "$P_RETRY_MAX" ]] && EXTRA_ENV+=("OPENCLAW_NO_PICKS_RETRY_MAX=${P_RETRY_MAX}")
  EXTRA_ENV+=("OPENCLAW_STRATEGY_CENTER_CONFIG=${STRATEGY_CENTER_CONFIG}")

  if [[ "${#EXTRA_ENV[@]}" -gt 0 ]]; then
    CMD=(env OPENCLAW_STRATEGY="$STRATEGY" "${EXTRA_ENV[@]}" bash openclaw/scripts_run_daily.sh)
  else
    CMD=(env OPENCLAW_STRATEGY="$STRATEGY" bash openclaw/scripts_run_daily.sh)
  fi

  if command -v timeout >/dev/null 2>&1; then
    RUNNER=(timeout "${STRATEGY_TIMEOUT_THIS}" "${CMD[@]}")
  else
    RUNNER=("${CMD[@]}")
    echo "[openclaw] timeout command not found, run without timeout for strategy=${STRATEGY}" >>"$LOG_PATH"
  fi

  if "${RUNNER[@]}" >>"$LOG_PATH" 2>&1; then
    S_OK=1
    OK_COUNT=$((OK_COUNT + 1))
  else
    S_OK=0
  fi

  AFTER_SUMMARY="$(ls -1t "${OUTPUT_DIR}"/run_summary_*.json 2>/dev/null | head -n 1 || true)"
  CURRENT_SUMMARY=""
  if [[ -n "$AFTER_SUMMARY" && -f "$AFTER_SUMMARY" ]]; then
    SUMMARY_STRATEGY="$("$PYTHON_BIN" - "$AFTER_SUMMARY" <<'PY'
import json
import sys
from pathlib import Path

p = Path(sys.argv[1])
try:
    data = json.loads(p.read_text(encoding="utf-8"))
    print(str(data.get("strategy") or "").strip())
except Exception:
    print("")
PY
)"
    if [[ "${SUMMARY_STRATEGY}" == "${STRATEGY}" ]]; then
      CURRENT_SUMMARY="$AFTER_SUMMARY"
      LATEST_SUMMARY="$AFTER_SUMMARY"
    fi
  fi

  if [[ -n "${CURRENT_SUMMARY}" && -f "${CURRENT_SUMMARY}" ]]; then
    TRACKING_JSON="$("${PYTHON_BIN}" - "$CURRENT_SUMMARY" <<'PY'
import json
import sys
from pathlib import Path

p = Path(sys.argv[1])
try:
    data = json.loads(p.read_text(encoding="utf-8"))
    t = data.get("tracking")
    if t is None:
        print("")
    else:
        print(json.dumps(t, ensure_ascii=False))
except Exception:
    print("")
PY
)"
    if [[ -n "${TRACKING_JSON}" ]]; then
      echo "${STRATEGY}|${TRACKING_JSON}" >> "$TRACKING_STATUS_FILE"
    fi
  fi
  echo "${STRATEGY}|${S_OK}|${CURRENT_SUMMARY}" >> "$RUN_STATUS_FILE"
done

# 默认容错：只要有策略成功，就允许日流程成功；可用 STRICT_ALL_STRATEGIES=1 改回严格模式。
if [[ "${TOTAL_COUNT}" -le 0 ]]; then
  RUN_OK=0
elif [[ "${STRICT_ALL_STRATEGIES}" == "1" ]]; then
  [[ "${OK_COUNT}" -eq "${TOTAL_COUNT}" ]] || RUN_OK=0
else
  if [[ "${OK_COUNT}" -lt "${MIN_OK_STRATEGIES}" ]]; then
    RUN_OK=0
  fi
fi

"${PYTHON_BIN}" - "$RUN_ID" "$RUN_OK" "$LOG_PATH" "$LATEST_SUMMARY" "$EXEC_JSON" "$EXEC_MD" "$OUTPUT_DIR" "$TRACKING_JSON" "$STRATEGY_LIST" "$RUN_STATUS_FILE" "$TRACKING_STATUS_FILE" <<'PY'
import json
import pathlib
import sys
from datetime import datetime

run_id, run_ok, log_path, summary_path, exec_json, exec_md, output_dir, tracking_json, strategy_list, run_status_file, tracking_status_file = sys.argv[1:12]
run_ok = run_ok == "1"
log_file = pathlib.Path(log_path)
summary_file = pathlib.Path(summary_path) if summary_path else None

summary = {}
if summary_file and summary_file.exists():
    try:
        summary = json.loads(summary_file.read_text(encoding="utf-8"))
    except Exception:
        summary = {}

scan_status = str((summary.get("scan") or {}).get("status", "missing"))
backtest_status = str((summary.get("backtest") or {}).get("status", "missing"))
risk_level = str((summary.get("risk") or {}).get("risk_level", "unknown"))
report_path = str((summary.get("report") or {}).get("markdown", ""))
csv_paths = (summary.get("report") or {}).get("csv_paths") or []
scoreboard_md = ""
scoreboard_csv = ""
tracking = None
if tracking_json:
    try:
        tracking = json.loads(tracking_json)
        board = ((tracking.get("scoreboard") or {}) if isinstance(tracking, dict) else {})
        scoreboard_md = str(board.get("markdown") or "")
        scoreboard_csv = str(board.get("csv") or "")
    except Exception:
        tracking = {"raw": tracking_json}
out_dir = pathlib.Path(output_dir)
mds = sorted(out_dir.glob("strategy_scoreboard_*.md"), reverse=True)
csvs = sorted(out_dir.glob("strategy_scoreboard_*.csv"), reverse=True)
if (not scoreboard_md) and mds:
    scoreboard_md = str(mds[0])
if (not scoreboard_csv) and csvs:
    scoreboard_csv = str(csvs[0])

stages = [
    {"stage": "scan", "status": scan_status if scan_status else "missing"},
    {
        "stage": "merge_signals",
        "status": "success" if (summary.get("fallback_plan") is not None) else "missing",
    },
    {"stage": "backtest", "status": backtest_status if backtest_status else "missing"},
    {"stage": "risk_check", "status": "success" if risk_level != "unknown" else "missing"},
    {
        "stage": "generate_report",
        "status": "success" if report_path else "missing",
    },
]

errors = []
if not run_ok:
    errors.append("daily workflow exited with non-zero code")
for s in stages:
    if s["status"] not in {"success", "ok"}:
        errors.append(f"stage_failed:{s['stage']}:{s['status']}")

overall = "success" if not errors else "failed"

artifacts = {
    "run_summary": str(summary_file) if summary_file and summary_file.exists() else "",
    "report_markdown": report_path,
    "report_csv_paths": csv_paths,
    "runner_log": str(log_file),
    "execution_json": exec_json,
    "execution_md": exec_md,
    "strategy_scoreboard_markdown": scoreboard_md,
    "strategy_scoreboard_csv": scoreboard_csv,
}

payload = {
    "run_id": run_id,
    "status": overall,
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "stages": stages,
    "risk_level": risk_level,
    "artifacts": artifacts,
    "errors": errors,
    "strategy_list": [s.strip() for s in str(strategy_list).split(",") if s.strip()],
}
strategy_runs = []
rsf = pathlib.Path(run_status_file)
if rsf.exists():
    for line in rsf.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|", 2)
        if len(parts) < 3:
            continue
        strategy_runs.append(
            {
                "strategy": parts[0],
                "ok": parts[1] == "1",
                "run_summary": parts[2],
            }
        )
if strategy_runs:
    payload["strategy_runs"] = strategy_runs
if tracking is not None:
    payload["tracking"] = tracking

tracking_details = []
tsf = pathlib.Path(tracking_status_file)
if tsf.exists():
    for line in tsf.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        strategy, raw = line.split("|", 1)
        item = {"strategy": strategy}
        try:
            item["result"] = json.loads(raw)
        except Exception:
            item["result"] = {"raw": raw}
        tracking_details.append(item)
if tracking_details:
    payload["tracking_by_strategy"] = tracking_details

pathlib.Path(exec_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

lines = [
    f"# Partner Daily Execution ({run_id})",
    "",
    f"- status: {overall}",
    f"- risk_level: {risk_level}",
    f"- run_summary: {artifacts['run_summary'] or 'N/A'}",
    f"- report_markdown: {artifacts['report_markdown'] or 'N/A'}",
    f"- runner_log: {artifacts['runner_log']}",
    "",
    "## stages",
]
for s in stages:
    lines.append(f"- {s['stage']}: {s['status']}")
if errors:
    lines += ["", "## errors"] + [f"- {e}" for e in errors]

pathlib.Path(exec_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
print(json.dumps(payload, ensure_ascii=False))
PY

echo "run_id=${RUN_ID}"
echo "execution_json=${EXEC_JSON}"
echo "execution_md=${EXEC_MD}"
echo "runner_log=${LOG_PATH}"

if [[ "$RUN_OK" != "1" ]]; then
  exit 1
fi

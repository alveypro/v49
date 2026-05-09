#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUTPUT_DIR="${OPENCLAW_OUTPUT_DIR:-logs/openclaw}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_ID="full_auto_${RUN_TS}"
RUN_LOG="${OUTPUT_DIR}/${RUN_ID}.log"
RUN_JSON="${OUTPUT_DIR}/${RUN_ID}.json"

mkdir -p "$OUTPUT_DIR"

log() {
  echo "[$(date '+%F %T')] $*" | tee -a "$RUN_LOG"
}

status_data="failed"
status_daily="failed"
status_opt="failed"

log "run_id=${RUN_ID} stage=data_update start"
if DATA_JSON="$(python3 openclaw/update_db_calendar.py 2>&1)"; then
  status_data="success"
  log "stage=data_update success ${DATA_JSON}"
else
  log "stage=data_update failed ${DATA_JSON}"
fi

log "run_id=${RUN_ID} stage=daily_pipeline start"
if bash tools/openclaw_partner_daily_run.sh >>"$RUN_LOG" 2>&1; then
  status_daily="success"
  log "stage=daily_pipeline success"
else
  log "stage=daily_pipeline failed"
fi

log "run_id=${RUN_ID} stage=optimize start"
if AUTO_EVOLVE_PHASE=optimize_only python3 auto_evolve.py >>"$RUN_LOG" 2>&1; then
  status_opt="success"
  log "stage=optimize success"
else
  log "stage=optimize failed"
fi

overall="success"
if [[ "$status_data" != "success" || "$status_daily" != "success" || "$status_opt" != "success" ]]; then
  overall="failed"
fi

python3 - "$RUN_ID" "$overall" "$status_data" "$status_daily" "$status_opt" "$RUN_LOG" "$RUN_JSON" <<'PY'
import json
import pathlib
import sys
from datetime import datetime

run_id, overall, status_data, status_daily, status_opt, run_log, run_json = sys.argv[1:]
payload = {
    "run_id": run_id,
    "status": overall,
    "created_at": datetime.now().isoformat(timespec="seconds"),
    "stages": [
        {"stage": "data_update", "status": status_data},
        {"stage": "daily_pipeline", "status": status_daily},
        {"stage": "optimize", "status": status_opt},
    ],
    "artifacts": {
        "log": run_log,
    },
    "errors": [],
}
if overall != "success":
    for st in payload["stages"]:
        if st["status"] != "success":
            payload["errors"].append(f"stage_failed:{st['stage']}")
pathlib.Path(run_json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(payload, ensure_ascii=False))
PY

log "run_id=${RUN_ID} done status=${overall} json=${RUN_JSON}"
[[ "$overall" == "success" ]]

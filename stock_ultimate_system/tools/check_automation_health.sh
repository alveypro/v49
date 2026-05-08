#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
STATUS_PATH="${1:-$ROOT_DIR/data/experiments/update_status_latest.json}"

if [[ ! -f "$STATUS_PATH" ]]; then
  echo "health=failed"
  echo "reason=missing_status_file"
  echo "status_path=$STATUS_PATH"
  exit 1
fi

python3 - "$STATUS_PATH" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
obj = json.loads(path.read_text(encoding="utf-8"))
status = str(obj.get("status", "-"))
stage = str(obj.get("stage", "-"))
started_at = str(obj.get("started_at", "-"))
ended_at = str(obj.get("ended_at", "-"))
post_candidates = obj.get("post_candidates")
post_candidates_meta = obj.get("post_candidates_meta") or {}
candidate_ok = bool(post_candidates.get("ok")) if isinstance(post_candidates, dict) else False
candidate_elapsed = post_candidates_meta.get("elapsed_sec", "-")
candidate_mode = post_candidates_meta.get("mode", "-")
candidate_effective = post_candidates_meta.get("effective_universe_size", "-")
candidate_attempt = post_candidates_meta.get("used_attempt", "-")
health = "healthy"
reason = "automation_ok"

if status in {"failed", "partial_success"}:
    health = "failed"
    reason = f"update_status_{status}"
elif not candidate_ok and post_candidates is not None:
    health = "failed"
    reason = "post_candidates_failed"
elif post_candidates_meta.get("degraded") is True:
    health = "degraded"
    reason = "post_candidates_degraded"
elif candidate_mode == "quick":
    health = "healthy"
    reason = "quick_path_ok"

print(f"health={health}")
print(f"reason={reason}")
print(f"status={status}")
print(f"stage={stage}")
print(f"started_at={started_at}")
print(f"ended_at={ended_at}")
print(f"candidate_ok={candidate_ok}")
print(f"candidate_mode={candidate_mode}")
print(f"candidate_elapsed_sec={candidate_elapsed}")
print(f"candidate_effective_universe_size={candidate_effective}")
print(f"candidate_used_attempt={candidate_attempt}")
if isinstance(post_candidates, dict):
    print(f"candidate_detail={str(post_candidates.get('detail', '-'))[:240]}")
PY

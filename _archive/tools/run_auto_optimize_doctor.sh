#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

OUT_DIR="${ROOT_DIR}/logs/doctor"
mkdir -p "$OUT_DIR"

TS="$(date '+%Y%m%d_%H%M%S')"
LOG_PATH="${OUT_DIR}/doctor_${TS}.log"
JSON_PATH="${OUT_DIR}/doctor_${TS}.json"

set +e
bash "${ROOT_DIR}/tools/auto_optimize_doctor.sh" >"$LOG_PATH" 2>&1
RC=$?
set -e

SUMMARY_LINE="$(grep -E '^GREEN=[0-9]+ YELLOW=[0-9]+ RED=[0-9]+' "$LOG_PATH" | tail -n 1 || true)"
OVERALL_LINE="$(grep -E '^overall=' "$LOG_PATH" | tail -n 1 || true)"

GREEN_COUNT=0
YELLOW_COUNT=0
RED_COUNT=0
OVERALL="UNKNOWN"

if [[ -n "$SUMMARY_LINE" ]]; then
  GREEN_COUNT="$(echo "$SUMMARY_LINE" | sed -E 's/.*GREEN=([0-9]+).*/\1/')"
  YELLOW_COUNT="$(echo "$SUMMARY_LINE" | sed -E 's/.*YELLOW=([0-9]+).*/\1/')"
  RED_COUNT="$(echo "$SUMMARY_LINE" | sed -E 's/.*RED=([0-9]+).*/\1/')"
fi

if [[ -n "$OVERALL_LINE" ]]; then
  OVERALL="$(echo "$OVERALL_LINE" | cut -d'=' -f2)"
fi

cat >"$JSON_PATH" <<EOF
{
  "run_at": "$(date '+%Y-%m-%d %H:%M:%S')",
  "overall": "${OVERALL}",
  "exit_code": ${RC},
  "counts": {
    "green": ${GREEN_COUNT},
    "yellow": ${YELLOW_COUNT},
    "red": ${RED_COUNT}
  },
  "log_path": "${LOG_PATH}",
  "json_path": "${JSON_PATH}"
}
EOF

ln -sf "$LOG_PATH" "${OUT_DIR}/latest.log"
ln -sf "$JSON_PATH" "${OUT_DIR}/latest.json"

echo "doctor run complete: overall=${OVERALL} rc=${RC}"
echo "json=${JSON_PATH}"
echo "log=${LOG_PATH}"

exit 0


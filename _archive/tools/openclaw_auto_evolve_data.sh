#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="${PY_BIN:-/opt/openclaw/venv311/bin/python}"
LOG_DIR="${ROOT_DIR}/logs/openclaw"
MODE="${1:-retry}"

mkdir -p "${LOG_DIR}"
cd "${ROOT_DIR}"

export AUTO_EVOLVE_PHASE="data_only"
export OPENCLAW_ENABLE_ADV_FACTORS="${OPENCLAW_ENABLE_ADV_FACTORS:-1}"
export OPENCLAW_ENABLE_TUSHARE_PLUS="${OPENCLAW_ENABLE_TUSHARE_PLUS:-1}"
export OPENCLAW_FACTOR_BONUS_ENABLED="${OPENCLAW_FACTOR_BONUS_ENABLED:-1}"

if [[ "${MODE}" == "full" ]]; then
  export UPDATE_DAYS="${UPDATE_DAYS:-30}"
  export CYQ_CHIPS_SYMBOL_LIMIT="${CYQ_CHIPS_SYMBOL_LIMIT:-0}"
  export CYQ_CHIPS_SYMBOL_OFFSET="${CYQ_CHIPS_SYMBOL_OFFSET:-0}"
  OUT_LOG="${LOG_DIR}/auto_evolve_data_full.log"
else
  # Retry mode: use rotating chunks after close.
  export UPDATE_DAYS="${UPDATE_DAYS:-7}"
  export CYQ_CHIPS_SYMBOL_LIMIT="${CYQ_CHIPS_SYMBOL_LIMIT:-400}"
  HM="$(date +%H%M)"
  case "${HM}" in
    1720) SLOT=0 ;;
    1800) SLOT=1 ;;
    1840) SLOT=2 ;;
    1920) SLOT=3 ;;
    2000) SLOT=4 ;;
    *) SLOT=$(( (10#$(date +%H)) % 5 )) ;;
  esac
  export CYQ_CHIPS_SYMBOL_OFFSET="${CYQ_CHIPS_SYMBOL_OFFSET:-$(( SLOT * CYQ_CHIPS_SYMBOL_LIMIT ))}"
  OUT_LOG="${LOG_DIR}/auto_evolve_data_retry.log"
fi

echo "[$(date '+%F %T')] mode=${MODE} days=${UPDATE_DAYS} cyq_limit=${CYQ_CHIPS_SYMBOL_LIMIT} cyq_offset=${CYQ_CHIPS_SYMBOL_OFFSET}" >> "${OUT_LOG}"
"${PY_BIN}" auto_evolve.py >> "${OUT_LOG}" 2>&1

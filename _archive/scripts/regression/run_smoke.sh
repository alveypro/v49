#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT_DIR"

LEGACY_TEST_DIR="versions/archive/test_scripts"

resolve_test_script() {
  local name="$1"
  if [[ -f "$name" ]]; then
    echo "$name"
    return 0
  fi
  if [[ -f "${LEGACY_TEST_DIR}/${name}" ]]; then
    echo "${LEGACY_TEST_DIR}/${name}"
    return 0
  fi
  return 1
}

MANUAL_TEST="$(resolve_test_script manual_test_v49.py || true)"
COMPREHENSIVE_TEST="$(resolve_test_script comprehensive_test_v49.py || true)"
DATA_HISTORY_TEST="$(resolve_test_script test_data_history.py || true)"

PY_COMPILE_TARGETS=(
  openclaw/run_daily.py
  openclaw/runtime/v49_handlers.py
  scripts/safety_check_v49_openclaw.py
  data/history.py
  data/migrations/runner.py
)

if [[ -n "${COMPREHENSIVE_TEST}" ]]; then
  PY_COMPILE_TARGETS+=("${COMPREHENSIVE_TEST}")
else
  echo "[smoke] warn: comprehensive_test_v49.py not found, skip compile target"
fi
if [[ -n "${MANUAL_TEST}" ]]; then
  PY_COMPILE_TARGETS+=("${MANUAL_TEST}")
else
  echo "[smoke] warn: manual_test_v49.py not found, skip compile target"
fi
if [[ -n "${DATA_HISTORY_TEST}" ]]; then
  PY_COMPILE_TARGETS+=("${DATA_HISTORY_TEST}")
else
  echo "[smoke] warn: test_data_history.py not found, skip compile target"
fi

echo "[smoke] py_compile"
python3 -m py_compile "${PY_COMPILE_TARGETS[@]}"

echo "[smoke] migrations"
PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 -m data.migrations.runner

echo "[smoke] data.history"
if [[ -n "${DATA_HISTORY_TEST}" ]]; then
  PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 "${DATA_HISTORY_TEST}"
else
  echo "[smoke] skip: test_data_history.py not found"
fi

echo "[smoke] openclaw demo"
PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 openclaw/run_daily.py --use-demo --strategy v6 --apply-migrations --output-dir /tmp/openclaw_smoke

echo "[smoke] safety check"
PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 scripts/safety_check_v49_openclaw.py --skip-real --output-dir logs/openclaw

echo "[smoke] done"

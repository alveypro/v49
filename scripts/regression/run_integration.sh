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

echo "[integration] migrations"
PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 -m data.migrations.runner

echo "[integration] manual test"
if [[ -n "${MANUAL_TEST}" ]]; then
  PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 "${MANUAL_TEST}"
else
  echo "[integration] warn: manual_test_v49.py not found, skip"
fi

echo "[integration] comprehensive test"
if [[ -n "${COMPREHENSIVE_TEST}" ]]; then
  PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 "${COMPREHENSIVE_TEST}"
else
  echo "[integration] warn: comprehensive_test_v49.py not found, skip"
fi

echo "[integration] openclaw real-lite"
PYTHONPATH="$ROOT_DIR:${PYTHONPATH:-}" python3 openclaw/run_daily.py --strategy v6 --apply-migrations --offline-stock-limit 50 --sample-size 60 --score-threshold 70 --output-dir /tmp/openclaw_integration

echo "[integration] done"

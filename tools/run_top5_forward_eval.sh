#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

PY_BIN="${TOP5_AUDIT_PYTHON:-}"
if [[ -z "$PY_BIN" ]]; then
  for candidate in \
    "$ROOT_DIR/.venv/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "$(command -v python3 2>/dev/null || true)" \
    "/usr/bin/python3"; do
    if [[ -n "$candidate" && -x "$candidate" ]]; then
      PY_BIN="$candidate"
      break
    fi
  done
fi

if [[ -z "$PY_BIN" || ! -x "$PY_BIN" ]]; then
  echo "[top5-forward-eval] ERROR: no available python interpreter" >&2
  exit 1
fi

OUTPUT_DIR="${STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR:-$ROOT_DIR/logs/openclaw/strategy_competition_audit}"
EXPORT_DIR="${TOP5_FORWARD_EVAL_EXPORT_DIR:-$ROOT_DIR/exports}"
HORIZONS="${TOP5_FORWARD_EVAL_HORIZONS:-5,20,60}"
COST_BPS="${TOP5_FORWARD_EVAL_ROUNDTRIP_COST_BPS:-0}"

mkdir -p "$EXPORT_DIR"

OUT_JSON="$EXPORT_DIR/top5_forward_eval_latest.json"

EXTRA_ARGS=()
if [[ "${TOP5_FORWARD_EVAL_FAIL_ON_EVAL_BLOCKING:-0}" == "1" ]]; then EXTRA_ARGS+=(--fail-on-eval-blocking); fi
if [[ "${TOP5_FORWARD_EVAL_FAIL_ON_INFERRED_AS_OF:-0}" == "1" ]]; then EXTRA_ARGS+=(--fail-on-inferred-as-of); fi
if [[ -n "${TOP5_FORWARD_EVAL_MIN_AVAILABLE_SYMBOLS_PER_HORIZON:-}" ]]; then
  EXTRA_ARGS+=(--min-available-symbols-per-horizon "$TOP5_FORWARD_EVAL_MIN_AVAILABLE_SYMBOLS_PER_HORIZON")
fi
if [[ -n "${TOP5_FORWARD_EVAL_MIN_AVAILABLE_RATIO_PER_HORIZON:-}" ]]; then
  EXTRA_ARGS+=(--min-available-ratio-per-horizon "$TOP5_FORWARD_EVAL_MIN_AVAILABLE_RATIO_PER_HORIZON")
fi

set +e
"$PY_BIN" "$ROOT_DIR/tools/evaluate_top5_forward_returns.py" \
  --latest-artifact-dir "$OUTPUT_DIR" \
  --horizons "$HORIZONS" \
  --subtract-roundtrip-cost-bps "$COST_BPS" \
  --output-json "$OUT_JSON" \
  "${EXTRA_ARGS[@]}"
rc=$?
set -e

echo "[top5-forward-eval] wrote $OUT_JSON rc=$rc"
exit "$rc"

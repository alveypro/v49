#!/usr/bin/env bash
# Run Top5 competition audit, aggregate evidence gate, then rebuild the trader-facing brief.
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
  echo "[top5-audit-chain] ERROR: no available python interpreter" >&2
  exit 1
fi

OUTPUT_DIR="${STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR:-$ROOT_DIR/logs/openclaw/strategy_competition_audit}"
SHADOW_EVIDENCE_DIR="${STRATEGY_COMPETITION_SHADOW_EVIDENCE_DIR:-$ROOT_DIR/logs/openclaw/strategy_competition_shadow_execution_evidence}"
AUDIT_MODE="${TOP5_AUDIT_MODE:-relaxed}"
if [[ "$AUDIT_MODE" != "strict" && "$AUDIT_MODE" != "relaxed" ]]; then
  AUDIT_MODE="relaxed"
fi
AUTO_SHADOW_INPUT="${TOP5_AUDIT_AUTO_SHADOW_INPUT:-}"
if [[ -z "$AUTO_SHADOW_INPUT" ]]; then
  if [[ "$AUDIT_MODE" == "strict" ]]; then
    AUTO_SHADOW_INPUT="0"
  else
    AUTO_SHADOW_INPUT="1"
  fi
fi

mkdir -p "$OUTPUT_DIR"
mkdir -p "$ROOT_DIR/logs/openclaw"
LOG_FILE="${TOP5_AUDIT_LOG_FILE:-$ROOT_DIR/logs/openclaw/top5_competition_audit_runner.log}"

AUDIT_CMD=(
  "$PY_BIN" "$ROOT_DIR/tools/build_current_strategy_competition_audit.py"
  --derive-pre-trade-risk-controls
  --audit-mode "$AUDIT_MODE"
  --output-dir "$OUTPUT_DIR"
)

LATEST_SHADOW_EVIDENCE=""
if [[ "$AUTO_SHADOW_INPUT" == "1" ]]; then
  LATEST_SHADOW_EVIDENCE="$(SHADOW_EVIDENCE_DIR="$SHADOW_EVIDENCE_DIR" "$PY_BIN" - <<'PY'
import os
from pathlib import Path

path = Path(os.environ.get("SHADOW_EVIDENCE_DIR", ""))
if path.exists():
    files = sorted(path.glob("strategy_competition_shadow_execution_evidence_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if files:
        print(str(files[0]))
PY
  )"
  if [[ -n "$LATEST_SHADOW_EVIDENCE" && -f "$LATEST_SHADOW_EVIDENCE" ]]; then
    AUDIT_CMD+=(--shadow-execution "$LATEST_SHADOW_EVIDENCE")
  fi
fi

{
  echo "[$(date '+%F %T')] [top5-audit] start"
  echo "[$(date '+%F %T')] [top5-audit] mode=$AUDIT_MODE auto_shadow_input=$AUTO_SHADOW_INPUT shadow_input=${LATEST_SHADOW_EVIDENCE:-none}"
  echo "[$(date '+%F %T')] [top5-audit] command: ${AUDIT_CMD[*]}"
} >>"$LOG_FILE"

set +e
"${AUDIT_CMD[@]}" >>"$LOG_FILE" 2>&1
audit_rc=$?
set -e

# 0=passed, 2=blocked with valid evidence artifact. Both allow aggregate gate review.
if [[ "$audit_rc" -ne 0 && "$audit_rc" -ne 2 ]]; then
  echo "[$(date '+%F %T')] [top5-audit] failed rc=$audit_rc" >>"$LOG_FILE"
  exit "$audit_rc"
fi
echo "[$(date '+%F %T')] [top5-audit] done rc=$audit_rc (accepted)" >>"$LOG_FILE"

"$PY_BIN" "$ROOT_DIR/tools/top5_audit_evidence_gate.py" "$@"
exec "$PY_BIN" "$ROOT_DIR/tools/rebuild_top5_trader_brief_exports.py"

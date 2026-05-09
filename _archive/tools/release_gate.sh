#!/usr/bin/env bash
set -euo pipefail
#
# 发布门槛验证: 三关全过方可部署
#   关1: 本地=GitHub=服务器 Git 哈希一致
#   关2: 默认组合连续多轮 scan 成功
#   关3: 全链路健康检查绿灯 + 关键指标有日志与阈值告警
#
# 用法:
#   bash tools/release_gate.sh                  # 默认 3 轮
#   bash tools/release_gate.sh --rounds 5       # 5 轮
#   bash tools/release_gate.sh --skip-remote    # 跳过远端哈希比对
#

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# shellcheck disable=SC1091
source "$ROOT_DIR/tools/lib/remote_access.sh"

ROUNDS=3
SKIP_REMOTE=false
REMOTE_HOST="${REMOTE_HOST:-$AIRIVO_REMOTE_TARGET}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-$AIRIVO_REMOTE_APP_DIR}"
SSH_KEY="${SSH_KEY:-$AIRIVO_REMOTE_KEY}"
SSH_PASS="${SSH_PASS:-$AIRIVO_REMOTE_PASS}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rounds) ROUNDS="$2"; shift 2 ;;
    --skip-remote) SKIP_REMOTE=true; shift ;;
    *) shift ;;
  esac
done

PASS=true
RELEASE_FACT_GATE_STATUS="skipped"
GIT_GATE_STATUS="failed"
REGRESSION_GATE_STATUS="failed"
HEALTH_GATE_STATUS="failed"
PROFESSIONAL_AUDIT_GATE_STATUS="failed"
LOG_FILE="$ROOT_DIR/logs/release_gate_$(date +%Y%m%d_%H%M%S).log"
RELEASE_READINESS_PAYLOAD_FILE="${LOG_FILE%.log}.release_readiness.json"
PROFESSIONAL_AUDIT_SUMMARY_FILE="${LOG_FILE%.log}.professional_audit.json"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"; }

resolve_python_bin() {
  local c
  for c in \
    "${PY_BIN:-}" \
    "$ROOT_DIR/.venv/bin/python" \
    "$ROOT_DIR/venv311/bin/python" \
    "/opt/openclaw/venv311/bin/python" \
    "/opt/airivo/app/.venv/bin/python" \
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

log "=========================================="
log "  发布门槛验证  rounds=$ROUNDS"
log "=========================================="

PY_BIN="$(resolve_python_bin)"
if ! assert_python_ge_311 "$PY_BIN"; then
  log "  ❌ Python 版本过低: $("$PY_BIN" -V 2>&1) (要求 >=3.11)"
  exit 2
fi

# ── 关0: 显式发布事实链 readiness 硬门 ───────────────────
log ""
log "▶ 关0: 发布事实链 readiness 硬门"

if [[ "${AIRIVO_ENABLE_RELEASE_FACT_GATE:-0}" == "1" ]]; then
  if [[ -z "${AIRIVO_RELEASE_DB_PATH:-}" ]]; then
    log "  ❌ 关0 失败: AIRIVO_RELEASE_DB_PATH 未设置"
    exit 2
  fi
  if "$PY_BIN" "$ROOT_DIR/tools/release_dry_run_audit.py" \
    --db "$AIRIVO_RELEASE_DB_PATH" \
    --code-root "$ROOT_DIR" \
    --output "$RELEASE_READINESS_PAYLOAD_FILE" \
    --operator release_gate 2>&1 | tee -a "$LOG_FILE"; then
    log "  ✅ 关0 通过: release readiness payload 允许进入真实发布门"
    RELEASE_FACT_GATE_STATUS="passed"
  else
    log "  ❌ 关0 失败: release readiness payload 阻断真实发布门"
    log "  payload: $RELEASE_READINESS_PAYLOAD_FILE"
    exit 1
  fi
else
  log "  ⏭️  关0 跳过: AIRIVO_ENABLE_RELEASE_FACT_GATE 未设置为 1"
fi

# ── 关1: 哈希一致性 ──────────────────────────────────────
log ""
log "▶ 关1: Git 哈希一致性"

LOCAL_HASH="$(git -C "$ROOT_DIR" rev-parse HEAD)"
LOCAL_SHORT="${LOCAL_HASH:0:12}"
log "  本地: $LOCAL_SHORT"

GITHUB_HASH="$(git -C "$ROOT_DIR" ls-remote origin HEAD 2>/dev/null | awk '{print $1}')" || true
GITHUB_SHORT="${GITHUB_HASH:0:12}"
if [[ -z "$GITHUB_SHORT" ]]; then
  log "  ⚠️  无法获取 GitHub 哈希 (网络?)"
  GITHUB_SHORT="(未知)"
fi
log "  GitHub: $GITHUB_SHORT"

if [[ "$SKIP_REMOTE" == "true" ]]; then
  log "  服务器: 跳过 (--skip-remote)"
  SERVER_SHORT="(跳过)"
elif [[ "${AIRIVO_RELEASE_REMOTE_MODE:-ssh}" == "local" ]]; then
  SERVER_HASH="$(git -C "$REMOTE_APP_DIR" rev-parse HEAD 2>/dev/null)" || true
  SERVER_SHORT="${SERVER_HASH:0:12}"
  if [[ -z "$SERVER_SHORT" ]]; then
    log "  ⚠️  无法获取本机部署目录哈希: $REMOTE_APP_DIR"
    SERVER_SHORT="(未知)"
  fi
  log "  服务器: $SERVER_SHORT (local:$REMOTE_APP_DIR)"
else
  _run_ssh() {
    AIRIVO_REMOTE_TARGET="$REMOTE_HOST" \
    AIRIVO_REMOTE_PASS="$SSH_PASS" \
    AIRIVO_REMOTE_KEY="$SSH_KEY" \
    airivo_remote_exec_ssh "$1"
  }

  SERVER_HASH="$(_run_ssh "cd '$REMOTE_APP_DIR' && git rev-parse HEAD 2>/dev/null" 2>/dev/null)" || true
  SERVER_SHORT="${SERVER_HASH:0:12}"
  if [[ -z "$SERVER_SHORT" ]]; then
    log "  ⚠️  无法获取服务器哈希"
    SERVER_SHORT="(未知)"
  fi
  log "  服务器: $SERVER_SHORT"
fi

if [[ "$LOCAL_SHORT" == "$GITHUB_SHORT" ]] && { [[ "$SKIP_REMOTE" == "true" ]] || [[ "$LOCAL_SHORT" == "$SERVER_SHORT" ]]; }; then
  log "  ✅ 关1 通过: 哈希一致"
  GIT_GATE_STATUS="passed"
else
  log "  ❌ 关1 失败: 哈希不一致"
  PASS=false
fi

# ── 关2: 默认组合多轮成功 ────────────────────────────────
log ""
log "▶ 关2: 默认组合回归验证 ($ROUNDS 轮)"

if "$PY_BIN" "$ROOT_DIR/tools/regression_combo_gate.py" --rounds "$ROUNDS" 2>&1 | tee -a "$LOG_FILE"; then
  log "  ✅ 关2 通过: $ROUNDS 轮全部成功"
  REGRESSION_GATE_STATUS="passed"
else
  log "  ❌ 关2 失败: 回归验证未通过"
  PASS=false
fi

# ── 关3: 全链路健康检查 ──────────────────────────────────
log ""
log "▶ 关3: 全链路健康检查"

if "$PY_BIN" "$ROOT_DIR/tools/openclaw_health_gate.py" 2>&1 | tee -a "$LOG_FILE"; then
  log "  ✅ 关3 通过: 健康检查绿灯"
  HEALTH_GATE_STATUS="passed"
else
  EXIT_CODE=$?
  if [[ "$EXIT_CODE" -eq 1 ]]; then
    log "  ⚠️  关3 警告: 健康检查有黄灯 (可选通过)"
    HEALTH_GATE_STATUS="warning"
  else
    log "  ❌ 关3 失败: 健康检查有红灯"
    PASS=false
  fi
fi

# ── 关4: 专业事实链审计 ──────────────────────────────────
log ""
log "▶ 关4: 专业事实链审计"

if RELEASE_GATE_PROFESSIONAL_AUDIT_SUMMARY_FILE="$PROFESSIONAL_AUDIT_SUMMARY_FILE" "$PY_BIN" -c '
import json
import os
import sys
from pathlib import Path

root = Path.cwd()
sys.path.insert(0, str(root))

from data.dao import resolve_db_path
from openclaw.services.release_gate_service import run_professional_fact_audit_gate

db = resolve_db_path(None)
summary = run_professional_fact_audit_gate(
    db,
    output_path=os.environ.get("RELEASE_GATE_PROFESSIONAL_AUDIT_SUMMARY_FILE", ""),
)
print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
raise SystemExit(0 if summary["passed"] else 1)
' 2>&1 | tee -a "$LOG_FILE"; then
  log "  ✅ 关4 通过: 四条事实链可审计"
  PROFESSIONAL_AUDIT_GATE_STATUS="passed"
else
  log "  ❌ 关4 失败: 四条事实链审计未通过"
  PASS=false
fi

record_release_ledger() {
  local overall="$1"
  RELEASE_GATE_LOG_FILE="$LOG_FILE" \
  RELEASE_GATE_OVERALL="$overall" \
  RELEASE_GATE_ROUNDS="$ROUNDS" \
  RELEASE_GATE_SKIP_REMOTE="$SKIP_REMOTE" \
  RELEASE_GATE_FACT_STATUS="$RELEASE_FACT_GATE_STATUS" \
  RELEASE_GATE_GIT_STATUS="$GIT_GATE_STATUS" \
  RELEASE_GATE_REGRESSION_STATUS="$REGRESSION_GATE_STATUS" \
  RELEASE_GATE_HEALTH_STATUS="$HEALTH_GATE_STATUS" \
  RELEASE_GATE_PROFESSIONAL_AUDIT_STATUS="$PROFESSIONAL_AUDIT_GATE_STATUS" \
  RELEASE_GATE_PROFESSIONAL_AUDIT_SUMMARY_FILE="$PROFESSIONAL_AUDIT_SUMMARY_FILE" \
  "$PY_BIN" -c '
import os
import sys
from pathlib import Path

root = Path.cwd()
sys.path.insert(0, str(root))

from data.dao import resolve_db_path
from openclaw.services.release_gate_service import load_release_gate_audit_summary, record_release_gate_ledger

db = resolve_db_path(None)
overall = os.environ.get("RELEASE_GATE_OVERALL", "failed")
log_file = os.environ.get("RELEASE_GATE_LOG_FILE", "")
audit_summary = load_release_gate_audit_summary(os.environ.get("RELEASE_GATE_PROFESSIONAL_AUDIT_SUMMARY_FILE", ""))
validations = {
        "release_fact_readiness": os.environ.get("RELEASE_GATE_FACT_STATUS", "skipped"),
        "git_hash": os.environ.get("RELEASE_GATE_GIT_STATUS", "failed"),
        "regression_combo": os.environ.get("RELEASE_GATE_REGRESSION_STATUS", "failed"),
        "health_gate": os.environ.get("RELEASE_GATE_HEALTH_STATUS", "failed"),
        "professional_fact_audit": os.environ.get("RELEASE_GATE_PROFESSIONAL_AUDIT_STATUS", "failed"),
}
release_id = record_release_gate_ledger(
    db_path=db,
    code_root=root,
    overall=overall,
    rounds=int(os.environ.get("RELEASE_GATE_ROUNDS", "0") or 0),
    skip_remote=os.environ.get("RELEASE_GATE_SKIP_REMOTE", "") == "true",
    log_file=log_file,
    validation_statuses=validations,
    audit_summary=audit_summary,
    operator_name=os.environ.get("USER", "system"),
)
print(f"[release-gate-ledger] release_id={release_id} overall={overall}")
' 2>&1 | tee -a "$LOG_FILE" || log "  ⚠️  release ledger write failed"
}

# ── 总结 ─────────────────────────────────────────────────
log ""
log "=========================================="
if [[ "$PASS" == "true" ]]; then
  record_release_ledger "passed"
  log "  ✅ 全部门槛通过 — 可发布部署"
  log "=========================================="
  log "  日志: $LOG_FILE"
  exit 0
else
  record_release_ledger "failed"
  log "  ❌ 发布门槛未满足 — 禁止部署"
  log "=========================================="
  log "  日志: $LOG_FILE"
  exit 1
fi

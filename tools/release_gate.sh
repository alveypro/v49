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

ROUNDS=3
SKIP_REMOTE=false
REMOTE_HOST="${REMOTE_HOST:-root@47.90.160.87}"
REMOTE_APP_DIR="${REMOTE_APP_DIR:-/opt/openclaw/app}"
SSH_KEY="${SSH_KEY:-}"
SSH_PASS="${SSH_PASS:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rounds) ROUNDS="$2"; shift 2 ;;
    --skip-remote) SKIP_REMOTE=true; shift ;;
    *) shift ;;
  esac
done

PASS=true
LOG_FILE="$ROOT_DIR/logs/release_gate_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$(dirname "$LOG_FILE")"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"; }

log "=========================================="
log "  发布门槛验证  rounds=$ROUNDS"
log "=========================================="

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
else
  _ssh_opts="-o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new"
  [[ -n "$SSH_KEY" ]] && _ssh_opts="$_ssh_opts -i $SSH_KEY"

  _run_ssh() {
    if [[ -n "$SSH_PASS" ]]; then
      sshpass -p "$SSH_PASS" ssh $_ssh_opts "$REMOTE_HOST" "$1"
    else
      ssh $_ssh_opts "$REMOTE_HOST" "$1"
    fi
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
else
  log "  ❌ 关1 失败: 哈希不一致"
  PASS=false
fi

# ── 关2: 默认组合多轮成功 ────────────────────────────────
log ""
log "▶ 关2: 默认组合回归验证 ($ROUNDS 轮)"

PY_BIN="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3 2>/dev/null || echo python3)"
fi

if "$PY_BIN" "$ROOT_DIR/tools/regression_combo_gate.py" --rounds "$ROUNDS" 2>&1 | tee -a "$LOG_FILE"; then
  log "  ✅ 关2 通过: $ROUNDS 轮全部成功"
else
  log "  ❌ 关2 失败: 回归验证未通过"
  PASS=false
fi

# ── 关3: 全链路健康检查 ──────────────────────────────────
log ""
log "▶ 关3: 全链路健康检查"

if "$PY_BIN" "$ROOT_DIR/tools/openclaw_health_gate.py" 2>&1 | tee -a "$LOG_FILE"; then
  log "  ✅ 关3 通过: 健康检查绿灯"
else
  EXIT_CODE=$?
  if [[ "$EXIT_CODE" -eq 1 ]]; then
    log "  ⚠️  关3 警告: 健康检查有黄灯 (可选通过)"
  else
    log "  ❌ 关3 失败: 健康检查有红灯"
    PASS=false
  fi
fi

# ── 总结 ─────────────────────────────────────────────────
log ""
log "=========================================="
if [[ "$PASS" == "true" ]]; then
  log "  ✅ 全部门槛通过 — 可发布部署"
  log "=========================================="
  log "  日志: $LOG_FILE"
  exit 0
else
  log "  ❌ 发布门槛未满足 — 禁止部署"
  log "=========================================="
  log "  日志: $LOG_FILE"
  exit 1
fi

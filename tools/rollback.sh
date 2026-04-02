#!/usr/bin/env bash
set -euo pipefail
#
# 可回滚部署: 基于 Git tag 的快速回退
#
# 用法:
#   bash tools/rollback.sh                      # 显示最近 10 个可回滚版本
#   bash tools/rollback.sh --to <tag|commit>    # 回滚到指定版本
#   bash tools/rollback.sh --create-snapshot    # 创建当前快照 tag
#
# 回滚步骤:
#   1) 停止所有 launchd 任务
#   2) git checkout 到目标版本
#   3) 重新加载 launchd 任务
#   4) 运行健康检查
#

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

ACTION="list"
TARGET=""
LABELS=(
  "com.airivo.v49.streamlit"
  "com.airivo.openclaw.stock-agent"
  "com.airivo.openclaw.telegram-bridge"
)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --to) ACTION="rollback"; TARGET="$2"; shift 2 ;;
    --create-snapshot) ACTION="snapshot"; shift ;;
    --list) ACTION="list"; shift ;;
    *) shift ;;
  esac
done

uid="$(id -u)"

stop_services() {
  echo "⏸  停止服务..."
  for label in "${LABELS[@]}"; do
    launchctl bootout "gui/$uid/$label" 2>/dev/null || true
  done
  sleep 2
}

start_services() {
  echo "▶  重启服务..."
  for plist in "$ROOT_DIR"/launchd/*.plist; do
    launchctl bootstrap "gui/$uid" "$plist" 2>/dev/null || true
  done
}

run_health() {
  echo ""
  echo "🏥 运行健康检查..."
  resolve_python_bin() {
    local c
    for c in \
      "$ROOT_DIR/.venv/bin/python" \
      "$ROOT_DIR/venv311/bin/python" \
      "/opt/openclaw/venv311/bin/python" \
      "/opt/airivo/app/.venv/bin/python" \
      "$(command -v python3 2>/dev/null || true)"; do
      [[ -n "${c}" && -x "${c}" ]] && { echo "${c}"; return 0; }
    done
    echo "python3"
  }
  PY_BIN="$(resolve_python_bin)"
  if ! "$PY_BIN" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
  then
    echo "❌ Python 版本过低: $("$PY_BIN" -V 2>&1) (要求 >=3.11)"
    return 2
  fi
  "$PY_BIN" "$ROOT_DIR/tools/openclaw_health_gate.py" || true
}

case "$ACTION" in
  list)
    echo "📋 最近可回滚版本 (tags + 最近10次提交):"
    echo ""
    echo "── Tags ──"
    git tag -l 'snapshot-*' --sort=-creatordate | head -10
    echo ""
    echo "── 最近提交 ──"
    git log --oneline -10
    echo ""
    echo "用法: bash tools/rollback.sh --to <tag|commit>"
    ;;

  snapshot)
    TAG="snapshot-$(date +%Y%m%d-%H%M%S)"
    git tag "$TAG"
    echo "✅ 已创建快照: $TAG"
    echo "   当前哈希: $(git rev-parse --short HEAD)"
    echo ""
    echo "回滚命令: bash tools/rollback.sh --to $TAG"
    ;;

  rollback)
    if [[ -z "$TARGET" ]]; then
      echo "❌ 请指定目标: --to <tag|commit>"
      exit 1
    fi

    CURRENT="$(git rev-parse --short HEAD)"
    echo "📍 当前版本: $CURRENT"
    echo "🎯 目标版本: $TARGET"
    echo ""

    if ! git rev-parse "$TARGET" >/dev/null 2>&1; then
      echo "❌ 目标 '$TARGET' 不存在"
      exit 1
    fi

    TARGET_HASH="$(git rev-parse --short "$TARGET")"
    if [[ "$CURRENT" == "$TARGET_HASH" ]]; then
      echo "⚠️  已在目标版本, 无需回滚"
      exit 0
    fi

    BACKUP_TAG="pre-rollback-$(date +%Y%m%d-%H%M%S)"
    git tag "$BACKUP_TAG"
    echo "💾 已保存回滚前快照: $BACKUP_TAG"

    stop_services

    echo "🔄 切换到 $TARGET..."
    git checkout "$TARGET" -- .
    echo "✅ 文件已回滚到 $TARGET"

    start_services
    run_health

    echo ""
    echo "✅ 回滚完成: $CURRENT → $TARGET_HASH"
    echo "   恢复命令: bash tools/rollback.sh --to $BACKUP_TAG"
    ;;
esac

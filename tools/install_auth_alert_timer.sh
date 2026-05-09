#!/usr/bin/env bash
set -euo pipefail

DEPLOY_HOST="${DEPLOY_HOST:-47.90.160.87}"
DEPLOY_USER="${DEPLOY_USER:-root}"
DEPLOY_PASS="${DEPLOY_PASS:-}"
RELEASE_DIR="${RELEASE_DIR:-/opt/openclaw/releases/airivo-v49-5135946}"
CURRENT_DIR="${CURRENT_DIR:-/opt/openclaw/current}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
VENV_PYTHON="${VENV_PYTHON:-/opt/openclaw/venv311/bin/python}"
LOG_DIR="${LOG_DIR:-/var/log/openclaw}"
LOCAL_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ -z "$DEPLOY_PASS" ]; then
  echo "[auth-alert-install] ERROR: 请通过环境变量 DEPLOY_PASS 提供服务器密码"
  exit 1
fi

FILES=(
  "tools/auth_decision_alert.py"
  "deploy/airivo-auth-decision-alert.service"
  "deploy/airivo-auth-decision-alert.timer"
)

do_ssh() {
  sshpass -p "$DEPLOY_PASS" ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new "$DEPLOY_USER@$DEPLOY_HOST" "$@"
}

do_scp() {
  sshpass -p "$DEPLOY_PASS" scp -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new "$@"
}

echo "[auth-alert-install] 预检查本地文件..."
for f in "${FILES[@]}"; do
  if [ ! -f "$LOCAL_DIR/$f" ]; then
    echo "[auth-alert-install] ERROR: 缺少本地文件 $f"
    exit 1
  fi
done

echo "[auth-alert-install] 同步告警脚本到 release 目录..."
do_ssh "mkdir -p \"$RELEASE_DIR/tools\""
do_scp "$LOCAL_DIR/tools/auth_decision_alert.py" "$DEPLOY_USER@$DEPLOY_HOST:$RELEASE_DIR/tools/auth_decision_alert.py"

echo "[auth-alert-install] 同步 systemd 单元文件..."
do_scp "$LOCAL_DIR/deploy/airivo-auth-decision-alert.service" "$DEPLOY_USER@$DEPLOY_HOST:$SYSTEMD_DIR/airivo-auth-decision-alert.service"
do_scp "$LOCAL_DIR/deploy/airivo-auth-decision-alert.timer" "$DEPLOY_USER@$DEPLOY_HOST:$SYSTEMD_DIR/airivo-auth-decision-alert.timer"

echo "[auth-alert-install] 校验 Python 与当前目录..."
do_ssh "
  set -e
  [ -x \"$VENV_PYTHON\" ] || { echo '[auth-alert-install] ERROR: venv python not found'; exit 1; }
  [ -d \"$CURRENT_DIR\" ] || { echo '[auth-alert-install] ERROR: current dir not found'; exit 1; }
  mkdir -p \"$LOG_DIR\"
"

echo "[auth-alert-install] 重载并启用 timer..."
do_ssh "
  set -e
  systemctl daemon-reload
  systemctl enable --now airivo-auth-decision-alert.timer
  systemctl start airivo-auth-decision-alert.service || true
  systemctl is-enabled airivo-auth-decision-alert.timer >/dev/null
"

echo "[auth-alert-install] 拉取状态摘要..."
do_ssh "
  set -e
  systemctl status airivo-auth-decision-alert.timer --no-pager -n 20
  echo '---'
  systemctl status airivo-auth-decision-alert.service --no-pager -n 20 || true
  echo '---'
  journalctl -u airivo-auth-decision-alert.service -n 20 --no-pager || true
"

echo "[auth-alert-install] ✅ 安装完成"
echo "[auth-alert-install] 建议继续执行: DEPLOY_PASS=*** bash tools/verify_airivo_system.sh"

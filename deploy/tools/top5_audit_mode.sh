#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
if [[ "$MODE" != "strict" && "$MODE" != "relaxed" ]]; then
  echo "用法: bash deploy/tools/top5_audit_mode.sh strict|relaxed"
  exit 1
fi

DEPLOY_HOST="${DEPLOY_HOST:-47.90.160.87}"
DEPLOY_USER="${DEPLOY_USER:-root}"
DEPLOY_PASS="${DEPLOY_PASS:-}"
SERVICE_PATH="${SERVICE_PATH:-/etc/systemd/system/airivo-top5-competition-audit.service}"
TIMER_NAME="${TIMER_NAME:-airivo-top5-competition-audit.timer}"
SERVICE_NAME="${SERVICE_NAME:-airivo-top5-competition-audit.service}"

if [ -z "$DEPLOY_PASS" ] && command -v security >/dev/null 2>&1; then
  DEPLOY_PASS="$(security find-generic-password -s openclaw_deploy_pass -w 2>/dev/null || true)"
fi

if [ -z "$DEPLOY_PASS" ]; then
  echo "[top5-audit-mode] ERROR: 请通过环境变量 DEPLOY_PASS 提供服务器密码"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
if [[ "${TOP5_AUDIT_REQUIRE_SCHEDULER_CONTRACT:-}" == "1" ]]; then
  echo "[top5-audit-mode] TOP5_AUDIT_REQUIRE_SCHEDULER_CONTRACT=1 → 本地必须通过单源漂移检查"
  bash "$ROOT_DIR/deploy/tools/check_top5_audit_schedulers_generated.sh"
fi
if [[ "$MODE" == "strict" ]]; then
  AUTO_SHADOW_INPUT="0"
else
  AUTO_SHADOW_INPUT="1"
fi

do_ssh() {
  sshpass -p "$DEPLOY_PASS" ssh -o ConnectTimeout=15 -o StrictHostKeyChecking=accept-new "$DEPLOY_USER@$DEPLOY_HOST" "$@"
}

echo "[top5-audit-mode] 切换模式: mode=$MODE auto_shadow_input=$AUTO_SHADOW_INPUT"

do_ssh "python3 - \"$SERVICE_PATH\" \"$MODE\" \"$AUTO_SHADOW_INPUT\" <<'PY'
from pathlib import Path
import sys

service_path = Path(sys.argv[1])
mode = sys.argv[2]
auto_shadow = sys.argv[3]

if not service_path.exists():
    raise SystemExit(f'[top5-audit-mode] ERROR: service file not found: {service_path}')

text = service_path.read_text(encoding='utf-8')
lines = text.splitlines()
new_lines = []
seen_mode = False
seen_shadow = False
for line in lines:
    if line.startswith('Environment=TOP5_AUDIT_MODE='):
        new_lines.append(f'Environment=TOP5_AUDIT_MODE={mode}')
        seen_mode = True
        continue
    if line.startswith('Environment=TOP5_AUDIT_AUTO_SHADOW_INPUT='):
        new_lines.append(f'Environment=TOP5_AUDIT_AUTO_SHADOW_INPUT={auto_shadow}')
        seen_shadow = True
        continue
    new_lines.append(line)

if not seen_mode or not seen_shadow:
    out = []
    inserted = False
    for line in new_lines:
        out.append(line)
        if line.startswith('Environment=STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR=') and not inserted:
            if not seen_mode:
                out.append(f'Environment=TOP5_AUDIT_MODE={mode}')
            if not seen_shadow:
                out.append(f'Environment=TOP5_AUDIT_AUTO_SHADOW_INPUT={auto_shadow}')
            inserted = True
    new_lines = out

new_text = '\n'.join(new_lines) + '\n'
if new_text != text:
    backup = service_path.with_suffix(service_path.suffix + '.bak')
    backup.write_text(text, encoding='utf-8')
    service_path.write_text(new_text, encoding='utf-8')
    print(f'[top5-audit-mode] 已更新: {service_path}')
    print(f'[top5-audit-mode] 备份: {backup}')
else:
    print(f'[top5-audit-mode] 无需修改: {service_path}')
PY"

echo "[top5-audit-mode] 重载 systemd 并应用 timer"
do_ssh "
  set -e
  systemctl daemon-reload
  systemctl enable --now \"$TIMER_NAME\"
  systemctl restart \"$TIMER_NAME\" || true
  systemctl is-enabled \"$TIMER_NAME\" >/dev/null
"

echo "[top5-audit-mode] 当前单元配置:"
do_ssh "systemctl cat \"$SERVICE_NAME\" | sed -n '1,40p'"

echo "[top5-audit-mode] 完成"
echo "[top5-audit-mode] 仓库契约: 若需与 Git/IaC 一致，请在 deploy/top5_audit_schedule.env.sh 对齐 TOP5_JOB_TOP5_AUDIT_MODE / TOP5_JOB_TOP5_AUDIT_AUTO_SHADOW_INPUT 后运行 bash deploy/tools/gen_top5_audit_schedulers.sh，再随发布同步 unit。"
echo "[top5-audit-mode] 可选门禁: TOP5_AUDIT_REQUIRE_SCHEDULER_CONTRACT=1 → 远端热修前强制执行本地漂移检查。"

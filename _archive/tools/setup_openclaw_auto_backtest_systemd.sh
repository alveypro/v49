#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${ROOT_DIR}/openclaw/systemd"
DST_DIR="/etc/systemd/system"

UNITS=(
  openclaw-auto-backtest-daily.service
  openclaw-auto-backtest-daily.timer
)

for u in "${UNITS[@]}"; do
  install -m 0644 "${SRC_DIR}/${u}" "${DST_DIR}/${u}"
  echo "installed:${u}"
done

systemctl daemon-reload
systemctl enable --now openclaw-auto-backtest-daily.timer

echo "timers:"
systemctl list-timers --all | grep -E "openclaw-auto-backtest-daily" || true

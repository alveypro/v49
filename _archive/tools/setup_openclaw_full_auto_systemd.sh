#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${ROOT_DIR}/openclaw/systemd"
DST_DIR="/etc/systemd/system"

UNITS=(
  openclaw-data-updater.service
  openclaw-data-updater.timer
  openclaw-daily-pipeline.service
  openclaw-daily-pipeline.timer
  openclaw-auto-evolve-opt.service
  openclaw-auto-evolve-opt.timer
  openclaw-auto-backtest-daily.service
  openclaw-auto-backtest-daily.timer
  openclaw-tracking-guard.service
  openclaw-tracking-guard.timer
)

for u in "${UNITS[@]}"; do
  install -m 0644 "${SRC_DIR}/${u}" "${DST_DIR}/${u}"
  echo "installed:${u}"
done

systemctl daemon-reload
systemctl enable --now openclaw-data-updater.timer
systemctl enable --now openclaw-daily-pipeline.timer
systemctl enable --now openclaw-auto-evolve-opt.timer
systemctl enable --now openclaw-auto-backtest-daily.timer
systemctl enable --now openclaw-tracking-guard.timer

echo "timers:"
systemctl list-timers --all | grep -E "openclaw-(data-updater|daily-pipeline|auto-evolve-opt|auto-backtest-daily|tracking-guard)" || true

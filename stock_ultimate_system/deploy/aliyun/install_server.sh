#!/usr/bin/env bash
set -euo pipefail

APP_ROOT=/opt/stock-ultimate
APP_DIR="$APP_ROOT/app"
DATA_DIR="$APP_ROOT/data"
VENV_DIR="$APP_ROOT/.venv"
LOG_DIR=/var/log/stock-ultimate
STAGING_DIR="$APP_ROOT/staging"
RELEASES_DIR="$APP_ROOT/releases"
DEPLOYMENTS_DIR="$APP_ROOT/deployments"

mkdir -p \
  "$APP_DIR" \
  "$DATA_DIR" \
  "$LOG_DIR" \
  "$APP_DIR/config/server" \
  "$STAGING_DIR" \
  "$RELEASES_DIR" \
  "$DEPLOYMENTS_DIR"

cp "$APP_DIR"/config/*.yaml "$APP_DIR/config/server/"
cp "$APP_DIR/deploy/aliyun/settings.server.yaml" "$APP_DIR/config/server/settings.yaml"

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install -U pip setuptools wheel
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"

cp "$APP_DIR/deploy/aliyun/stock-ultimate-dashboard.service" /etc/systemd/system/
mkdir -p /etc/systemd/system/stock-ultimate-dashboard.service.d
cp "$APP_DIR/deploy/aliyun/stock-ultimate-dashboard.service.d/canonical-artifacts.conf" /etc/systemd/system/stock-ultimate-dashboard.service.d/canonical-artifacts.conf
cp "$APP_DIR/deploy/aliyun/stock-ultimate-entry-guard.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-entry-guard.timer" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-main-site.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-t12.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-update.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-update.timer" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-daily-research.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-daily-research.timer" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-nightly-research.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-nightly-research.timer" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-weekly-long.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-weekly-long.timer" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-healthcheck.service" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/stock-ultimate-healthcheck.timer" /etc/systemd/system/
cp "$APP_DIR/deploy/aliyun/logrotate.stock-ultimate" /etc/logrotate.d/stock-ultimate

systemctl daemon-reload
systemctl enable --now stock-ultimate-dashboard.service
systemctl start stock-ultimate-entry-guard.service
systemctl enable --now stock-ultimate-entry-guard.timer
systemctl enable --now stock-ultimate-main-site.service
systemctl enable --now stock-ultimate-t12.service
systemctl enable --now stock-ultimate-update.timer
systemctl enable --now stock-ultimate-daily-research.timer
systemctl enable --now stock-ultimate-nightly-research.timer
systemctl enable --now stock-ultimate-weekly-long.timer
systemctl enable --now stock-ultimate-healthcheck.timer

echo "Install complete."

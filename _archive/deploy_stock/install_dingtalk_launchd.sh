#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LAUNCHD_SRC="$ROOT_DIR/launchd"
LAUNCHD_DST="$HOME/Library/LaunchAgents"
ENABLE_NGROK_TUNNEL="${ENABLE_NGROK_TUNNEL:-1}"

mkdir -p "$LAUNCHD_DST" "$ROOT_DIR/logs/openclaw"

cp "$LAUNCHD_SRC/com.airivo.openclaw.stock-agent.plist" "$LAUNCHD_DST/"
if [[ -f "$LAUNCHD_DST/com.airivo.openclaw.dingtalk-bridge.plist" ]]; then
  echo "Preserve existing dingtalk-bridge plist in LaunchAgents (keep local secrets)."
else
  cp "$LAUNCHD_SRC/com.airivo.openclaw.dingtalk-bridge.plist" "$LAUNCHD_DST/"
fi
if [[ "$ENABLE_NGROK_TUNNEL" == "1" ]]; then
  cp "$LAUNCHD_SRC/com.airivo.openclaw.ngrok-callback.plist" "$LAUNCHD_DST/"
fi

launchctl bootout "gui/$(id -u)/com.airivo.openclaw.stock-agent" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.airivo.openclaw.dingtalk-bridge" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)/com.airivo.openclaw.ngrok-callback" >/dev/null 2>&1 || true

launchctl bootstrap "gui/$(id -u)" "$LAUNCHD_DST/com.airivo.openclaw.stock-agent.plist"
launchctl bootstrap "gui/$(id -u)" "$LAUNCHD_DST/com.airivo.openclaw.dingtalk-bridge.plist"
if [[ "$ENABLE_NGROK_TUNNEL" == "1" ]]; then
  launchctl bootstrap "gui/$(id -u)" "$LAUNCHD_DST/com.airivo.openclaw.ngrok-callback.plist" || true
fi

launchctl enable "gui/$(id -u)/com.airivo.openclaw.stock-agent"
launchctl enable "gui/$(id -u)/com.airivo.openclaw.dingtalk-bridge"
launchctl kickstart -k "gui/$(id -u)/com.airivo.openclaw.stock-agent"
launchctl kickstart -k "gui/$(id -u)/com.airivo.openclaw.dingtalk-bridge"
if [[ "$ENABLE_NGROK_TUNNEL" == "1" ]]; then
  launchctl enable "gui/$(id -u)/com.airivo.openclaw.ngrok-callback" || true
  if ! launchctl kickstart -k "gui/$(id -u)/com.airivo.openclaw.ngrok-callback"; then
    echo "WARN: ngrok tunnel not started. Run: ngrok config add-authtoken <TOKEN> then re-run this script."
  fi
fi

echo "Installed and started:"
echo "  - com.airivo.openclaw.stock-agent"
echo "  - com.airivo.openclaw.dingtalk-bridge"
if [[ "$ENABLE_NGROK_TUNNEL" == "1" ]]; then
  echo "  - com.airivo.openclaw.ngrok-callback"
fi
echo
echo "Check status:"
echo "  launchctl print gui/$(id -u)/com.airivo.openclaw.stock-agent | rg state"
echo "  launchctl print gui/$(id -u)/com.airivo.openclaw.dingtalk-bridge | rg state"
if [[ "$ENABLE_NGROK_TUNNEL" == "1" ]]; then
  echo "  launchctl print gui/$(id -u)/com.airivo.openclaw.ngrok-callback | rg state"
  echo "  python3 deploy_stock/print_public_callback_url.py"
fi

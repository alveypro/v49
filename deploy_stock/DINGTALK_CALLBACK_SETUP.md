# DingTalk Callback Setup (Mac + OpenClaw)

## 1) Start local services with launchd

```bash
chmod +x deploy_stock/install_dingtalk_launchd.sh
./deploy_stock/install_dingtalk_launchd.sh
```

Expected local endpoints:
- `http://127.0.0.1:5101/health` (stock agent)
- `http://127.0.0.1:8601/health` (dingtalk bridge)

## 2) Expose callback endpoint to internet (HTTPS required)

DingTalk callback must be public HTTPS.

### Option A: use ngrok (recommended for quick setup)

1. Register ngrok and get authtoken.
2. Run once:

```bash
ngrok config add-authtoken <YOUR_NGROK_TOKEN>
```

3. Install/reload launchd services:

```bash
./deploy_stock/install_dingtalk_launchd.sh
```

4. Get public callback URL:

```bash
python3 deploy_stock/print_public_callback_url.py
```

Use printed URL like:
- `https://xxxxx.ngrok-free.app/dingtalk/callback`

### Option B: use your own domain/reverse proxy

Target local endpoint:
- `http://127.0.0.1:8601/dingtalk/callback`

External callback URL example:
- `https://your-domain.example.com/dingtalk/callback`

## 3) Configure DingTalk bot/app side

In DingTalk developer console, configure:
- **Message receive mode**: callback/event subscription (not webhook-only send mode)
- **Callback URL**: your ngrok or domain callback URL
- **Event type**: receive group text / robot mentions
- **Signature secret**: set the same value as `DINGTALK_CALLBACK_SECRET`

## 4) Set callback secret in launchd plist

Edit:
- `launchd/com.airivo.openclaw.dingtalk-bridge.plist`

Set in `launchd/com.airivo.openclaw.dingtalk-bridge.plist`:
- `DINGTALK_CALLBACK_SECRET`: old-style sign secret (optional)
- `DINGTALK_CALLBACK_TOKEN`: callback token from DingTalk callback page
- `DINGTALK_CALLBACK_AES_KEY`: callback AESKey (43 chars)
- `DINGTALK_CALLBACK_APP_KEY`: your app key (`dingdbqrxeuebuckvgkm`)

Then reload:

```bash
./deploy_stock/install_dingtalk_launchd.sh
```

## 5) Verify end-to-end

Local callback simulation:

```bash
python3 deploy_stock/test_dingtalk_bridge_local.py --text "@ClawAlpha 给我市场概览和3只候选"
```

DingTalk group test:
- In group, send `@机器人 你好`
- Expected: bot replies with stock-agent answer text

## 6) Troubleshooting quick checks

- Bridge logs:
  - `logs/openclaw/dingtalk_bridge.launchd.log`
  - `logs/openclaw/dingtalk_bridge.launchd.err.log`
- Agent logs:
  - `logs/openclaw/stock_agent.launchd.log`
  - `logs/openclaw/stock_agent.launchd.err.log`
- Launchd status:
  - `launchctl print gui/$(id -u)/com.airivo.openclaw.stock-agent | rg state`
  - `launchctl print gui/$(id -u)/com.airivo.openclaw.dingtalk-bridge | rg state`
  - `launchctl print gui/$(id -u)/com.airivo.openclaw.ngrok-callback | rg state`
- Ngrok URL:
  - `python3 deploy_stock/print_public_callback_url.py`

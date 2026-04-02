# OpenClaw Full Auto (Trade-Calendar Aware)

This setup runs a 3-stage nightly pipeline:

1. `openclaw-data-updater.timer`  
   Updates DB by SSE trade calendar and close+delay window.

2. `openclaw-daily-pipeline.timer`  
   Runs `tools/openclaw_partner_daily_run.sh` with freshness gate.

3. `openclaw-auto-evolve-opt.timer`  
   Runs `auto_evolve.py` in optimize-only phase.

4. `openclaw-tracking-guard.timer`  
   Runs nightly tracking health check and sends alert on consecutive abnormal `inserted=0` (excluding `no_picks`).

## Install

```bash
cd /opt/openclaw/app
chmod +x tools/setup_openclaw_full_auto_systemd.sh
sudo tools/setup_openclaw_full_auto_systemd.sh
```

## Verify

```bash
systemctl list-timers --all | grep openclaw-
journalctl -u openclaw-data-updater.service -n 80 --no-pager
journalctl -u openclaw-daily-pipeline.service -n 80 --no-pager
journalctl -u openclaw-auto-evolve-opt.service -n 80 --no-pager
journalctl -u openclaw-tracking-guard.service -n 80 --no-pager
```

## Manual run

```bash
bash tools/openclaw_full_auto.sh
```

## Rollback

```bash
bash tools/openclaw_snapshot_stable.sh
bash tools/openclaw_rollback_last_stable.sh
```

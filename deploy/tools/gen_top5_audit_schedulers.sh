#!/usr/bin/env bash
# Regenerate systemd unit + timer + macOS launchd plist.example from deploy/top5_audit_schedule.env.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_SH="$ROOT/deploy/top5_audit_schedule.env.sh"

if [[ ! -f "$ENV_SH" ]]; then
  echo "[gen-top5-schedulers] ERROR: missing $ENV_SH" >&2
  exit 1
fi

# shellcheck source=../deploy/top5_audit_schedule.env.sh
source "$ENV_SH"

if declare -p TOP5_AUDIT_SCHEDULES >/dev/null 2>&1; then
  # shellcheck disable=SC2154
  schedule_items=("${TOP5_AUDIT_SCHEDULES[@]}")
else
  schedule_items=("$(printf '%02d:%02d' "$TOP5_AUDIT_SCHEDULE_HOUR" "$TOP5_AUDIT_SCHEDULE_MINUTE")")
fi
if [[ "${#schedule_items[@]}" -eq 0 ]]; then
  echo "[gen-top5-schedulers] ERROR: no schedules configured" >&2
  exit 1
fi

systemd_calendar_lines=""
launchd_interval_xml=""
for item in "${schedule_items[@]}"; do
  if [[ ! "$item" =~ ^([0-9]{1,2}):([0-9]{2})$ ]]; then
    echo "[gen-top5-schedulers] ERROR: invalid TOP5_AUDIT_SCHEDULES item: $item" >&2
    exit 1
  fi
  hour="${BASH_REMATCH[1]}"
  minute="${BASH_REMATCH[2]}"
  if (( 10#$hour < 0 || 10#$hour > 23 || 10#$minute < 0 || 10#$minute > 59 )); then
    echo "[gen-top5-schedulers] ERROR: out-of-range schedule: $item" >&2
    exit 1
  fi
  hour_pad="$(printf '%02d' "$((10#$hour))")"
  minute_pad="$(printf '%02d' "$((10#$minute))")"
  systemd_calendar_lines+="OnCalendar=*-*-* ${hour_pad}:${minute_pad}:00"$'\n'
  launchd_interval_xml+="  <dict>
    <key>Hour</key>
    <integer>${hour_pad}</integer>
    <key>Minute</key>
    <integer>${minute_pad}</integer>
  </dict>
"
done

out_dir_abs="${TOP5_AUDIT_SYSTEMD_DEPLOY_ROOT}/${TOP5_JOB_REL_AUDIT_OUTPUT_DIR}"
runner_abs="/opt/openclaw/venv311/bin/python ${TOP5_AUDIT_SYSTEMD_DEPLOY_ROOT}/tools/run_daily_v9_evidence_pipeline.py --output-dir logs/openclaw/daily_evidence_pipeline --record-planned-observations"

cat >"$ROOT/deploy/airivo-top5-competition-audit.service" <<EOF
[Unit]
Description=Airivo Daily V9 Evidence Pipeline
After=${TOP5_AUDIT_SYSTEMD_AFTER}
Wants=${TOP5_AUDIT_SYSTEMD_WANTS}

[Service]
Type=oneshot
User=${TOP5_AUDIT_SYSTEMD_USER}
WorkingDirectory=${TOP5_AUDIT_SYSTEMD_DEPLOY_ROOT}
# Generated from deploy/top5_audit_schedule.env.sh — edit that file and re-run deploy/tools/gen_top5_audit_schedulers.sh
# Audit + forward gate can exceed systemd default start timeout.
TimeoutStartSec=${TOP5_AUDIT_TIMEOUT_START_SEC}
Environment=PYTHONUNBUFFERED=${TOP5_JOB_PYTHON_UNBUFFERED}
Environment=STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR=${out_dir_abs}
Environment=TOP5_GATE_PROJECT_ROOT=${TOP5_AUDIT_SYSTEMD_DEPLOY_ROOT}
Environment=TOP5_GATE_ENFORCE_RELAXED=${TOP5_JOB_TOP5_GATE_ENFORCE_RELAXED}
Environment=TOP5_AUDIT_MODE=${TOP5_JOB_TOP5_AUDIT_MODE}
Environment=TOP5_AUDIT_AUTO_SHADOW_INPUT=${TOP5_JOB_TOP5_AUDIT_AUTO_SHADOW_INPUT}
Environment=AIRIVO_SITE_HEALTH_URL=https://airivo.online/
Environment=AIRIVO_RECORD_PLANNED_OBSERVATIONS=1
Environment=AIRIVO_REQUIRE_EXECUTION_CLOSURE=0
Environment=AIRIVO_REQUIRE_NO_STALE_OPEN=0
Environment=PERMANENT_DB_PATH=${TOP5_JOB_PERMANENT_DB_PATH}
Environment=OPENCLAW_DB_PATH=${TOP5_JOB_PERMANENT_DB_PATH}
Environment=AIRIVO_DB_PATH=${TOP5_JOB_PERMANENT_DB_PATH}
Environment=SIM_TRADING_DB_PATH=${TOP5_JOB_SIM_TRADING_DB_PATH}
Environment=TRADING_ASSISTANT_DB_PATH=${TOP5_JOB_TRADING_ASSISTANT_DB_PATH}
# Full evidence loop: data health -> v9/stable evidence -> Top5 audit/gate -> manifest -> page health.
ExecStart=${runner_abs}
StandardOutput=append:${TOP5_AUDIT_SYSTEMD_STDOUT_LOG}
StandardError=append:${TOP5_AUDIT_SYSTEMD_STDERR_LOG}
EOF

cat >"$ROOT/deploy/airivo-top5-competition-audit.timer" <<EOF
[Unit]
Description=Run Airivo Top5 Competition Audit

[Timer]
${systemd_calendar_lines}Persistent=true
Unit=airivo-top5-competition-audit.service

[Install]
WantedBy=timers.target
EOF

runner_launch="__REPO_ROOT__/tools/run_top5_competition_audit_then_gate.sh"
audit_launch="__REPO_ROOT__/${TOP5_JOB_REL_AUDIT_OUTPUT_DIR}"
stdout_launch="__REPO_ROOT__/${TOP5_LAUNCHD_REL_STDOUT_LOG}"
stderr_launch="__REPO_ROOT__/${TOP5_LAUNCHD_REL_STDERR_LOG}"

cat >"$ROOT/deploy/airivo-top5-competition-audit.launchd.plist.example" <<EOFPLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${TOP5_LAUNCHD_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${runner_launch}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>__REPO_ROOT__</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONUNBUFFERED</key>
    <string>${TOP5_JOB_PYTHON_UNBUFFERED}</string>
    <key>STRATEGY_COMPETITION_AUDIT_OUTPUT_DIR</key>
    <string>${audit_launch}</string>
    <key>TOP5_GATE_PROJECT_ROOT</key>
    <string>__REPO_ROOT__</string>
    <key>TOP5_GATE_ENFORCE_RELAXED</key>
    <string>${TOP5_JOB_TOP5_GATE_ENFORCE_RELAXED}</string>
    <key>TOP5_AUDIT_MODE</key>
    <string>${TOP5_JOB_TOP5_AUDIT_MODE}</string>
    <key>TOP5_AUDIT_AUTO_SHADOW_INPUT</key>
    <string>${TOP5_JOB_TOP5_AUDIT_AUTO_SHADOW_INPUT}</string>
    <key>PERMANENT_DB_PATH</key>
    <string>${TOP5_JOB_PERMANENT_DB_PATH}</string>
    <key>OPENCLAW_DB_PATH</key>
    <string>${TOP5_JOB_PERMANENT_DB_PATH}</string>
    <key>AIRIVO_DB_PATH</key>
    <string>${TOP5_JOB_PERMANENT_DB_PATH}</string>
    <key>SIM_TRADING_DB_PATH</key>
    <string>${TOP5_JOB_SIM_TRADING_DB_PATH}</string>
    <key>TRADING_ASSISTANT_DB_PATH</key>
    <string>${TOP5_JOB_TRADING_ASSISTANT_DB_PATH}</string>
  </dict>
  <key>StartCalendarInterval</key>
  <array>
${launchd_interval_xml}  </array>
  <key>RunAtLoad</key>
  <false/>
  <key>StandardOutPath</key>
  <string>${stdout_launch}</string>
  <key>StandardErrorPath</key>
  <string>${stderr_launch}</string>
</dict>
</plist>
EOFPLIST

echo "[gen-top5-schedulers] wrote deploy/airivo-top5-competition-audit.service"
echo "[gen-top5-schedulers] wrote deploy/airivo-top5-competition-audit.timer"
echo "[gen-top5-schedulers] wrote deploy/airivo-top5-competition-audit.launchd.plist.example"

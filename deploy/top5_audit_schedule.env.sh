# Single source for Top5 competition audit scheduler semantics (Linux systemd + defaults for macOS launchd).
#
# Workflow:
#   1. Edit variables below only.
#   2. Regenerate tracked unit files: bash deploy/tools/gen_top5_audit_schedulers.sh
#   3. Commit deploy/airivo-top5-competition-audit.service,
#      deploy/airivo-top5-competition-audit.timer,
#      and deploy/airivo-top5-competition-audit.launchd.plist.example
#
# CI enforces that generated files match this file (see deploy/tools/check_top5_audit_schedulers_generated.sh).
#
# Platform truth:
# - systemd-only: After=, TimeoutStartSec, StandardOutput=append exist only in generated .service (no launchd twin).
# - Cross-platform parity = job Environment* + schedule; TZ alignment remains operational glue.
# - launchd render: textual __REPO_ROOT__ replace from .launchd.plist.example; plistlib runs only when local overrides diverge from that template (may tweak XML whitespace — still launchd-valid).
# - deploy/tools/top5_audit_mode.sh edits the live unit on a host; persist repo contract by mirroring edits here then bash deploy/tools/gen_top5_audit_schedulers.sh.
# - Optional governance hook (local): TOP5_AUDIT_REQUIRE_SCHEDULER_CONTRACT=1 with deploy/tools/top5_audit_mode.sh or deploy/tools/install_top5_audit_timer.sh blocks if generated deploy/* drifts.
# - Deploy manifest (--require-manifest or AIRIVO_TOP5_SCHEDULER_MANIFEST): SCP only if tracked files hash-match CI artifact top5_audit_scheduler_manifest.json; use top5_audit_scheduler_manifest.sha256 with sha256sum -c from repo root.
# - Phase C Cosign keyless (optional): set Actions repo variable COSIGN_SIGN_TOP5_MANIFEST=true; installer uses --require-cosign-bundle and COSIGN_CERT_IDENTITY_REGEXP (OIDC issuer default https://token.actions.githubusercontent.com).

# --- wall clock (timer / StartCalendarInterval), Asia/Shanghai host clock ---
# 08:45: pre-open execution brief refresh after overnight data/feedback sync.
# 18:10: post-close final audit after A-share EOD data is expected to be available.
TOP5_AUDIT_SCHEDULES=("08:45" "18:10")
# Backward-compatible fallback for older generator/runtime snippets.
TOP5_AUDIT_SCHEDULE_HOUR=18
TOP5_AUDIT_SCHEDULE_MINUTE=10

# --- systemd production layout ---
TOP5_AUDIT_SYSTEMD_DEPLOY_ROOT=/opt/openclaw/current
TOP5_AUDIT_SYSTEMD_USER=root
TOP5_AUDIT_SYSTEMD_AFTER="network-online.target openclaw-streamlit.service"
TOP5_AUDIT_SYSTEMD_WANTS=network-online.target
TOP5_AUDIT_SYSTEMD_STDOUT_LOG=/var/log/openclaw/top5_competition_audit.log
TOP5_AUDIT_SYSTEMD_STDERR_LOG=/var/log/openclaw/top5_competition_audit.err.log
TOP5_AUDIT_TIMEOUT_START_SEC=7200

# --- shared process environment (systemd Environment= and launchd EnvironmentVariables) ---
TOP5_JOB_PYTHON_UNBUFFERED=1
TOP5_JOB_REL_AUDIT_OUTPUT_DIR=logs/openclaw/strategy_competition_audit
TOP5_JOB_TOP5_AUDIT_MODE=strict
TOP5_JOB_TOP5_AUDIT_AUTO_SHADOW_INPUT=0
TOP5_JOB_TOP5_GATE_ENFORCE_RELAXED=0
TOP5_JOB_PERMANENT_DB_PATH=/opt/openclaw/permanent_stock_database.db
TOP5_JOB_SIM_TRADING_DB_PATH=/opt/openclaw/sim_trading.db
TOP5_JOB_TRADING_ASSISTANT_DB_PATH=/opt/openclaw/trading_assistant.db

# --- launchd plist example (committed template; install replaces __REPO_ROOT__) ---
TOP5_LAUNCHD_LABEL=com.airivo.top5-competition-audit
TOP5_LAUNCHD_REL_STDOUT_LOG=logs/openclaw/top5_competition_audit.launchd.log
TOP5_LAUNCHD_REL_STDERR_LOG=logs/openclaw/top5_competition_audit.launchd.err.log

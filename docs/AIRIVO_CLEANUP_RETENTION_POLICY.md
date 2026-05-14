# Airivo Cleanup And Retention Policy

This policy defines how OpenClaw/Airivo local and production storage is cleaned.
The goal is to remove iteration debris without deleting production evidence,
current databases, rollback material, or assets owned by other systems.

## Scope

Allowed roots:

- Local repository root.
- Production OpenClaw root: `/opt/openclaw`.

Out of scope:

- `/opt/airivo`, system-wide application directories, user home directories,
  package manager caches, nginx configs, and any path not explicitly listed in
  a cleanup manifest.

## Required Workflow

Every cleanup must run in this order:

1. `dry-run`: generate a cleanup audit manifest.
2. Review: classify each candidate as `delete`, `keep`, or `manual_review`.
3. `apply`: delete only manifest-supported `delete` candidates.
4. Verify: service status, Top5 manifest readability, disk usage.
5. Archive: keep the cleanup manifest under `logs/openclaw/cleanup_audit/`.

No production cleanup may run without a manifest.

## Retention Rules

### Always Keep

- Active production DB: `/opt/openclaw/permanent_stock_database.db`.
- The newest full DB backup required for rollback.
- Current release pointed to by `/opt/openclaw/current`.
- Active `exports/top5_trader_brief_latest_manifest.json` and referenced CSV/MD.
- Strategy evidence artifacts created in the last 14 days.
- Service units and current runtime scripts.

### Safe Delete

- Python bytecode caches: `__pycache__`, `*.pyc`.
- macOS metadata: `.DS_Store`, `._*`.
- Empty SQLite WAL/SHM fragments from one-off syncs.
- Oversized error logs after truncation, not deletion, when the filename is an
  active service log.
- DB sync intermediate uncompressed copies when a current DB and one rollback DB
  are both present.

### Manual Review

- Any `.db`, `.sqlite`, `.db.gz`, `.bak` larger than 100 MB.
- Release directories not pointed to by `/opt/openclaw/current`.
- `node_modules`, virtualenvs, model files, and package caches.
- Anything outside `/opt/openclaw`.

## Production Guardrails

- Never scan or delete outside `/opt/openclaw` unless a separate owner-approved
  manifest exists.
- Never delete the active DB path resolved from `PERMANENT_DB_PATH`.
- Never delete the target of `/opt/openclaw/current`.
- Prefer truncating active logs over deleting them.
- Deleting candidate DBs is allowed only if no systemd unit, current script, or
  current environment references the path.

## Verification Checklist

After cleanup:

- `systemctl is-active openclaw-streamlit.service` returns `active`.
- `PERMANENT_DB_PATH` points to `/opt/openclaw/permanent_stock_database.db`.
- `exports/top5_trader_brief_latest_manifest.json` loads successfully.
- `top5_canary_gate.passed` is `true` when a current Top5 exists.
- `df -h /` shows expected reclaimed capacity.

## Cadence

- Daily: truncate runaway logs over 500 MB after capture.
- Weekly: dry-run cleanup audit.
- Monthly: apply approved cleanup for safe-delete candidates.
- Quarterly: review old full DB backups and release directories.

## Commands

Local dry-run:

```bash
python3 tools/archive/maintenance/openclaw_cleanup_audit.py \
  --root . \
  --output-dir logs/openclaw/cleanup_audit \
  --operator "$USER"
```

Local apply for safe-delete candidates only:

```bash
python3 tools/archive/maintenance/openclaw_cleanup_audit.py \
  --root . \
  --output-dir logs/openclaw/cleanup_audit \
  --operator "$USER" \
  --apply
```

Production dry-run:

```bash
cd /opt/openclaw/current
/opt/openclaw/venv311/bin/python tools/archive/maintenance/openclaw_cleanup_audit.py \
  --root /opt/openclaw/current \
  --output-dir /opt/openclaw/current/logs/openclaw/cleanup_audit \
  --operator production_dry_run
```

Production apply for safe-delete candidates only:

```bash
cd /opt/openclaw/current
/opt/openclaw/venv311/bin/python tools/archive/maintenance/openclaw_cleanup_audit.py \
  --root /opt/openclaw/current \
  --output-dir /opt/openclaw/current/logs/openclaw/cleanup_audit \
  --operator production_apply \
  --apply
```

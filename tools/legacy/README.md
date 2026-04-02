# Legacy Scripts Quarantine

This folder is for one-off hotfix/recovery scripts that are not approved for routine production operations.

## Policy

- Do not run legacy scripts directly in production.
- Use `tools/production/run_allowed.py` for routine operations.
- When retiring old scripts, move them here with a short note of origin and date.

## Current mode

Mechanism-first rollout: whitelist is enforced now; gradual script relocation can be done in batches.

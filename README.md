# M-agent2026

This repository has been reorganized.

Active project:
- `stock_ultimate_system/`

Runtime data retained outside the active project:
- `_archive/permanent_stock_database.db`

Common commands:

```bash
pytest -q
cd stock_ultimate_system && python run_dashboard.py
cd stock_ultimate_system && python run_top_candidates.py --help
```


## Governance Gate

- Standard runbook: `_archive/docs/AIRIVO_GOVERNANCE_GATE_RUNBOOK.md`
- One-page flow: `_archive/docs/AIRIVO_GOVERNANCE_ONE_PAGE_FLOW.md`
- Local/CI unified entry: `_archive/tools/run_governance_gate_ci.sh`

## Config

If you need local secrets or service settings, keep them out of Git. Prefer environment variables such as `TUSHARE_TOKEN`, or a local ignored config file.

## Notes

- Root-level `pytest` is scoped to `stock_ultimate_system/tests`.
- `_archive/permanent_stock_database.db` is still referenced by `stock_ultimate_system/config/settings.yaml`; do not delete it unless the SQLite path is migrated first.
- Historical backup databases, old logs, and unused archive code have been removed from `_archive/`.
- Do NOT commit database files or tokens to Git.
- Set `TUSHARE_TOKEN` in env or `config.json` on the server.

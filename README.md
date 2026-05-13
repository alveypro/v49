# M-agent2026

This repository is being slimmed around a frozen runtime boundary map.

Current boundary map:
- `docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md`

Active public product surface:
- `stock_ultimate_system/` for `/stock` page, primary-result API, and public rendering.

Private runtime and service layers:
- `openclaw/runtime/` for private Streamlit/operator runtime modules.
- `openclaw/services/` for testable durable services.
- `strategies/` for strategy implementation and registry.
- `tools/` for stable daily operations entrypoints.

Compatibility entrypoint:
- `v49_app.py` remains a private Streamlit launch shell and compatibility surface.
  Do not add new production business logic to it.

Runtime data retained outside Git source control:
- `permanent_stock_database.db`
- `permanent_stock_database.backup.db`

Boundary validation commands:

```bash
python tools/tool_boundary_audit.py --fail-on-archive-candidates --max-manual-review 0 --max-support-review 2
pytest stock_ultimate_system/tests/test_ci_dashboard_boundary_gate.py stock_ultimate_system/tests/test_stock_dashboard_page_sections.py stock_ultimate_system/tests/test_stock_dashboard_render_inputs.py stock_ultimate_system/tests/test_stock_dashboard_http_routes.py stock_ultimate_system/tests/test_run_dashboard_primary_result_api.py -q
pytest tests/test_governance_docs_links.py tests/test_pr_template_governance_checklist.py tests/test_release_gate_script.py -q
```

Run product entrypoints separately:

```bash
cd stock_ultimate_system && python run_dashboard.py
cd stock_ultimate_system && python run_top_candidates.py --help
```


## Governance Gate

- Runtime boundary map: `docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md`
- Current hard CI gates: `tools/tool_boundary_audit.py` and stock dashboard boundary tests.
- Archived governance runner: `_archive/tools/run_governance_gate_ci.sh`
  - CI still runs it for traceability and PR comments.
  - It blocks CI only when `AIRIVO_BLOCK_ON_ARCHIVED_GOVERNANCE_GATE=true`.
  - Do not use it to define the current top-level `tools/` operator surface.

## Config

If you need local secrets or service settings, keep them out of Git. Prefer environment variables such as `TUSHARE_TOKEN`, or a local ignored config file.

## Notes

- Root-level `pytest` is scoped by `pytest.ini`; keep the default gate focused on production contracts and current evidence chain.
- The local SQLite path is `permanent_stock_database.db` through `stock_ultimate_system/config/settings.yaml`; do not move or delete it until `PERMANENT_DB_PATH` or another documented runtime path replaces it.
- Cleanup must start with a dry-run manifest under `logs/openclaw/cleanup_audit/`.
- Do NOT commit database files or tokens to Git.
- Set `TUSHARE_TOKEN` in env or `config.json` on the server.

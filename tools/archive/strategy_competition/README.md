# Strategy Competition Tool Archive

This namespace contains retired or non-daily strategy competition and research-flow tools.

These scripts were moved out of top-level `tools/` during the Airivo slimming work so the normal operator surface stays focused on daily Top5 evidence, cleanup, verification, and deployment handoff.

Rules:

- Do not call these scripts from daily operations without explicit review.
- Prefer durable service APIs in `openclaw/services/` for new work.
- If a script becomes operationally active again, promote it through `tools/README.md` and `docs/AIRIVO_RUNTIME_BOUNDARY_MAP.md` before moving it back.
- Historical docs may reference the old top-level path; new executable references should use this archive path.

# Tools Archive

This namespace contains executable history that is no longer part of the
top-level daily operator surface.

Archive rules:

- Do not add new daily entrypoints here.
- Do not import archived scripts from production services unless the call site
  explicitly labels the behavior as legacy or research.
- If an archived script must return to `tools/`, add it to
  `tools/tool_boundary_audit.py` first and document why it is a stable
  operator command.
- Keep manifests in each subdirectory current when moving scripts.

Namespaces:

- `strategy_competition/`: one-off adjudication, review, remediation, and
  backfill tools from the historical strategy competition workflow.
- `research/`: research and experiment runners that can still be invoked
  intentionally, but are not daily operator commands.
- `maintenance/`: extraction, assembly, and transitional maintenance helpers.
- `governance/`: legacy governance wrappers retained for forensic compatibility.


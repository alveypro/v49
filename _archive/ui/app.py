from __future__ import annotations

from openclaw.runtime.ui_launcher import run_legacy_ui, ui_metadata


def run() -> None:
    """Modular UI entry that forwards to legacy root main during migration."""
    _ = ui_metadata()
    run_legacy_ui()

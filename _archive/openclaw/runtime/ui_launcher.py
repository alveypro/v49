from __future__ import annotations

import runpy
from pathlib import Path
from typing import Dict

from strategies.registry import ui_primary_strategies


ROOT_MAIN = Path(__file__).resolve().parents[2] / "v49_app.py"


def ui_metadata() -> Dict[str, object]:
    return {
        "legacy_entry": str(ROOT_MAIN),
        "primary_strategies": ui_primary_strategies(),
    }


def run_legacy_ui() -> None:
    if not ROOT_MAIN.exists():
        raise FileNotFoundError(f"root main app not found: {ROOT_MAIN}")
    runpy.run_path(str(ROOT_MAIN), run_name="__main__")

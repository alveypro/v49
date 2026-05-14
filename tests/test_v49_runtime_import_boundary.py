from __future__ import annotations

import re
import subprocess
from pathlib import Path


def test_v49_app_direct_runtime_imports_are_tracked() -> None:
    app_text = Path("v49_app.py").read_text(encoding="utf-8")
    modules = sorted(set(re.findall(r"from openclaw\.runtime\.([A-Za-z0-9_]+) import", app_text)))
    tracked = set(
        subprocess.check_output(
            ["git", "ls-files", "openclaw/runtime"],
            text=True,
        ).splitlines()
    )

    missing = [f"openclaw/runtime/{module}.py" for module in modules if f"openclaw/runtime/{module}.py" not in tracked]

    assert not missing

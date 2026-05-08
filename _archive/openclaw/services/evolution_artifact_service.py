from __future__ import annotations

import json
import os
from typing import Any, Dict


def load_evolve_params(app_root: str, filename: str) -> Dict[str, Any]:
    try:
        evolve_path = os.path.join(app_root, "evolution", filename)
        if os.path.exists(evolve_path):
            with open(evolve_path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}

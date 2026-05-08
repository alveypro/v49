from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from openclaw.assistant.remote_bridge import query_openclaw_remote as _query_master_bridge


def query_openclaw_remote(question: str, timeout: int = 30):
    """Gateway now delegates to unified master bridge."""
    return _query_master_bridge(question=question, timeout=max(timeout, 20), session_id="stock-system")


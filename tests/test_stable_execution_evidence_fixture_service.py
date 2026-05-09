from __future__ import annotations

import sqlite3
from pathlib import Path

from openclaw.services.stable_execution_evidence_fixture_service import seed_stable_shadow_execution_evidence


def test_seed_stable_shadow_execution_evidence(tmp_db: Path, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    payload = seed_stable_shadow_execution_evidence(
        conn,
        linked_run_id="run_backtest_stable_fixture_001",
        output_dir=str(tmp_path),
        operator_name="pytest_fixture",
    )
    conn.close()

    evidence = payload.get("execution_evidence") or {}
    status_counts = evidence.get("status_counts") or {}

    assert evidence.get("passed") is True
    assert status_counts.get("filled", 0) >= 2
    assert status_counts.get("partial_fill", 0) >= 1
    assert status_counts.get("cancelled", 0) >= 1
    assert status_counts.get("expired", 0) >= 1
    assert status_counts.get("manual_override", 0) >= 1
    assert status_counts.get("rejected", 0) >= 1
    assert len(evidence.get("high_slippage_orders") or []) >= 1
    assert Path((payload.get("artifacts") or {}).get("json", "")).exists()

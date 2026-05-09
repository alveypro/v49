from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
DRILL = ROOT / "tools" / "run_benchmark_contract_security_drill.py"


def test_security_drill_covers_dual_key_overlap_and_cutover():
    result = subprocess.run(
        [sys.executable, str(DRILL)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    payload = json.loads(result.stdout)
    rows = payload.get("drill_results") if isinstance(payload.get("drill_results"), list) else []
    by_name = {
        str(item.get("scenario") or ""): item
        for item in rows
        if isinstance(item, dict) and str(item.get("scenario") or "")
    }
    assert "dual_key_overlap_accepts_rotating_key" in by_name
    assert "post_cutover_blocks_legacy_key" in by_name
    assert "kms_iam_policy_blocks_unauthorized_key" in by_name
    assert by_name["dual_key_overlap_accepts_rotating_key"].get("failures") == []
    assert any(
        "strategy competition audit benchmark contract key not active: benchmark_default_key" in str(reason)
        for reason in (by_name["post_cutover_blocks_legacy_key"].get("failures") or [])
    )
    assert any(
        "iam_key_not_allowed:benchmark_governance_gate:benchmark_revoked_key_example" in str(reason)
        for reason in (by_name["kms_iam_policy_blocks_unauthorized_key"].get("failures") or [])
    )

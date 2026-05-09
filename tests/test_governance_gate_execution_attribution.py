from __future__ import annotations

import importlib.util
import json
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "_archive" / "tools" / "governance_gate.py"
_SPEC = importlib.util.spec_from_file_location("governance_gate_for_execution_attribution_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate)


class _Completed:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_execution_attribution_tool_registered_in_governance_constants():
    assert "tools/backfill_execution_attribution.py" in gate.REQUIRED_MAINLINE_FILES
    assert "tools/backfill_execution_attribution.py" in gate.GOVERNANCE_SENSITIVE_PATHS
    assert gate._execution_attribution_scope_changed(["openclaw/services/execution_evidence_service.py"])
    assert gate._execution_attribution_scope_changed(["tests/test_execution_attribution_backfill_service.py"])


def test_execution_attribution_hygiene_gate_requires_db_env(monkeypatch):
    monkeypatch.delenv("AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE", raising=False)
    monkeypatch.delenv(gate.EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV, raising=False)
    monkeypatch.delenv("AIRIVO_STRATEGY_OPTIMIZATION_DB_PATH", raising=False)

    failures = gate.run_execution_attribution_hygiene_gate(["openclaw/services/execution_evidence_service.py"])

    assert f"execution attribution hygiene gate requires {gate.EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV}" in failures[0]


def test_execution_attribution_hygiene_gate_blocks_when_dry_run_finds_gaps(monkeypatch):
    monkeypatch.delenv("AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE", raising=False)
    monkeypatch.setenv(gate.EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV, "/tmp/fake.db")

    payload = {"patched_count": 3, "artifact_path": "logs/openclaw/execution_attribution_backfill_dry_run_demo.json"}
    monkeypatch.setattr(
        gate.subprocess,
        "run",
        lambda *args, **kwargs: _Completed(0, stdout=json.dumps(payload), stderr=""),
    )

    failures = gate.run_execution_attribution_hygiene_gate(["openclaw/services/execution_evidence_service.py"])

    assert failures
    assert "execution attribution hygiene found stale missing attribution rows" in failures[0]
    assert "patched_count=3" in failures[0]


def test_execution_attribution_hygiene_gate_passes_with_clean_dry_run(monkeypatch):
    monkeypatch.delenv("AIRIVO_ENABLE_EXECUTION_ATTRIBUTION_HYGIENE_GATE", raising=False)
    monkeypatch.setenv(gate.EXECUTION_ATTRIBUTION_HYGIENE_DB_ENV, "/tmp/fake.db")

    payload = {"patched_count": 0, "artifact_path": "logs/openclaw/execution_attribution_backfill_dry_run_clean.json"}
    monkeypatch.setattr(
        gate.subprocess,
        "run",
        lambda *args, **kwargs: _Completed(0, stdout=json.dumps(payload), stderr=""),
    )

    failures = gate.run_execution_attribution_hygiene_gate(["openclaw/services/execution_evidence_service.py"])

    assert failures == []

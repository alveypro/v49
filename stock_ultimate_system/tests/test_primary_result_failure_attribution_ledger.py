from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_failure_attribution_ledger import PrimaryResultFailureAttributionLedger


def _write_attribution(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "attribution_version": "primary_result_failure_attribution.v1",
                "generated_at": "2026-05-01T00:00:00+00:00",
                "status": "passed",
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "stock_name": "平安银行",
                "observation_status": "failed",
                "outcome": "failed",
                "attribution_required": True,
                "primary_failure_category": "risk_control_failure",
                "contributing_categories": [
                    {"category": "risk_control_failure"},
                    {"category": "market_drag"},
                    {"category": "data_quality_failure"},
                    {"category": "source_risk_mismatch"},
                    {"category": "weak_source_signal"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_primary_result_failure_attribution_ledger_appends_and_refreshes_summary(tmp_path):
    attribution_path = tmp_path / "attribution.json"
    _write_attribution(attribution_path)

    ledger = PrimaryResultFailureAttributionLedger(
        ledger_path=tmp_path / "ledger.jsonl",
        summary_path=tmp_path / "summary.json",
    )
    entry = ledger.append_attribution(attribution_path=attribution_path)

    assert entry["primary_failure_category"] == "risk_control_failure"
    summary = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert summary["entry_total"] == 1
    assert summary["false_positive_cases"] == 1
    assert summary["rank_too_low_cases"] == 1
    assert summary["risk_gate_blocked_but_later_strong_cases"] == 1
    assert summary["evidence_insufficient_cases"] == 1
    assert summary["regime_mismatch_cases"] == 1
    assert summary["risk_control_failure_cases"] == 1
    assert summary["source_risk_mismatch_cases"] == 1
    assert summary["weak_source_signal_cases"] == 1


def test_register_primary_result_failure_attribution_cli(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script_path = project_root / "scripts" / "register_primary_result_failure_attribution.py"
    attribution_path = tmp_path / "attribution.json"
    _write_attribution(attribution_path)

    completed = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--attribution-json",
            str(attribution_path),
            "--ledger-jsonl",
            str(tmp_path / "ledger.jsonl"),
            "--summary-json",
            str(tmp_path / "summary.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    payload = json.loads(completed.stdout)
    assert payload["status"] == "ok"
    assert json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))["entry_total"] == 1

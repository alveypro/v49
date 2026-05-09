import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.proof_report import build_candidate_quality_proof_report


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_candidate_quality_proof_report_blocks_unproven_quality(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    for name in [
        "data_quality_report_latest.json",
        "candidate_data_quality_gate_latest.json",
        "candidate_lineage_latest.json",
        "realistic_backtest_latest.json",
        "candidate_observation_snapshot_latest.json",
        "candidate_observation_result_latest.json",
        "candidate_failure_attribution_latest.json",
        "candidate_portfolio_latest.json",
        "portfolio_capacity_report_latest.json",
        "candidate_risk_state_latest.json",
        "candidate_public_explanation_latest.json",
        "candidate_internal_explanation_latest.json",
        "candidate_rejection_explanation_latest.json",
    ]:
        _write_json(exp_dir / name, {"schema_version": "x", "status": "passed", "candidate_count": 1})
    _write_json(exp_dir / "candidate_quality_20_sample_report.json", {"status": "blocked", "sample_count": 0})
    _write_json(exp_dir / "candidate_portfolio_quality_latest.json", {"status": "blocked", "quality_score": 22.0})
    _write_json(
        exp_dir / "candidate_public_explanation_latest.json",
        {"schema_version": "candidate_public_explanation.v1", "status": "passed", "items": [{"external_display_allowed": False}]},
    )
    _write_json(exp_dir / "candidate_rejection_explanation_latest.json", {"status": "passed", "candidate_count": 1})

    report = build_candidate_quality_proof_report(exp_dir=exp_dir)

    assert report["status"] == "blocked"
    assert report["decision"]["quality_proven"] is False
    assert report["decision"]["external_page_mode"] == "degraded_review"
    assert "candidate_quality_proven" in report["prohibited_claims"]
    assert "normal_external_watchlist" in report["prohibited_claims"]


def test_candidate_quality_proof_report_cli_writes_output(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_quality_proof_report.py"),
            "--exp-dir",
            str(exp_dir),
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["status"] == "blocked"
    assert (exp_dir / "candidate_quality_proof_report_latest.json").exists()

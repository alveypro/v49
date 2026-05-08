import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_competitive_gap_assessment import build_primary_result_competitive_gap_assessment


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_competitive_gap_assessment_reports_real_gaps(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(exp_dir / "primary_result_observation_latest.json", {"observation_status": "observing"})
    _write_json(exp_dir / "primary_result_daily_closure_latest.json", {"status": "blocked"})
    _write_json(exp_dir / "primary_result_market_data_readiness_latest.json", {"status": "blocked", "latest_update_log": {"status": "partial_success"}})
    _write_json(artifacts / "primary_result_candidate_baskets" / "current.json", {"basket_id": "basket-001", "status": "approved"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "observation_latest.json", {"status": "blocked"})
    (artifacts / "primary_result_candidate_baskets").mkdir(parents=True, exist_ok=True)
    (artifacts / "primary_result_candidate_baskets" / "performance_ledger.jsonl").write_text(
        '{"entry_id":"basket-001:2026-04-16:completed"}\n',
        encoding="utf-8",
    )
    output = tmp_path / "gap.json"

    exit_code, payload = build_primary_result_competitive_gap_assessment(
        exp_dir=exp_dir,
        artifacts_dir=artifacts,
        output_path=output,
    )

    assert exit_code == 0
    assert payload["assessment_version"] == "primary_result_competitive_gap_assessment.v1"
    assert payload["benchmark_model"] == "industry_leader_public_capability_archetype"
    assert "private implementation" in payload["truth_boundary"]
    ids = {item["capability_id"] for item in payload["capabilities"]}
    assert "performance_evidence" in ids
    assert "candidate_basket" in ids
    assert output.exists()


def test_competitive_gap_assessment_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_competitive_gap_assessment.py"
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(exp_dir / "primary_result_observation_latest.json", {"observation_status": "observing"})

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--exp-dir",
            str(exp_dir),
            "--artifacts-dir",
            str(artifacts),
            "--output",
            str(tmp_path / "gap.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["assessment_version"] == "primary_result_competitive_gap_assessment.v1"
    assert json.loads((tmp_path / "gap.json").read_text(encoding="utf-8"))["assessment_version"] == payload["assessment_version"]

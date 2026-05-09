import json
import subprocess
from scripts.run_candidate_quality_daily_closure import run_candidate_quality_daily_closure


def _completed(payload: dict, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[],
        returncode=returncode,
        stdout=json.dumps(payload, ensure_ascii=False),
        stderr="",
    )


def test_candidate_quality_daily_closure_runs_r1_to_r6_without_candidate_overwrite(tmp_path):
    calls: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        joined = " ".join(command)
        if "build_candidate_quality_proof_report.py" in joined:
            return _completed({"status": "blocked", "quality_proven": False})
        if "build_candidate_quality_remediation_plan.py" in joined:
            return _completed({"status": "blocked", "action_count": 6, "next_run_order": ["R1", "R2", "R3", "R4", "R5", "R6"]})
        return _completed({"status": "passed"})

    payload = run_candidate_quality_daily_closure(exp_dir=tmp_path, runner=runner)

    assert payload["status"] == "blocked"
    assert payload["generate_candidates"] is False
    assert payload["steps"][0]["id"] == "R3_candidate_generation"
    assert payload["steps"][0]["payload"]["status"] == "skipped"
    assert all("run_top_candidates.py" not in " ".join(call) for call in calls)
    assert payload["next_run_order"] == ["R1", "R2", "R3", "R4", "R5", "R6"]


def test_candidate_quality_daily_closure_can_explicitly_run_expanded_candidate_generation(tmp_path):
    calls: list[list[str]] = []

    def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        joined = " ".join(command)
        if "build_candidate_quality_remediation_plan.py" in joined:
            return _completed({"status": "passed", "action_count": 0, "next_run_order": []})
        if "build_candidate_quality_proof_report.py" in joined:
            return _completed({"status": "passed", "quality_proven": True})
        return _completed({"status": "passed"})

    payload = run_candidate_quality_daily_closure(
        exp_dir=tmp_path,
        generate_candidates=True,
        expanded_universe_size=500,
        top_n=5,
        runner=runner,
    )

    assert payload["status"] == "passed"
    generation_call = calls[0]
    assert "run_top_candidates.py" in generation_call
    assert "--universe-size" in generation_call
    assert "500" in generation_call
    assert "--quick-mode" in generation_call

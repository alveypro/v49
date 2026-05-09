import json
import subprocess
import sys
from pathlib import Path

from src.primary_result_daily_operations_scoreboard import build_primary_result_daily_operations_scoreboard


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, total: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f'{{"entry_id":"entry-{index}"}}\n' for index in range(total)), encoding="utf-8")


def _seed_green(exp_dir: Path, artifacts: Path) -> None:
    _write_json(exp_dir / "update_status_latest.json", {"status": "completed", "post_candidates": {"ok": True}})
    _write_json(exp_dir / "primary_result_market_data_readiness_latest.json", {"status": "passed"})
    _write_json(artifacts / "primary_result_candidate_handoff_gate_latest.json", {"status": "passed", "decision": "aligned"})
    _write_json(exp_dir / "primary_result_daily_closure_latest.json", {"status": "closed_success"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "current.json", {"status": "approved", "basket_id": "basket-001"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "observation_latest.json", {"status": "completed"})
    _write_json(artifacts / "primary_result_performance" / "summary.json", {"entry_total": 20})
    _write_json(artifacts / "primary_result_candidate_baskets" / "performance_summary.json", {"entry_total": 20})
    _write_json(
        artifacts / "primary_result_competitive_gap_assessment_latest.json",
        {"status": "credible challenger", "overall_score": 76, "critical_gaps": []},
    )
    _write_json(exp_dir / "primary_result_feedback_loop_latest.json", {"status": "completed", "queue_status": "not_required"})
    _write_json(
        artifacts / "primary_result_feedback_review_queue" / "summary.json",
        {
            "status_counts": {"open": 0},
            "open_high_severity_total": 0,
            "open_high_priority_total": 0,
            "open_critical_priority_total": 0,
            "priority_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {},
        },
    )
    _write_json(artifacts / "primary_result_performance_evidence_latest.json", {"status": "ready", "next_actions": []})
    _write_json(
        artifacts / "primary_result_promotion_readiness_gate_latest.json",
        {"status": "passed", "decision": "promotion_review_allowed", "next_actions": []},
    )


def test_daily_operations_scoreboard_green_path(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _seed_green(exp_dir, artifacts)

    exit_code, payload = build_primary_result_daily_operations_scoreboard(
        exp_dir=exp_dir,
        artifacts_dir=artifacts,
        output_path=tmp_path / "scoreboard.json",
    )

    assert exit_code == 0
    assert payload["scoreboard_version"] == "primary_result_daily_operations_scoreboard.v1"
    assert payload["overall_status"] == "green"
    assert payload["operational_state"] == "clean_operating_day"
    assert "competitor private core" in payload["truth_boundary"]
    feedback_section = next(section for section in payload["sections"] if section["section_id"] == "feedback_learning_loop")
    assert feedback_section["queue_priority_counts"]["high"] == 0
    assert feedback_section["queue_open_high_priority_total"] == 0
    assert (tmp_path / "scoreboard.json").exists()


def test_daily_operations_scoreboard_surfaces_feedback_priority_scheduling_actions(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _seed_green(exp_dir, artifacts)
    _write_json(
        artifacts / "primary_result_feedback_review_queue" / "summary.json",
        {
            "status_counts": {"open": 2},
            "open_high_severity_total": 1,
            "open_high_priority_total": 2,
            "open_critical_priority_total": 1,
            "priority_counts": {"critical": 1, "high": 1, "medium": 0, "low": 0, "none": 0},
            "taxonomy_hotspots": {"risk_control_failure": 2},
            "open_owner_workloads": {"reviewer-a": {"open_total": 2, "critical_priority_total": 1, "high_priority_total": 2}},
        },
    )

    exit_code, payload = build_primary_result_daily_operations_scoreboard(exp_dir=exp_dir, artifacts_dir=artifacts)

    assert exit_code == 0
    feedback_section = next(section for section in payload["sections"] if section["section_id"] == "feedback_learning_loop")
    assert feedback_section["queue_open_critical_priority_total"] == 1
    assert feedback_section["queue_taxonomy_hotspots"]["risk_control_failure"] == 2
    assert feedback_section["queue_open_owner_workloads"]["reviewer-a"]["open_total"] == 2
    assert any("critical-priority feedback review items" in action for action in payload["next_actions"])
    assert any("current feedback hotspot: risk_control_failure" in action for action in payload["next_actions"])
    assert any("rebalance feedback workload: owner=reviewer-a" in action for action in payload["next_actions"])


def test_daily_operations_scoreboard_treats_expected_blocking_as_yellow(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(exp_dir / "update_status_latest.json", {"status": "completed", "post_candidates": {"ok": True}})
    _write_json(
        exp_dir / "primary_result_market_data_readiness_latest.json",
        {"status": "blocked", "blocking_reasons": ["insufficient price rows for 300383.SZ: 1"]},
    )
    _write_json(
        exp_dir / "primary_result_daily_closure_latest.json",
        {"status": "blocked", "blocking_reasons": ["insufficient price rows for 300383.SZ: 1"]},
    )
    _write_json(artifacts / "primary_result_candidate_baskets" / "current.json", {"status": "approved", "basket_id": "basket-001"})
    _write_json(
        artifacts / "primary_result_candidate_baskets" / "observation_latest.json",
        {"status": "blocked", "blocking_reasons": ["insufficient price rows for 603601.SH: 1"]},
    )
    _write_jsonl(artifacts / "primary_result_performance" / "ledger.jsonl", 1)
    _write_jsonl(artifacts / "primary_result_candidate_baskets" / "performance_ledger.jsonl", 1)

    exit_code, payload = build_primary_result_daily_operations_scoreboard(exp_dir=exp_dir, artifacts_dir=artifacts)

    assert exit_code == 0
    assert payload["overall_status"] == "yellow"
    assert payload["operational_state"] == "blocked_or_accumulating"
    assert any("insufficient price rows" in reason for reason in payload["blocking_reasons"])


def test_daily_operations_scoreboard_treats_pending_window_as_yellow(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(exp_dir / "update_status_latest.json", {"status": "completed", "post_candidates": {"ok": True}})
    _write_json(exp_dir / "primary_result_market_data_readiness_latest.json", {"status": "pending_window"})
    _write_json(artifacts / "primary_result_candidate_handoff_gate_latest.json", {"status": "passed", "decision": "aligned"})
    _write_json(exp_dir / "primary_result_daily_closure_latest.json", {"status": "pending_window"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "current.json", {"status": "approved", "basket_id": "basket-001"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "observation_latest.json", {"status": "pending_window"})
    _write_jsonl(artifacts / "primary_result_performance" / "ledger.jsonl", 1)
    _write_jsonl(artifacts / "primary_result_candidate_baskets" / "performance_ledger.jsonl", 1)

    exit_code, payload = build_primary_result_daily_operations_scoreboard(exp_dir=exp_dir, artifacts_dir=artifacts)

    assert exit_code == 0
    section_levels = {section["section_id"]: section["level"] for section in payload["sections"]}
    assert section_levels["market_data_readiness"] == "yellow"
    assert section_levels["primary_result_daily_closure"] == "yellow"


def test_daily_operations_scoreboard_treats_conditional_basket_pointer_as_yellow(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _write_json(exp_dir / "update_status_latest.json", {"status": "completed", "post_candidates": {"ok": True}})
    _write_json(exp_dir / "primary_result_market_data_readiness_latest.json", {"status": "passed"})
    _write_json(exp_dir / "primary_result_daily_closure_latest.json", {"status": "closed_success"})
    _write_json(artifacts / "primary_result_candidate_handoff_gate_latest.json", {"status": "passed", "decision": "aligned"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "current.json", {"status": "conditional", "basket_id": "basket-conditional"})
    _write_json(artifacts / "primary_result_candidate_baskets" / "observation_latest.json", {"status": "completed"})
    _write_jsonl(artifacts / "primary_result_performance" / "ledger.jsonl", 20)
    _write_jsonl(artifacts / "primary_result_candidate_baskets" / "performance_ledger.jsonl", 20)
    _write_json(artifacts / "primary_result_performance_evidence_latest.json", {"status": "ready", "next_actions": []})
    _write_json(artifacts / "primary_result_promotion_readiness_gate_latest.json", {"status": "passed", "decision": "promotion_review_allowed", "next_actions": []})

    exit_code, payload = build_primary_result_daily_operations_scoreboard(exp_dir=exp_dir, artifacts_dir=artifacts)

    assert exit_code == 0
    section_levels = {section["section_id"]: section["level"] for section in payload["sections"]}
    assert section_levels["candidate_basket_registry"] == "yellow"


def test_daily_operations_scoreboard_missing_critical_reports_is_red(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"

    exit_code, payload = build_primary_result_daily_operations_scoreboard(exp_dir=exp_dir, artifacts_dir=artifacts)

    assert exit_code == 1
    assert payload["overall_status"] == "red"
    section_levels = {section["section_id"]: section["level"] for section in payload["sections"]}
    assert section_levels["daily_update"] == "red"
    assert section_levels["primary_result_daily_closure"] == "red"
    assert section_levels["candidate_handoff_gate"] == "yellow"
    assert section_levels["candidate_basket_registry"] == "red"
    assert section_levels["feedback_learning_loop"] == "yellow"
    assert section_levels["promotion_readiness_gate"] == "yellow"


def test_daily_operations_scoreboard_cli_outputs_json(tmp_path):
    project_root = Path(__file__).resolve().parents[1]
    script = project_root / "scripts" / "build_primary_result_daily_operations_scoreboard.py"
    exp_dir = tmp_path / "data" / "experiments"
    artifacts = tmp_path / "artifacts"
    _seed_green(exp_dir, artifacts)

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--exp-dir",
            str(exp_dir),
            "--artifacts-dir",
            str(artifacts),
            "--output",
            str(tmp_path / "scoreboard.json"),
            "--json",
        ],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["overall_status"] == "green"
    assert json.loads((tmp_path / "scoreboard.json").read_text(encoding="utf-8"))["scoreboard_version"] == payload["scoreboard_version"]

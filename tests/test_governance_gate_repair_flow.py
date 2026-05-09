from __future__ import annotations

import importlib.util
import json
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "_archive" / "tools" / "governance_gate.py"
_SPEC = importlib.util.spec_from_file_location("governance_gate_for_repair_flow_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate)


def test_repair_flow_gate_requires_evidence_when_scope_changed(monkeypatch):
    monkeypatch.delenv(gate.REPAIR_FLOW_EVIDENCE_ENV, raising=False)
    failures = gate.run_repair_flow_gate(["openclaw/services/research_repair_iteration_flow_service.py"])

    assert failures == [f"repair flow PR requires {gate.REPAIR_FLOW_EVIDENCE_ENV}"]


def test_repair_flow_gate_blocks_formal_attempt_and_mismatch(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "unthrottled_summary_json": {"passed": True},
            "watch_risks": [{"risk_code": "neutral_signal_sparsity"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260227"], "h2"))
    review = _write_json(
        tmp_path / "review.json",
        {
            "risk_off_alpha_repair_passed": True,
            "formal_candidate_allowed": True,
            "production_candidate_allowed": False,
        },
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "neutral_signal_sparsity"}]})
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    failures = gate.validate_repair_flow_evidence_file()

    assert "repair_review_artifact attempted formal eligibility" in failures
    assert "repair paired benchmark window set mismatch" in failures
    assert "repair paired benchmark rule hash mismatch" in failures


def test_repair_flow_gate_accepts_observation_discussion_only_manifest(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "unthrottled_summary_json": {"passed": True},
            "watch_risks": [{"risk_code": "neutral_signal_sparsity"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260226"], "h1"))
    review = _write_json(
        tmp_path / "review.json",
        {
            "risk_off_alpha_repair_passed": True,
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "neutral_signal_sparsity"}]})
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    assert gate.validate_repair_flow_evidence_file() == []


def test_repair_flow_gate_blocks_block_level_risk_with_observation_discussion(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "next_allowed_actions_json": ["observation_watch_discussion"],
            "unthrottled_summary_json": {"passed": True},
            "watch_risks": [{"risk_code": "over_veto_risk", "risk_level": "block"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260226"], "h1"))
    review = _write_json(
        tmp_path / "review.json",
        {"risk_off_alpha_repair_passed": True, "formal_candidate_allowed": False, "production_candidate_allowed": False},
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "over_veto_risk", "risk_level": "block"}]})
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    failures = gate.validate_repair_flow_evidence_file()

    assert "repair flow block-level watch risk cannot allow observation discussion: over_veto_risk" in failures


def test_repair_flow_gate_blocks_missing_unthrottled_summary(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "watch_risks": [{"risk_code": "neutral_signal_sparsity"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260226"], "h1"))
    review = _write_json(
        tmp_path / "review.json",
        {"risk_off_alpha_repair_passed": True, "formal_candidate_allowed": False, "production_candidate_allowed": False},
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "neutral_signal_sparsity"}]})
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    assert "repair flow missing unthrottled summary" in gate.validate_repair_flow_evidence_file()


def test_repair_flow_gate_requires_review_and_oos_plan_for_block_watch_risk(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "next_allowed_actions_json": ["watch_risk_register_review", "out_of_sample_monitoring_plan"],
            "unthrottled_summary_json": {"passed": True},
            "watch_risks": [{"risk_code": "over_veto_risk", "risk_level": "block"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260226"], "h1"))
    review = _write_json(
        tmp_path / "review.json",
        {"risk_off_alpha_repair_passed": True, "formal_candidate_allowed": False, "production_candidate_allowed": False},
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "over_veto_risk", "risk_level": "block"}]})
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    failures = gate.validate_repair_flow_evidence_file()

    assert "repair flow block-level watch risk requires watch_risk_review_artifact" in failures
    assert "repair flow block-level watch risk requires oos_monitoring_plan_artifact" in failures


def test_repair_flow_gate_accepts_block_risk_with_review_and_oos_plan(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "next_allowed_actions_json": ["watch_risk_register_review", "out_of_sample_monitoring_plan"],
            "unthrottled_summary_json": {"passed": True},
            "watch_risks": [{"risk_code": "over_veto_risk", "risk_level": "info"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260226"], "h1"))
    review = _write_json(
        tmp_path / "review.json",
        {"risk_off_alpha_repair_passed": True, "formal_candidate_allowed": False, "production_candidate_allowed": False},
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "over_veto_risk", "risk_level": "info"}]})
    watch_review = _write_json(
        tmp_path / "watch_review.json",
        {
            "review_status": "watch_risk_review_blocked",
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    oos_plan = _write_json(
        tmp_path / "oos_plan.json",
        {
            "rule_hash": "h1",
            "oos_windows": ["20260317", "20260318"],
            "paired_run_requirement": {"unthrottled_required": True, "throttled_required": True},
            "pass_conditions": {"block_level_watch_risk_allowed": False},
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
            "watch_risk_review_artifact": str(watch_review),
            "oos_monitoring_plan_artifact": str(oos_plan),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    assert gate.validate_repair_flow_evidence_file() == []


def test_repair_flow_gate_blocks_oos_result_promotion_attempt(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "observation_watch_discussion_allowed",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "next_allowed_actions_json": ["watch_risk_register_review", "out_of_sample_monitoring_plan"],
            "unthrottled_summary_json": {"passed": True},
            "watch_risks": [{"risk_code": "over_veto_risk", "risk_level": "block"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled = _write_json(tmp_path / "unthrottled.json", _benchmark(["20260226"], "h1"))
    throttled = _write_json(tmp_path / "throttled.json", _benchmark(["20260226"], "h1"))
    review = _write_json(tmp_path / "review.json", {"risk_off_alpha_repair_passed": True})
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "over_veto_risk", "risk_level": "block"}]})
    watch_review = _write_json(tmp_path / "watch_review.json", {"review_status": "watch_risk_review_blocked"})
    oos_plan = _write_json(
        tmp_path / "oos_plan.json",
        {
            "rule_hash": "h1",
            "oos_windows": ["20260317"],
            "paired_run_requirement": {"unthrottled_required": True, "throttled_required": True},
            "pass_conditions": {"block_level_watch_risk_allowed": False},
        },
    )
    oos_result = _write_json(
        tmp_path / "oos_result.json",
        {
            "result_status": "oos_monitoring_passed_discussion_only",
            "allowed_next_status": "formal_review_candidate",
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
            "watch_risk_review_artifact": str(watch_review),
            "oos_monitoring_plan_artifact": str(oos_plan),
            "oos_monitoring_result_artifact": str(oos_result),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    failures = gate.validate_repair_flow_evidence_file()

    assert "repair OOS passed result can only allow observation_watch_discussion_allowed" in failures
    assert "repair OOS result attempted promotion status" in failures


def test_repair_flow_gate_accepts_predeclared_v6_without_replay_artifacts(monkeypatch, tmp_path: Path):
    oos_plan = _write_json(
        tmp_path / "oos_plan.json",
        {
            "rule_hash": "h6",
            "oos_windows": ["20260331", "20260401", "20260402"],
            "paired_run_requirement": {"unthrottled_required": True, "throttled_required": True},
            "pass_conditions": {"block_level_watch_risk_allowed": False},
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    failure = _write_json(tmp_path / "v5_oos_failed.json", {"result_status": "oos_monitoring_failed_blocked"})
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "repair_attempt_predeclared",
            "rule_version": "hard_event_alpha_candidate.neutral_over_veto_rebalance_guard.v6",
            "rule_hash": "h6",
            "predeclared_rules_json": {"rule_version": "v6"},
            "fixed_window_set_json": ["20260317"],
            "prohibited_actions_json": ["formal", "top", "production"],
            "artifacts": [
                {"artifact_type": "failure_attribution", "artifact_path": str(failure)},
                {"artifact_type": "oos_monitoring_plan", "artifact_path": str(oos_plan)},
            ],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h6",
            "flow_snapshot_artifact": str(flow),
            "oos_monitoring_plan_artifact": str(oos_plan),
            "unthrottled_benchmark_artifact": "",
            "throttled_benchmark_artifact": "",
            "repair_review_artifact": "",
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    assert gate.validate_repair_flow_evidence_file() == []


def test_repair_flow_gate_accepts_blocked_replay_with_failed_unthrottled(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "repair_review_blocked",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "watch_risks": [{"risk_code": "over_veto_risk", "risk_level": "info"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled_payload = _benchmark(["20260226"], "h1")
    unthrottled_payload["benchmark"]["passed"] = False
    unthrottled = _write_json(tmp_path / "unthrottled.json", unthrottled_payload)
    throttled = _write_json(tmp_path / "throttled.json", unthrottled_payload)
    review = _write_json(
        tmp_path / "review.json",
        {
            "risk_off_alpha_repair_passed": False,
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "over_veto_risk", "risk_level": "info"}]})
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    assert gate.validate_repair_flow_evidence_file() == []


def test_repair_flow_gate_blocks_archive_without_reopen_conditions(monkeypatch, tmp_path: Path):
    flow = _write_json(
        tmp_path / "flow.json",
        {
            "current_status": "repair_review_blocked",
            "rule_hash": "h1",
            "prohibited_actions_json": ["formal", "top", "production"],
            "watch_risks": [{"risk_code": "over_veto_risk", "risk_level": "info"}],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    unthrottled_payload = _benchmark(["20260226"], "h1")
    unthrottled_payload["benchmark"]["passed"] = False
    unthrottled = _write_json(tmp_path / "unthrottled.json", unthrottled_payload)
    throttled = _write_json(tmp_path / "throttled.json", unthrottled_payload)
    review = _write_json(tmp_path / "review.json", {"risk_off_alpha_repair_passed": False})
    risk = _write_json(tmp_path / "risks.json", {"risks": [{"risk_code": "over_veto_risk", "risk_level": "info"}]})
    archive = _write_json(
        tmp_path / "archive.json",
        {
            "archive_status": "failed_research_candidate_archived",
            "observation_watch_allowed": False,
            "prohibited_actions": ["formal", "top", "production", "posthoc_parameter_search"],
            "formal_candidate_allowed": False,
            "production_candidate_allowed": False,
        },
    )
    manifest = _write_json(
        tmp_path / "manifest.json",
        {
            "rule_hash": "h1",
            "flow_snapshot_artifact": str(flow),
            "unthrottled_benchmark_artifact": str(unthrottled),
            "throttled_benchmark_artifact": str(throttled),
            "repair_review_artifact": str(review),
            "watch_risk_register_artifact": str(risk),
            "failed_research_candidate_archive_artifact": str(archive),
        },
    )
    monkeypatch.setenv(gate.REPAIR_FLOW_EVIDENCE_ENV, str(manifest))

    assert "failed research candidate archive missing reopen conditions" in gate.validate_repair_flow_evidence_file()


def _benchmark(windows: list[str], rule_hash: str) -> dict:
    return {
        "rule_freeze": {"rule_hash": rule_hash},
        "benchmark": {"passed": True},
        "windows": [{"as_of_date": item} for item in windows],
        "formal_candidate_allowed": False,
        "production_candidate_allowed": False,
    }


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path

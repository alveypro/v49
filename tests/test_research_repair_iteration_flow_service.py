from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from openclaw.services.research_repair_iteration_flow_service import (
    attach_repair_artifact,
    close_repair_review,
    create_oos_monitoring_plan_artifact,
    create_oos_monitoring_result_artifact,
    create_repair_flow,
    create_watch_risk_review_artifact,
    export_repair_flow_evidence_manifest,
    register_rule_freeze,
    validate_repair_flow_artifacts,
)


def test_repair_flow_closes_v5_to_observation_discussion_only(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    create_repair_flow(
        conn,
        flow_id="flow_v5",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5",
        rule_hash="h1",
        repair_objective="fix neutral unthrottled and turnover",
        fixed_window_set=["20260226", "20260302"],
    )
    register_rule_freeze(
        conn,
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        rule_version="hard_event_alpha_candidate.neutral_consensus_turnover_guard.v5",
        rule_hash="h1",
        rule_spec={"rule_version": "v5"},
    )

    snapshot = close_repair_review(
        conn,
        flow_id="flow_v5",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )

    assert snapshot["current_status"] == "observation_watch_discussion_allowed"
    assert snapshot["formal_candidate_allowed"] is False
    assert snapshot["production_candidate_allowed"] is False
    assert "formal" in snapshot["prohibited_actions_json"]
    assert len(snapshot["watch_risks"]) >= 5
    assert snapshot["warning_reasons_json"] == []


def test_repair_flow_blocks_missing_unthrottled(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    flow = create_repair_flow(
        conn,
        flow_id="flow_missing",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
    )

    failures = validate_repair_flow_artifacts(
        flow,
        unthrottled_artifact_path="",
        throttled_artifact_path=str(artifacts["throttled"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )

    assert "missing_unthrottled_artifact" in failures


def test_repair_flow_blocks_window_and_hash_mismatch(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    _write_json(artifacts["throttled"], _benchmark(["20260227"], "h2"))
    flow = create_repair_flow(
        conn,
        flow_id="flow_mismatch",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
    )

    failures = validate_repair_flow_artifacts(
        flow,
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )

    assert "paired_benchmark_window_set_mismatch" in failures
    assert "paired_benchmark_rule_hash_mismatch" in failures


def test_repair_flow_close_is_idempotent_and_replaces_artifacts_and_risks(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    create_repair_flow(
        conn,
        flow_id="flow_idempotent",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
    )

    first = close_repair_review(
        conn,
        flow_id="flow_idempotent",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )
    second = close_repair_review(
        conn,
        flow_id="flow_idempotent",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )

    assert len(first["artifacts"]) == len(second["artifacts"]) == 4
    assert len(first["watch_risks"]) == len(second["watch_risks"]) == 5


def test_repair_flow_records_block_level_watch_risk_without_formal_promotion(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1", vetoed_count=30)
    create_repair_flow(
        conn,
        flow_id="flow_block_risk",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
    )

    snapshot = close_repair_review(
        conn,
        flow_id="flow_block_risk",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )

    assert snapshot["current_status"] == "observation_watch_discussion_allowed"
    assert "observation_watch_discussion" not in snapshot["next_allowed_actions_json"]
    assert "watch_risk_block_level:over_veto_risk" in snapshot["warning_reasons_json"]
    assert {item["risk_code"]: item["risk_level"] for item in snapshot["watch_risks"]}["over_veto_risk"] == "block"


def test_watch_risk_review_blocks_over_veto_and_records_artifact(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1", vetoed_count=30)
    create_repair_flow(
        conn,
        flow_id="flow_watch_review",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
        fixed_window_set=["20260226", "20260302"],
    )
    close_repair_review(
        conn,
        flow_id="flow_watch_review",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )
    manifest = export_repair_flow_evidence_manifest(conn, flow_id="flow_watch_review", output_dir=str(tmp_path / "flow"))[
        "repair_flow_evidence_manifest"
    ]

    review = create_watch_risk_review_artifact(
        conn,
        flow_id="flow_watch_review",
        evidence_manifest_path=manifest,
        output_dir=str(tmp_path / "review"),
    )
    exported = export_repair_flow_evidence_manifest(conn, flow_id="flow_watch_review", output_dir=str(tmp_path / "flow2"))

    assert review["review_status"] == "watch_risk_review_blocked"
    assert "over_veto_risk_indicates_possible_refusal_to_trade" in review["blocking_reasons"]
    assert review["formal_candidate_allowed"] is False
    assert exported["watch_risk_review_artifact"].endswith(".json")


def test_oos_monitoring_plan_is_predeclared_and_cannot_overlap_repair_windows(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    create_repair_flow(
        conn,
        flow_id="flow_oos_plan",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
        fixed_window_set=["20260226", "20260302"],
    )
    close_repair_review(
        conn,
        flow_id="flow_oos_plan",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )
    manifest = export_repair_flow_evidence_manifest(conn, flow_id="flow_oos_plan", output_dir=str(tmp_path / "flow"))[
        "repair_flow_evidence_manifest"
    ]

    plan = create_oos_monitoring_plan_artifact(
        conn,
        flow_id="flow_oos_plan",
        evidence_manifest_path=manifest,
        oos_windows=["20260317", "20260318", "20260319"],
        output_dir=str(tmp_path / "oos"),
    )

    assert plan["plan_status"] == "predeclared_oos_monitoring_only"
    assert plan["paired_run_requirement"]["unthrottled_required"] is True
    assert plan["pass_conditions"]["neutral_hit_rate_gte"] == 0.5
    assert plan["formal_candidate_allowed"] is False
    try:
        create_oos_monitoring_plan_artifact(
            conn,
            flow_id="flow_oos_plan",
            evidence_manifest_path=manifest,
            oos_windows=["20260302"],
            output_dir=str(tmp_path / "bad_oos"),
        )
    except ValueError as exc:
        assert "oos_windows_overlap_repair_windows:20260302" in str(exc)
    else:
        raise AssertionError("expected overlapping OOS window to fail")


def test_oos_monitoring_result_blocks_missing_regime_and_low_coverage(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    create_repair_flow(
        conn,
        flow_id="flow_oos_result",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
        fixed_window_set=["20260226", "20260302"],
    )
    close_repair_review(
        conn,
        flow_id="flow_oos_result",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )
    manifest = export_repair_flow_evidence_manifest(conn, flow_id="flow_oos_result", output_dir=str(tmp_path / "flow"))[
        "repair_flow_evidence_manifest"
    ]
    plan = create_oos_monitoring_plan_artifact(
        conn,
        flow_id="flow_oos_result",
        evidence_manifest_path=manifest,
        oos_windows=["20260317", "20260318"],
        output_dir=str(tmp_path / "oos"),
    )
    missing_regime_payload = _benchmark(["20260226", "20260302"], "h1")
    missing_regime_payload["benchmark"]["regime_split"].pop("risk_on")
    _write_json(artifacts["unthrottled"], missing_regime_payload)
    _write_json(artifacts["throttled"], missing_regime_payload)

    result = create_oos_monitoring_result_artifact(
        conn,
        flow_id="flow_oos_result",
        oos_plan_path=plan["oos_monitoring_plan_artifact"],
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        output_dir=str(tmp_path / "result"),
    )

    assert result["result_status"] == "oos_monitoring_failed_blocked"
    assert "oos_windows_do_not_match_predeclared_plan" in result["blocking_reasons"]
    assert "oos_missing_required_regime_split:risk_on" in result["blocking_reasons"]
    assert result["allowed_next_status"] == "research_blocked"
    assert result["formal_candidate_allowed"] is False


def test_oos_monitoring_result_passes_discussion_only_when_all_conditions_hold(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    artifacts = _write_artifacts(tmp_path, rule_hash="h1")
    oos_unthrottled = tmp_path / "oos_unthrottled.json"
    oos_throttled = tmp_path / "oos_throttled.json"
    _write_json(oos_unthrottled, _oos_benchmark(["20260317", "20260318", "20260319"], "h1"))
    _write_json(oos_throttled, _oos_benchmark(["20260317", "20260318", "20260319"], "h1"))
    create_repair_flow(
        conn,
        flow_id="flow_oos_pass",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=5,
        rule_version="v5",
        rule_hash="h1",
        repair_objective="fix neutral",
        fixed_window_set=["20260226", "20260302"],
    )
    close_repair_review(
        conn,
        flow_id="flow_oos_pass",
        unthrottled_artifact_path=str(artifacts["unthrottled"]),
        throttled_artifact_path=str(artifacts["throttled"]),
        attribution_artifact_path=str(artifacts["attribution"]),
        repair_review_artifact_path=str(artifacts["review"]),
    )
    manifest = export_repair_flow_evidence_manifest(conn, flow_id="flow_oos_pass", output_dir=str(tmp_path / "flow"))[
        "repair_flow_evidence_manifest"
    ]
    plan = create_oos_monitoring_plan_artifact(
        conn,
        flow_id="flow_oos_pass",
        evidence_manifest_path=manifest,
        oos_windows=["20260317", "20260318", "20260319"],
        output_dir=str(tmp_path / "oos"),
    )

    result = create_oos_monitoring_result_artifact(
        conn,
        flow_id="flow_oos_pass",
        oos_plan_path=plan["oos_monitoring_plan_artifact"],
        unthrottled_artifact_path=str(oos_unthrottled),
        throttled_artifact_path=str(oos_throttled),
        output_dir=str(tmp_path / "result"),
    )

    assert result["result_status"] == "oos_monitoring_passed_discussion_only"
    assert result["allowed_next_status"] == "observation_watch_discussion_allowed"
    assert result["formal_candidate_allowed"] is False


def test_archived_failed_research_candidate_blocks_new_repair_without_governance_reopen(tmp_path: Path):
    conn = sqlite3.connect(":memory:")
    archive = tmp_path / "archive.json"
    _write_json(archive, {"archive_status": "failed_research_candidate_archived"})
    create_repair_flow(
        conn,
        flow_id="flow_archive",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=6,
        rule_version="v6",
        rule_hash="h6",
        repair_objective="archive",
    )
    attach_repair_artifact(
        conn,
        flow_id="flow_archive",
        artifact_type="failed_research_candidate_archive",
        artifact_path=str(archive),
    )

    try:
        create_repair_flow(
            conn,
            flow_id="flow_v7",
            strategy="ensemble_core",
            candidate="hard_event_alpha_candidate",
            attempt_no=7,
            rule_version="v7",
            rule_hash="h7",
            repair_objective="should be blocked",
        )
    except ValueError as exc:
        assert "failed_research_candidate_archived:hard_event_alpha_candidate" in str(exc)
    else:
        raise AssertionError("expected archived candidate to block new repair")

    reopened = create_repair_flow(
        conn,
        flow_id="flow_v7_reopened",
        strategy="ensemble_core",
        candidate="hard_event_alpha_candidate",
        attempt_no=7,
        rule_version="v7",
        rule_hash="h7",
        repair_objective="governance approved reopen",
        governance_reopen_approved=True,
    )
    assert reopened["current_status"] == "repair_attempt_predeclared"


def _write_artifacts(tmp_path: Path, *, rule_hash: str, vetoed_count: int = 1) -> dict[str, Path]:
    paths = {
        "unthrottled": tmp_path / "unthrottled.json",
        "throttled": tmp_path / "throttled.json",
        "attribution": tmp_path / "attribution.json",
        "review": tmp_path / "review.json",
    }
    _write_json(paths["unthrottled"], _benchmark(["20260226", "20260302"], rule_hash, vetoed_count=vetoed_count))
    _write_json(paths["throttled"], _benchmark(["20260226", "20260302"], rule_hash, vetoed_count=vetoed_count))
    _write_json(paths["attribution"], {"summary": {"allocator_throttle_excess_delta": 0.0}})
    _write_json(
        paths["review"],
        {
            "risk_off_alpha_repair_passed": True,
            "formal_candidate_allowed": False,
            "formal_ranking_allowed": False,
            "production_candidate_allowed": False,
            "blocking_reasons": [],
            "warning_reasons": [],
        },
    )
    return paths


def _benchmark(windows: list[str], rule_hash: str, *, vetoed_count: int = 1) -> dict:
    return {
        "rule_freeze": {"rule_hash": rule_hash},
        "benchmark": {
            "passed": True,
            "after_cost_excess_return": 1.0,
            "hit_rate": 1.0,
            "max_drawdown": -1.0,
            "turnover": 0.5,
            "capacity_utilization": 0.01,
            "industry_concentration": 0.2,
            "regime_split": {
                "neutral": {"avg_after_cost_excess_return": 1.0, "hit_rate": 1.0, "window_count": 1},
                "risk_off": {"avg_after_cost_excess_return": 1.0, "hit_rate": 1.0, "window_count": 1},
                "risk_on": {"avg_after_cost_excess_return": 1.0, "hit_rate": 1.0, "window_count": 1},
            },
            "risk_contribution": {"source_strategy_weight_share": {"v4": 0.55}},
        },
        "windows": [
            {
                "as_of_date": item,
                "market_regime_label": "neutral" if idx == 0 else "risk_off",
                "shadow_portfolio": {
                    "shadow_weights": [{"ts_code": "000001.SZ"}, {"ts_code": "000002.SZ"}],
                    "neutral_vetoed_signals": [{"ts_code": f"000{i:03d}.SZ"} for i in range(vetoed_count)],
                    "cash_weight": 0.75,
                },
            }
            for idx, item in enumerate(windows)
        ],
    }


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _oos_benchmark(windows: list[str], rule_hash: str) -> dict:
    regimes = ["neutral", "risk_off", "risk_on"]
    return {
        "rule_freeze": {"rule_hash": rule_hash},
        "benchmark": {
            "passed": True,
            "after_cost_excess_return": 1.0,
            "hit_rate": 1.0,
            "max_drawdown": -1.0,
            "turnover": 0.5,
            "capacity_utilization": 0.01,
            "industry_concentration": 0.2,
            "regime_split": {
                "neutral": {"avg_after_cost_excess_return": 1.0, "hit_rate": 1.0, "window_count": 1},
                "risk_off": {"avg_after_cost_excess_return": 1.0, "hit_rate": 1.0, "window_count": 1},
                "risk_on": {"avg_after_cost_excess_return": 1.0, "hit_rate": 1.0, "window_count": 1},
            },
            "risk_contribution": {"source_strategy_weight_share": {"v4": 0.4, "v8": 0.3, "v9": 0.3}},
        },
        "windows": [
            {
                "as_of_date": item,
                "market_regime_label": regimes[idx],
                "shadow_portfolio": {
                    "shadow_weights": [{"ts_code": f"00000{j}.SZ"} for j in range(4)],
                    "neutral_vetoed_signals": [{"ts_code": "300001.SZ"}] if regimes[idx] == "neutral" else [],
                    "cash_weight": 0.55 if regimes[idx] == "neutral" else 0.25,
                },
            }
            for idx, item in enumerate(windows)
        ],
    }

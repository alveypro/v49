import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.explanation import NON_INVESTMENT_ADVICE, PUBLIC_REQUIRED_FIELDS, build_candidate_explanations


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _base_inputs(tmp_path: Path) -> dict[str, Path]:
    snapshot = _write_json(
        tmp_path / "candidate_observation_snapshot_latest.json",
        {
            "schema_version": "candidate_observation_snapshot.v1",
            "snapshot_date": "20260506",
            "items": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "stock_name": "A Corp",
                    "industry": "Tech",
                    "rank": 1,
                    "final_score": 150.0,
                    "selection_reason": "factor_strong(45), model_aligned(90%)",
                    "data_quality_level": "pass",
                    "data_quality_score": 99.0,
                    "lineage_run_id": "candidate-test",
                    "lineage_hash": "hash-a",
                }
            ],
        },
    )
    risk_state = _write_json(
        tmp_path / "candidate_risk_state_latest.json",
        {
            "schema_version": "candidate_risk_state.v1",
            "status": "passed",
            "items": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "risk_state": "degrade",
                    "reasons": ["portfolio_quality_blocked"],
                    "evidence": [{"source": "candidate_portfolio_quality", "quality_score": 22.0}],
                    "normal_observation_allowed": False,
                }
            ],
        },
    )
    observation = _write_json(
        tmp_path / "candidate_observation_result_latest.json",
        {
            "schema_version": "candidate_observation_result.v1",
            "status": "pending",
            "candidates": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "status": "pending",
                    "returns": {"5d": None},
                    "blocking_reasons": ["insufficient_5d_trade_dates"],
                }
            ],
        },
    )
    attribution = _write_json(
        tmp_path / "candidate_failure_attribution_latest.json",
        {
            "schema_version": "candidate_failure_attribution.v1",
            "items": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "primary_failure_category": "insufficient_observation_window",
                }
            ],
        },
    )
    portfolio = _write_json(
        tmp_path / "candidate_portfolio_latest.json",
        {
            "schema_version": "candidate_portfolio.v1",
            "items": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "weight": 0.35,
                    "portfolio_role": "core",
                }
            ],
        },
    )
    quality = _write_json(
        tmp_path / "candidate_portfolio_quality_latest.json",
        {
            "schema_version": "candidate_portfolio_quality.v1",
            "status": "blocked",
            "quality_score": 22.0,
            "blocking_reasons": ["portfolio_quality_score_below_minimum"],
        },
    )
    capacity = _write_json(
        tmp_path / "portfolio_capacity_report_latest.json",
        {
            "schema_version": "portfolio_capacity_report.v1",
            "items": [
                {
                    "ts_code": "AAA",
                    "state": "review",
                    "participation_rate": 0.06,
                    "reasons": ["capacity_participation_exceeds_watch_limit"],
                }
            ],
        },
    )
    return {
        "snapshot": snapshot,
        "risk_state": risk_state,
        "observation": observation,
        "attribution": attribution,
        "portfolio": portfolio,
        "quality": quality,
        "capacity": capacity,
    }


def test_public_explanation_has_four_plain_fields_and_boundary(tmp_path):
    paths = _base_inputs(tmp_path)

    public, internal, rejection = build_candidate_explanations(
        snapshot_path=paths["snapshot"],
        risk_state_path=paths["risk_state"],
        observation_result_path=paths["observation"],
        failure_attribution_path=paths["attribution"],
        portfolio_path=paths["portfolio"],
        portfolio_quality_path=paths["quality"],
        portfolio_capacity_path=paths["capacity"],
    )

    item = public["items"][0]
    assert public["status"] == "passed"
    assert all(item[field] for field in PUBLIC_REQUIRED_FIELDS)
    assert item["boundary"] == NON_INVESTMENT_ADVICE
    assert item["external_display_allowed"] is False
    assert internal["status"] == "passed"
    assert rejection["items"][0]["rejection_reasons"] == ["portfolio_quality_blocked"]


def test_internal_explanation_marks_review_when_evidence_missing(tmp_path):
    paths = _base_inputs(tmp_path)
    paths["risk_state"].write_text(
        json.dumps(
            {
                "schema_version": "candidate_risk_state.v1",
                "items": [{"observation_id": "20260506:AAA", "ts_code": "AAA", "risk_state": "review", "reasons": []}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    _, internal, _ = build_candidate_explanations(snapshot_path=paths["snapshot"], risk_state_path=paths["risk_state"])

    assert internal["status"] == "review"
    assert internal["review_reasons"]


def test_candidate_explanation_cli_writes_three_outputs(tmp_path):
    _base_inputs(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_explanations.py"),
            "--exp-dir",
            str(tmp_path),
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)

    assert payload["candidate_count"] == 1
    assert payload["rejection_count"] == 1
    assert (tmp_path / "candidate_public_explanation_latest.json").exists()
    assert (tmp_path / "candidate_internal_explanation_latest.json").exists()
    assert (tmp_path / "candidate_rejection_explanation_latest.json").exists()

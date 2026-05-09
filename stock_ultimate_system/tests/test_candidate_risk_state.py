import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.risk_state import ALLOWED_RISK_STATES, build_candidate_risk_state


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _snapshot(tmp_path: Path) -> Path:
    return _write_json(
        tmp_path / "candidate_observation_snapshot_latest.json",
        {
            "schema_version": "candidate_observation_snapshot.v1",
            "status": "frozen",
            "snapshot_date": "20260506",
            "items": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "stock_name": "A",
                    "industry": "Tech",
                    "selected_at": "20260506",
                    "data_quality_level": "pass",
                    "lineage_hash": "hash-a",
                },
                {
                    "observation_id": "20260506:BBB",
                    "ts_code": "BBB",
                    "stock_name": "B",
                    "industry": "Bank",
                    "selected_at": "20260506",
                    "data_quality_level": "pass",
                    "lineage_hash": "hash-b",
                },
            ],
        },
    )


def test_candidate_risk_state_uses_fixed_enum_and_reasons(tmp_path):
    snapshot = _snapshot(tmp_path)
    result = _write_json(
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
                    "hit_status": {"5d": "pending"},
                    "blocking_reasons": ["insufficient_5d_trade_dates"],
                },
                {
                    "observation_id": "20260506:BBB",
                    "ts_code": "BBB",
                    "status": "pending",
                    "returns": {"5d": None},
                    "hit_status": {"5d": "pending"},
                    "blocking_reasons": ["insufficient_5d_trade_dates"],
                },
            ],
        },
    )

    state, transitions, audit = build_candidate_risk_state(snapshot_path=snapshot, observation_result_path=result)

    assert set(state["allowed_states"]) == ALLOWED_RISK_STATES
    assert state["status"] == "passed"
    assert all(item["risk_state"] == "review" for item in state["items"])
    assert all(item["reasons"] for item in state["items"])
    assert len(transitions) == 2
    assert audit["summary"]["missing_reason_count"] == 0


def test_candidate_risk_state_degrades_when_portfolio_quality_blocked(tmp_path):
    snapshot = _snapshot(tmp_path)
    quality = _write_json(
        tmp_path / "candidate_portfolio_quality_latest.json",
        {
            "schema_version": "candidate_portfolio_quality.v1",
            "status": "blocked",
            "quality_score": 22.0,
            "blocking_reasons": ["portfolio_quality_score_below_minimum"],
        },
    )

    state, _, _ = build_candidate_risk_state(snapshot_path=snapshot, portfolio_quality_path=quality)

    assert state["state_counts"] == {"degrade": 2}
    assert all(not item["normal_observation_allowed"] for item in state["items"])


def test_candidate_risk_state_invalidates_failed_drawdown(tmp_path):
    snapshot = _snapshot(tmp_path)
    result = _write_json(
        tmp_path / "candidate_observation_result_latest.json",
        {
            "schema_version": "candidate_observation_result.v1",
            "status": "blocked",
            "candidates": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "status": "completed",
                    "returns": {"5d": -0.12},
                    "max_drawdown": -0.13,
                    "hit_status": {"5d": "miss"},
                }
            ],
        },
    )

    state, transitions, _ = build_candidate_risk_state(snapshot_path=snapshot, observation_result_path=result)

    item = next(row for row in state["items"] if row["ts_code"] == "AAA")
    assert item["risk_state"] == "invalid"
    assert "max_drawdown_exceeds_invalid_threshold" in item["reasons"]
    assert any(row["to_state"] == "invalid" for row in transitions)


def test_candidate_risk_state_closes_completed_observation(tmp_path):
    snapshot = _snapshot(tmp_path)
    result = _write_json(
        tmp_path / "candidate_observation_result_latest.json",
        {
            "schema_version": "candidate_observation_result.v1",
            "status": "passed",
            "candidates": [
                {
                    "observation_id": "20260506:AAA",
                    "ts_code": "AAA",
                    "status": "completed",
                    "returns": {"5d": 0.02},
                    "hit_status": {"5d": "hit"},
                    "max_drawdown": -0.01,
                }
            ],
        },
    )

    state, _, _ = build_candidate_risk_state(snapshot_path=snapshot, observation_result_path=result)

    item = next(row for row in state["items"] if row["ts_code"] == "AAA")
    assert item["risk_state"] == "closed"
    assert "observation_horizons_completed" in item["reasons"]


def test_candidate_risk_state_cli_writes_outputs(tmp_path):
    _snapshot(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_risk_state.py"),
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

    assert payload["candidate_count"] == 2
    assert (tmp_path / "candidate_risk_state_latest.json").exists()
    assert (tmp_path / "candidate_risk_state_transition.jsonl").exists()
    assert (tmp_path / "candidate_state_audit_latest.json").exists()

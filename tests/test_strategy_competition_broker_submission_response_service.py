from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_broker_submission_response_service import (
    build_strategy_competition_broker_submission_response_evidence,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _guard(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_broker_submission_guard.v1",
        "competition_run_id": "comp_test",
        "guard_status": "broker_submission_guard_passed" if passed else "broker_submission_guard_blocked",
        "passed": passed,
        "broker_submission_allowed": passed,
        "broker_guard_hash": "guard_hash_1",
        "broker_adapter": "paper_broker_adapter",
        "idempotency_key": "idem_1",
        "orders": [
            {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100},
            {"ts_code": "000002.SZ", "side": "buy", "target_qty": 200},
        ],
    }


def _response(guard_hash: str = "guard_hash_1") -> dict:
    return {
        "artifact_version": "strategy_competition_broker_submission_response.v1",
        "competition_run_id": "comp_test",
        "broker_guard_hash": guard_hash,
        "broker_adapter": "paper_broker_adapter",
        "idempotency_key": "idem_1",
        "order_responses": [
            {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100, "status": "dry_run_ack"},
            {"ts_code": "000002.SZ", "side": "buy", "target_qty": 200, "status": "accepted", "broker_order_ref": "B2"},
        ],
    }


def test_broker_submission_response_blocks_when_guard_not_passed(tmp_db: Path, tmp_path: Path):
    guard_path = _write_json(tmp_path / "guard.json", _guard(False))
    response_path = _write_json(tmp_path / "response.json", _response())
    conn = sqlite3.connect(str(tmp_db))

    evidence = build_strategy_competition_broker_submission_response_evidence(
        conn,
        broker_submission_guard_artifact_path=str(guard_path),
        broker_submission_response_artifact_path=str(response_path),
        output_dir=tmp_path / "evidence",
    )

    conn.close()
    assert evidence["response_status"] == "broker_submission_response_blocked"
    assert evidence["passed"] is False
    assert evidence["broker_submission_confirmed"] is False
    assert evidence["execution_fills_confirmed"] is False
    assert "broker_submission_guard_not_passed" in evidence["blocking_reasons"]


def test_broker_submission_response_requires_guard_hash_and_no_fills(tmp_db: Path, tmp_path: Path):
    guard_path = _write_json(tmp_path / "guard.json", _guard(True))
    response = _response("wrong_hash")
    response["order_responses"][1]["fills"] = [{"fill_qty": 200, "fill_price": 10.0}]
    response_path = _write_json(tmp_path / "response.json", response)
    conn = sqlite3.connect(str(tmp_db))

    evidence = build_strategy_competition_broker_submission_response_evidence(
        conn,
        broker_submission_guard_artifact_path=str(guard_path),
        broker_submission_response_artifact_path=str(response_path),
        output_dir=tmp_path / "evidence",
    )

    conn.close()
    assert evidence["passed"] is False
    assert "broker_submission_response_guard_hash_mismatch" in evidence["blocking_reasons"]
    assert "broker_submission_response_must_not_include_fills:000002.SZ" in evidence["blocking_reasons"]


def test_broker_submission_response_accepts_valid_adapter_response_without_fills(tmp_db: Path, tmp_path: Path):
    guard_path = _write_json(tmp_path / "guard.json", _guard(True))
    response_path = _write_json(tmp_path / "response.json", _response())
    conn = sqlite3.connect(str(tmp_db))

    evidence = build_strategy_competition_broker_submission_response_evidence(
        conn,
        broker_submission_guard_artifact_path=str(guard_path),
        broker_submission_response_artifact_path=str(response_path),
        output_dir=tmp_path / "evidence",
    )

    conn.close()
    assert evidence["response_status"] == "broker_submission_response_accepted"
    assert evidence["passed"] is True
    assert evidence["broker_submission_confirmed"] is True
    assert evidence["execution_fills_confirmed"] is False
    assert evidence["response_contract"]["does_not_confirm_fills"] is True
    assert "broker_submission_response_is_not_fill_evidence" in evidence["hard_boundaries"]
    assert evidence["response_evidence_hash"]

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_broker_submission_guard_service import (
    build_strategy_competition_broker_submission_guard,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _authority(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_live_order_authority_check.v1",
        "competition_run_id": "comp_test",
        "authority_status": "live_order_submission_allowed" if passed else "live_order_submission_blocked",
        "passed": passed,
        "live_order_submission_allowed": passed,
        "authority_hash": "authority_hash_1",
    }


def _broker_intent(authority_hash: str = "authority_hash_1") -> dict:
    return {
        "artifact_version": "strategy_competition_broker_submission_intent.v1",
        "competition_run_id": "comp_test",
        "live_order_authority_hash": authority_hash,
        "submission_mode": "dry_run",
        "broker_adapter": "paper_broker_adapter",
        "idempotency_key": "comp_test:authority_hash_1:dry_run",
        "orders": [
            {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100},
            {"ts_code": "000002.SZ", "side": "buy", "target_qty": 200},
        ],
    }


def test_broker_submission_guard_blocks_without_live_order_authority(tmp_db: Path, tmp_path: Path):
    authority_path = _write_json(tmp_path / "authority.json", _authority(False))
    intent_path = _write_json(tmp_path / "intent.json", _broker_intent())
    conn = sqlite3.connect(str(tmp_db))

    guard = build_strategy_competition_broker_submission_guard(
        conn,
        live_order_authority_artifact_path=str(authority_path),
        broker_submission_intent_artifact_path=str(intent_path),
        output_dir=tmp_path / "guard",
    )

    conn.close()
    assert guard["guard_status"] == "broker_submission_guard_blocked"
    assert guard["passed"] is False
    assert guard["broker_submission_allowed"] is False
    assert "live_order_authority_not_allowed" in guard["blocking_reasons"]


def test_broker_submission_guard_requires_authority_hash_match(tmp_db: Path, tmp_path: Path):
    authority_path = _write_json(tmp_path / "authority.json", _authority(True))
    intent_path = _write_json(tmp_path / "intent.json", _broker_intent("wrong_hash"))
    conn = sqlite3.connect(str(tmp_db))

    guard = build_strategy_competition_broker_submission_guard(
        conn,
        live_order_authority_artifact_path=str(authority_path),
        broker_submission_intent_artifact_path=str(intent_path),
        output_dir=tmp_path / "guard",
    )

    conn.close()
    assert guard["passed"] is False
    assert "broker_submission_intent_authority_hash_mismatch" in guard["blocking_reasons"]


def test_broker_submission_guard_passes_only_for_allowed_authority_and_valid_intent(tmp_db: Path, tmp_path: Path):
    authority_path = _write_json(tmp_path / "authority.json", _authority(True))
    intent_path = _write_json(tmp_path / "intent.json", _broker_intent())
    conn = sqlite3.connect(str(tmp_db))

    guard = build_strategy_competition_broker_submission_guard(
        conn,
        live_order_authority_artifact_path=str(authority_path),
        broker_submission_intent_artifact_path=str(intent_path),
        output_dir=tmp_path / "guard",
    )

    conn.close()
    assert guard["guard_status"] == "broker_submission_guard_passed"
    assert guard["passed"] is True
    assert guard["broker_submission_allowed"] is True
    assert guard["broker_guard_contract"]["does_not_record_fills"] is True
    assert "broker_submission_guard_does_not_execute_orders" in guard["hard_boundaries"]
    assert guard["broker_guard_hash"]

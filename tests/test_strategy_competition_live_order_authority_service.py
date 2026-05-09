from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from openclaw.services.strategy_competition_live_order_authority_service import (
    build_strategy_competition_live_order_authority_check,
)


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def _approval(passed: bool = True) -> dict:
    return {
        "artifact_version": "strategy_competition_human_release_approval.v1",
        "competition_run_id": "comp_test",
        "approval_status": "human_release_approved" if passed else "human_release_approval_blocked",
        "passed": passed,
        "production_release_authorized": passed,
        "live_order_authority_granted": passed,
        "approval_hash": "approval_hash_1",
    }


def _intent(approval_hash: str = "approval_hash_1") -> dict:
    return {
        "artifact_version": "strategy_competition_live_order_intent.v1",
        "competition_run_id": "comp_test",
        "human_release_approval_hash": approval_hash,
        "orders": [
            {"ts_code": "000001.SZ", "side": "buy", "target_qty": 100},
            {"ts_code": "000002.SZ", "side": "buy", "target_qty": 200},
            {"ts_code": "000003.SZ", "side": "buy", "target_qty": 300},
            {"ts_code": "000004.SZ", "side": "buy", "target_qty": 400},
            {"ts_code": "000005.SZ", "side": "buy", "target_qty": 500},
        ],
    }


def test_live_order_authority_blocks_current_blocked_human_approval(tmp_db: Path, tmp_path: Path):
    approval_path = _write_json(tmp_path / "approval.json", _approval(False))
    intent_path = _write_json(tmp_path / "intent.json", _intent())
    conn = sqlite3.connect(str(tmp_db))

    check = build_strategy_competition_live_order_authority_check(
        conn,
        human_release_approval_artifact_path=str(approval_path),
        live_order_intent_artifact_path=str(intent_path),
        output_dir=tmp_path / "authority",
    )

    conn.close()
    assert check["authority_status"] == "live_order_submission_blocked"
    assert check["passed"] is False
    assert check["live_order_submission_allowed"] is False
    assert "human_release_approval_not_authorized" in check["blocking_reasons"]


def test_live_order_authority_requires_intent_hash_match(tmp_db: Path, tmp_path: Path):
    approval_path = _write_json(tmp_path / "approval.json", _approval(True))
    intent_path = _write_json(tmp_path / "intent.json", _intent("different_hash"))
    conn = sqlite3.connect(str(tmp_db))

    check = build_strategy_competition_live_order_authority_check(
        conn,
        human_release_approval_artifact_path=str(approval_path),
        live_order_intent_artifact_path=str(intent_path),
        output_dir=tmp_path / "authority",
    )

    conn.close()
    assert check["passed"] is False
    assert "live_order_intent_approval_hash_mismatch" in check["blocking_reasons"]


def test_live_order_authority_allows_only_after_approved_human_gate_and_matching_intent(tmp_db: Path, tmp_path: Path):
    approval_path = _write_json(tmp_path / "approval.json", _approval(True))
    intent_path = _write_json(tmp_path / "intent.json", _intent())
    conn = sqlite3.connect(str(tmp_db))

    check = build_strategy_competition_live_order_authority_check(
        conn,
        human_release_approval_artifact_path=str(approval_path),
        live_order_intent_artifact_path=str(intent_path),
        output_dir=tmp_path / "authority",
    )

    conn.close()
    assert check["authority_status"] == "live_order_submission_allowed"
    assert check["passed"] is True
    assert check["live_order_submission_allowed"] is True
    assert check["authority_contract"]["does_not_execute_orders"] is True
    assert "live_order_authority_check_does_not_execute_orders" in check["hard_boundaries"]
    assert check["authority_hash"]

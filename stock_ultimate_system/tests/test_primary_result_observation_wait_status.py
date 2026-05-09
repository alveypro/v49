import json
from datetime import datetime, timezone
from pathlib import Path

from tests.primary_result_test_support import seed_current_primary_pointer
from src.primary_result_observation_wait_status import build_primary_result_observation_wait_status


def _write_artifacts(tmp_path: Path) -> Path:
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n"
        "002463.SZ,沪电股份,元器件,strong_buy,medium,120\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"002463.SZ"}]}', encoding="utf-8")
    (exp_dir / "primary_result_audit_latest.json").write_text(
        '{"audit_status":"passed","result_id":"primary:002463.SZ","ts_code":"002463.SZ"}',
        encoding="utf-8",
    )
    (exp_dir / "primary_result_execution_latest.json").write_text(
        '{"execution_status":"ready","result_id":"primary:002463.SZ","ts_code":"002463.SZ"}',
        encoding="utf-8",
    )
    (exp_dir / "primary_result_rollback_latest.json").write_text(
        '{"rollback_status":"not_required","result_id":"primary:002463.SZ","ts_code":"002463.SZ"}',
        encoding="utf-8",
    )
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(
            {
                "observation_status": "observing",
                "result_id": "primary:002463.SZ",
                "ts_code": "002463.SZ",
                "observation_window": {"started_at": "2026-04-20T01:30:00Z", "ended_at": None},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    seed_current_primary_pointer(tmp_path, ts_code="002463.SZ", stock_name="沪电股份", lifecycle_stage="L4")
    return exp_dir


def test_observation_wait_status_reports_pending_before_window_start(tmp_path):
    exp_dir = _write_artifacts(tmp_path)

    exit_code, payload = build_primary_result_observation_wait_status(
        exp_dir=exp_dir,
        now=datetime(2026, 4, 18, 3, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["status"] == "pending_window"
    assert payload["observation_window"]["start_date"] == "2026-04-20"
    assert payload["observation_window"]["has_started"] is False


def test_observation_wait_status_reports_ready_after_window_start(tmp_path):
    exp_dir = _write_artifacts(tmp_path)

    exit_code, payload = build_primary_result_observation_wait_status(
        exp_dir=exp_dir,
        now=datetime(2026, 4, 20, 2, 0, tzinfo=timezone.utc),
    )

    assert exit_code == 0
    assert payload["status"] == "ready_for_data_check"
    assert payload["observation_window"]["has_started"] is True

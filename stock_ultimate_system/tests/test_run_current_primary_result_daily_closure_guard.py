import json

import pytest

from scripts.run_current_primary_result_daily_closure import _observation_window_start, run_current_primary_result_daily_closure
from tests.primary_result_test_support import seed_current_primary_pointer


def test_current_daily_closure_rejects_observation_for_different_candidate(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(
            {
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "observation_window": {"started_at": "2026-04-15T09:30:00Z"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="does not match current primary result"):
        _observation_window_start(
            exp_dir,
            expected_result_id="primary:000002.SZ",
            expected_ts_code="000002.SZ",
        )


def test_current_daily_closure_accepts_matching_observation(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(
            {
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "observation_window": {"started_at": "2026-04-15T09:30:00Z"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    assert (
        _observation_window_start(
            exp_dir,
            expected_result_id="primary:000001.SZ",
            expected_ts_code="000001.SZ",
        )
        == "2026-04-15T09:30:00Z"
    )


def test_current_daily_closure_returns_pending_when_window_has_not_started(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    config = tmp_path / "config" / "settings.yaml"
    output = exp_dir / "primary_result_daily_closure_latest.json"
    readiness_output = exp_dir / "primary_result_market_data_readiness_latest.json"
    exp_dir.mkdir(parents=True)
    config.parent.mkdir(parents=True)
    config.write_text(
        "data:\n  sqlite_db_path: /tmp/unused.db\n  sqlite_table: daily_trading_data\n  benchmark_indices: [000001.SH]\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n000001.SZ,平安银行,银行,strong_buy,medium,155\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"000001.SZ"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    seed_current_primary_pointer(tmp_path, ts_code="000001.SZ", stock_name="平安银行", lifecycle_stage="L4")
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(
            {
                "result_id": "primary:000001.SZ",
                "ts_code": "000001.SZ",
                "observation_window": {"started_at": "2026-04-20T09:30:00Z"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    exit_code, payload = run_current_primary_result_daily_closure(
        config_path=config,
        exp_dir=exp_dir,
        window_end="2026-04-18",
        output_path=output,
    )

    assert exit_code == 0
    assert payload["status"] == "pending_window"
    assert payload["blocking_reasons"] == []
    assert payload["window_start"] == "2026-04-20T09:30:00Z"
    assert payload["window_end"] == "2026-04-18"
    assert json.loads(output.read_text(encoding="utf-8"))["status"] == "pending_window"
    readiness = json.loads(readiness_output.read_text(encoding="utf-8"))
    assert readiness["status"] == "pending_window"
    assert readiness["ts_code"] == "000001.SZ"
    assert readiness["benchmark_ts_code"] == "000001.SH"
    assert readiness["window_start"] == "2026-04-20T09:30:00Z"
    assert readiness["window_end"] == "2026-04-18"


def test_current_daily_closure_writes_blocked_report_when_observation_is_for_other_candidate(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    config = tmp_path / "config" / "settings.yaml"
    output = exp_dir / "primary_result_daily_closure_latest.json"
    readiness_output = exp_dir / "primary_result_market_data_readiness_latest.json"
    exp_dir.mkdir(parents=True)
    config.parent.mkdir(parents=True)
    config.write_text(
        "data:\n  sqlite_db_path: /tmp/unused.db\n  sqlite_table: daily_trading_data\n  benchmark_indices: [000001.SH]\n",
        encoding="utf-8",
    )
    (exp_dir / "candidates_top_latest.csv").write_text(
        "ts_code,stock_name,industry,signal,risk_level,final_score\n600522.SH,中天科技,通信设备,strong_buy,medium,142.01\n",
        encoding="utf-8",
    )
    (exp_dir / "daily_research_status_latest.json").write_text('{"state":"completed"}', encoding="utf-8")
    (exp_dir / "buylist_latest.json").write_text('{"items":[{"ts_code":"600522.SH"}]}', encoding="utf-8")
    (exp_dir / "governance_audit_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t1_execution_checklist_latest.json").write_text("{}", encoding="utf-8")
    (exp_dir / "t12_rollback_drill_latest.json").write_text('{"triggered": false}', encoding="utf-8")
    seed_current_primary_pointer(tmp_path, ts_code="600522.SH", stock_name="中天科技", lifecycle_stage="L4")
    (exp_dir / "primary_result_observation_latest.json").write_text(
        json.dumps(
            {
                "result_id": "primary:002463.SZ",
                "ts_code": "002463.SZ",
                "observation_window": {"started_at": "2026-04-20T09:30:00Z"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    exit_code, payload = run_current_primary_result_daily_closure(
        config_path=config,
        exp_dir=exp_dir,
        output_path=output,
    )

    assert exit_code == 1
    assert payload["status"] == "blocked"
    assert "does not match current primary result" in payload["blocking_reasons"][0]
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["status"] == "blocked"
    readiness = json.loads(readiness_output.read_text(encoding="utf-8"))
    assert readiness["status"] == "blocked"
    assert readiness["ts_code"] == "600522.SH"
    assert readiness["benchmark_ts_code"] == "000001.SH"
    assert "does not match current primary result" in readiness["blocking_reasons"][0]

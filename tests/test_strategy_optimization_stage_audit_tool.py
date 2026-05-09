from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

from openclaw.services.lineage_service import apply_professional_migrations, insert_signal_run, new_run_id, replace_signal_items


_P = Path(__file__).resolve().parents[1] / "_archive" / "tools" / "strategy_optimization_stage_audit.py"
_SPEC = importlib.util.spec_from_file_location("strategy_optimization_stage_audit_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
stage_audit_tool = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(stage_audit_tool)


def _credible_backtest() -> dict:
    return {
        "point_in_time_data": True,
        "suspension_and_limit_handling": True,
        "volume_constraint": True,
        "cost_model": True,
        "slippage_model": True,
        "in_sample_out_of_sample_split": True,
        "parameter_sensitivity": True,
        "failed_backtests_recorded": True,
        "sample": {"in_sample": "train_windows:3", "out_of_sample": "test_windows:3"},
        "metrics": {"win_rate": 0.56, "max_drawdown": 0.09, "signal_density": 0.04, "test_windows": 3},
    }


def test_strategy_optimization_stage_audit_tool_writes_artifacts(tmp_db, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "v5")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="v5",
        trade_date="2026-05-05",
        data_version="trade_date:20260505",
        code_version="git:test:stage-tool",
        param_version="param:v5:test",
        status="success",
        summary={"backtest_credibility": _credible_backtest()},
    )
    replace_signal_items(conn, run_id=run_id, items=[{"ts_code": "000001.SZ", "score": 91, "rank_idx": 1}])
    conn.close()
    rejected_path = tmp_path / "rejected.json"
    rejected_path.write_text(
        json.dumps(
            {
                "rejected_artifacts": [
                    {
                        "artifact_path": "logs/openclaw/backtest_sweep_v8_rejected.json",
                        "strategy": "v8",
                        "reason": "eligible_for_formal_ranking_false",
                        "reused_as_runtime_default": False,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = stage_audit_tool.run_stage_audit(
        db_path=str(tmp_db),
        trade_date="2026-05-05",
        rejected_artifacts_path=str(rejected_path),
        output_dir=str(tmp_path / "audit"),
    )

    assert payload["passed"] is True
    assert Path(payload["artifacts"]["json"]).exists()
    assert Path(payload["artifacts"]["markdown"]).exists()
    saved = json.loads(Path(payload["artifacts"]["json"]).read_text(encoding="utf-8"))
    assert saved["policy"]["source_document"] == "docs/AIRIVO_STRATEGY_OPTIMIZATION_UPGRADE_EXECUTION_PLAN.md"
    assert saved["eligible_strategies"] == ["v5"]


def test_strategy_optimization_stage_audit_tool_reads_rejected_jsonl(tmp_db, tmp_path: Path):
    conn = sqlite3.connect(str(tmp_db))
    apply_professional_migrations(conn)
    run_id = new_run_id("scan", "v5")
    insert_signal_run(
        conn,
        run_id=run_id,
        run_type="scan",
        strategy="v5",
        trade_date="2026-05-05",
        data_version="trade_date:20260505",
        code_version="git:test:stage-tool",
        param_version="param:v5:test",
        status="success",
        summary={"backtest_credibility": _credible_backtest()},
    )
    replace_signal_items(conn, run_id=run_id, items=[{"ts_code": "000001.SZ", "score": 91, "rank_idx": 1}])
    conn.close()
    rejected_path = tmp_path / "rejected.jsonl"
    rejected_path.write_text(
        '{"artifact_path":"logs/openclaw/backtest_sweep_v6_failed.json","strategy":"v6","reason":"quality_floor_failed","reused_as_runtime_default":true}\n',
        encoding="utf-8",
    )

    payload = stage_audit_tool.run_stage_audit(
        db_path=str(tmp_db),
        trade_date="2026-05-05",
        rejected_artifacts_path=str(rejected_path),
        output_dir=str(tmp_path / "audit"),
    )

    assert payload["passed"] is False
    assert "rejected_artifact_reused_or_missing_reason" in payload["blocking_reasons"]
    assert payload["rejected_artifact_violations"][0]["artifact_path"].endswith("backtest_sweep_v6_failed.json")

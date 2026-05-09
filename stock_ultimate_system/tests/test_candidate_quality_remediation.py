import json
import subprocess
import sys
from pathlib import Path

from src.candidate_quality.remediation import build_candidate_quality_remediation_plan


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def test_candidate_quality_remediation_plan_maps_real_blockers(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)
    _write_json(exp_dir / "candidate_quality_proof_report_latest.json", {"status": "blocked"})
    _write_json(
        exp_dir / "candidate_quality_20_sample_report.json",
        {"status": "blocked", "sample_count": 3, "required_sample_count": 20},
    )
    _write_json(
        exp_dir / "realistic_backtest_latest.json",
        {"status": "failed", "summary": {"rule_block_stats": {"insufficient_future_trade_dates": 2}}},
    )
    _write_json(
        exp_dir / "candidate_portfolio_latest.json",
        {
            "status": "review",
            "policy": {"target_max_industry_weight": 0.5},
            "summary": {"top_industry": "通信设备", "top_industry_weight": 0.65},
            "items": [
                {"ts_code": "AAA", "stock_name": "A", "industry": "通信设备", "weight": 0.35, "final_score": 150},
                {"ts_code": "BBB", "stock_name": "B", "industry": "半导体", "weight": 0.35, "final_score": 145},
                {"ts_code": "CCC", "stock_name": "C", "industry": "通信设备", "weight": 0.30, "final_score": 130},
            ],
        },
    )
    _write_json(
        exp_dir / "portfolio_capacity_report_latest.json",
        {
            "status": "review",
            "summary": {"worst_participation_rate": 0.052},
            "items": [
                {"ts_code": "CCC", "state": "review", "warn_participation_rate": 0.05, "reasons": ["capacity_participation_exceeds_watch_limit"]}
            ],
        },
    )
    _write_json(
        exp_dir / "candidate_portfolio_quality_latest.json",
        {
            "status": "blocked",
            "quality_score": 24.0,
            "minimum_quality_score": 60.0,
            "penalties": [{"name": "realistic_backtest_blocked", "points": 18}],
        },
    )
    _write_json(exp_dir / "candidate_risk_state_latest.json", {"state_counts": {"degrade": 3}})

    plan = build_candidate_quality_remediation_plan(exp_dir=exp_dir)

    assert plan["status"] == "blocked"
    assert plan["next_run_order"] == ["R1", "R2", "R3", "R4", "R5", "R6"]
    assert all(action["can_be_fixed_now"] is False for action in plan["actions"])
    r3 = next(action for action in plan["actions"] if action["id"] == "R3")
    assert r3["preferred_replacement_target"]["ts_code"] == "CCC"
    assert r3["required_weight_reduction"] == 0.15


def test_candidate_quality_remediation_cli_writes_output(tmp_path):
    exp_dir = tmp_path / "data" / "experiments"
    exp_dir.mkdir(parents=True)

    completed = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "build_candidate_quality_remediation_plan.py"),
            "--exp-dir",
            str(exp_dir),
            "--json",
        ],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)

    assert payload["status"] == "blocked"
    assert (exp_dir / "candidate_quality_remediation_plan_latest.json").exists()

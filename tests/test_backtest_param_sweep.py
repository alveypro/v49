from __future__ import annotations

import json
from pathlib import Path

from openclaw.research.backtest_param_sweep import (
    SweepConfig,
    _build_sweep_aggregate_result,
    _run_engine_with_timeout,
    compute_objective,
    parse_float_list,
    parse_int_list,
    run_param_sweep,
)
from openclaw.services.backtest_credibility_service import build_backtest_credibility_audit, evaluate_backtest_credibility


def test_parse_int_list_fallback():
    assert parse_int_list("", [1, 2, 3]) == [1, 2, 3]


def test_parse_int_list_values():
    assert parse_int_list(" 10, 20,30 ", [1]) == [10, 20, 30]


def test_parse_float_list_values():
    assert parse_float_list(" 0.06, 0.08,0.1 ", [0.08]) == [0.06, 0.08, 0.1]


def test_parse_float_list_fallback():
    assert parse_float_list("", [0.08]) == [0.08]


def test_compute_objective_penalizes_failure():
    score = compute_objective(
        summary={"win_rate": 0.8, "max_drawdown": 0.1, "signal_density": 0.05},
        rolling={},
        test_rows=[],
        status="failed",
    )
    assert score < -1000


def test_compute_objective_prefers_better_win_rate():
    a = compute_objective(
        summary={"win_rate": 0.70, "max_drawdown": 0.1, "signal_density": 0.03},
        rolling={"windows_total": 6, "failed_windows": []},
        test_rows=[{"summary": {"win_rate": 0.65}}, {"summary": {"win_rate": 0.75}}],
        status="success",
    )
    b = compute_objective(
        summary={"win_rate": 0.50, "max_drawdown": 0.1, "signal_density": 0.03},
        rolling={"windows_total": 6, "failed_windows": []},
        test_rows=[{"summary": {"win_rate": 0.45}}, {"summary": {"win_rate": 0.55}}],
        status="success",
    )
    assert a > b


def test_compute_objective_preserves_zero_drawdown():
    zero_dd = compute_objective(
        summary={"win_rate": 1.0, "max_drawdown": 0.0, "signal_density": 0.1},
        rolling={"windows_total": 3, "failed_windows": []},
        test_rows=[],
        status="success",
    )
    default_dd = compute_objective(
        summary={"win_rate": 1.0, "signal_density": 0.1},
        rolling={"windows_total": 3, "failed_windows": []},
        test_rows=[],
        status="success",
    )

    assert zero_dd > default_dd


def test_run_engine_with_timeout_passthrough():
    cfg = SweepConfig(
        strategy="v8",
        module_path=Path("/tmp/fake.py"),
        output_dir=Path("/tmp"),
        date_from="2025-01-01",
        date_to="2025-06-01",
        mode="single",
        train_window_days=180,
        test_window_days=60,
        step_days=60,
        score_thresholds=[50],
        sample_sizes=[100],
        holding_days=[5],
        per_run_timeout_sec=None,
    )

    class FakeEngine:
        def run(self, strategy, date_from, date_to, params):
            return {"status": "success", "result": {"summary": {"win_rate": 0.5}}}

    out = _run_engine_with_timeout(
        engine=FakeEngine(),
        cfg=cfg,
        params={},
    )
    assert out["status"] == "success"


def test_run_engine_with_timeout_converts_timeout_to_failed():
    cfg = SweepConfig(
        strategy="combo",
        module_path=Path("/tmp/fake.py"),
        output_dir=Path("/tmp"),
        date_from="2025-01-01",
        date_to="2025-06-01",
        mode="single",
        train_window_days=180,
        test_window_days=60,
        step_days=60,
        score_thresholds=[60],
        sample_sizes=[100],
        holding_days=[6],
        per_run_timeout_sec=3,
    )

    class FakeEngine:
        def run(self, strategy, date_from, date_to, params):
            raise TimeoutError("per-run timeout after 3s")

    out = _run_engine_with_timeout(
        engine=FakeEngine(),
        cfg=cfg,
        params={},
    )
    assert out["status"] == "failed"
    assert out["error"]


def test_param_sweep_passes_v8_risk_control_grid(monkeypatch, tmp_path):
    seen = []

    def fake_run_engine_with_timeout(*, engine, cfg, params):
        seen.append(dict(params))
        return {
            "run_id": f"run_{len(seen)}",
            "status": "success",
            "result": {
                "summary": {
                    "win_rate": 0.55,
                    "max_drawdown": 0.18,
                    "signal_density": 0.04,
                    "tradeability_filter_enabled": True,
                    "volume_constraint_enabled": True,
                    "trading_cost": {"slippage_bp": 10.0, "expected_cost_pct": 0.01},
                    "risk_control": {
                        "max_stop_loss_pct": params["max_stop_loss_pct"],
                        "max_take_profit_pct": params["max_take_profit_pct"],
                    },
                    "defensive_allocator": {
                        "contract": {"role": "defensive_allocator_overlay"},
                        "promotion_eligible": False,
                    },
                },
                "rolling": {
                    "train_test_separated": True,
                    "train_windows": 2,
                    "test_windows": 2,
                    "windows_total": 2,
                    "failed_windows": [],
                },
                "window_results": {"test": [{"summary": {"win_rate": 0.55}}, {"summary": {"win_rate": 0.56}}]},
            },
        }

    monkeypatch.setattr("openclaw.research.backtest_param_sweep._run_engine_with_timeout", fake_run_engine_with_timeout)

    cfg = SweepConfig(
        strategy="v8",
        module_path=Path("/tmp/fake.py"),
        output_dir=tmp_path,
        date_from="2025-01-01",
        date_to="2025-06-01",
        mode="rolling",
        train_window_days=180,
        test_window_days=60,
        step_days=60,
        score_thresholds=[40],
        sample_sizes=[80],
        holding_days=[4, 6],
        max_stop_loss_pcts=[0.06, 0.08],
        max_take_profit_pcts=[0.10],
        per_run_timeout_sec=1,
    )

    out = run_param_sweep(cfg)

    assert out["status"] == "success"
    assert len(seen) == 4
    assert {x["holding_days"] for x in seen} == {4, 6}
    assert {x["max_stop_loss_pct"] for x in seen} == {0.06, 0.08}
    assert {x["max_take_profit_pct"] for x in seen} == {0.10}
    assert out["best"]["max_take_profit_pct"] == 0.10
    artifact = json.loads(Path(out["artifacts"]["json"]).read_text(encoding="utf-8"))
    assert artifact["rows"][0]["defensive_allocator"]["contract"]["role"] == "defensive_allocator_overlay"


def test_param_sweep_passes_return_exit_grid(monkeypatch, tmp_path):
    seen = []

    def fake_run_engine_with_timeout(*, engine, cfg, params):
        seen.append(dict(params))
        return {
            "run_id": f"run_{len(seen)}",
            "status": "success",
            "result": {
                "summary": {
                    "win_rate": 0.52,
                    "max_drawdown": 0.20,
                    "signal_density": 0.05,
                    "tradeability_filter_enabled": True,
                    "volume_constraint_enabled": True,
                    "trading_cost": {"slippage_bp": 10.0},
                    "risk_control": {
                        "stop_loss": params["stop_loss"],
                        "take_profit": params["take_profit"],
                    },
                },
                "rolling": {
                    "train_test_separated": True,
                    "train_windows": 2,
                    "test_windows": 2,
                    "windows_total": 2,
                    "failed_windows": [],
                },
                "window_results": {"test": [{"summary": {"win_rate": 0.52}}, {"summary": {"win_rate": 0.50}}]},
            },
        }

    monkeypatch.setattr("openclaw.research.backtest_param_sweep._run_engine_with_timeout", fake_run_engine_with_timeout)

    cfg = SweepConfig(
        strategy="v6",
        module_path=Path("/tmp/fake.py"),
        output_dir=tmp_path,
        date_from="2025-01-01",
        date_to="2025-06-01",
        mode="rolling",
        train_window_days=180,
        test_window_days=60,
        step_days=60,
        score_thresholds=[65],
        sample_sizes=[80],
        holding_days=[4],
        stop_losses=[-0.03, -0.05],
        take_profits=[0.04, 0.08],
        per_run_timeout_sec=1,
    )

    out = run_param_sweep(cfg)

    assert out["status"] == "success"
    assert len(seen) == 4
    assert {x["stop_loss"] for x in seen} == {-0.03, -0.05}
    assert {x["take_profit"] for x in seen} == {0.04, 0.08}
    assert out["best"]["stop_loss"] in {-0.03, -0.05}
    assert out["best"]["take_profit"] in {0.04, 0.08}


def test_param_sweep_preserves_zero_drawdown(monkeypatch, tmp_path):
    def fake_run_engine_with_timeout(*, engine, cfg, params):
        return {
            "run_id": "run_zero_dd",
            "status": "success",
            "result": {
                "summary": {
                    "win_rate": 1.0,
                    "max_drawdown": 0.0,
                    "signal_density": 0.1,
                    "tradeability_filter_enabled": True,
                    "volume_constraint_enabled": True,
                },
                "rolling": {
                    "train_test_separated": True,
                    "train_windows": 2,
                    "test_windows": 2,
                    "windows_total": 2,
                    "failed_windows": [],
                },
                "window_results": {"test": [{"summary": {"win_rate": 1.0}}, {"summary": {"win_rate": 1.0}}]},
            },
        }

    monkeypatch.setattr("openclaw.research.backtest_param_sweep._run_engine_with_timeout", fake_run_engine_with_timeout)

    cfg = SweepConfig(
        strategy="combo",
        module_path=Path("/tmp/fake.py"),
        output_dir=tmp_path,
        date_from="2025-01-01",
        date_to="2025-06-01",
        mode="rolling",
        train_window_days=180,
        test_window_days=60,
        step_days=60,
        score_thresholds=[65],
        sample_sizes=[80],
        holding_days=[4],
        per_run_timeout_sec=1,
    )

    out = run_param_sweep(cfg)

    assert out["best"]["max_drawdown"] == 0.0
    assert out["backtest_credibility"]["metrics"]["max_drawdown"] == 0.0
    assert "drawdown_above_quality_floor" not in out["strategy_backtest_diagnostics"]["failure_classes"]


def test_sweep_aggregate_preserves_zero_drawdown_for_credibility():
    result = _build_sweep_aggregate_result(
        strategy="combo",
        status="success",
        best={
            "run_id": "rolling_combo_zero_dd",
            "status": "success",
            "win_rate": 1.0,
            "max_drawdown": 0.0,
            "signal_density": 0.1,
            "rolling_test_windows": 2,
            "objective": 100.0,
            "tradeability_filter_enabled": True,
            "volume_constraint_enabled": True,
        },
        rows=[
            {"idx": 1, "status": "success"},
            {"idx": 2, "status": "success"},
        ],
        errors=[],
        artifacts={"json": "artifact://sweep.json"},
    )
    audit = build_backtest_credibility_audit(result=result, params={"mode": "rolling"}, param_runs=2, failed_runs=[])

    assert result["result"]["summary"]["max_drawdown"] == 0.0
    assert audit["metrics"]["max_drawdown"] == 0.0


def test_sweep_aggregate_audit_requires_multiple_successful_parameter_runs():
    result = _build_sweep_aggregate_result(
        strategy="v9",
        status="success",
        best={
            "run_id": "rolling_v9_test",
            "status": "success",
            "win_rate": 0.6,
            "max_drawdown": 0.12,
            "signal_density": 0.05,
            "rolling_test_windows": 3,
            "objective": 50.0,
            "tradeability_filter_enabled": True,
            "volume_constraint_enabled": True,
            "trading_cost": {"slippage_bp": 10.0, "expected_cost_pct": 0.01},
            "risk_diagnostics": {"tail_loss_count_5pct": 1},
            "risk_control": {"max_stop_loss_pct": 0.08},
        },
        rows=[
            {"idx": 1, "status": "success"},
            {"idx": 2, "status": "success"},
        ],
        errors=[],
        artifacts={"json": "artifact://sweep.json"},
    )
    audit = build_backtest_credibility_audit(result=result, params={"mode": "rolling"}, param_runs=2, failed_runs=[])

    review = evaluate_backtest_credibility(audit)

    assert review["passed"] is True
    assert audit["artifact_path"] == ""
    assert result["result"]["summary"]["risk_diagnostics"] == {"tail_loss_count_5pct": 1}
    assert result["result"]["summary"]["risk_control"] == {"max_stop_loss_pct": 0.08}

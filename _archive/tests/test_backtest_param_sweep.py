from __future__ import annotations

from pathlib import Path

from openclaw.research.backtest_param_sweep import (
    SweepConfig,
    _run_engine_with_timeout,
    compute_objective,
    parse_int_list,
)


def test_parse_int_list_fallback():
    assert parse_int_list("", [1, 2, 3]) == [1, 2, 3]


def test_parse_int_list_values():
    assert parse_int_list(" 10, 20,30 ", [1]) == [10, 20, 30]


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

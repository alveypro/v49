"""Tests for openclaw.adapters.v49_adapter — V49Adapter contract."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from openclaw.adapters.v49_adapter import V49Adapter


def _make_adapter() -> V49Adapter:
    return V49Adapter(module_path=Path("/tmp/fake_module.py"))


class TestRunScan:
    def test_success_with_registered_handler(self):
        adapter = _make_adapter()
        adapter.register_scan_handler("v7", lambda params: {
            "picks": [{"ts_code": "000001.SZ", "score": 85.0, "strategy": "v7", "reason": "test"}],
            "metrics": {"count": 1},
        })
        result = adapter.run_scan("v7", {"score_threshold": 60})
        assert result["status"] == "success"
        assert len(result["result"]["picks"]) == 1
        assert result["result"]["picks"][0]["ts_code"] == "000001.SZ"

    def test_fail_without_handler(self):
        adapter = _make_adapter()
        result = adapter.run_scan("v99")
        assert result["status"] == "failed"
        assert "not registered" in result["error"]

    def test_handler_exception_returns_failed(self):
        adapter = _make_adapter()
        adapter.register_scan_handler("v7", lambda params: (_ for _ in ()).throw(RuntimeError("boom")))
        result = adapter.run_scan("v7")
        assert result["status"] == "failed"
        assert "boom" in result["error"]


class TestRunBacktest:
    def test_success_with_registered_handler(self):
        adapter = _make_adapter()
        adapter.register_backtest_handler("v7", lambda params: {
            "summary": {"win_rate": 0.55, "max_drawdown": 0.08, "signal_density": 0.03},
        })
        result = adapter.run_backtest("v7", "2025-01-01", "2025-06-01")
        assert result["status"] == "success"
        assert result["result"]["summary"]["win_rate"] == 0.55

    def test_fail_without_handler(self):
        adapter = _make_adapter()
        result = adapter.run_backtest("v99", "2025-01-01", "2025-06-01")
        assert result["status"] == "failed"


class TestMergeSignals:
    def test_merge_and_rank(self):
        adapter = _make_adapter()
        inputs = [
            {"ts_code": "000001.SZ", "score": 85.0, "strategy": "v7"},
            {"ts_code": "600000.SH", "score": 90.0, "strategy": "v7"},
            {"ts_code": "000001.SZ", "score": 80.0, "strategy": "v4"},
        ]
        result = adapter.merge_signals(inputs, {"v7": 1.0, "v4": 0.8})
        ranked = result["ranked_list"]
        assert len(ranked) == 2
        # 000001.SZ has two sources: 85*1.0 + 80*0.8 = 149.0
        # 600000.SH has one source: 90*1.0 = 90.0
        sz = next(r for r in ranked if r["ts_code"] == "000001.SZ")
        assert sz["weighted_score"] == 85.0 + 80.0 * 0.8
        assert "v7" in sz["strategies"]
        assert "v4" in sz["strategies"]
        assert ranked[0]["ts_code"] == "000001.SZ"

    def test_empty_input(self):
        adapter = _make_adapter()
        result = adapter.merge_signals([])
        assert result["ranked_list"] == []

    def test_skips_empty_ts_code(self):
        adapter = _make_adapter()
        result = adapter.merge_signals([{"ts_code": "", "score": 50, "strategy": "v7"}])
        assert result["ranked_list"] == []


class TestRiskCheck:
    def test_green(self):
        adapter = _make_adapter()
        result = adapter.risk_check(
            {"win_rate": 0.55, "max_drawdown": 0.08, "signal_density": 0.05},
            {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02},
        )
        assert result["risk_level"] == "green"

    def test_red_on_drawdown(self):
        adapter = _make_adapter()
        result = adapter.risk_check(
            {"win_rate": 0.55, "max_drawdown": 0.20, "signal_density": 0.05},
            {"win_rate_min": 0.45, "max_drawdown_max": 0.12, "signal_density_min": 0.02},
        )
        assert result["risk_level"] == "red"
        assert "drawdown_breach" in result["triggered_rules"]


class TestGenerateReport:
    def test_report_creates_files(self, tmp_path: Path):
        adapter = _make_adapter()
        result = adapter.generate_report(
            "daily_brief",
            {
                "summary": {"strategy": "v7", "count": 1},
                "opportunities": [{"ts_code": "000001.SZ", "score": 85, "strategy": "v7", "reason": "test"}],
            },
            output_dir=tmp_path,
        )
        assert result["run_id"].startswith("report_daily_brief_")
        md_path = Path(result["markdown"])
        assert md_path.exists()
        assert "daily_brief" in md_path.read_text(encoding="utf-8")

    def test_report_includes_overnight_sections(self, tmp_path: Path):
        adapter = _make_adapter()
        result = adapter.generate_report(
            "daily_brief",
            {
                "summary": {"strategy": "v5", "count": 2, "risk_level": "green", "execution_mode": "normal"},
                "validation_review": {"recommended_count": 8, "executed_count": 6, "execution_rate": 0.75, "avg_realized_return_pct": 1.2, "win_rate": 0.58, "gates": ["ok_marker"]},
                "validation_streak": {"consecutive_severe_runs": 2},
                "opportunities": [
                    {"ts_code": "000001.SZ", "stock_name": "平安银行", "weighted_score": 85, "strategy": "v5", "expected_return_pct": 2.8, "risk_value": 31, "action": "buy", "trade_window": {"window": "09:35-09:50"}, "reasons": ["consensus_strong"]},
                ],
                "overnight_decision": {
                    "trade_date": "2026-04-06",
                    "calibration": {"samples": 12},
                    "selection_policy": {"selected_count": 1, "candidate_pool_size": 8, "min_expected_return": 1.2, "max_risk_value": 68.0, "second_pick_min_gap": 1.0},
                    "recommendations": [
                        {"ts_code": "000001.SZ", "stock_name": "平安银行", "expected_return_pct": 2.8, "risk_value": 31, "action": "buy", "trade_window": {"window": "09:35-09:50", "reason": "test"}},
                    ],
                    "holding_comparisons": [
                        {"ts_code": "600000.SH", "stock_name": "浦发银行", "predicted_return_pct": 1.2, "risk_value": 48, "profit_loss_pct": 3.1},
                    ],
                    "position_decisions": [
                        {"current_ts_code": "600000.SH", "candidate_ts_code": "000001.SZ", "decision": "switch", "switch_score": 1.6, "rationale": "test"},
                    ],
                    "policy_note": "仅输出研究与人工执行建议",
                },
            },
            output_dir=tmp_path,
        )
        md = Path(result["markdown"]).read_text(encoding="utf-8")
        assert "Overnight Two-Pick Plan" in md
        assert "Current Holdings Review" in md
        assert "Switch Decision" in md
        assert "Validation Review" in md
        assert "consecutive_severe_runs: 2" in md
        assert "Pick Policy" in md

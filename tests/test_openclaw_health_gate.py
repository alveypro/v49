from __future__ import annotations

import importlib.util
import json
from pathlib import Path


_P = Path(__file__).resolve().parents[1] / "tools" / "openclaw_health_gate.py"
_SPEC = importlib.util.spec_from_file_location("openclaw_health_gate_for_test", str(_P))
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gate)


def test_risk_limits_for_strategy_override():
    cfg = {"risk_overrides": {"v9": {"win_rate_min": 0.38, "max_drawdown_max": 0.45, "signal_density_min": 0.05}}}
    out = gate._risk_limits_for_strategy("v9", cfg)
    assert out["win_rate_min"] == 0.38
    assert out["max_drawdown_max"] == 0.45
    assert out["signal_density_min"] == 0.05


def test_metric_breaches_detects_all():
    limits = {"win_rate_min": 0.5, "max_drawdown_max": 0.3, "signal_density_min": 0.1}
    stats = {"win_rate": 0.42, "max_drawdown": 0.35, "signal_density": 0.05}
    out = gate._metric_breaches(stats, limits)
    assert set(out) == {"win_rate", "max_drawdown", "signal_density"}


def test_metric_breaches_no_breach():
    limits = {"win_rate_min": 0.4, "max_drawdown_max": 0.5, "signal_density_min": 0.01}
    stats = {"win_rate": 0.42, "max_drawdown": 0.35, "signal_density": 0.05}
    out = gate._metric_breaches(stats, limits)
    assert out == []


def test_run_summary_dir_prefers_explicit_env(tmp_path, monkeypatch):
    explicit = tmp_path / "real_ops_logs"
    explicit.mkdir()
    (explicit / "run_summary_20260502_010000.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("OPENCLAW_RUN_SUMMARY_DIR", str(explicit))

    assert gate._run_summary_dir() == explicit


def test_recent_run_warns_when_no_summary(tmp_path, monkeypatch):
    empty = tmp_path / "empty_logs"
    empty.mkdir()
    monkeypatch.setenv("OPENCLAW_RUN_SUMMARY_DIR", str(empty))
    monkeypatch.setattr(gate, "log_dir", lambda: empty)

    out = gate.check_recent_run()

    assert out.status == "orange"
    assert "未找到 run_summary" in out.detail


def test_recent_run_accepts_real_summary_dir(tmp_path, monkeypatch):
    logs = tmp_path / "ops_logs"
    logs.mkdir()
    summary = {
        "strategy": "combo",
        "scan": {"status": "success"},
        "backtest": {"status": "success"},
        "risk": {
            "risk_level": "green",
            "evidence": {"market_stats": {"win_rate": 0.7, "max_drawdown": 0.03, "signal_density": 0.2}},
        },
    }
    (logs / "run_summary_20260502_010000.json").write_text(json.dumps(summary), encoding="utf-8")
    monkeypatch.setenv("OPENCLAW_RUN_SUMMARY_DIR", str(logs))
    monkeypatch.setattr(gate, "_load_strategy_center_config", lambda: {})

    out = gate.check_recent_run()

    assert out.status == "green"
    assert "策略=combo" in out.detail


def test_systemd_timer_check_uses_enabled_active_units(monkeypatch):
    calls = []

    class Result:
        returncode = 0
        stdout = "active"
        stderr = ""

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return Result()

    monkeypatch.setattr(gate, "_service_manager", lambda: "systemd")
    monkeypatch.setenv("OPENCLAW_HEALTH_SYSTEMD_UNITS", "openclaw-data-updater.timer,openclaw-daily-pipeline.timer")
    monkeypatch.setattr(gate.subprocess, "run", fake_run)

    out = gate.check_timed_services()

    assert out.status == "green"
    assert "systemd timers active" in out.detail
    assert ["systemctl", "is-enabled", "openclaw-data-updater.timer"] in calls
    assert ["systemctl", "is-active", "openclaw-daily-pipeline.timer"] in calls

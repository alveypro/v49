from src.stock_dashboard_operations_section import build_stock_operations_section


def _base_kwargs() -> dict[str, object]:
    return {
        "effective_update_status": {
            "status": "partial_success",
            "stage": "running",
            "last_run": "2026-04-05 17:40:00",
            "duration": "120.0秒",
            "db_latest": "2026-04-05",
            "written_rows": "1234",
            "post_candidates": "成功",
            "post_daily_research": "成功",
            "post_candidates_mode": "quick",
            "post_candidates_elapsed_sec": "18.2",
            "post_candidates_used_attempt": "0",
        },
        "automation_health": {"label": "completed", "detail": "自动链路正常完成。"},
        "update_health": {"success_rate_7d": "85.7%"},
        "update_timeline_panel": "<div>timeline</div>",
        "update_alerts_panel": "<div>alerts</div>",
        "daily_research_runtime": {
            "state": "completed",
            "stage": "done",
            "started_at": "2026-04-05 18:00:00",
            "duration": "300.0秒",
            "health_score": "91.20",
            "alert_count": "0",
            "active_profile": "short",
            "active_progress": "backtest 4/4",
            "completed_profiles": "short, medium",
            "failed_profiles": "-",
            "search_mode": "nightly",
            "experiment": "stable",
            "liquidity_filtered_out": "12",
        },
        "research_topology": {
            "candidate_scan_scope": "全A",
            "candidate_top_n": "20",
            "formal_research_pool_rule": "50亿-500亿市值",
            "formal_research_pool_size": "60",
            "nightly_universe_size": "600",
            "weekly_long_pool_size": "80",
        },
        "research_batch_status": {
            "status": "failed",
            "last_run": "2026-04-05 23:59:00",
            "candidate_universe_size": "600",
            "candidate_top_n": "20",
            "stock_pool_size": "60",
            "failed_step": "-",
            "daily_profiles": "short, medium",
            "backtest_profile": "weekly_long",
            "search_mode": "nightly",
            "experiment": "stable",
            "liquidity_min_turnover": "20000000",
            "liquidity_filtered_out": "12",
        },
        "evolution_status": {
            "champion_version": "evo_1",
            "latest_action": "promote",
            "champion_walk_forward_score": "0.1234",
            "champion_stability": "0.5678",
            "champion_models": "lightgbm / xgboost",
            "latest_reason": "better",
        },
        "grid_backtest_status": {
            "last_run": "2026-04-05 20:00:00",
            "rows": "128",
            "latest_csv": "/tmp/latest.csv",
            "validation_window": "2025-01-01 ~ 2025-06-30",
            "replay_runs": "3",
            "regime_coverage_score": "0.7500",
            "parameter_sensitivity_score": "0.2400",
            "observed_regimes": "trend, range, volatile",
            "sampling_mode": "stratified",
        },
        "progress_pct_label": "80%",
        "server_sync_preflight": {},
    }


def test_build_stock_operations_section_returns_empty_when_hidden():
    html = build_stock_operations_section(visible=False, **_base_kwargs())

    assert html == ""


def test_build_stock_operations_section_surfaces_translated_statuses():
    html = build_stock_operations_section(visible=True, **_base_kwargs())

    assert 'id="ops"' in html
    assert "待补齐" in html
    assert "已完成" in html
    assert "失败" in html
    assert "latest.csv" in html


def test_build_stock_operations_section_accepts_status_label_override():
    html = build_stock_operations_section(
        visible=True,
        status_label=lambda value: f"label:{value}",
        **_base_kwargs(),
    )

    assert "label:completed / label:done" in html
    assert "label:failed / 2026-04-05 23:59:00" in html

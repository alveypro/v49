from src.dashboard_operations import build_operations_render_contract, render_operations_section


def test_render_operations_section_contains_review_surface_blocks():
    html_text = render_operations_section(build_operations_render_contract(
        visible=True,
        effective_update_status={
            "status": "completed",
            "stage": "done",
            "last_run": "2026-04-05 17:40:00",
            "duration": "120.0秒",
            "db_latest": "2026-04-05",
            "written_rows": "1234",
            "post_candidates": "成功",
            "post_daily_research": "成功",
            "post_candidates_mode": "quick",
            "post_candidates_elapsed_sec": "18.2",
            "post_candidates_effective_universe_size": "300",
            "post_candidates_used_attempt": "0",
        },
        automation_health={"label": "健康", "detail": "自动链路正常完成。"},
        update_health={"success_rate_7d": "85.7%"},
        update_timeline_panel="<div>timeline</div>",
        update_alerts_panel="<div>alerts</div>",
        daily_research_runtime={
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
        research_topology={
            "candidate_scan_scope": "全A",
            "candidate_top_n": "20",
            "formal_research_pool_rule": "50亿-500亿市值",
            "formal_research_pool_size": "60",
            "nightly_universe_size": "600",
            "weekly_long_pool_size": "80",
        },
        research_batch_status={
            "status": "completed",
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
        evolution_status={
            "champion_version": "evo_1",
            "latest_action": "promote",
            "champion_walk_forward_score": "0.1234",
            "champion_stability": "0.5678",
            "champion_models": "lightgbm / xgboost",
            "latest_reason": "better",
        },
        grid_backtest_status={
            "last_run": "2026-04-05 20:00:00",
            "rows": "128",
            "latest_csv": "/tmp/grid_backtest_latest.csv",
            "validation_window": "2025-01-01 ~ 2025-06-30",
            "replay_runs": "3",
            "regime_coverage_score": "0.7500",
            "parameter_sensitivity_score": "0.2400",
            "observed_regimes": "trend, range, volatile",
            "sampling_mode": "stratified",
        },
        progress_pct_label="66.7%",
        server_sync_preflight={
            "preflight_version": "server_sync_preflight.v1",
            "sync_decision": {
                "allowed_to_sync": False,
                "next_action": "fix blocking preflight checks before building a file list or staging rsync",
                "blocking_checks": ["manifest_classification"],
            },
            "manifest_summary": {"allowed_total": 607, "denied_total": 19},
        },
    ))

    assert "当前链路复核" in html_text
    assert "当前是否可复核" in html_text
    assert "当前不能推进的原因" in html_text
    assert "先看复核材料" in html_text
    assert "当前复核提醒" in html_text
    assert "夜间候选与验证批次" in html_text
    assert "机制迭代摘要" in html_text
    assert "长窗验证材料" in html_text
    assert "服务器同步门禁" in html_text
    assert "阻断同步" in html_text
    assert "manifest_classification" in html_text
    assert "grid_backtest_latest.csv" in html_text
    assert "2025-01-01 ~ 2025-06-30" in html_text
    assert "0.7500 / 0.2400" in html_text
    assert "66.7%" in html_text


def test_build_operations_render_contract_derives_latest_csv_name():
    contract = build_operations_render_contract(
        visible=True,
        effective_update_status={},
        automation_health={},
        update_health={},
        update_timeline_panel="",
        update_alerts_panel="",
        daily_research_runtime={},
        research_topology={},
        research_batch_status={},
        evolution_status={},
        grid_backtest_status={"latest_csv": "/tmp/nightly/latest_grid.csv"},
        progress_pct_label="0%",
    )
    assert contract["latest_csv_name"] == "latest_grid.csv"

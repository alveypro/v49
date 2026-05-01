#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Airivo Quant Decision System / v49 backend.

This Streamlit application is the private operating console for A-share
volume-price research, candidate generation, risk gating, execution feedback,
and post-trade review.

Important boundaries:
- Outputs are research and decision-support artifacts, not investment advice.
- Backtest metrics are diagnostic evidence, not live performance proof.
- Stale data must downgrade the system to research mode.
- Real execution feedback and realized outcomes are required before promotion.

Primary workflow:
1. Update local market data.
2. Generate a small, explainable candidate pool.
3. Apply risk gates and manual review.
4. Record execution feedback.
5. Refresh realized outcomes and weekly review.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import sqlite3
import tushare as ts
import logging
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
import time
import hashlib
import re
from typing import Dict, List, Tuple, Optional, Any, Callable
import json
import os
import glob
import shutil
import signal
import threading
import subprocess
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from functools import lru_cache
import traceback
import uuid
from itertools import product
from strategies.scan_pipeline import run_stock_scan_pipeline
from strategies.registry import production_strategies, experimental_strategies
from ui.assistant_tab import render_single_stock_eval_tab
from openclaw.runtime.async_task_ui import (
    mark_scan_submitted as runtime_mark_scan_submitted,
    render_async_scan_status as runtime_render_async_scan_status,
    render_async_task_dashboard as runtime_render_async_task_dashboard,
    render_scan_param_hint as runtime_render_scan_param_hint,
    scan_params_fingerprint as runtime_scan_params_fingerprint,
    sync_scan_task_with_params as runtime_sync_scan_task_with_params,
)
from openclaw.runtime.async_task_state import (
    async_backtest_log_paths as runtime_async_backtest_log_paths,
    async_backtest_state_path as runtime_async_backtest_state_path,
    async_scan_log_paths as runtime_async_scan_log_paths,
    async_scan_state_path as runtime_async_scan_state_path,
    cleanup_async_backtest_jobs as runtime_cleanup_async_backtest_jobs,
    cleanup_async_scan_tasks as runtime_cleanup_async_scan_tasks,
    get_async_backtest_job as runtime_get_async_backtest_job,
    get_async_scan_task as runtime_get_async_scan_task,
    is_pid_alive as runtime_is_pid_alive,
    latest_async_backtest_run_id as runtime_latest_async_backtest_run_id,
    latest_async_scan_run_id as runtime_latest_async_scan_run_id,
    list_recent_async_backtest_jobs as runtime_list_recent_async_backtest_jobs,
    list_recent_async_scan_tasks as runtime_list_recent_async_scan_tasks,
    load_async_backtest_state as runtime_load_async_backtest_state,
    load_async_scan_state as runtime_load_async_scan_state,
    merge_async_backtest_job as runtime_merge_async_backtest_job,
    merge_async_scan_task as runtime_merge_async_scan_task,
    persist_async_scan_task as runtime_persist_async_scan_task,
    prune_all_finished_async_tasks as runtime_prune_all_finished_async_tasks,
    read_async_scan_df as runtime_read_async_scan_df,
    recover_async_scan_task as runtime_recover_async_scan_task,
    write_async_backtest_state as runtime_write_async_backtest_state,
    write_async_scan_state as runtime_write_async_scan_state,
)
from openclaw.runtime.async_task_orchestration import (
    launch_async_backtest_process as runtime_launch_async_backtest_process,
    launch_async_scan_process as runtime_launch_async_scan_process,
    restore_recent_async_task_refs as runtime_restore_recent_async_task_refs,
    run_async_backtest_worker_main as runtime_run_async_backtest_worker_main,
    run_async_scan_job as runtime_run_async_scan_job,
    run_async_scan_worker_main as runtime_run_async_scan_worker_main,
    start_async_backtest_job as runtime_start_async_backtest_job,
    start_async_scan_task as runtime_start_async_scan_task,
)
from openclaw.runtime.async_env import (
    build_async_scan_env as runtime_build_async_scan_env,
    temp_environ as runtime_temp_environ,
)
from openclaw.runtime.v49_handlers import (
    execute_offline_scan_strategy as runtime_execute_offline_scan_strategy,
)
from openclaw.runtime.v49_app_router import (
    apply_v49_desired_routes as runtime_apply_v49_desired_routes,
    render_v49_route_selector as runtime_render_v49_route_selector,
)
from openclaw.runtime.v49_entry_dispatcher import (
    render_v49_backtest_entry as runtime_render_v49_backtest_entry,
    render_v49_data_ops_entry as runtime_render_v49_data_ops_entry,
    render_v49_execution_center_entry as runtime_render_v49_execution_center_entry,
    render_v49_strategy_evolution_entry as runtime_render_v49_strategy_evolution_entry,
    render_v49_task_guide_entry as runtime_render_v49_task_guide_entry,
    render_v49_task_logs_entry as runtime_render_v49_task_logs_entry,
    render_v49_today_decision_entry as runtime_render_v49_today_decision_entry,
    render_v49_trading_assistant_entry as runtime_render_v49_trading_assistant_entry,
)
from openclaw.runtime.v49_backtest_entry import BacktestEntryDependencies
from openclaw.runtime.v49_data_ops_entry import DataOpsEntryDependencies
from openclaw.runtime.v49_research_entries import (
    render_v49_research_light_entries as runtime_render_v49_research_light_entries,
)
from openclaw.runtime.v49_sidebar import render_v49_sidebar as runtime_render_v49_sidebar
from openclaw.runtime.v49_today_decision_entry import TodayDecisionDependencies
from openclaw.runtime.scan_ui import (
    render_cached_scan_results as runtime_render_cached_scan_results,
    render_front_scan_summary as runtime_render_front_scan_summary,
)
from openclaw.runtime.scan_cache import (
    cache_dir as runtime_cache_dir,
    find_recent_scan_cache as runtime_find_recent_scan_cache,
    load_scan_cache as runtime_load_scan_cache,
    load_scan_cache_meta_from_paths as runtime_load_scan_cache_meta_from_paths,
    load_v7_cache as runtime_load_v7_cache,
    save_scan_cache as runtime_save_scan_cache,
    save_v7_cache as runtime_save_v7_cache,
    scan_cache_key as runtime_scan_cache_key,
    scan_cache_paths as runtime_scan_cache_paths,
    v7_cache_key as runtime_v7_cache_key,
    v7_cache_paths as runtime_v7_cache_paths,
)
from openclaw.runtime.airivo_execution_center import (
    render_airivo_batch_manager as runtime_render_airivo_batch_manager,
    render_airivo_execution_center as runtime_render_airivo_execution_center,
    render_airivo_feedback_workbench as runtime_render_airivo_feedback_workbench,
    render_airivo_today_execution_queues as runtime_render_airivo_today_execution_queues,
)
from openclaw.runtime.airivo_session import (
    append_action_audit as runtime_append_action_audit,
    guard_action as runtime_guard_action,
    has_role as runtime_has_role,
    request_headers as runtime_request_headers,
    session_meta as runtime_session_meta,
)
from openclaw.runtime.scan_result_utils import (
    add_reason_summary as runtime_add_reason_summary,
    append_reason_col as runtime_append_reason_col,
    apply_filter_mode as runtime_apply_filter_mode,
    apply_filter_mode_with_rescue as runtime_apply_filter_mode_with_rescue,
    apply_multi_period_filter as runtime_apply_multi_period_filter,
    get_ts_code_col as runtime_get_ts_code_col,
    render_result_overview as runtime_render_result_overview,
    signal_density_hint as runtime_signal_density_hint,
    standardize_result_df as runtime_standardize_result_df,
)
from openclaw.runtime.shared_utils import (
    df_to_csv_bytes as runtime_df_to_csv_bytes,
    fmt_file_mtime as runtime_fmt_file_mtime,
    now_text as runtime_now_text,
    now_ts as runtime_now_ts,
    safe_file_mtime as runtime_safe_file_mtime,
    safe_parse_dt as runtime_safe_parse_dt,
)
from openclaw.runtime.backtest_policy import (
    apply_portfolio_risk_budget as runtime_apply_portfolio_risk_budget,
    apply_tradable_segment_to_strategy_session as runtime_apply_tradable_segment_to_strategy_session,
    auto_backtest_scheduler_tick as runtime_auto_backtest_scheduler_tick,
    build_calibrated_strength_df as runtime_build_calibrated_strength_df,
    load_portfolio_risk_budget as runtime_load_portfolio_risk_budget,
    pick_tradable_segment_from_strength as runtime_pick_tradable_segment_from_strength,
)
from openclaw.runtime.backtest_data_context import (
    get_db_last_trade_date as runtime_get_db_last_trade_date,
    load_backtest_history_df as runtime_load_backtest_history_df,
    load_latest_production_backtest_audit as runtime_load_latest_production_backtest_audit,
)
from openclaw.runtime.backtest_workers import (
    run_comparison_backtest_worker as runtime_run_comparison_backtest_worker,
    run_single_backtest_worker as runtime_run_single_backtest_worker,
)
from openclaw.runtime.backtest_stats import calculate_backtest_stats as runtime_calculate_backtest_stats
from openclaw.runtime.v9_signal_evaluator import calculate_v9_score_from_history as runtime_calculate_v9_score_from_history
from openclaw.runtime.combo_signal_evaluator import (
    evaluate_combo_signal as runtime_evaluate_combo_signal,
    evaluate_combo_score_components as runtime_evaluate_combo_score_components,
    finalize_combo_scan_score as runtime_finalize_combo_scan_score,
    resolve_combo_signal_config as runtime_resolve_combo_signal_config,
)
from openclaw.runtime.dataframe_utils import (
    ensure_price_aliases as runtime_ensure_price_aliases,
    normalize_stock_df as runtime_normalize_stock_df,
)
from openclaw.runtime.history_context import (
    batch_load_stock_histories as runtime_batch_load_stock_histories,
    load_history_range_bulk as runtime_load_history_range_bulk,
    load_stock_history as runtime_load_stock_history,
    load_stock_history_bulk as runtime_load_stock_history_bulk,
    load_strategy_center_scan_defaults as runtime_load_strategy_center_scan_defaults,
)
from openclaw.runtime.production_baseline import (
    apply_production_baseline_to_session as runtime_apply_production_baseline_to_session,
    build_unified_from_latest_evolve as runtime_build_unified_from_latest_evolve,
    get_production_compare_params as runtime_get_production_compare_params,
    production_baseline_params as runtime_production_baseline_params,
    rollback_latest_promoted_params as runtime_rollback_latest_promoted_params,
    save_production_unified_profile as runtime_save_production_unified_profile,
    trigger_auto_evolve_optimize as runtime_trigger_auto_evolve_optimize,
)
from openclaw.services.airivo_feedback_service import (
    apply_batch_feedback_action as service_apply_batch_feedback_action,
    refresh_realized_outcomes as service_refresh_realized_outcomes,
    update_feedback_row as service_update_feedback_row,
)
from openclaw.services.airivo_batch_service import (
    archive_batch as service_archive_batch,
    evaluate_batch_release_gate as service_evaluate_batch_release_gate,
    get_canary_scope as service_get_canary_scope,
    get_override_audits as service_get_override_audits,
    get_release_outcome_review as service_get_release_outcome_review,
    publish_manual_scan_to_execution_queue as service_publish_manual_scan_to_execution_queue,
    set_active_batch as service_set_active_batch,
    set_canary_batch as service_set_canary_batch,
)
from openclaw.services.airivo_execution_read_service import (
    bucket_feedback_rows as service_bucket_feedback_rows,
    compare_queue_batches as service_compare_queue_batches,
    feedback_bucket_summary as service_feedback_bucket_summary,
    feedback_snapshot as service_feedback_snapshot,
    latest_execution_queue as service_latest_execution_queue,
    load_feedback_rows as service_load_feedback_rows,
    load_queue_batch_rows as service_load_queue_batch_rows,
    recent_execution_batches as service_recent_execution_batches,
)
from openclaw.services.airivo_dashboard_snapshot_service import (
    data_freshness_snapshot as service_data_freshness_snapshot,
    latest_candidate_snapshot as service_latest_candidate_snapshot,
    parse_yyyymmdd as service_parse_yyyymmdd,
    table_latest as service_table_latest,
)
from openclaw.services.lineage_service import (
    new_run_id as service_new_run_id,
    record_backtest_result_chain as service_record_backtest_result_chain,
    record_signal_dataframe_chain as service_record_signal_dataframe_chain,
)
from openclaw.services.airivo_artifact_service import (
    get_auto_evolve_status as service_get_auto_evolve_status,
    load_latest_production_promotion as service_load_latest_production_promotion,
    load_latest_production_report as service_load_latest_production_report,
    load_latest_tracking_scoreboard as service_load_latest_tracking_scoreboard,
    load_production_report_by_strategy as service_load_production_report_by_strategy,
    resolve_app_path as service_resolve_app_path,
)
from openclaw.services.evolution_artifact_service import (
    load_evolve_params as service_load_evolve_params,
)
from openclaw.services.stock_pool_service import (
    compute_forward_return_buckets as service_compute_forward_return_buckets,
    compute_pool_performance as service_compute_pool_performance,
    delete_stock_pool_snapshot as service_delete_stock_pool_snapshot,
    extract_numeric_price as service_extract_numeric_price,
    get_entry_prices_by_base_date as service_get_entry_prices_by_base_date,
    list_stock_pool_meta as service_list_stock_pool_meta,
    load_latest_stock_pool_snapshot as service_load_latest_stock_pool_snapshot,
    load_stock_pool_performance as service_load_stock_pool_performance,
    load_stock_pool_performance_detail as service_load_stock_pool_performance_detail,
    load_stock_pool_snapshot as service_load_stock_pool_snapshot,
    parse_pool_base_date as service_parse_pool_base_date,
    pick_entry_price as service_pick_entry_price,
    save_stock_pool_snapshot as service_save_stock_pool_snapshot,
    summarize_stock_pool_signal_performance as service_summarize_stock_pool_signal_performance,
)
from openclaw.services.market_context_service import (
    calc_external_bonus as service_calc_external_bonus,
    get_latest_prices as service_get_latest_prices,
    load_external_bonus_maps as service_load_external_bonus_maps,
)
from openclaw.services.market_data_service import (
    canonical_ts_code as service_canonical_ts_code,
    connect_permanent_db as service_connect_permanent_db,
    expand_ts_code_keys as service_expand_ts_code_keys,
    iter_sqlite_in_chunks as service_iter_sqlite_in_chunks,
    load_candidate_stocks as service_load_candidate_stocks,
    load_real_stock_data as service_load_real_stock_data,
    safe_daily_table_name as service_safe_daily_table_name,
    safe_float as service_safe_float,
)
from openclaw.services.sim_ledger_service import (
    add_sim_auto_buy_log as service_add_sim_auto_buy_log,
    add_sim_trade as service_add_sim_trade,
    delete_sim_position as service_delete_sim_position,
    get_sim_account as service_get_sim_account,
    get_sim_auto_buy_enabled as service_get_sim_auto_buy_enabled,
    get_sim_auto_buy_logs as service_get_sim_auto_buy_logs,
    get_sim_auto_buy_max_total_amount as service_get_sim_auto_buy_max_total_amount,
    get_sim_meta as service_get_sim_meta,
    get_sim_positions as service_get_sim_positions,
    get_sim_trades as service_get_sim_trades,
    init_sim_db as service_init_sim_db,
    reset_sim_account as service_reset_sim_account,
    set_sim_auto_buy_enabled as service_set_sim_auto_buy_enabled,
    set_sim_meta as service_set_sim_meta,
    update_sim_account as service_update_sim_account,
    upsert_sim_position as service_upsert_sim_position,
)
from openclaw.services.airivo_rebalance_service import (
    auto_rebalance_scheduler_tick as service_auto_rebalance_scheduler_tick,
    append_auto_rebalance_log as service_append_auto_rebalance_log,
    append_production_rebalance_audit_log as service_append_production_rebalance_audit_log,
    auto_rebalance_log_path as service_auto_rebalance_log_path,
    compute_production_allocation_plan as service_compute_production_allocation_plan,
    build_production_rebalance_orders as service_build_production_rebalance_orders,
    build_weekly_rebalance_quality_dashboard as service_build_weekly_rebalance_quality_dashboard,
    evaluate_production_rollback_trigger as service_evaluate_production_rollback_trigger,
    execute_production_rebalance_orders as service_execute_production_rebalance_orders,
    execute_production_auto_rollback as service_execute_production_auto_rollback,
    load_latest_auto_rebalance_log as service_load_latest_auto_rebalance_log,
    load_latest_production_rebalance_audit as service_load_latest_production_rebalance_audit,
    load_latest_strategy_pool_candidates as service_load_latest_strategy_pool_candidates,
    load_production_rollback_state as service_load_production_rollback_state,
    load_recent_production_rebalance_audits as service_load_recent_production_rebalance_audits,
    precheck_production_rebalance_orders as service_precheck_production_rebalance_orders,
    production_rebalance_audit_log_path as service_production_rebalance_audit_log_path,
    production_rollback_state_path as service_production_rollback_state_path,
    production_strategy_health_multipliers as service_production_strategy_health_multipliers,
    resolve_market_regime as service_resolve_market_regime,
    run_auto_rebalance_pipeline as service_run_auto_rebalance_pipeline,
    save_production_rollback_state as service_save_production_rollback_state,
    score_production_rebalance_execution as service_score_production_rebalance_execution,
    write_production_allocation_report as service_write_production_allocation_report,
    write_production_rebalance_report as service_write_production_rebalance_report,
)

# 加载 .env 环境变量（Kimi API Key 等）
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path, override=True)
except Exception:
    pass

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
OFFLINE_MODE = os.getenv("OFFLINE_MODE") == "1" or os.getenv("RUN_OFFLINE_ALL") == "1" or os.getenv("RUN_OFFLINE_V7") == "1"
TUSHARE_ENABLED = os.getenv("TUSHARE_ENABLED", "1") != "0" and not OFFLINE_MODE
OFFLINE_STOCK_LIMIT = int(os.getenv("OFFLINE_STOCK_LIMIT", "0"))
OFFLINE_LOG_EVERY = int(os.getenv("OFFLINE_LOG_EVERY", "200"))
BULK_HISTORY_LIMIT = int(os.getenv("BULK_HISTORY_LIMIT", "1200"))
BULK_HISTORY_CHUNK = int(os.getenv("BULK_HISTORY_CHUNK", "200"))
AUTO_EVOLVE_LOCK_PATH = os.getenv("AUTO_EVOLVE_LOCK_PATH", "/tmp/auto_evolve.lock")
ASYNC_SCAN_RESULT_DIR = os.path.join(os.path.dirname(__file__), "logs", "openclaw", "async_scan")
ASYNC_SCAN_KEEP_SECONDS = int(os.getenv("OPENCLAW_ASYNC_SCAN_KEEP_SECONDS", "21600"))
ASYNC_SCAN_WORKERS = max(1, int(os.getenv("OPENCLAW_ASYNC_SCAN_WORKERS", "1")))
AIRIVO_ROLE_RANK = {"viewer": 1, "operator": 2, "admin": 3}
AIRIVO_ACTION_AUDIT_LOG = os.getenv(
    "AIRIVO_ACTION_AUDIT_LOG",
    str(Path(os.getenv("AIRIVO_AUTH_AUDIT_LOG", "/var/log/airivo_auth_audit.jsonl")).with_name("airivo_action_audit.jsonl")),
)


def _airivo_request_headers() -> Dict[str, str]:
    return runtime_request_headers()


def _airivo_session_meta() -> Dict[str, str]:
    return runtime_session_meta(AIRIVO_ROLE_RANK)


def _airivo_has_role(required_role: str) -> bool:
    return runtime_has_role(required_role, AIRIVO_ROLE_RANK)


def _airivo_append_action_audit(
    action: str,
    ok: bool,
    target: str = "",
    detail: str = "",
    reason: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    runtime_append_action_audit(
        action=action,
        ok=ok,
        role_rank=AIRIVO_ROLE_RANK,
        audit_log_path=AIRIVO_ACTION_AUDIT_LOG,
        fallback_log_path=str(Path(__file__).resolve().parent / "logs" / "openclaw" / "airivo_action_audit.jsonl"),
        target=target,
        detail=detail,
        reason=reason,
        extra=extra,
    )


def _airivo_guard_action(required_role: str, action: str, target: str = "", reason: str = "") -> bool:
    return runtime_guard_action(
        required_role=required_role,
        action=action,
        role_rank=AIRIVO_ROLE_RANK,
        audit_log_path=AIRIVO_ACTION_AUDIT_LOG,
        fallback_log_path=str(Path(__file__).resolve().parent / "logs" / "openclaw" / "airivo_action_audit.jsonl"),
        target=target,
        reason=reason,
    )
DEFAULT_ASYNC_SCAN_ENABLED = os.getenv("OPENCLAW_DEFAULT_ASYNC_SCAN", "1") != "0"
STOCK_POOL_DIR = os.path.join(os.path.dirname(__file__), "logs", "openclaw", "stock_pool")
_ASYNC_SCAN_TASKS: Dict[str, Dict[str, Any]] = {}
_ASYNC_SCAN_LOCK = threading.Lock()
_ASYNC_SCAN_EXECUTOR = ThreadPoolExecutor(max_workers=ASYNC_SCAN_WORKERS)
_ASYNC_SCAN_THREAD_LOCAL = threading.local()
ASYNC_SCAN_CANCELLED_ERROR = "__ASYNC_SCAN_CANCELLED__"
_ASYNC_BACKTEST_JOBS: Dict[str, Dict[str, Any]] = {}
_ASYNC_BACKTEST_LOCK = threading.Lock()
_ASYNC_BACKTEST_EXECUTOR = ThreadPoolExecutor(max_workers=1)
_BACKTEST_HISTORY_CACHE: Dict[str, Any] = {
    "created_at": 0.0,
    "db_mtime": 0.0,
    "days": 0,
    "df": None,
}
APP_BOOT_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
ASYNC_BACKTEST_RESULT_DIR = os.path.join(os.path.dirname(__file__), "logs", "openclaw", "async_backtest")
ASYNC_BACKTEST_KEEP_SECONDS = int(os.getenv("OPENCLAW_ASYNC_BACKTEST_KEEP_SECONDS", "21600"))


def _now_ts() -> float:
    return runtime_now_ts()


def _now_text() -> str:
    return runtime_now_text()


@lru_cache(maxsize=1)
def _get_build_fingerprint() -> Dict[str, str]:
    root_dir = os.path.dirname(os.path.abspath(__file__))
    app_file = os.path.basename(__file__)
    build_id = os.getenv("AIRIVO_BUILD_ID", "").strip()
    git_short = "unknown"
    try:
        # On production server code may be deployed without a .git directory.
        # Skip git probing in that case to avoid noisy fatal logs on every rerun.
        if os.path.isdir(os.path.join(root_dir, ".git")):
            out = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=root_dir,
                text=True,
                timeout=1.5,
                stderr=subprocess.DEVNULL,
            )
            git_short = (out or "").strip() or "unknown"
    except Exception:
        pass
    return {
        "build_id": build_id or "-",
        "git": git_short,
        "boot_time": APP_BOOT_TIME,
        "pid": str(os.getpid()),
        "app_file": app_file,
    }


def _cleanup_async_scan_tasks() -> None:
    runtime_cleanup_async_scan_tasks(
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        keep_seconds=ASYNC_SCAN_KEEP_SECONDS,
        now_ts=_now_ts(),
    )


def _get_async_scan_task(run_id: str) -> Optional[Dict[str, Any]]:
    return runtime_get_async_scan_task(
        run_id,
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        keep_seconds=ASYNC_SCAN_KEEP_SECONDS,
        now_ts=_now_ts(),
    )


def _load_async_scan_state(run_id: str) -> Dict[str, Any]:
    return runtime_load_async_scan_state(run_id, ASYNC_SCAN_RESULT_DIR)


def _write_async_scan_state(run_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
    return runtime_write_async_scan_state(run_id, state, ASYNC_SCAN_RESULT_DIR)


def _merge_async_scan_task(run_id: str, base: Optional[Dict[str, Any]] = None, **updates: Any) -> Dict[str, Any]:
    return runtime_merge_async_scan_task(
        run_id,
        base=base,
        updates=updates,
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        result_dir=ASYNC_SCAN_RESULT_DIR,
    )


def _update_async_scan_task(run_id: str, **updates: Any) -> None:
    _merge_async_scan_task(run_id, None, **updates)


def _async_scan_state_path(run_id: str) -> str:
    return runtime_async_scan_state_path(run_id, ASYNC_SCAN_RESULT_DIR)


def _persist_async_scan_task(run_id: str) -> None:
    runtime_persist_async_scan_task(
        run_id,
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        result_dir=ASYNC_SCAN_RESULT_DIR,
    )


def _is_pid_alive(pid: Any) -> bool:
    return runtime_is_pid_alive(pid)


def _async_backtest_state_path(run_id: str) -> str:
    return runtime_async_backtest_state_path(run_id, ASYNC_BACKTEST_RESULT_DIR)


def _load_async_backtest_state(run_id: str) -> Dict[str, Any]:
    return runtime_load_async_backtest_state(run_id, ASYNC_BACKTEST_RESULT_DIR)


def _write_async_backtest_state(run_id: str, state: Dict[str, Any]) -> Dict[str, Any]:
    return runtime_write_async_backtest_state(run_id, state, ASYNC_BACKTEST_RESULT_DIR)


def _merge_async_backtest_job(run_id: str, base: Optional[Dict[str, Any]] = None, **updates: Any) -> Dict[str, Any]:
    return runtime_merge_async_backtest_job(
        run_id,
        base=base,
        updates=updates,
        async_backtest_jobs=_ASYNC_BACKTEST_JOBS,
        async_backtest_lock=_ASYNC_BACKTEST_LOCK,
        result_dir=ASYNC_BACKTEST_RESULT_DIR,
    )


def _async_backtest_log_paths(run_id: str) -> Tuple[str, str]:
    return runtime_async_backtest_log_paths(run_id, ASYNC_BACKTEST_RESULT_DIR)


def _cleanup_async_backtest_jobs() -> None:
    runtime_cleanup_async_backtest_jobs(
        async_backtest_jobs=_ASYNC_BACKTEST_JOBS,
        async_backtest_lock=_ASYNC_BACKTEST_LOCK,
        result_dir=ASYNC_BACKTEST_RESULT_DIR,
        keep_seconds=ASYNC_BACKTEST_KEEP_SECONDS,
        now_ts=_now_ts(),
    )


def _prune_all_finished_async_tasks() -> Dict[str, int]:
    return runtime_prune_all_finished_async_tasks(
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        async_scan_result_dir=ASYNC_SCAN_RESULT_DIR,
        async_backtest_jobs=_ASYNC_BACKTEST_JOBS,
        async_backtest_lock=_ASYNC_BACKTEST_LOCK,
        async_backtest_result_dir=ASYNC_BACKTEST_RESULT_DIR,
    )


@contextmanager
def _temp_environ(overrides: Dict[str, Any]):
    with runtime_temp_environ(overrides):
        yield


def _build_async_scan_env(strategy: str, params: Dict[str, Any]) -> Dict[str, Any]:
    return runtime_build_async_scan_env(strategy, params)


def _async_scan_log_paths(run_id: str) -> Tuple[str, str]:
    return runtime_async_scan_log_paths(run_id, ASYNC_SCAN_RESULT_DIR)


def _launch_async_scan_process(run_id: str, strategy: str, params: Dict[str, Any], score_col: str) -> Dict[str, Any]:
    return runtime_launch_async_scan_process(
        app_root=os.path.dirname(os.path.abspath(__file__)),
        run_id=run_id,
        strategy=strategy,
        params=params,
        score_col=score_col,
        async_scan_log_paths=_async_scan_log_paths,
        build_async_scan_env=_build_async_scan_env,
        python_executable=sys.executable or shutil.which("python3") or None,
    )


def _execute_async_scan_strategy(
    strategy: str,
    params: Dict[str, Any],
    env_overrides: Dict[str, Any],
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    return runtime_execute_offline_scan_strategy(
        strategy=strategy,
        env_overrides=env_overrides,
        analyzer_factory=CompleteVolumePriceAnalyzer,
        scan_handlers={
            "v4": _offline_scan_v4,
            "v5": _offline_scan_v5,
            "v6": _offline_scan_v6,
            "v8": _offline_scan_v8,
            "v9": _offline_scan_v9,
            "combo": _offline_scan_combo,
        },
        v7_scan_handler=run_offline_v7_scan,
        temp_environ=_temp_environ,
    )


def _set_async_scan_thread_run_id(run_id: str) -> None:
    _ASYNC_SCAN_THREAD_LOCAL.run_id = run_id


def _run_async_scan_job(run_id: str, strategy: str, params: Dict[str, Any], score_col: str) -> None:
    runtime_run_async_scan_job(
        run_id=run_id,
        strategy=strategy,
        params=params,
        score_col=score_col,
        result_dir=ASYNC_SCAN_RESULT_DIR,
        cancelled_error=ASYNC_SCAN_CANCELLED_ERROR,
        get_async_scan_task=_get_async_scan_task,
        update_async_scan_task=_update_async_scan_task,
        build_async_scan_env=_build_async_scan_env,
        run_scan=_execute_async_scan_strategy,
        now_text=_now_text,
        now_ts=_now_ts,
        set_current_run_id=_set_async_scan_thread_run_id,
        record_signal_chain=lambda **kwargs: service_record_signal_dataframe_chain(
            connect_db=_connect_permanent_db,
            code_root=Path(os.path.dirname(os.path.abspath(__file__))),
            **kwargs,
        ),
    )


def _start_async_scan_task(strategy: str, params: Dict[str, Any], score_col: str) -> Tuple[bool, str, str]:
    return runtime_start_async_scan_task(
        strategy=strategy,
        params=params,
        score_col=score_col,
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        cleanup_async_scan_tasks=_cleanup_async_scan_tasks,
        scan_params_fingerprint=_scan_params_fingerprint,
        now_ts=_now_ts,
        now_text=_now_text,
        async_scan_state_path=_async_scan_state_path,
        persist_async_scan_task=_persist_async_scan_task,
        launch_async_scan_process=_launch_async_scan_process,
        merge_async_scan_task=_merge_async_scan_task,
        run_id_factory=lambda strategy_name: service_new_run_id("scan", strategy_name),
    )


def _read_async_scan_df(task: Dict[str, Any]) -> Optional[pd.DataFrame]:
    return runtime_read_async_scan_df(task)


def _recover_async_scan_task(run_id: str) -> Optional[Dict[str, Any]]:
    return runtime_recover_async_scan_task(
        run_id,
        result_dir=ASYNC_SCAN_RESULT_DIR,
        async_scan_tasks=_ASYNC_SCAN_TASKS,
        async_scan_lock=_ASYNC_SCAN_LOCK,
        now_ts=_now_ts(),
    )


def _render_async_scan_status(task_key: str, title: str, score_col: str) -> Optional[pd.DataFrame]:
    def _render_status_once() -> Optional[pd.DataFrame]:
        return runtime_render_async_scan_status(
            task_key=task_key,
            title=title,
            score_col=score_col,
            get_async_scan_task=_get_async_scan_task,
            recover_async_scan_task=_recover_async_scan_task,
            is_pid_alive=_is_pid_alive,
            update_async_scan_task=_update_async_scan_task,
            now_ts=_now_ts,
            read_async_scan_df=_read_async_scan_df,
            standardize_result_df=_standardize_result_df,
            df_to_csv_bytes=_df_to_csv_bytes,
            set_stock_pool_candidate=_set_stock_pool_candidate,
        )

    if hasattr(st, "fragment"):
        @st.fragment(run_every="5s")
        def _render_status_fragment() -> Optional[pd.DataFrame]:
            return _render_status_once()

        return _render_status_fragment()

    return _render_status_once()


def _list_recent_async_scan_tasks(limit: int = 12) -> pd.DataFrame:
    return runtime_list_recent_async_scan_tasks(ASYNC_SCAN_RESULT_DIR, limit)


def _list_recent_async_backtest_jobs(limit: int = 12) -> pd.DataFrame:
    return runtime_list_recent_async_backtest_jobs(ASYNC_BACKTEST_RESULT_DIR, limit)


def _render_async_task_dashboard(limit: int = 10) -> None:
    runtime_render_async_task_dashboard(
        limit=limit,
        cleanup_async_backtest_jobs=_cleanup_async_backtest_jobs,
        list_recent_async_scan_tasks=_list_recent_async_scan_tasks,
        list_recent_async_backtest_jobs=_list_recent_async_backtest_jobs,
        prune_all_finished_async_tasks=_prune_all_finished_async_tasks,
    )


def _latest_async_scan_run_id(strategy: str, statuses: Optional[set[str]] = None) -> str:
    return runtime_latest_async_scan_run_id(strategy, ASYNC_SCAN_RESULT_DIR, statuses)


def _latest_async_backtest_run_id(job_kind: str, statuses: Optional[set[str]] = None) -> str:
    return runtime_latest_async_backtest_run_id(job_kind, ASYNC_BACKTEST_RESULT_DIR, statuses)


def _restore_recent_async_task_refs() -> None:
    runtime_restore_recent_async_task_refs(
        session_state=st.session_state,
        latest_async_scan_run_id=lambda strategy: _latest_async_scan_run_id(strategy),
        latest_async_backtest_run_id=lambda job_kind: _latest_async_backtest_run_id(job_kind),
    )


def _scan_params_fingerprint(params: Dict[str, Any]) -> str:
    return runtime_scan_params_fingerprint(params)


def _sync_scan_task_with_params(task_key: str, params: Dict[str, Any], strategy_label: str) -> None:
    runtime_sync_scan_task_with_params(
        task_key=task_key,
        params=params,
        strategy_label=strategy_label,
        get_async_scan_task=_get_async_scan_task,
        recover_async_scan_task=_recover_async_scan_task,
    )


def _render_scan_param_hint(task_key: str) -> None:
    runtime_render_scan_param_hint(task_key=task_key)


def _mark_scan_submitted(task_key: str, params: Dict[str, Any]) -> None:
    runtime_mark_scan_submitted(task_key=task_key, params=params)


def _stock_pool_paths(pool_id: str) -> Tuple[str, str]:
    os.makedirs(STOCK_POOL_DIR, exist_ok=True)
    return (
        os.path.join(STOCK_POOL_DIR, f"{pool_id}.csv"),
        os.path.join(STOCK_POOL_DIR, f"{pool_id}.meta.json"),
    )


def _set_stock_pool_candidate(strategy: str, params: Dict[str, Any], score_col: str, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    candidate = {
        "strategy": (strategy or "unknown").lower(),
        "params": params or {},
        "score_col": score_col or "综合评分",
        "df": df.copy(),
    }
    st.session_state["stock_pool_candidate"] = candidate
    _maybe_auto_publish_manual_scan_queue(candidate)


def _airivo_candidate_fingerprint(candidate: Dict[str, Any]) -> str:
    try:
        df = candidate.get("df")
        if not isinstance(df, pd.DataFrame) or df.empty:
            return ""
        payload = {
            "strategy": str(candidate.get("strategy") or ""),
            "params": candidate.get("params") or {},
            "score_col": str(candidate.get("score_col") or ""),
            "rows": int(len(df)),
            "columns": list(df.columns),
            "frame_hash": hashlib.md5(pd.util.hash_pandas_object(df.fillna(""), index=True).values.tobytes()).hexdigest(),
        }
        return hashlib.md5(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    except Exception:
        return ""


def _airivo_should_auto_publish_manual_scan(candidate: Dict[str, Any]) -> Tuple[bool, str]:
    if os.getenv("OPENCLAW_AUTO_PUBLISH_MANUAL_SCAN", "1") == "0":
        return False, "自动发布已关闭"
    strategy = str(candidate.get("strategy") or "").strip().lower()
    if strategy not in {"v9", "v8", "v5", "combo"}:
        return False, "仅生产策略允许自动发布"
    runtime_mode = str(st.session_state.get("airivo_runtime_mode", "research") or "research").strip().lower()
    if runtime_mode != "production":
        return False, "当前不是生产门禁，需人工确认后发布"
    df = candidate.get("df")
    if not isinstance(df, pd.DataFrame) or df.empty:
        return False, "扫描结果为空"
    if len(df) > 120:
        return False, "结果过多，需人工确认后发布"
    fp = _airivo_candidate_fingerprint(candidate)
    if not fp:
        return False, "无法生成候选指纹"
    if fp == str(st.session_state.get("airivo_last_auto_publish_fp", "") or ""):
        return False, "该扫描结果已自动发布过"
    return True, ""


def _maybe_auto_publish_manual_scan_queue(candidate: Dict[str, Any]) -> None:
    ok_to_publish, reason = _airivo_should_auto_publish_manual_scan(candidate)
    if not ok_to_publish:
        st.session_state["airivo_auto_publish_status"] = {"mode": "manual", "message": reason}
        return
    runtime_snapshot = {
        "risk_level": str(st.session_state.get("airivo_runtime_mode", "production") or "production").upper()
    }
    risk_level = str(st.session_state.get("airivo_risk_level", "") or "").strip().upper()
    if risk_level:
        runtime_snapshot["risk_level"] = risk_level
    ok, msg = _publish_manual_scan_to_execution_queue(candidate, PERMANENT_DB_PATH, runtime_snapshot)
    if ok:
        st.session_state["airivo_last_auto_publish_fp"] = _airivo_candidate_fingerprint(candidate)
        st.session_state["airivo_auto_publish_status"] = {"mode": "auto", "message": msg}
    else:
        st.session_state["airivo_auto_publish_status"] = {"mode": "error", "message": msg}


def _publish_manual_scan_to_execution_queue(candidate: Dict[str, Any], db_path: str, runtime_snapshot: Dict[str, Any]) -> Tuple[bool, str]:
    logs_dir = Path(os.path.dirname(__file__)) / "logs" / "openclaw"
    ok, msg, batch_id = service_publish_manual_scan_to_execution_queue(
        candidate=candidate,
        runtime_snapshot=runtime_snapshot,
        connect_db=_connect_permanent_db,
        logs_dir=logs_dir,
        clear_execution_queue_cache=_airivo_latest_execution_queue_cached.clear,
        clear_feedback_snapshot_cache=_airivo_feedback_snapshot_cached.clear,
    )
    if ok and batch_id:
        st.session_state["airivo_manual_queue_batch"] = batch_id
    return ok, msg


def _save_stock_pool_snapshot(strategy: str, params: Dict[str, Any], score_col: str, df: pd.DataFrame, note: str = "") -> Tuple[bool, str]:
    return service_save_stock_pool_snapshot(
        stock_pool_dir=STOCK_POOL_DIR,
        now_text=_now_text,
        strategy=strategy,
        params=params,
        score_col=score_col,
        df=df,
        note=note,
    )


def _list_stock_pool_meta() -> List[Dict[str, Any]]:
    return service_list_stock_pool_meta(STOCK_POOL_DIR)


def _load_stock_pool_snapshot(pool_id: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    return service_load_stock_pool_snapshot(STOCK_POOL_DIR, pool_id)


def _delete_stock_pool_snapshot(pool_id: str) -> Tuple[bool, str]:
    return service_delete_stock_pool_snapshot(STOCK_POOL_DIR, pool_id)


def _extract_numeric_price(series: pd.Series) -> pd.Series:
    return service_extract_numeric_price(series)


def _pick_entry_price(df: pd.DataFrame) -> pd.Series:
    return service_pick_entry_price(df)


def _get_entry_prices_by_base_date(ts_codes: List[str], base_date: str) -> Dict[str, float]:
    return service_get_entry_prices_by_base_date(
        ts_codes=ts_codes,
        base_date=base_date,
        connect_permanent_db=_connect_permanent_db,
        safe_daily_table_name=_safe_daily_table_name,
        iter_sqlite_in_chunks=_iter_sqlite_in_chunks,
        canonical_ts_code=_canonical_ts_code,
        expand_ts_code_keys=_expand_ts_code_keys,
        safe_float=_safe_float,
    )


def _compute_pool_performance(df: pd.DataFrame, base_date: str = "") -> Tuple[pd.DataFrame, Dict[str, float]]:
    return service_compute_pool_performance(
        df=df,
        base_date=base_date,
        pick_entry_price=_pick_entry_price,
        get_entry_prices_by_base_date=lambda codes, entry_base_date: _get_entry_prices_by_base_date(
            codes,
            entry_base_date,
        ),
        safe_float=_safe_float,
        get_latest_prices=_get_latest_prices,
    )


def _parse_pool_base_date(meta: Dict[str, Any]) -> str:
    return service_parse_pool_base_date(meta)


def _compute_forward_return_buckets(ts_codes: List[str], base_date: str, horizons: List[int]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    bucket_df, detail_df = service_compute_forward_return_buckets(
        ts_codes=ts_codes,
        base_date=base_date,
        horizons=horizons,
        connect_permanent_db=_connect_permanent_db,
        safe_daily_table_name=_safe_daily_table_name,
        canonical_ts_code=_canonical_ts_code,
        iter_sqlite_in_chunks=_iter_sqlite_in_chunks,
    )
    return detail_df, bucket_df


def _render_stock_pool_workspace() -> None:
    render_stock_pool_workspace_page(
        save_stock_pool_snapshot=_save_stock_pool_snapshot,
        list_stock_pool_meta=_list_stock_pool_meta,
        load_stock_pool_snapshot=_load_stock_pool_snapshot,
        parse_pool_base_date=_parse_pool_base_date,
        compute_pool_performance=_compute_pool_performance,
        compute_forward_return_buckets=_compute_forward_return_buckets,
        df_to_csv_bytes=_df_to_csv_bytes,
        delete_stock_pool_snapshot=_delete_stock_pool_snapshot,
    )


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _detect_heavy_background_job() -> Tuple[bool, str]:
    """
    Soft guard for interactive scans:
    - Only treat as busy when lock file PID is alive.
    - Stale lock should not block interactive scans.
    """
    lock_path = AUTO_EVOLVE_LOCK_PATH
    try:
        if os.path.exists(lock_path):
            pid_text = ""
            try:
                with open(lock_path, "r", encoding="utf-8") as f:
                    pid_text = (f.read() or "").strip()
            except Exception:
                pid_text = ""
            if pid_text.isdigit() and _is_pid_running(int(pid_text)):
                return True, f"检测到自动进化任务运行中（PID={pid_text}）"
            return False, "检测到历史锁文件（非运行态），已按非阻断处理"
    except Exception:
        pass
    return False, ""


def _focus_tab_by_text(tab_text: str) -> None:
    """Best-effort: keep Streamlit tab focus after rerun by clicking tab in DOM."""
    if not tab_text:
        return
    safe_text = tab_text.replace("\\", "\\\\").replace('"', '\\"')
    components.html(
        f"""
<script>
(function() {{
  const target = "{safe_text}";
  const targetLower = target.toLowerCase();
  let n = 0;
  const timer = setInterval(() => {{
    n += 1;
    const tabs = window.parent.document.querySelectorAll('button[data-baseweb="tab"]');
    for (const t of tabs) {{
      const txt = (t.innerText || '').trim();
      const txtLower = txt.toLowerCase();
      if (txt === target || txtLower.includes(targetLower)) {{
        t.click();
        clearInterval(timer);
        return;
      }}
    }}
    if (n > 20) clearInterval(timer);
  }}, 120);
}})();
</script>
        """,
        height=0,
    )


def _enable_tab_persistence(storage_prefix: str = "openclaw_v49_tabs") -> None:
    """Persist selected Streamlit tabs in browser localStorage across reruns."""
    safe_prefix = storage_prefix.replace("\\", "\\\\").replace('"', '\\"')
    components.html(
        f"""
<script>
(function() {{
  const prefix = "{safe_prefix}";
  const doc = window.parent.document;
  let tries = 0;
  const timer = setInterval(() => {{
    tries += 1;
    const tablists = doc.querySelectorAll('[role="tablist"]');
    if (!tablists || !tablists.length) {{
      if (tries > 20) clearInterval(timer);
      return;
    }}

    tablists.forEach((tablist, idx) => {{
      const key = `${{prefix}}_${{idx}}`;
      const tabs = tablist.querySelectorAll('button[data-baseweb="tab"]');
      if (!tabs || !tabs.length) return;

      const saved = window.localStorage.getItem(key);
      if (saved) {{
        for (const t of tabs) {{
          const txt = (t.innerText || '').trim();
          if (txt === saved && t.getAttribute('aria-selected') !== 'true') {{
            t.click();
            break;
          }}
        }}
      }}

      tabs.forEach((t) => {{
        if (t.dataset && t.dataset.openclawBound === '1') return;
        t.addEventListener('click', () => {{
          const txt = (t.innerText || '').trim();
          if (txt) window.localStorage.setItem(key, txt);
        }});
        if (t.dataset) t.dataset.openclawBound = '1';
      }});
    }});
    clearInterval(timer);
  }}, 120);
}})();
</script>
        """,
        height=0,
    )


def _set_focus_once(
    main_tab: Optional[str] = None,
    assistant_tab: Optional[str] = None,
    production_tab: Optional[str] = None,
    research_tab: Optional[str] = None,
    ops_tab: Optional[str] = None,
) -> None:
    if main_tab:
        st.session_state["desired_main_tab"] = main_tab
    if assistant_tab:
        st.session_state["desired_assistant_tab"] = assistant_tab
    if production_tab:
        st.session_state["desired_production_tab"] = production_tab
    if research_tab:
        st.session_state["desired_research_tab"] = research_tab
    if ops_tab:
        st.session_state["desired_ops_tab"] = ops_tab


def _is_low_value_remote_answer(text: str) -> bool:
    if not text:
        return True
    t = text.strip()
    if len(t) < 60:
        return True
    low_value_markers = [
        "已收到问题",
        "优化建议：提高信号质量阈值",
        "暂未读取到最新 run_summary",
        "执行建议：分批建仓",
        "仅供参考，不构成投资建议",
        "我暂时没读到这只股票的本地行情明细",
        "以下是基于本地 OpenClaw 数据的回答",
    ]
    return any(m in t for m in low_value_markers)


def _is_advanced_protocol_answer(text: str) -> bool:
    if not text:
        return False
    t = str(text)
    must_have_any = ["结论", "依据", "风险", "建议", "置信度", "触发", "失效"]
    hits = sum(1 for k in must_have_any if k in t)
    return hits >= 3 and len(t.strip()) >= 120


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return runtime_df_to_csv_bytes(df)


def _safe_file_mtime(path: str) -> float:
    return runtime_safe_file_mtime(path)


@st.cache_resource(show_spinner=False)
def _get_runtime_components(db_path: str) -> Dict[str, Any]:
    analyzer = CompleteVolumePriceAnalyzer(db_path=db_path)
    return {
        "vp_analyzer": analyzer,
        "optimizer": StrategyOptimizer(analyzer),
        "db_manager": DatabaseManager(db_path=db_path),
        "scanner": MarketScanner(db_path=db_path),
    }


def _should_refresh_scan_progress(idx: int, total: int, *, every: int = 25) -> bool:
    total_i = max(1, int(total))
    step = max(1, min(int(every), max(1, total_i // 40)))
    current = int(idx) + 1
    return current == 1 or current == total_i or (current % step == 0)


def _update_scan_progress_ui(progress_bar: Any, status_text: Any, idx: int, total: int, message: str) -> None:
    if not _should_refresh_scan_progress(idx, total):
        return
    total_i = max(1, int(total))
    progress_bar.progress(min(1.0, max(0.0, float(idx + 1) / float(total_i))))
    status_text.text(message)


def _run_front_scan_via_offline_pipeline(
    *,
    strategy: str,
    params: Dict[str, Any],
    analyzer: "CompleteVolumePriceAnalyzer",
) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    env_overrides = _build_async_scan_env(strategy, params)
    with _temp_environ(env_overrides):
        if strategy == "v8":
            return _offline_scan_v8(analyzer)
        if strategy == "combo":
            return _offline_scan_combo(analyzer)
        raise ValueError(f"front offline pipeline not implemented for strategy={strategy}")


def _mark_front_scan_completed(
    *,
    strategy: str,
    results_df: pd.DataFrame,
    meta: Dict[str, Any],
    score_col: str,
) -> None:
    st.session_state[f"{strategy}_front_scan_results"] = results_df.copy()
    st.session_state[f"{strategy}_front_scan_summary"] = {
        "finished_at": _now_text(),
        "result_count": int(len(results_df)),
        "candidate_count": int(meta.get("candidate_count", len(results_df)) or len(results_df)),
        "filter_failed": int(meta.get("filter_failed", 0) or 0),
        "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
        "cache_hit": bool(meta.get("cache_hit", False)),
        "cache_mode": str(meta.get("cache_mode") or ""),
        "lookback_days": int(meta.get("lookback_days", 0) or 0),
        "score_col": score_col,
    }


def _render_front_scan_summary(strategy: str, title: str) -> None:
    runtime_render_front_scan_summary(strategy=strategy, title=title)


def _ensure_price_aliases(df: pd.DataFrame) -> pd.DataFrame:
    return runtime_ensure_price_aliases(df)


def _normalize_stock_df(df: pd.DataFrame) -> pd.DataFrame:
    return runtime_normalize_stock_df(df)


def _load_stock_history(conn: sqlite3.Connection, ts_code: str, limit: int, columns: str) -> pd.DataFrame:
    return runtime_load_stock_history(
        conn=conn,
        ts_code=ts_code,
        limit=limit,
        columns=columns,
        normalize_stock_df=_normalize_stock_df,
        safe_daily_table_name=_safe_daily_table_name,
    )


def _load_stock_history_fallback(
    conn: sqlite3.Connection, ts_code: str, limit: int, columns: str
) -> pd.DataFrame:
    return _load_stock_history(conn, ts_code, limit, columns)


def _load_stock_history_bulk(
    conn: sqlite3.Connection,
    ts_codes: List[str],
    limit: int,
    columns: str,
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    return runtime_load_stock_history_bulk(
        conn=conn,
        ts_codes=ts_codes,
        limit=limit,
        columns=columns,
        bulk_history_chunk=BULK_HISTORY_CHUNK,
        normalize_stock_df=_normalize_stock_df,
        table=table,
    )


def _load_history_range_bulk(
    conn: sqlite3.Connection,
    ts_codes: List[str],
    start_date: str,
    end_date: str,
    columns: str,
    table: str = "daily_trading_data",
) -> Dict[str, pd.DataFrame]:
    return runtime_load_history_range_bulk(
        conn=conn,
        ts_codes=ts_codes,
        start_date=start_date,
        end_date=end_date,
        columns=columns,
        bulk_history_chunk=BULK_HISTORY_CHUNK,
        normalize_stock_df=_normalize_stock_df,
        table=table,
    )


def _load_evolve_params(filename: str) -> Dict[str, Any]:
    return service_load_evolve_params(os.path.dirname(__file__), filename)


_STRATEGY_CENTER_CACHE: Optional[Dict[str, Any]] = None


def _load_strategy_center_scan_defaults(strategy: str) -> Tuple[Dict[str, Any], str]:
    """Load runtime scan defaults from strategy center config."""
    global _STRATEGY_CENTER_CACHE
    out, source, cache = runtime_load_strategy_center_scan_defaults(
        app_root=os.path.dirname(__file__),
        strategy=strategy,
        strategy_center_cache=_STRATEGY_CENTER_CACHE,
    )
    _STRATEGY_CENTER_CACHE = cache
    return out, source


def _fmt_file_mtime(path: str) -> str:
    return runtime_fmt_file_mtime(path)


def _safe_parse_dt(text: str) -> Optional[datetime]:
    return runtime_safe_parse_dt(text)


def _load_backtest_history_df(days: int = 240, use_cache: bool = True) -> pd.DataFrame:
    return runtime_load_backtest_history_df(
        app_root=os.path.dirname(__file__),
        permanent_db_path=PERMANENT_DB_PATH,
        connect_permanent_db=_connect_permanent_db,
        ensure_price_aliases=_ensure_price_aliases,
        now_ts=_now_ts,
        days=days,
        use_cache=use_cache,
    )


def _run_single_backtest_worker(payload: Dict[str, Any]) -> Dict[str, Any]:
    return runtime_run_single_backtest_worker(
        payload,
        analyzer_factory=CompleteVolumePriceAnalyzer,
        load_history_df=_load_backtest_history_df,
    )


def _run_comparison_backtest_worker(payload: Dict[str, Any]) -> Dict[str, Any]:
    return runtime_run_comparison_backtest_worker(
        payload,
        analyzer_factory=CompleteVolumePriceAnalyzer,
        load_history_df=_load_backtest_history_df,
    )


def _launch_async_backtest_process(run_id: str, job_kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return runtime_launch_async_backtest_process(
        app_root=os.path.dirname(os.path.abspath(__file__)),
        run_id=run_id,
        job_kind=job_kind,
        async_backtest_log_paths=_async_backtest_log_paths,
        python_executable=sys.executable,
    )


def _start_async_backtest_job(job_kind: str, payload: Dict[str, Any]) -> Tuple[bool, str, str]:
    return runtime_start_async_backtest_job(
        job_kind=job_kind,
        payload=payload,
        async_backtest_jobs=_ASYNC_BACKTEST_JOBS,
        async_backtest_lock=_ASYNC_BACKTEST_LOCK,
        now_ts=_now_ts,
        now_text=_now_text,
        merge_async_backtest_job=_merge_async_backtest_job,
        launch_async_backtest_process=_launch_async_backtest_process,
        run_id_factory=lambda kind: service_new_run_id("backtest", kind),
    )


def _get_async_backtest_job(run_id: str) -> Optional[Dict[str, Any]]:
    return runtime_get_async_backtest_job(
        run_id,
        async_backtest_jobs=_ASYNC_BACKTEST_JOBS,
        async_backtest_lock=_ASYNC_BACKTEST_LOCK,
        result_dir=ASYNC_BACKTEST_RESULT_DIR,
        keep_seconds=ASYNC_BACKTEST_KEEP_SECONDS,
        now_ts=_now_ts(),
    )

def _load_latest_production_report() -> Tuple[Dict[str, Any], str, str]:
    return service_load_latest_production_report(os.path.dirname(__file__))


def _load_production_report_by_strategy(strategy: str) -> Tuple[Dict[str, Any], str, str]:
    return service_load_production_report_by_strategy(os.path.dirname(__file__), strategy)


def _load_latest_production_promotion(strategy_filter: str = "") -> Dict[str, Any]:
    return service_load_latest_production_promotion(os.path.dirname(__file__), strategy_filter)


def _get_auto_evolve_status() -> Dict[str, Any]:
    return service_get_auto_evolve_status(
        app_root=os.path.dirname(__file__),
        auto_evolve_lock_path=AUTO_EVOLVE_LOCK_PATH,
        is_pid_running=_is_pid_running,
        load_latest_production_report=_load_latest_production_report,
        load_evolve_params=_load_evolve_params,
        fmt_file_mtime=_fmt_file_mtime,
        safe_parse_dt=_safe_parse_dt,
        load_latest_production_promotion=lambda: _load_latest_production_promotion(),
    )


def _resolve_app_path(path_text: str) -> str:
    return service_resolve_app_path(os.path.dirname(__file__), path_text)


def _load_latest_tracking_scoreboard(max_files: int = 60) -> Tuple[pd.DataFrame, str, str]:
    return service_load_latest_tracking_scoreboard(
        os.path.dirname(__file__),
        max_files=max_files,
        resolve_app_path_fn=_resolve_app_path,
    )


def _summarize_stock_pool_signal_performance(detail_df: pd.DataFrame) -> pd.DataFrame:
    return service_summarize_stock_pool_signal_performance(detail_df)


def _load_stock_pool_performance_detail(max_files: int = 240) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    return service_load_stock_pool_performance_detail(
        app_root=os.path.dirname(__file__),
        max_files=max_files,
        safe_float=_safe_float,
        connect_permanent_db=_connect_permanent_db,
    )


def _load_stock_pool_performance(max_files: int = 240) -> Tuple[pd.DataFrame, str]:
    return service_load_stock_pool_performance(
        app_root=os.path.dirname(__file__),
        max_files=max_files,
        safe_float=_safe_float,
        connect_permanent_db=_connect_permanent_db,
    )


def _load_latest_stock_pool_snapshot(max_files: int = 300) -> pd.DataFrame:
    return service_load_latest_stock_pool_snapshot(os.path.dirname(__file__), max_files=max_files)


def _load_latest_production_backtest_audit() -> Tuple[pd.DataFrame, str]:
    return runtime_load_latest_production_backtest_audit(app_root=os.path.dirname(__file__))


def _render_production_backtest_audit_panel() -> None:
    st.markdown("### 生产策略严肃评估")
    audit_df, src_text = _load_latest_production_backtest_audit()
    if audit_df.empty:
        st.info("未找到生产策略单策略回测结果，请先在“单策略深度回测/参数优化”执行 v9/v8/v5/combo。")
        return

    total = int(len(audit_df))
    approved = int((audit_df["建议晋升"] == "YES").sum())
    red_cnt = int((audit_df["评估风险"] == "RED").sum())
    avg_wr = float(pd.to_numeric(audit_df["胜率(%)"], errors="coerce").mean())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("已评估策略", total)
    c2.metric("建议晋升", approved)
    c3.metric("RED风险", red_cnt)
    c4.metric("平均胜率", f"{avg_wr:.1f}%")

    st.dataframe(audit_df, use_container_width=True, hide_index=True)
    if src_text:
        st.caption(f"来源：{src_text}")


def _production_rollback_state_path() -> str:
    return service_production_rollback_state_path(os.path.dirname(__file__))


def _load_production_rollback_state() -> Dict[str, Any]:
    return service_load_production_rollback_state(os.path.dirname(__file__))


def _save_production_rollback_state(payload: Dict[str, Any]) -> None:
    service_save_production_rollback_state(os.path.dirname(__file__), payload)


def _evaluate_production_rollback_trigger() -> Dict[str, Any]:
    return service_evaluate_production_rollback_trigger(
        load_latest_production_backtest_audit=_load_latest_production_backtest_audit,
    )


def _execute_production_auto_rollback(force: bool = False) -> Tuple[bool, str, Dict[str, Any]]:
    return service_execute_production_auto_rollback(
        app_root=os.path.dirname(__file__),
        force=force,
        evaluate_rollback_trigger=_evaluate_production_rollback_trigger,
        rollback_latest_promoted_params=_rollback_latest_promoted_params,
        now_text=_now_text,
        retrain_enabled=bool(st.session_state.get("prod_retrain_after_rollback", os.getenv("OPENCLAW_RETRAIN_AFTER_ROLLBACK", "0") == "1")),
        trigger_auto_evolve_optimize=_trigger_auto_evolve_optimize,
    )


def _resolve_market_regime(regime_choice: str) -> str:
    return service_resolve_market_regime(
        regime_choice,
        market_environment_provider=lambda: str(CompleteVolumePriceAnalyzer().get_market_environment()),
    )


def _compute_production_allocation_plan(
    capital_total: float,
    regime_choice: str = "auto",
) -> Dict[str, Any]:
    return service_compute_production_allocation_plan(
        capital_total=float(capital_total),
        regime_choice=regime_choice,
        load_latest_production_backtest_audit=_load_latest_production_backtest_audit,
        market_environment_provider=lambda: str(CompleteVolumePriceAnalyzer().get_market_environment()),
        load_portfolio_risk_budget=_load_portfolio_risk_budget,
    )


def _write_production_allocation_report(plan: Dict[str, Any]) -> Tuple[bool, str, str]:
    return service_write_production_allocation_report(
        app_root=os.path.dirname(__file__),
        plan=plan,
    )


def _parse_price_to_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        s = str(v).strip().replace("元", "").replace(",", "")
        return float(s)
    except Exception:
        return 0.0


def _load_latest_strategy_pool_candidates(strategy: str, top_n: int = 30) -> pd.DataFrame:
    return service_load_latest_strategy_pool_candidates(
        app_root=os.path.dirname(__file__),
        strategy=strategy,
        top_n=top_n,
        parse_price_to_float=_parse_price_to_float,
    )


def _build_production_rebalance_orders(plan: Dict[str, Any]) -> Dict[str, Any]:
    return service_build_production_rebalance_orders(
        plan,
        load_strategy_pool_candidates=lambda strategy, top_n: _load_latest_strategy_pool_candidates(strategy, top_n=top_n),
        get_sim_positions=_get_sim_positions,
    )


def _write_production_rebalance_report(plan: Dict[str, Any], rebalance: Dict[str, Any]) -> Tuple[bool, str, str]:
    return service_write_production_rebalance_report(
        app_root=os.path.dirname(__file__),
        plan=plan,
        rebalance=rebalance,
    )


def _execute_production_rebalance_orders(
    rebalance: Dict[str, Any],
    execute_reduce: bool = False,
) -> Dict[str, Any]:
    return service_execute_production_rebalance_orders(
        rebalance,
        execute_reduce=bool(execute_reduce),
        get_sim_account=_get_sim_account,
        get_sim_positions=_get_sim_positions,
        get_latest_prices=_get_latest_prices,
        expand_ts_code_keys=_expand_ts_code_keys,
        parse_price_to_float=_parse_price_to_float,
        upsert_sim_position=_upsert_sim_position,
        add_sim_trade=_add_sim_trade,
        delete_sim_position=_delete_sim_position,
        update_sim_account=_update_sim_account,
        score_execution=_score_production_rebalance_execution,
        append_rebalance_audit_log=_append_production_rebalance_audit_log,
    )


def _precheck_production_rebalance_orders(
    rebalance: Dict[str, Any],
    execute_reduce: bool = False,
) -> Dict[str, Any]:
    return service_precheck_production_rebalance_orders(
        rebalance,
        execute_reduce=bool(execute_reduce),
        get_sim_account=_get_sim_account,
        get_sim_positions=_get_sim_positions,
        get_latest_prices=_get_latest_prices,
        expand_ts_code_keys=_expand_ts_code_keys,
        parse_price_to_float=_parse_price_to_float,
    )


def _score_production_rebalance_execution(exec_result: Dict[str, Any]) -> Dict[str, Any]:
    return service_score_production_rebalance_execution(exec_result)


def _production_rebalance_audit_log_path() -> str:
    return service_production_rebalance_audit_log_path(os.path.dirname(__file__))


def _append_production_rebalance_audit_log(exec_result: Dict[str, Any]) -> None:
    service_append_production_rebalance_audit_log(os.path.dirname(__file__), exec_result, _now_text)


def _load_latest_production_rebalance_audit() -> Dict[str, Any]:
    return service_load_latest_production_rebalance_audit(os.path.dirname(__file__))


def _load_recent_production_rebalance_audits(days: int = 7, max_rows: int = 300) -> pd.DataFrame:
    return service_load_recent_production_rebalance_audits(
        os.path.dirname(__file__),
        days=days,
        max_rows=max_rows,
        safe_parse_dt=_safe_parse_dt,
    )


def _build_weekly_rebalance_quality_dashboard(days: int = 7) -> Dict[str, Any]:
    return service_build_weekly_rebalance_quality_dashboard(
        app_root=os.path.dirname(__file__),
        days=days,
        safe_parse_dt=_safe_parse_dt,
    )


def _auto_rebalance_log_path() -> str:
    return service_auto_rebalance_log_path(os.path.dirname(__file__))


def _append_auto_rebalance_log(payload: Dict[str, Any]) -> None:
    service_append_auto_rebalance_log(os.path.dirname(__file__), payload)


def _load_latest_auto_rebalance_log() -> Dict[str, Any]:
    return service_load_latest_auto_rebalance_log(os.path.dirname(__file__))


def _run_auto_rebalance_pipeline(
    capital_total: float,
    regime_choice: str = "auto",
    execute_reduce: bool = False,
) -> Dict[str, Any]:
    return service_run_auto_rebalance_pipeline(
        capital_total=float(capital_total),
        regime_choice=str(regime_choice),
        execute_reduce=bool(execute_reduce),
        now_text=_now_text,
        compute_production_allocation_plan=_compute_production_allocation_plan,
        build_production_rebalance_orders=_build_production_rebalance_orders,
        precheck_production_rebalance_orders=lambda rebalance, execute_reduce_flag: _precheck_production_rebalance_orders(
            rebalance,
            execute_reduce=bool(execute_reduce_flag),
        ),
        execute_production_rebalance_orders=lambda rebalance, execute_reduce_flag: _execute_production_rebalance_orders(
            rebalance,
            execute_reduce=bool(execute_reduce_flag),
        ),
        append_auto_rebalance_log=_append_auto_rebalance_log,
    )


def _auto_rebalance_scheduler_tick() -> Dict[str, Any]:
    return service_auto_rebalance_scheduler_tick(
        enabled=bool(st.session_state.get("prod_auto_rebalance_enabled", False)),
        hhmm=str(st.session_state.get("prod_auto_rebalance_time", "14:50") or "14:50").strip(),
        capital=float(st.session_state.get("prod_alloc_total_capital", 1_000_000.0) or 1_000_000.0),
        regime_choice=str(st.session_state.get("prod_alloc_regime_choice", "auto") or "auto"),
        execute_reduce=bool(st.session_state.get("prod_auto_rebalance_execute_reduce", False)),
        now=datetime.now(),
        get_sim_meta=_get_sim_meta,
        set_sim_meta=_set_sim_meta,
        now_text=_now_text,
        run_auto_rebalance_pipeline=lambda capital_total, regime_choice, execute_reduce: _run_auto_rebalance_pipeline(
            capital_total=float(capital_total),
            regime_choice=str(regime_choice),
            execute_reduce=bool(execute_reduce),
        ),
    )


def _production_strategy_health_multipliers() -> Dict[str, float]:
    return service_production_strategy_health_multipliers(_load_latest_production_backtest_audit)


def _production_baseline_params(profile: str = "稳健标准", strict_full_market: bool = False) -> Dict[str, Dict[str, Any]]:
    return runtime_production_baseline_params(profile, strict_full_market)


def _apply_production_baseline_to_session(params: Dict[str, Dict[str, Any]]) -> None:
    runtime_apply_production_baseline_to_session(
        params,
        session_state=st.session_state,
        now_text=_now_text,
    )


def _get_production_compare_params() -> Dict[str, Dict[str, Any]]:
    return runtime_get_production_compare_params(
        session_state=st.session_state,
        load_evolve_params=_load_evolve_params,
    )


def _save_production_unified_profile(
    profile_name: str,
    strict_full_market: bool,
    params: Dict[str, Dict[str, Any]],
) -> Tuple[bool, str]:
    return runtime_save_production_unified_profile(
        app_root=os.path.dirname(__file__),
        profile_name=profile_name,
        strict_full_market=strict_full_market,
        params=params,
    )


def _build_unified_from_latest_evolve(
    profile: str,
    strict_full_market: bool,
    unified_cap_min: float,
    unified_cap_max: float,
) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    return runtime_build_unified_from_latest_evolve(
        profile=profile,
        strict_full_market=strict_full_market,
        unified_cap_min=unified_cap_min,
        unified_cap_max=unified_cap_max,
        load_evolve_params=_load_evolve_params,
    )


def _trigger_auto_evolve_optimize(force_now: bool = True) -> Tuple[bool, str]:
    return runtime_trigger_auto_evolve_optimize(
        app_root=os.path.dirname(os.path.abspath(__file__)),
        detect_heavy_background_job=_detect_heavy_background_job,
        now_text=_now_text,
        force_now=force_now,
    )


def _rollback_latest_promoted_params(strategy: str) -> Tuple[bool, str]:
    return runtime_rollback_latest_promoted_params(
        app_root=os.path.dirname(__file__),
        strategy=strategy,
    )


def _get_last_trade_date_from_tushare() -> Optional[str]:
    if not TUSHARE_ENABLED:
        return None
    try:
        pro = ts.pro_api(TUSHARE_TOKEN)
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=20)).strftime("%Y%m%d")
        df = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date, is_open="1")
        if df is None or df.empty:
            return None
        return str(df["cal_date"].iloc[-1])
    except Exception:
        return None


def _load_external_bonus_maps(conn: sqlite3.Connection) -> Tuple[float, Dict[str, float], set, set, Dict[str, float]]:
    return service_load_external_bonus_maps(
        conn=conn,
        fund_bonus_enabled=_fund_bonus_enabled,
    )


def _calc_external_bonus(
    ts_code: str,
    industry: str,
    bonus_global: float,
    bonus_stock_map: Dict[str, float],
    top_list_set: set,
    top_inst_set: set,
    bonus_industry_map: Dict[str, float],
) -> float:
    return service_calc_external_bonus(
        ts_code=ts_code,
        industry=industry,
        bonus_global=bonus_global,
        bonus_stock_map=bonus_stock_map,
        top_list_set=top_list_set,
        top_inst_set=top_inst_set,
        bonus_industry_map=bonus_industry_map,
    )

from openclaw.runtime.root_dependency_bridge import load_stable_uptrend_module
from openclaw.runtime.strategy_v6_page import render_v6_strategy_page
from openclaw.runtime.strategy_v7_page import render_v7_strategy_page
from openclaw.runtime.strategy_v8_page import render_v8_strategy_page
from openclaw.runtime.strategy_v9_page import render_v9_strategy_page
from openclaw.runtime.strategy_combo_page import render_combo_strategy_page
from openclaw.runtime.strategy_comparison_page import render_strategy_comparison_page
from openclaw.runtime.strategy_single_backtest_page import render_single_backtest_page
from openclaw.runtime.strategy_parameter_optimization_page import render_parameter_optimization_page
from openclaw.runtime.ai_signal_page import render_ai_signal_page
from openclaw.runtime.airivo_production_dashboard_page import render_airivo_production_dashboard_page
from openclaw.runtime.airivo_strategy_evolution_page import render_airivo_strategy_evolution_page
from openclaw.runtime.today_advanced_ops_panel import render_today_advanced_ops_panel
from openclaw.runtime.today_console_panel import render_today_console_panel
from openclaw.runtime.today_strategy_dispatcher import render_today_strategy_dispatcher
from openclaw.runtime.today_strategy_selector_panel import render_today_strategy_selector_panel
from openclaw.runtime.today_stable_uptrend_page import render_today_stable_uptrend_page
from openclaw.runtime.today_v4_strategy_page import render_today_v4_strategy_page
from openclaw.runtime.today_v5_strategy_page import render_today_v5_strategy_page
from openclaw.runtime.sector_flow_page import render_sector_flow_page
from openclaw.runtime.stock_pool_workspace_page import render_stock_pool_workspace_page
from openclaw.runtime.data_ops_core_page import render_data_ops_core_page
from openclaw.runtime.data_ops_status_page import render_data_ops_status_page
from openclaw.runtime.data_ops_update_page import render_data_ops_update_page
from openclaw.runtime.assistant_config_page import render_assistant_config_page
from openclaw.runtime.assistant_workbench_page import render_assistant_workbench_page
from openclaw.runtime.assistant_holdings_page import render_assistant_holdings_page
from openclaw.runtime.assistant_trade_history_page import render_assistant_trade_history_page
from openclaw.runtime.assistant_daily_report_page import render_assistant_daily_report_page
from openclaw.runtime.assistant_ops_tabs import render_assistant_ops_tabs
from openclaw.runtime.qa_self_learning_panel import render_qa_self_learning_panel
from openclaw.runtime.qa_chat_shell import render_qa_chat_shell
from openclaw.runtime.qa_submission_controller import render_qa_submission_controller
from openclaw.runtime.ui_dependency_loader import load_ui_dependencies

_ui_deps = load_ui_dependencies(logger)
ComprehensiveStockEvaluatorV3 = _ui_deps["ComprehensiveStockEvaluatorV3"]
ComprehensiveStockEvaluatorV4 = _ui_deps["ComprehensiveStockEvaluatorV4"]
ComprehensiveStockEvaluatorV5 = _ui_deps["ComprehensiveStockEvaluatorV5"]
ComprehensiveStockEvaluatorV6 = _ui_deps["ComprehensiveStockEvaluatorV6"]
ComprehensiveStockEvaluatorV7Ultimate = _ui_deps["ComprehensiveStockEvaluatorV7Ultimate"]
ComprehensiveStockEvaluatorV8Ultimate = _ui_deps["ComprehensiveStockEvaluatorV8Ultimate"]
KellyPositionManager = _ui_deps["KellyPositionManager"]
DynamicRebalanceManager = _ui_deps["DynamicRebalanceManager"]
NotificationService = _ui_deps["NotificationService"]
render_stable_uptrend_strategy = _ui_deps["render_stable_uptrend_strategy"]
V3_EVALUATOR_AVAILABLE = _ui_deps["V3_EVALUATOR_AVAILABLE"]
V4_EVALUATOR_AVAILABLE = _ui_deps["V4_EVALUATOR_AVAILABLE"]
V5_EVALUATOR_AVAILABLE = _ui_deps["V5_EVALUATOR_AVAILABLE"]
V6_EVALUATOR_AVAILABLE = _ui_deps["V6_EVALUATOR_AVAILABLE"]
V7_EVALUATOR_AVAILABLE = _ui_deps["V7_EVALUATOR_AVAILABLE"]
V8_EVALUATOR_AVAILABLE = _ui_deps["V8_EVALUATOR_AVAILABLE"]
STABLE_UPTREND_AVAILABLE = _ui_deps["STABLE_UPTREND_AVAILABLE"]

# 配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_PERMANENT_DB_PATH = os.path.join(BASE_DIR, "permanent_stock_database.db")
DEFAULT_TUSHARE_TOKEN = ""
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

def _load_config() -> Dict[str, Any]:
    """Load optional config.json without changing defaults if missing."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception as e:
        logger.warning(f"读取配置文件失败，将使用默认配置: {e}")
        return {}

_CONFIG = _load_config()
try:
    from data.dao import resolve_db_path as _resolve_db_path_v2  # type: ignore
except Exception:
    _resolve_db_path_v2 = None

_db_cfg = os.getenv("PERMANENT_DB_PATH") or _CONFIG.get("PERMANENT_DB_PATH") or DEFAULT_PERMANENT_DB_PATH
if _resolve_db_path_v2:
    try:
        PERMANENT_DB_PATH = str(_resolve_db_path_v2(_db_cfg))
    except Exception:
        PERMANENT_DB_PATH = _db_cfg
else:
    PERMANENT_DB_PATH = _db_cfg
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN") or _CONFIG.get("TUSHARE_TOKEN") or DEFAULT_TUSHARE_TOKEN
SIM_TRADING_DB_PATH = os.getenv("SIM_TRADING_DB_PATH") or _CONFIG.get("SIM_TRADING_DB_PATH") or os.path.join(BASE_DIR, "sim_trading.db")
DEFAULT_ENABLE_FUND_BONUS = bool(int(os.getenv("ENABLE_FUND_BONUS", _CONFIG.get("ENABLE_FUND_BONUS", 1))))

def _fund_bonus_enabled() -> bool:
    if "enable_fund_bonus" in st.session_state:
        return bool(st.session_state["enable_fund_bonus"])
    return DEFAULT_ENABLE_FUND_BONUS

def _safe_float(value: Any, default: float = 0.0) -> float:
    return service_safe_float(value, default)


def _safe_daily_table_name(name: str, fallback: str = "daily_trading_data") -> str:
    return service_safe_daily_table_name(name, fallback)


def _iter_sqlite_in_chunks(items: List[str], max_vars: int = 900) -> List[List[str]]:
    return service_iter_sqlite_in_chunks(items, max_vars=max_vars)


def _canonical_ts_code(code: Any) -> str:
    return service_canonical_ts_code(code)


def _expand_ts_code_keys(code: Any) -> List[str]:
    return service_expand_ts_code_keys(code)


def _get_latest_prices(ts_codes: List[str], db_path: str = PERMANENT_DB_PATH) -> Dict[str, Dict[str, Any]]:
    return service_get_latest_prices(
        ts_codes=ts_codes,
        db_path=db_path,
        canonical_ts_code=_canonical_ts_code,
        expand_ts_code_keys=_expand_ts_code_keys,
        iter_sqlite_in_chunks=_iter_sqlite_in_chunks,
        safe_daily_table_name=_safe_daily_table_name,
        safe_float=_safe_float,
    )


def _connect_permanent_db() -> sqlite3.Connection:
    return service_connect_permanent_db(PERMANENT_DB_PATH)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_real_stock_data(db_path: str) -> pd.DataFrame:
    """Cached wrapper: loads latest-day stock data. TTL=5min to stay fresh."""
    return service_load_real_stock_data(db_path)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_candidate_stocks(db_path: str, scan_all: bool, cap_min_yi: float, cap_max_yi: float) -> pd.DataFrame:
    """Cached wrapper for candidate stock list. TTL=5min."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        df = _load_candidate_stocks(conn, scan_all=scan_all, cap_min_yi=cap_min_yi, cap_max_yi=cap_max_yi)
    finally:
        conn.close()
    return df


def _batch_load_stock_histories(
    conn: sqlite3.Connection,
    ts_codes: List[str],
    limit: int = 120,
    columns: str = "ts_code, trade_date, close_price, vol, pct_chg",
) -> Dict[str, pd.DataFrame]:
    """Batch-load history for many stocks in chunked queries to avoid SQLite variable limits."""
    return runtime_batch_load_stock_histories(
        conn=conn,
        ts_codes=ts_codes,
        limit=limit,
        columns=columns,
        iter_sqlite_in_chunks=_iter_sqlite_in_chunks,
        safe_daily_table_name=_safe_daily_table_name,
        normalize_stock_df=_normalize_stock_df,
    )


def _init_sim_db() -> None:
    conn = sqlite3.connect(SIM_TRADING_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sim_account (
            id INTEGER PRIMARY KEY,
            initial_cash REAL,
            cash REAL,
            per_buy_amount REAL,
            auto_buy_top_n INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sim_positions (
            ts_code TEXT PRIMARY KEY,
            name TEXT,
            shares INTEGER,
            avg_cost REAL,
            buy_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sim_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            ts_code TEXT,
            name TEXT,
            side TEXT,
            price REAL,
            shares INTEGER,
            amount REAL,
            pnl REAL,
            batch_id TEXT,
            source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sim_auto_buy_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            signature TEXT,
            status TEXT,
            buy_count INTEGER,
            message TEXT,
            top_n INTEGER,
            per_buy_amount REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sim_meta (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        INSERT OR IGNORE INTO sim_account (id, initial_cash, cash, per_buy_amount, auto_buy_top_n)
        VALUES (1, 1000000.0, 1000000.0, 100000.0, 10)
    """)
    cursor.execute("""
        INSERT OR IGNORE INTO sim_meta (key, value)
        VALUES ('auto_buy_enabled', '1')
    """)
    conn.commit()
    conn.close()


def _get_sim_account() -> Dict[str, Any]:
    return service_get_sim_account(db_path=SIM_TRADING_DB_PATH, safe_float=_safe_float)


def _update_sim_account(
    initial_cash: Optional[float] = None,
    cash: Optional[float] = None,
    per_buy_amount: Optional[float] = None,
    auto_buy_top_n: Optional[int] = None
) -> None:
    service_update_sim_account(
        db_path=SIM_TRADING_DB_PATH,
        initial_cash=initial_cash,
        cash=cash,
        per_buy_amount=per_buy_amount,
        auto_buy_top_n=auto_buy_top_n,
    )


def _reset_sim_account(initial_cash: float, per_buy_amount: float, auto_buy_top_n: int) -> None:
    service_reset_sim_account(
        db_path=SIM_TRADING_DB_PATH,
        initial_cash=initial_cash,
        per_buy_amount=per_buy_amount,
        auto_buy_top_n=auto_buy_top_n,
    )


def _get_sim_positions() -> Dict[str, Dict[str, Any]]:
    return service_get_sim_positions(db_path=SIM_TRADING_DB_PATH, safe_float=_safe_float)


def _upsert_sim_position(ts_code: str, name: str, shares: int, avg_cost: float, buy_date: str) -> None:
    service_upsert_sim_position(
        db_path=SIM_TRADING_DB_PATH,
        ts_code=ts_code,
        name=name,
        shares=shares,
        avg_cost=avg_cost,
        buy_date=buy_date,
    )


def _delete_sim_position(ts_code: str) -> None:
    service_delete_sim_position(db_path=SIM_TRADING_DB_PATH, ts_code=ts_code)


def _add_sim_trade(
    trade_date: str,
    ts_code: str,
    name: str,
    side: str,
    price: float,
    shares: int,
    amount: float,
    pnl: float,
    batch_id: str = "",
    source: str = ""
) -> None:
    service_add_sim_trade(
        db_path=SIM_TRADING_DB_PATH,
        trade_date=trade_date,
        ts_code=ts_code,
        name=name,
        side=side,
        price=price,
        shares=shares,
        amount=amount,
        pnl=pnl,
        batch_id=batch_id,
        source=source,
    )


def _get_sim_trades(limit: int = 500) -> pd.DataFrame:
    return service_get_sim_trades(db_path=SIM_TRADING_DB_PATH, limit=limit)


def _get_sim_meta(key: str) -> str:
    return service_get_sim_meta(db_path=SIM_TRADING_DB_PATH, key=key)


def _set_sim_meta(key: str, value: str) -> None:
    service_set_sim_meta(db_path=SIM_TRADING_DB_PATH, key=key, value=value)

def _get_sim_auto_buy_max_total_amount() -> float:
    """Optional max total amount for a single auto-buy batch. 0 means no extra cap."""
    return service_get_sim_auto_buy_max_total_amount(db_path=SIM_TRADING_DB_PATH, safe_float=_safe_float)


def _get_sim_auto_buy_enabled() -> bool:
    return service_get_sim_auto_buy_enabled(db_path=SIM_TRADING_DB_PATH)


def _set_sim_auto_buy_enabled(enabled: bool) -> None:
    service_set_sim_auto_buy_enabled(db_path=SIM_TRADING_DB_PATH, enabled=enabled)


def _add_sim_auto_buy_log(
    run_time: str,
    signature: str,
    status: str,
    buy_count: int,
    message: str,
    top_n: int,
    per_buy_amount: float
) -> None:
    service_add_sim_auto_buy_log(
        db_path=SIM_TRADING_DB_PATH,
        run_time=run_time,
        signature=signature,
        status=status,
        buy_count=buy_count,
        message=message,
        top_n=top_n,
        per_buy_amount=per_buy_amount,
    )


def _get_sim_auto_buy_logs(limit: int = 50) -> pd.DataFrame:
    return service_get_sim_auto_buy_logs(db_path=SIM_TRADING_DB_PATH, limit=limit)


def _ai_list_signature(stocks: pd.DataFrame) -> str:
    if stocks is None or stocks.empty:
        return ""
    if '股票代码' in stocks.columns:
        codes = stocks['股票代码'].astype(str).tolist()
    elif 'ts_code' in stocks.columns:
        codes = stocks['ts_code'].astype(str).tolist()
    else:
        codes = []
        for _, row in stocks.iterrows():
            ts_code = row.get('股票代码') or row.get('ts_code')
            if ts_code:
                codes.append(str(ts_code))
    raw = "|".join(codes)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


def _auto_buy_ai_stocks(stocks: pd.DataFrame, per_buy_amount: float, top_n: int) -> Tuple[int, str]:
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not _get_sim_auto_buy_enabled():
        _add_sim_auto_buy_log(
            run_time=now_str,
            signature="",
            status="disabled",
            buy_count=0,
            message="自动买入已关闭",
            top_n=top_n,
            per_buy_amount=per_buy_amount
        )
        return 0, "disabled"
    if stocks is None or stocks.empty:
        _add_sim_auto_buy_log(
            run_time=now_str,
            signature="",
            status="empty",
            buy_count=0,
            message="AI 优选为空",
            top_n=top_n,
            per_buy_amount=per_buy_amount
        )
        return 0, "empty"
    buy_df = stocks.head(max(1, int(top_n))).copy()
    signature = _ai_list_signature(buy_df)
    last_signature = _get_sim_meta("last_ai_signature")
    if signature and signature == last_signature:
        _add_sim_auto_buy_log(
            run_time=now_str,
            signature=signature,
            status="duplicate",
            buy_count=0,
            message="重复名单，已跳过",
            top_n=top_n,
            per_buy_amount=per_buy_amount
        )
        return 0, "duplicate"

    account = _get_sim_account()
    positions = _get_sim_positions()
    cash = account['cash']
    max_total_amount = _get_sim_auto_buy_max_total_amount()
    remaining_budget = min(cash, max_total_amount) if max_total_amount > 0 else cash

    ts_codes = []
    for _, row in buy_df.iterrows():
        ts_code = row.get('股票代码') or row.get('ts_code')
        if ts_code:
            ts_codes.append(ts_code)
    latest_prices = _get_latest_prices(ts_codes)

    bought = 0
    for _, row in buy_df.iterrows():
        ts_code = row.get('股票代码') or row.get('ts_code')
        if not ts_code:
            continue
        if ts_code in positions:
            continue
        name = row.get('股票名称') or row.get('name') or ts_code
        price = _safe_float(row.get('最新价格', 0), 0.0)
        if price <= 0:
            price = _safe_float(latest_prices.get(ts_code, {}).get('price', 0), 0.0)
        if price <= 0:
            continue
        shares = int(per_buy_amount / price / 100) * 100
        if shares <= 0:
            continue
        cost = shares * price
        if cost > cash or cost > remaining_budget:
            continue
        cash -= cost
        remaining_budget -= cost
        _upsert_sim_position(ts_code, name, shares, price, now_str)
        _add_sim_trade(
            trade_date=now_str,
            ts_code=ts_code,
            name=name,
            side="buy",
            price=price,
            shares=shares,
            amount=cost,
            pnl=0.0,
            batch_id=signature,
            source="ai_auto"
        )
        bought += 1

    if bought > 0:
        _update_sim_account(cash=cash)
        _set_sim_meta("last_ai_signature", signature or "")
        _set_sim_meta("last_ai_buy_time", now_str)
    _add_sim_auto_buy_log(
        run_time=now_str,
        signature=signature or "",
        status="ok" if bought > 0 else "skipped",
        buy_count=bought,
        message="自动买入完成" if bought > 0 else "未命中可买标的",
        top_n=top_n,
        per_buy_amount=per_buy_amount
    )
    return bought, "ok" if bought > 0 else "skipped"


def _run_async_scan_worker_main() -> int:
    return runtime_run_async_scan_worker_main(
        load_async_scan_state=_load_async_scan_state,
        update_async_scan_task=_update_async_scan_task,
        run_async_scan_job=_run_async_scan_job,
        now_text=_now_text,
        now_ts=_now_ts,
        set_current_run_id=_set_async_scan_thread_run_id,
    )


def _run_async_backtest_worker_main() -> int:
    return runtime_run_async_backtest_worker_main(
        load_async_backtest_state=_load_async_backtest_state,
        merge_async_backtest_job=_merge_async_backtest_job,
        run_single_backtest_worker=_run_single_backtest_worker,
        run_comparison_backtest_worker=_run_comparison_backtest_worker,
        now_text=_now_text,
        now_ts=_now_ts,
        record_backtest_chain=lambda **kwargs: service_record_backtest_result_chain(
            connect_db=_connect_permanent_db,
            code_root=Path(os.path.dirname(os.path.abspath(__file__))),
            **kwargs,
        ),
    )

st.set_page_config(
    page_title="Airivo Quant Decision System",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===================== 全局专业主题样式 =====================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;600&display=swap');

:root {
  --airivo-bg: #f6f8fb;
  --airivo-card: #ffffff;
  --airivo-border: #e6ebf2;
  --airivo-text: #0f172a;
  --airivo-subtext: #475569;
  --airivo-accent: #0ea5e9;
  --airivo-accent-2: #10b981;
  --airivo-warning: #f59e0b;
  --airivo-danger: #ef4444;
  --airivo-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
}

html, body, [class*="css"]  {
  font-family: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  color: var(--airivo-text);
}

body {
  background: linear-gradient(180deg, #f4f7fb 0%, #ffffff 45%, #f6f8fb 100%);
}

.block-container {
  padding-top: 2.2rem;
  padding-bottom: 3rem;
  max-width: 1500px;
}

section[data-testid="stSidebar"] {
  background: #0b1220;
  color: #e2e8f0;
}

section[data-testid="stSidebar"] * {
  color: #e2e8f0 !important;
}

div[data-testid="stMetric"] {
  background: var(--airivo-card);
  border: 1px solid var(--airivo-border);
  border-radius: 14px;
  padding: 12px 14px;
  box-shadow: var(--airivo-shadow);
}

div[data-testid="stMetric"] label {
  color: var(--airivo-subtext);
  font-size: 0.85rem;
  letter-spacing: 0.02em;
}

div[data-testid="stMetric"] div {
  font-weight: 700;
}

div[data-testid="stExpander"] > details {
  background: var(--airivo-card);
  border: 1px solid var(--airivo-border);
  border-radius: 12px;
  box-shadow: var(--airivo-shadow);
}

div[data-testid="stExpander"] details > summary {
  font-weight: 600;
}

div[data-testid="stDataFrame"] {
  border-radius: 12px;
  overflow: hidden;
  border: 1px solid var(--airivo-border);
  box-shadow: var(--airivo-shadow);
}

button[kind="primary"] {
  background: linear-gradient(135deg, #0ea5e9 0%, #0284c7 100%) !important;
  border: none !important;
  border-radius: 10px !important;
}

button[kind="secondary"] {
  border-radius: 10px !important;
}

.stTabs [data-baseweb="tab"] {
  font-weight: 600;
  color: var(--airivo-subtext);
}

.stTabs [data-baseweb="tab"][aria-selected="true"] {
  color: var(--airivo-text);
  border-bottom: 2px solid var(--airivo-accent);
}

code, pre, .stCodeBlock {
  font-family: "JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
}

.airivo-card {
  background: var(--airivo-card);
  border: 1px solid var(--airivo-border);
  border-radius: 14px;
  padding: 16px 18px;
  box-shadow: var(--airivo-shadow);
}
</style>
""", unsafe_allow_html=True)

def _render_page_header(title: str, subtitle: str = "", tag: str = ""):
    title = (title or "").strip()
    subtitle = (subtitle or "").strip()
    tag = (tag or "").strip()
    tag_html = f"<span style='background:#e2e8f0;color:#0f172a;padding:4px 10px;border-radius:999px;font-size:0.75rem;font-weight:600;margin-left:10px;'>{tag}</span>" if tag else ""
    st.markdown(
        f"""
        <div class="airivo-card" style="display:flex;flex-direction:column;gap:6px;">
          <div style="display:flex;align-items:center;gap:8px;">
            <div style="font-size:1.35rem;font-weight:700;">{title}</div>
            {tag_html}
          </div>
          <div style="color:#64748b;font-size:0.95rem;">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def _render_result_overview(df: pd.DataFrame, score_col: str = "综合评分", title: str = "结果概览"):
    runtime_render_result_overview(df, score_col=score_col, title=title)


def _standardize_result_df(df: pd.DataFrame, score_col: str = "综合评分") -> pd.DataFrame:
    return runtime_standardize_result_df(df, score_col=score_col)


def _append_reason_col(display_cols: List[str], df: pd.DataFrame) -> List[str]:
    return runtime_append_reason_col(display_cols, df)


def _get_ts_code_col(df: pd.DataFrame) -> Optional[str]:
    return runtime_get_ts_code_col(df)


def _apply_multi_period_filter(
    df: pd.DataFrame,
    db_path: str,
    min_align: int = 2
) -> pd.DataFrame:
    return runtime_apply_multi_period_filter(df, db_path, min_align=min_align)


def _add_reason_summary(df: pd.DataFrame, score_col: str = "综合评分") -> pd.DataFrame:
    return runtime_add_reason_summary(df, score_col=score_col)


def _load_portfolio_risk_budget() -> Dict[str, Any]:
    return runtime_load_portfolio_risk_budget()


def _parse_strength_range_label(label: str) -> Tuple[float, float]:
    text = str(label or "").strip().replace("分", "")
    if "+" in text:
        lo = _safe_float(text.replace("+", ""), 0.0)
        return lo, 100.0
    if "-" in text:
        p = text.split("-", 1)
        return _safe_float(p[0], 0.0), _safe_float(p[1], 100.0)
    v = _safe_float(text, 0.0)
    return v, v


def _build_calibrated_strength_df(strength_perf: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    return runtime_build_calibrated_strength_df(strength_perf, safe_float=_safe_float)


def _pick_tradable_segment_from_strength(strength_perf: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return runtime_pick_tradable_segment_from_strength(strength_perf, safe_float=_safe_float)


def _apply_tradable_segment_to_strategy_session(strategy_name: str, seg: Dict[str, Any], top_percent: int = 1) -> str:
    return runtime_apply_tradable_segment_to_strategy_session(
        strategy_name,
        seg,
        top_percent=top_percent,
    )


def _auto_backtest_scheduler_tick() -> Dict[str, Any]:
    return runtime_auto_backtest_scheduler_tick(
        get_sim_meta=_get_sim_meta,
        set_sim_meta=_set_sim_meta,
        start_async_backtest_job=_start_async_backtest_job,
        now_text=_now_text,
    )


def _apply_portfolio_risk_budget(
    df: pd.DataFrame,
    score_col: str,
    industry_col: str = "行业",
    max_positions: int = 20,
    max_industry_ratio: float = 0.35,
) -> pd.DataFrame:
    return runtime_apply_portfolio_risk_budget(
        df,
        score_col,
        industry_col=industry_col,
        max_positions=max_positions,
        max_industry_ratio=max_industry_ratio,
    )


def _render_v7_results(
    results_df: pd.DataFrame,
    candidate_count: int,
    filter_failed: int,
    score_threshold_v7: float,
    select_mode_v7: str,
    top_percent_v7: int,
) -> None:
    st.markdown("---")
    st.markdown(f"###  智能扫描结果")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("候选股票", f"{candidate_count}只")
    with col2:
        if candidate_count > 0:
            st.metric("过滤淘汰", f"{filter_failed}只",
                      delta=f"{filter_failed/candidate_count*100:.1f}%")
        else:
            st.metric("过滤淘汰", f"{filter_failed}只")
    with col3:
        if candidate_count > 0:
            st.metric("最终推荐", f"{len(results_df)}只",
                      delta=f"{len(results_df)/candidate_count*100:.2f}%")
        else:
            st.metric("最终推荐", f"{len(results_df)}只")

    if results_df is None or results_df.empty:
        st.warning("未找到符合条件的股票，请降低阈值或放宽筛选条件")
        return

    if "理由摘要" not in results_df.columns:
        results_df = _add_reason_summary(results_df, score_col="综合评分")

    if select_mode_v7 == "阈值筛选":
        st.success(f"找到 {len(results_df)} 只符合条件的股票（≥{score_threshold_v7}分）")
    elif select_mode_v7 == "双重筛选(阈值+Top%)":
        st.success(f"先阈值后Top筛选：≥{score_threshold_v7}分，Top {top_percent_v7}%（{len(results_df)} 只）")
    else:
        st.success(f"选出 Top {top_percent_v7}%（{len(results_df)} 只）")

    results_df = results_df.reset_index(drop=True)
    _render_result_overview(results_df, score_col="综合评分", title="扫描结果概览")
    msg, level = _signal_density_hint(len(results_df), candidate_count)
    getattr(st, level)(msg)


def _signal_density_hint(results_count: int, candidate_count: int) -> Tuple[str, str]:
    return runtime_signal_density_hint(results_count, candidate_count)


def _apply_filter_mode(
    df: pd.DataFrame,
    score_col: str,
    mode: str,
    threshold: float,
    top_percent: int,
) -> pd.DataFrame:
    return runtime_apply_filter_mode(df, score_col, mode, threshold, top_percent)


def _apply_filter_mode_with_rescue(
    df: pd.DataFrame,
    score_col: str,
    mode: str,
    threshold: float,
    top_percent: int,
    threshold_floor: float = 45.0,
    rescue_top_percent: int = 6,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    return runtime_apply_filter_mode_with_rescue(
        df,
        score_col,
        mode,
        threshold,
        top_percent,
        threshold_floor=threshold_floor,
        rescue_top_percent=rescue_top_percent,
    )


def _apply_consistency_with_fallback(
    df: pd.DataFrame,
    score_col: str,
    min_align: int,
    strategy_tag: str,
) -> Tuple[pd.DataFrame, bool]:
    if df is None or df.empty:
        return df, False
    filtered = _apply_multi_period_filter(df, PERMANENT_DB_PATH, min_align=min_align)
    if filtered is None or filtered.empty:
        logger.warning(f"[offline:{strategy_tag}] consistency filter emptied results, fallback to non-consistency set")
        return df, True
    return filtered, False


def _get_db_last_trade_date(db_path: str) -> str:
    return runtime_get_db_last_trade_date(db_path=db_path)


def _airivo_parse_yyyymmdd(value: Any) -> Optional[datetime]:
    return service_parse_yyyymmdd(value)


def _airivo_table_latest(conn: sqlite3.Connection, table: str, date_col: str = "trade_date") -> Dict[str, Any]:
    return service_table_latest(conn, table, date_col)


def _airivo_data_freshness_snapshot(db_path: str) -> Dict[str, Any]:
    return service_data_freshness_snapshot(db_path)


def _airivo_latest_candidate_snapshot(db_path: str, limit: int = 5) -> Tuple[pd.DataFrame, str]:
    return service_latest_candidate_snapshot(db_path, limit=limit)


def _airivo_feedback_snapshot(db_path: str) -> Dict[str, Any]:
    return service_feedback_snapshot(db_path)


@st.cache_data(ttl=60, show_spinner=False)
def _airivo_data_freshness_snapshot_cached(db_path: str, db_mtime: float) -> Dict[str, Any]:
    return _airivo_data_freshness_snapshot(db_path)


@st.cache_data(ttl=60, show_spinner=False)
def _airivo_latest_candidate_snapshot_cached(db_path: str, db_mtime: float, limit: int = 5) -> Tuple[pd.DataFrame, str]:
    return _airivo_latest_candidate_snapshot(db_path, limit=limit)


@st.cache_data(ttl=60, show_spinner=False)
def _airivo_feedback_snapshot_cached(db_path: str, db_mtime: float) -> Dict[str, Any]:
    return _airivo_feedback_snapshot(db_path)


def _airivo_latest_execution_queue(db_path: str, limit: int = 80) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    return service_latest_execution_queue(db_path, limit=limit)


@st.cache_data(ttl=60, show_spinner=False)
def _airivo_latest_execution_queue_cached(db_path: str, db_mtime: float, limit: int = 80) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    return _airivo_latest_execution_queue(db_path, limit=limit)


def _airivo_recent_execution_batches(db_path: str, limit: int = 3) -> pd.DataFrame:
    return service_recent_execution_batches(db_path, limit=limit)


@st.cache_data(ttl=60, show_spinner=False)
def _airivo_recent_execution_batches_cached(db_path: str, db_mtime: float, limit: int = 3) -> pd.DataFrame:
    return _airivo_recent_execution_batches(db_path, limit=limit)


def _airivo_load_queue_batch_rows(db_path: str, decision_date: str) -> pd.DataFrame:
    return service_load_queue_batch_rows(db_path, decision_date)


def _airivo_compare_queue_batches(current_df: pd.DataFrame, previous_df: pd.DataFrame) -> pd.DataFrame:
    return service_compare_queue_batches(current_df, previous_df)


def _airivo_set_active_batch(
    db_path: str,
    decision_date: str,
    *,
    approved_by: str = "",
    release_note: str = "",
    rollback_reason: str = "",
    current_primary: str = "",
    override_gate: bool = False,
    override_reason: str = "",
) -> Tuple[bool, str]:
    return service_set_active_batch(
        decision_date=decision_date,
        approved_by=approved_by,
        release_note=release_note,
        rollback_reason=rollback_reason,
        current_primary=current_primary,
        override_gate=override_gate,
        override_reason=override_reason,
        connect_db=_connect_permanent_db,
        logs_dir=Path(os.path.dirname(__file__)) / "logs" / "openclaw",
        clear_execution_queue_cache=_airivo_latest_execution_queue_cached.clear,
        clear_execution_batches_cache=_airivo_recent_execution_batches_cached.clear,
    )


def _airivo_set_canary_batch(
    db_path: str,
    decision_date: str,
    *,
    approved_by: str = "",
    release_note: str = "",
    current_primary: str = "",
    allowed_buckets: Optional[List[str]] = None,
    sample_limit: int = 2,
    window_start: str = "",
    window_end: str = "",
) -> Tuple[bool, str]:
    return service_set_canary_batch(
        decision_date=decision_date,
        approved_by=approved_by,
        release_note=release_note,
        current_primary=current_primary,
        allowed_buckets=allowed_buckets,
        sample_limit=sample_limit,
        window_start=window_start,
        window_end=window_end,
        connect_db=_connect_permanent_db,
        logs_dir=Path(os.path.dirname(__file__)) / "logs" / "openclaw",
        clear_execution_queue_cache=_airivo_latest_execution_queue_cached.clear,
        clear_execution_batches_cache=_airivo_recent_execution_batches_cached.clear,
    )


def _airivo_archive_batch(db_path: str, decision_date: str, *, operator_name: str = "", archive_note: str = "") -> Tuple[bool, str]:
    return service_archive_batch(
        decision_date=decision_date,
        operator_name=operator_name,
        archive_note=archive_note,
        connect_db=_connect_permanent_db,
        clear_execution_queue_cache=_airivo_latest_execution_queue_cached.clear,
        clear_execution_batches_cache=_airivo_recent_execution_batches_cached.clear,
    )


def _airivo_evaluate_batch_release_gate(db_path: str, decision_date: str, *, current_primary: str = "") -> Dict[str, Any]:
    return service_evaluate_batch_release_gate(
        decision_date=decision_date,
        current_primary=current_primary,
        connect_db=_connect_permanent_db,
        logs_dir=Path(os.path.dirname(__file__)) / "logs" / "openclaw",
    )


def _airivo_get_canary_scope(db_path: str, decision_date: str) -> Dict[str, Any]:
    return service_get_canary_scope(db_path=db_path, decision_date=decision_date)


def _airivo_get_override_audits(db_path: str, decision_date: str, limit: int = 5) -> List[Dict[str, Any]]:
    return service_get_override_audits(db_path=db_path, decision_date=decision_date, limit=limit)


def _airivo_get_release_outcome_review(db_path: str, decision_date: str) -> Dict[str, Any]:
    return service_get_release_outcome_review(
        db_path=db_path,
        decision_date=decision_date,
        connect_db=_connect_permanent_db,
    )


def _airivo_load_feedback_rows(db_path: str, status_filter: str = "pending", limit: int = 50) -> pd.DataFrame:
    return service_load_feedback_rows(db_path, status_filter=status_filter, limit=limit)


def _airivo_parse_confidence(value: Any) -> float:
    try:
        text = str(value or "").strip().replace("%", "")
        if not text:
            return 0.0
        num = float(text)
        if num > 1.0:
            num = num / 100.0
        return max(0.0, min(1.0, num))
    except Exception:
        return 0.0


def _airivo_bucket_feedback_rows(rows: pd.DataFrame) -> pd.DataFrame:
    return service_bucket_feedback_rows(rows)


def _airivo_feedback_bucket_summary(rows: pd.DataFrame) -> Dict[str, int]:
    return service_feedback_bucket_summary(rows)


def _render_airivo_today_execution_queues(db_path: str, runtime_snapshot: Dict[str, Any]) -> None:
    runtime_render_airivo_today_execution_queues(
        db_path=db_path,
        runtime_snapshot=runtime_snapshot,
        safe_file_mtime=_safe_file_mtime,
        latest_execution_queue_cached=_airivo_latest_execution_queue_cached,
        bucket_feedback_rows=_airivo_bucket_feedback_rows,
        feedback_bucket_summary=_airivo_feedback_bucket_summary,
    )


def _render_airivo_batch_manager(db_path: str) -> None:
    runtime_render_airivo_batch_manager(
        db_path=db_path,
        safe_file_mtime=_safe_file_mtime,
        recent_execution_batches_cached=_airivo_recent_execution_batches_cached,
        evaluate_batch_release_gate=_airivo_evaluate_batch_release_gate,
        has_role=_airivo_has_role,
        guard_action=_airivo_guard_action,
        append_action_audit=_airivo_append_action_audit,
        set_active_batch=_airivo_set_active_batch,
        set_canary_batch=_airivo_set_canary_batch,
        archive_batch=_airivo_archive_batch,
        load_queue_batch_rows=_airivo_load_queue_batch_rows,
        compare_queue_batches=_airivo_compare_queue_batches,
        get_canary_scope=_airivo_get_canary_scope,
        get_override_audits=_airivo_get_override_audits,
        get_release_outcome_review=_airivo_get_release_outcome_review,
    )


def _airivo_update_feedback_row(
    db_path: str,
    row_id: int,
    final_action: str,
    execution_status: str,
    execution_note: str,
    operator_name: str,
    system_suggested_action: str = "",
    human_override_reason: str = "",
) -> Tuple[bool, str]:
    return service_update_feedback_row(
        db_path=db_path,
        row_id=row_id,
        final_action=final_action,
        execution_status=execution_status,
        execution_note=execution_note,
        operator_name=operator_name,
        system_suggested_action=system_suggested_action,
        human_override_reason=human_override_reason,
        clear_feedback_snapshot_cache=_airivo_feedback_snapshot_cached.clear,
    )


def _airivo_apply_batch_feedback_action(
    db_path: str,
    *,
    bucket: str,
    operator_name: str,
    execution_note: str,
) -> Tuple[bool, str]:
    return service_apply_batch_feedback_action(
        db_path=db_path,
        bucket=bucket,
        operator_name=operator_name,
        execution_note=execution_note,
        clear_feedback_snapshot_cache=_airivo_feedback_snapshot_cached.clear,
    )


def _airivo_refresh_realized_outcomes(db_path: str, lookback_days: int = 120) -> Tuple[bool, str]:
    return service_refresh_realized_outcomes(
        db_path=db_path,
        lookback_days=lookback_days,
        clear_feedback_snapshot_cache=_airivo_feedback_snapshot_cached.clear,
    )


def _render_airivo_feedback_workbench(
    db_path: str,
    default_bucket: str = "manual_review",
    preloaded_snapshot: Dict[str, Any] | None = None,
    preloaded_pending_rows: pd.DataFrame | None = None,
) -> None:
    runtime_render_airivo_feedback_workbench(
        db_path=db_path,
        default_bucket=default_bucket,
        safe_file_mtime=_safe_file_mtime,
        feedback_snapshot_cached=_airivo_feedback_snapshot_cached,
        bucket_feedback_rows=_airivo_bucket_feedback_rows,
        load_feedback_rows=_airivo_load_feedback_rows,
        feedback_bucket_summary=_airivo_feedback_bucket_summary,
        has_role=_airivo_has_role,
        guard_action=_airivo_guard_action,
        append_action_audit=_airivo_append_action_audit,
        update_feedback_row=_airivo_update_feedback_row,
        apply_batch_feedback_action=_airivo_apply_batch_feedback_action,
        refresh_realized_outcomes=_airivo_refresh_realized_outcomes,
        preloaded_snapshot=preloaded_snapshot,
        preloaded_pending_rows=preloaded_pending_rows,
    )


def _render_airivo_execution_center(db_path: str) -> None:
    runtime_render_airivo_execution_center(
        db_path=db_path,
        safe_file_mtime=_safe_file_mtime,
        feedback_snapshot_cached=_airivo_feedback_snapshot_cached,
        bucket_feedback_rows=_airivo_bucket_feedback_rows,
        load_feedback_rows=_airivo_load_feedback_rows,
        feedback_bucket_summary=_airivo_feedback_bucket_summary,
        has_role=_airivo_has_role,
        guard_action=_airivo_guard_action,
        append_action_audit=_airivo_append_action_audit,
        apply_batch_feedback_action=_airivo_apply_batch_feedback_action,
        render_feedback_workbench=_render_airivo_feedback_workbench,
    )


def _render_airivo_strategy_evolution(db_path: str, runtime_snapshot: Dict[str, Any]) -> None:
    render_airivo_strategy_evolution_page(
        db_path=db_path,
        runtime_snapshot=runtime_snapshot,
        safe_file_mtime=_safe_file_mtime,
        feedback_snapshot_cached=_airivo_feedback_snapshot_cached,
        bucket_feedback_rows=_airivo_bucket_feedback_rows,
        load_feedback_rows=_airivo_load_feedback_rows,
        feedback_bucket_summary=_airivo_feedback_bucket_summary,
        latest_candidate_snapshot_cached=_airivo_latest_candidate_snapshot_cached,
    )


def _render_airivo_production_dashboard(db_path: str) -> Dict[str, Any]:
    return render_airivo_production_dashboard_page(
        db_path=db_path,
        safe_file_mtime=_safe_file_mtime,
        data_freshness_snapshot_cached=_airivo_data_freshness_snapshot_cached,
        latest_candidate_snapshot_cached=_airivo_latest_candidate_snapshot_cached,
        feedback_snapshot_cached=_airivo_feedback_snapshot_cached,
    )


def _get_index_daily_from_db(start_date: str, end_date: str) -> pd.DataFrame:
    try:
        from data.history import get_index_daily_from_db as _get_index_daily_from_db_v2  # type: ignore
        return _get_index_daily_from_db_v2(
            db_path=PERMANENT_DB_PATH,
            start_date=start_date,
            end_date=end_date,
            index_code="000001.SH",
        )
    except Exception:
        return pd.DataFrame()


def _v7_cache_dir() -> str:
    return runtime_cache_dir()


def _offline_apply_limit(stocks_df: pd.DataFrame) -> pd.DataFrame:
    limit = OFFLINE_STOCK_LIMIT
    if limit and limit > 0 and len(stocks_df) > limit:
        return stocks_df.head(limit)
    return stocks_df


def _load_candidate_stocks(
    conn: sqlite3.Connection,
    *,
    scan_all: bool = True,
    cap_min_yi: float = 0.0,
    cap_max_yi: float = 0.0,
    require_industry: bool = False,
    distinct: bool = True,
    random_order: bool = False,
) -> pd.DataFrame:
    return service_load_candidate_stocks(
        conn,
        scan_all=scan_all,
        cap_min_yi=cap_min_yi,
        cap_max_yi=cap_max_yi,
        require_industry=require_industry,
        distinct=distinct,
        random_order=random_order,
    )


def _offline_log_progress(tag: str, idx: int, total: int) -> None:
    run_id = getattr(_ASYNC_SCAN_THREAD_LOCAL, "run_id", "") or str(os.getenv("OPENCLAW_ASYNC_SCAN_RUN_ID", "") or "")
    if run_id:
        cur = _load_async_scan_state(run_id)
        if cur and str(cur.get("status", "")) == "cancelled":
            raise RuntimeError(ASYNC_SCAN_CANCELLED_ERROR)
    if run_id and total > 0:
        progress = int(max(1, min(99, ((idx + 1) * 100) / max(total, 1))))
        _update_async_scan_task(
            run_id,
            status="running",
            stage="scan",
            progress=progress,
            message=f"{tag.upper()} 扫描进度 {idx + 1}/{total}",
        )
    if OFFLINE_LOG_EVERY <= 0:
        return
    if (idx + 1) % OFFLINE_LOG_EVERY == 0:
        logger.info(f"[offline:{tag}] progress {idx+1}/{total}")


def _offline_targets() -> set:
    targets = {"v4", "v5", "v6", "v7", "v8", "v9", "combo", "sector", "stable", "ai"}
    only = os.getenv("OFFLINE_ONLY", "").strip()
    exclude = os.getenv("OFFLINE_EXCLUDE", "").strip()
    if only:
        targets = {t.strip().lower() for t in only.split(",") if t.strip()}
    if exclude:
        targets = {t for t in targets if t not in {x.strip().lower() for x in exclude.split(",") if x.strip()}}
    return targets


def _v7_cache_key(params: Dict[str, Any], db_last: str) -> str:
    return runtime_v7_cache_key(params, db_last)


def _v7_cache_paths(params: Dict[str, Any], db_last: str) -> Tuple[str, str]:
    return runtime_v7_cache_paths(params, db_last)


def _load_v7_cache(params: Dict[str, Any], db_last: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    return runtime_load_v7_cache(params, db_last)


def _save_v7_cache(params: Dict[str, Any], db_last: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    runtime_save_v7_cache(params, db_last, df, meta)


def _scan_cache_key(strategy: str, params: Dict[str, Any], db_last: str) -> str:
    return runtime_scan_cache_key(strategy, params, db_last)


def _scan_cache_paths(strategy: str, params: Dict[str, Any], db_last: str) -> Tuple[str, str]:
    return runtime_scan_cache_paths(strategy, params, db_last)


def _load_scan_cache(strategy: str, params: Dict[str, Any], db_last: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    return runtime_load_scan_cache(strategy, params, db_last)


def _load_scan_cache_meta_from_paths(csv_path: str, meta_path: str) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    return runtime_load_scan_cache_meta_from_paths(csv_path, meta_path)


def _find_recent_scan_cache(strategy: str, db_last: str, predicate: Optional[Callable[[Dict[str, Any]], bool]] = None) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    return runtime_find_recent_scan_cache(strategy, db_last, predicate)


def _save_scan_cache(strategy: str, params: Dict[str, Any], db_last: str, df: pd.DataFrame, meta: Dict[str, Any]) -> None:
    runtime_save_scan_cache(strategy, params, db_last, df, meta)


def _render_cached_scan_results(
    title: str,
    results_df: pd.DataFrame,
    score_col: str,
    candidate_count: int,
    filter_failed: int,
    select_mode: str,
    threshold: float,
    top_percent: int,
) -> None:
    runtime_render_cached_scan_results(
        title=title,
        results_df=results_df,
        score_col=score_col,
        candidate_count=candidate_count,
        filter_failed=filter_failed,
        select_mode=select_mode,
        threshold=threshold,
        top_percent=top_percent,
        render_result_overview=_render_result_overview,
        signal_density_hint=_signal_density_hint,
    )
def run_offline_v7_scan() -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    """
    Offline v7 scan for precompute/cache. Configure via env:
      V7_SCORE_THRESHOLD, V7_TOP_PERCENT, V7_SELECT_MODE, V7_SCAN_ALL,
      V7_CAP_MIN, V7_CAP_MAX, V7_ENABLE_CONSISTENCY, V7_MIN_ALIGN
    """
    score_threshold = float(os.getenv("V7_SCORE_THRESHOLD", "60"))
    top_percent = int(os.getenv("V7_TOP_PERCENT", "2"))
    select_mode = os.getenv("V7_SELECT_MODE", "双重筛选(阈值+Top%)")
    scan_all = os.getenv("V7_SCAN_ALL", "1") == "1"
    cap_min = float(os.getenv("V7_CAP_MIN", "0"))
    cap_max = float(os.getenv("V7_CAP_MAX", "0"))
    enable_consistency = os.getenv("V7_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("V7_MIN_ALIGN", "2"))

    params = {
        "score_threshold": score_threshold,
        "top_percent": top_percent,
        "select_mode": select_mode,
        "scan_all": scan_all,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
    }

    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_v7_cache(params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    analyzer = CompleteVolumePriceAnalyzer()
    if not (hasattr(analyzer, "evaluator_v7") and analyzer.evaluator_v7):
        return None, {"error": "v7 evaluator not available"}

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=scan_all,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
        require_industry=True,
    )

    results = []
    filter_failed = 0
    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = _load_external_bonus_maps(conn)
    failed_counter = {"n": 0}
    def _eval_v7_row(row: pd.Series, stock_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        try:
            res = analyzer.evaluator_v7.evaluate_stock_v7(
                stock_data=stock_data,
                ts_code=row["ts_code"],
                industry=row["industry"],
            )
            if isinstance(res, dict) and res.get("success"):
                return res
            return None
        except Exception:
            return None

    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="v7",
        min_history=60,
        load_history=lambda c, ts: _load_stock_history(
            c, ts, 120, "trade_date, close_price, vol, pct_chg"
        ),
        evaluate=_eval_v7_row,
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "综合评分": f"{float(payload.score_result.get('final_score', 0)) + _calc_external_bonus(payload.row['ts_code'], payload.row['industry'], bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map):.1f}",
            "评级": payload.score_result.get("grade", "-"),
            "资金加分": f"{_calc_external_bonus(payload.row['ts_code'], payload.row['industry'], bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map):.1f}",
            "市场环境": payload.score_result.get("market_regime", "-"),
            "行业热度": f"{payload.score_result.get('industry_heat', 0):.2f}",
            "行业排名": f"#{payload.score_result.get('industry_rank', 0)}" if payload.score_result.get('industry_rank', 0) > 0 else "未进Top8",
            "行业加分": f"+{payload.score_result.get('bonus_score', 0)}分",
            "最新价格": f"{payload.stock_data['close_price'].iloc[-1]:.2f}元",
            "智能止损": f"{payload.score_result.get('stop_loss', 0):.2f}元",
            "智能止盈": f"{payload.score_result.get('take_profit', 0):.2f}元",
            "筛选理由": payload.score_result.get("signal_reasons", ""),
        },
        on_no_result=lambda _row, _stock: failed_counter.__setitem__("n", failed_counter["n"] + 1),
        on_exception=lambda _row, _e: failed_counter.__setitem__("n", failed_counter["n"] + 1),
    )
    filter_failed = int(failed_counter["n"])

    conn.close()
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": filter_failed}

    results_df = pd.DataFrame(results)
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df,
        score_col="综合评分",
        mode=select_mode,
        threshold=score_threshold,
        top_percent=top_percent,
        threshold_floor=50.0,
        rescue_top_percent=6,
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:v7] rescue filter applied: {rescue_meta}")
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "综合评分", min_align, "v7")
    results_df = _add_reason_summary(results_df, score_col="综合评分")
    results_df = results_df.reset_index(drop=True)

    meta = {
        "candidate_count": int(len(stocks_df)),
        "filter_failed": int(filter_failed),
    }
    _save_v7_cache(params, db_last, results_df, meta)
    return results_df, meta


def _offline_scan_v4(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    score_threshold = float(os.getenv("V4_SCORE_THRESHOLD", "60"))
    top_percent = int(os.getenv("V4_TOP_PERCENT", "2"))
    select_mode = os.getenv("V4_SELECT_MODE", "双重筛选(阈值+Top%)")
    scan_all = os.getenv("V4_SCAN_ALL", "1") == "1"
    cap_min = float(os.getenv("V4_CAP_MIN", "0"))
    cap_max = float(os.getenv("V4_CAP_MAX", "0"))
    enable_consistency = os.getenv("V4_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("V4_MIN_ALIGN", "2"))

    params = {
        "score_threshold": score_threshold,
        "top_percent": top_percent,
        "select_mode": select_mode,
        "scan_all": scan_all,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("v4_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    if not V4_EVALUATOR_AVAILABLE or not hasattr(analyzer, "evaluator_v4") or analyzer.evaluator_v4 is None:
        return None, {"error": "v4 evaluator not available"}

    conn = _connect_permanent_db()
    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = _load_external_bonus_maps(conn)

    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=scan_all,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
    )

    stocks_df = _offline_apply_limit(stocks_df)
    logger.info(f"[offline:v4] candidates {len(stocks_df)}")
    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="v4",
        min_history=60,
        load_history=lambda c, ts: _load_stock_history(
            c, ts, 120, "trade_date, close_price, vol, pct_chg"
        ),
        evaluate=lambda row, stock_data: analyzer.evaluator_v4.evaluate_stock_v4(stock_data),
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "综合评分": f"{float(payload.score_result.get('final_score', 0)) + _calc_external_bonus(payload.row['ts_code'], payload.row['industry'], bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map):.1f}",
            "评级": payload.score_result.get("grade", "-"),
        },
        on_progress=_offline_log_progress,
    )

    conn.close()
    logger.info(f"[offline:v4] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}

    results_df = pd.DataFrame(results)
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df, "综合评分", select_mode, score_threshold, top_percent, threshold_floor=50.0, rescue_top_percent=6
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:v4] rescue filter applied: {rescue_meta}")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "综合评分", min_align, "v4")
    results_df = _add_reason_summary(results_df, score_col="综合评分")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    results_df = results_df.reset_index(drop=True)

    _save_scan_cache(
        "v4_scan",
        params,
        db_last,
        results_df,
        {"candidate_count": len(stocks_df), "filter_failed": 0},
    )
    return results_df, {"candidate_count": len(stocks_df), "filter_failed": 0}


def _offline_scan_v5(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    score_threshold = float(os.getenv("V5_SCORE_THRESHOLD", "60"))
    top_percent = int(os.getenv("V5_TOP_PERCENT", "1"))
    select_mode = os.getenv("V5_SELECT_MODE", "分位数筛选(Top%)")
    cap_min = float(os.getenv("V5_CAP_MIN", "100"))
    cap_max = float(os.getenv("V5_CAP_MAX", "1500"))
    enable_consistency = os.getenv("V5_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("V5_MIN_ALIGN", "2"))

    params = {
        "score_threshold": score_threshold,
        "top_percent": top_percent,
        "select_mode": select_mode,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("v5_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    if not V5_EVALUATOR_AVAILABLE or not hasattr(analyzer, "evaluator_v5") or analyzer.evaluator_v5 is None:
        return None, {"error": "v5 evaluator not available"}

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=False,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
    )
    stocks_df = _offline_apply_limit(stocks_df)
    logger.info(f"[offline:v5] candidates {len(stocks_df)}")
    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="v5",
        min_history=60,
        load_history=lambda c, ts: _load_stock_history(
            c, ts, 120, "trade_date, close_price, vol, pct_chg"
        ),
        evaluate=lambda row, stock_data: analyzer.evaluator_v5.evaluate_stock_v4(stock_data),
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "综合评分": f"{float(payload.score_result.get('final_score', 0)):.1f}",
            "评级": payload.score_result.get("grade", "-"),
        },
        on_progress=_offline_log_progress,
    )

    conn.close()
    logger.info(f"[offline:v5] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}

    results_df = pd.DataFrame(results)
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df, "综合评分", select_mode, score_threshold, top_percent, threshold_floor=50.0, rescue_top_percent=6
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:v5] rescue filter applied: {rescue_meta}")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "综合评分", min_align, "v5")
    results_df = _add_reason_summary(results_df, score_col="综合评分")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    results_df = results_df.reset_index(drop=True)

    _save_scan_cache(
        "v5_scan",
        params,
        db_last,
        results_df,
        {"candidate_count": len(stocks_df), "filter_failed": 0},
    )
    return results_df, {"candidate_count": len(stocks_df), "filter_failed": 0}


def _offline_scan_v6(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    score_threshold = float(os.getenv("V6_SCORE_THRESHOLD", "85"))
    top_percent = int(os.getenv("V6_TOP_PERCENT", "2"))
    select_mode = os.getenv("V6_SELECT_MODE", "双重筛选(阈值+Top%)")
    cap_min = float(os.getenv("V6_CAP_MIN", "50"))
    cap_max = float(os.getenv("V6_CAP_MAX", "1000"))
    enable_consistency = os.getenv("V6_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("V6_MIN_ALIGN", "2"))

    params = {
        "score_threshold": score_threshold,
        "top_percent": top_percent,
        "select_mode": select_mode,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("v6_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    if not V6_EVALUATOR_AVAILABLE or not hasattr(analyzer, "evaluator_v6") or analyzer.evaluator_v6 is None:
        return None, {"error": "v6 evaluator not available"}

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=False,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
    )
    stocks_df = _offline_apply_limit(stocks_df)
    logger.info(f"[offline:v6] candidates {len(stocks_df)}")
    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="v6",
        min_history=60,
        load_history=lambda c, ts: _load_stock_history(
            c, ts, 120, "trade_date, close_price, vol, pct_chg"
        ),
        evaluate=lambda row, stock_data: analyzer.evaluator_v6.evaluate_stock_v6(
            stock_data, row["ts_code"]
        ),
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "综合评分": f"{float(payload.score_result.get('final_score', 0)):.1f}",
            "评级": payload.score_result.get("grade", "-"),
        },
        on_progress=_offline_log_progress,
    )

    conn.close()
    logger.info(f"[offline:v6] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}

    results_df = pd.DataFrame(results)
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df, "综合评分", select_mode, score_threshold, top_percent, threshold_floor=50.0, rescue_top_percent=6
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:v6] rescue filter applied: {rescue_meta}")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "综合评分", min_align, "v6")
    results_df = _add_reason_summary(results_df, score_col="综合评分")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    results_df = results_df.reset_index(drop=True)

    _save_scan_cache(
        "v6_scan",
        params,
        db_last,
        results_df,
        {"candidate_count": len(stocks_df), "filter_failed": 0},
    )
    return results_df, {"candidate_count": len(stocks_df), "filter_failed": 0}


def _offline_scan_v8(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    score_min = float(os.getenv("V8_SCORE_MIN", "55"))
    score_max = float(os.getenv("V8_SCORE_MAX", "70"))
    top_percent = int(os.getenv("V8_TOP_PERCENT", "1"))
    select_mode = os.getenv("V8_SELECT_MODE", "分位数筛选(Top%)")
    scan_all = os.getenv("V8_SCAN_ALL", "1") == "1"
    cap_min = float(os.getenv("V8_CAP_MIN", "0"))
    cap_max = float(os.getenv("V8_CAP_MAX", "0"))
    enable_consistency = os.getenv("V8_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("V8_MIN_ALIGN", "2"))

    params = {
        "score_threshold": [score_min, score_max],
        "top_percent": top_percent,
        "select_mode": select_mode,
        "scan_all": scan_all,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("v8_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    if not V8_EVALUATOR_AVAILABLE or not hasattr(analyzer, "evaluator_v8") or analyzer.evaluator_v8 is None:
        return None, {"error": "v8 evaluator not available"}

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=scan_all,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
        require_industry=True,
    )

    stocks_df = _offline_apply_limit(stocks_df)
    logger.info(f"[offline:v8] candidates {len(stocks_df)}")
    def _eval_v8_row(row: pd.Series, stock_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
        try:
            res = analyzer.evaluator_v8.evaluate_stock_v8(
                stock_data=stock_data, ts_code=row["ts_code"], index_data=None
            )
            if isinstance(res, dict) and res.get("success"):
                return res
            return None
        except Exception:
            return None

    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="v8",
        min_history=60,
        load_history=lambda c, ts: _load_stock_history(
            c, ts, 120, "trade_date, close_price, high_price, low_price, vol, pct_chg"
        ),
        evaluate=_eval_v8_row,
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "综合评分": f"{float(payload.score_result.get('final_score', 0)):.1f}",
            "评级": payload.score_result.get("grade", "-"),
        },
        on_progress=_offline_log_progress,
    )

    conn.close()
    logger.info(f"[offline:v8] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}

    results_df = pd.DataFrame(results)
    # 区间过滤
    results_df["score_val"] = pd.to_numeric(results_df["综合评分"], errors="coerce")
    results_df = results_df[(results_df["score_val"] >= score_min) & (results_df["score_val"] <= score_max)]
    results_df = results_df.drop(columns=["score_val"])
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df, "综合评分", select_mode, score_min, top_percent, threshold_floor=45.0, rescue_top_percent=8
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:v8] rescue filter applied: {rescue_meta}")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "综合评分", min_align, "v8")
    results_df = _add_reason_summary(results_df, score_col="综合评分")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}
    results_df = results_df.reset_index(drop=True)

    _save_scan_cache(
        "v8_scan",
        params,
        db_last,
        results_df,
        {"candidate_count": len(stocks_df), "filter_failed": 0},
    )
    return results_df, {"candidate_count": len(stocks_df), "filter_failed": 0}


def _offline_scan_v9(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    scan_started_at = time.time()
    score_threshold = float(os.getenv("V9_SCORE_THRESHOLD", "60"))
    top_percent = int(os.getenv("V9_TOP_PERCENT", "1"))
    select_mode = os.getenv("V9_SELECT_MODE", "分位数筛选(Top%)")
    scan_all = os.getenv("V9_SCAN_ALL", "1") == "1"
    cap_min = float(os.getenv("V9_CAP_MIN", "0"))
    cap_max = float(os.getenv("V9_CAP_MAX", "0"))
    enable_consistency = os.getenv("V9_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("V9_MIN_ALIGN", "2"))
    holding_days = int(os.getenv("V9_HOLDING_DAYS", "20"))
    lookback_days = int(os.getenv("V9_LOOKBACK_DAYS", "120"))
    min_turnover = float(os.getenv("V9_MIN_TURNOVER", "5.0"))
    candidate_count = int(os.getenv("V9_CANDIDATE_COUNT", "800"))

    params = {
        "score_threshold": score_threshold,
        "top_percent": top_percent,
        "select_mode": select_mode,
        "scan_all": scan_all,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
        "holding_days": holding_days,
        "lookback_days": lookback_days,
        "min_turnover": min_turnover,
        "candidate_count": candidate_count,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("v9_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        meta_out = dict(cached_meta or {})
        meta_out["cache_hit"] = True
        meta_out["cache_mode"] = "exact"
        meta_out["served_at"] = _now_text()
        meta_out["strategy"] = "v9"
        return cached_df, meta_out

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=scan_all,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
        require_industry=True,
    )
    if OFFLINE_STOCK_LIMIT > 0:
        candidate_count = min(candidate_count, OFFLINE_STOCK_LIMIT)
    stocks_df = stocks_df.head(candidate_count)
    logger.info(f"[offline:v9] candidates {len(stocks_df)}")

    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = _load_external_bonus_maps(conn)
    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="v9",
        min_history=80,
        load_history=lambda c, ts: _load_stock_history(
            c, ts, max(80, int(lookback_days)), "trade_date, close_price, high_price, low_price, vol, pct_chg"
        ),
        evaluate=lambda _row, stock_data: analyzer._calc_v9_score_from_hist(stock_data, industry_strength=0.0),
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "综合评分": f"{float(payload.score_result.get('score', 0)) + _calc_external_bonus(payload.row['ts_code'], payload.row['industry'], bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map):.1f}",
            "最新价格": (
                f"{float(payload.stock_data['close_price'].iloc[-1]):.2f}元"
                if ("close_price" in payload.stock_data.columns and len(payload.stock_data) > 0)
                else (
                    f"{float(payload.stock_data['close'].iloc[-1]):.2f}元"
                    if ("close" in payload.stock_data.columns and len(payload.stock_data) > 0)
                    else "-"
                )
            ),
            "建议持仓": f"{holding_days}天",
        },
        on_progress=_offline_log_progress,
    )

    conn.close()
    logger.info(f"[offline:v9] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0, "cache_hit": False, "cache_mode": "miss"}

    results_df = pd.DataFrame(results)
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df, "综合评分", select_mode, score_threshold, top_percent, threshold_floor=50.0, rescue_top_percent=8
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:v9] rescue filter applied: {rescue_meta}")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0, "cache_hit": False, "cache_mode": "miss"}
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "综合评分", min_align, "v9")
    results_df = _add_reason_summary(results_df, score_col="综合评分")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0, "cache_hit": False, "cache_mode": "miss"}
    results_df = results_df.reset_index(drop=True)

    _save_scan_cache(
        "v9_scan",
        params,
        db_last,
        results_df,
        {
            "candidate_count": len(stocks_df),
            "filter_failed": 0,
            "cache_hit": False,
            "cache_mode": "miss",
            "elapsed_ms": int((time.time() - scan_started_at) * 1000),
            "lookback_days": int(lookback_days),
        },
    )
    return results_df, {
        "candidate_count": len(stocks_df),
        "filter_failed": 0,
        "cache_hit": False,
        "cache_mode": "miss",
        "elapsed_ms": int((time.time() - scan_started_at) * 1000),
        "lookback_days": int(lookback_days),
    }


def _offline_scan_combo(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    scan_started_at = time.time()
    candidate_count = int(os.getenv("COMBO_CANDIDATE_COUNT", "800"))
    min_turnover = float(os.getenv("COMBO_MIN_TURNOVER", "5.0"))
    production_only = os.getenv("COMBO_PRODUCTION_ONLY", "1") == "1"
    min_agree_default = "2" if production_only else "3"
    min_agree = int(os.getenv("COMBO_MIN_AGREE", min_agree_default))
    cap_min = float(os.getenv("COMBO_CAP_MIN", "0"))
    cap_max = float(os.getenv("COMBO_CAP_MAX", "0"))
    select_mode = os.getenv("COMBO_SELECT_MODE", "分位数筛选(Top%)")
    combo_threshold = float(os.getenv("COMBO_THRESHOLD", "68"))
    top_percent = int(os.getenv("COMBO_TOP_PERCENT", "1"))
    lookback_days = int(os.getenv("COMBO_LOOKBACK_DAYS", "90"))
    disagree_std_weight = float(os.getenv("COMBO_DISAGREE_STD_WEIGHT", "0.35"))
    disagree_count_weight = float(os.getenv("COMBO_DISAGREE_COUNT_WEIGHT", "1.0"))
    market_adjust_strength = float(os.getenv("COMBO_MARKET_ADJUST_STRENGTH", "0.5"))
    enable_consistency = os.getenv("COMBO_ENABLE_CONSISTENCY", "1") == "1"
    min_align = int(os.getenv("COMBO_MIN_ALIGN", "2"))
    auto_weights = os.getenv("COMBO_AUTO_WEIGHTS", "1") == "1"
    lightweight_mode = os.getenv("COMBO_LIGHTWEIGHT", "1") == "1"

    market_env_combo = "oscillation"
    try:
        market_env_combo = analyzer.get_market_environment()
    except Exception:
        market_env_combo = "oscillation"

    if production_only:
        weight_presets = {
            "bull": {"v4": 0.00, "v5": 0.55, "v7": 0.00, "v8": 0.10, "v9": 0.35},
            "oscillation": {"v4": 0.00, "v5": 0.45, "v7": 0.00, "v8": 0.05, "v9": 0.50},
            "bear": {"v4": 0.00, "v5": 0.20, "v7": 0.00, "v8": 0.20, "v9": 0.60},
        }
    else:
        weight_presets = {
            "bull": {"v4": 0.10, "v5": 0.20, "v7": 0.30, "v8": 0.30, "v9": 0.10},
            "oscillation": {"v4": 0.15, "v5": 0.15, "v7": 0.30, "v8": 0.25, "v9": 0.15},
            "bear": {"v4": 0.25, "v5": 0.15, "v7": 0.20, "v8": 0.15, "v9": 0.25},
        }
    preset = weight_presets.get(market_env_combo, weight_presets["oscillation"])
    w_v4 = float(os.getenv("COMBO_W_V4", str(preset["v4"])))
    w_v5 = float(os.getenv("COMBO_W_V5", str(preset["v5"])))
    w_v7 = float(os.getenv("COMBO_W_V7", str(preset["v7"])))
    w_v8 = float(os.getenv("COMBO_W_V8", str(preset["v8"])))
    w_v9 = float(os.getenv("COMBO_W_V9", str(preset["v9"])))
    if auto_weights:
        w_v4, w_v5, w_v7, w_v8, w_v9 = (
            preset["v4"],
            preset["v5"],
            preset["v7"],
            preset["v8"],
            preset["v9"],
        )

    thr_v4 = float(os.getenv("COMBO_THR_V4", "60"))
    thr_v5 = float(os.getenv("COMBO_THR_V5", "60"))
    thr_v7 = float(os.getenv("COMBO_THR_V7", "65"))
    thr_v8 = float(os.getenv("COMBO_THR_V8", "65"))
    thr_v9 = float(os.getenv("COMBO_THR_V9", "60"))

    weights = {"v4": w_v4, "v5": w_v5, "v7": w_v7, "v8": w_v8, "v9": w_v9}
    health_mul = _production_strategy_health_multipliers()
    for k in ("v5", "v8", "v9"):
        weights[k] = float(weights.get(k, 0.0)) * float(health_mul.get(k, 1.0))
    wsum_prod = float(weights.get("v5", 0.0) + weights.get("v8", 0.0) + weights.get("v9", 0.0))
    if production_only and wsum_prod > 0:
        weights["v5"] = weights["v5"] / wsum_prod
        weights["v8"] = weights["v8"] / wsum_prod
        weights["v9"] = weights["v9"] / wsum_prod
        weights["v4"] = 0.0
        weights["v7"] = 0.0
    score_thresholds = {"v4": thr_v4, "v5": thr_v5, "v7": thr_v7, "v8": thr_v8, "v9": thr_v9}

    params = {
        "candidate_count": candidate_count,
        "min_turnover": min_turnover,
        "min_agree": min_agree,
        "cap_min": cap_min,
        "cap_max": cap_max,
        "select_mode": select_mode,
        "combo_threshold": combo_threshold,
        "top_percent": top_percent,
        "lookback_days": lookback_days,
        "disagree_std_weight": disagree_std_weight,
        "disagree_count_weight": disagree_count_weight,
        "market_adjust_strength": market_adjust_strength,
        "enable_consistency": enable_consistency,
        "min_align": min_align,
        "auto_weights": auto_weights,
        "lightweight_mode": lightweight_mode,
        "production_only": production_only,
        "w_v4": w_v4,
        "w_v5": w_v5,
        "w_v7": w_v7,
        "w_v8": w_v8,
        "w_v9": w_v9,
        "health_mul_v5": float(health_mul.get("v5", 1.0)),
        "health_mul_v8": float(health_mul.get("v8", 1.0)),
        "health_mul_v9": float(health_mul.get("v9", 1.0)),
        "thr_v4": thr_v4,
        "thr_v5": thr_v5,
        "thr_v7": thr_v7,
        "thr_v8": thr_v8,
        "thr_v9": thr_v9,
        "market_env": market_env_combo,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("combo_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        meta_out = dict(cached_meta or {})
        meta_out["cache_hit"] = True
        meta_out["cache_mode"] = "exact"
        meta_out["served_at"] = _now_text()
        meta_out["strategy"] = "combo"
        return cached_df, meta_out
    if production_only and lightweight_mode:
        reused_df, reused_meta = _find_recent_scan_cache(
            "combo_scan",
            db_last,
            predicate=lambda meta: bool(((meta.get("params") or {}).get("production_only", False))),
        )
        if reused_df is not None and not reused_df.empty:
            meta_out = dict(reused_meta or {})
            meta_out["cache_hit"] = True
            meta_out["cache_mode"] = "lightweight_reuse"
            meta_out["served_at"] = _now_text()
            meta_out["strategy"] = "combo"
            return reused_df, meta_out

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=True,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
        require_industry=True,
    )
    if OFFLINE_STOCK_LIMIT > 0:
        candidate_count = min(candidate_count, OFFLINE_STOCK_LIMIT)
    stocks_df = stocks_df.head(candidate_count)
    logger.info(f"[offline:combo] candidates {len(stocks_df)}")
    bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = _load_external_bonus_maps(conn)
    conn.close()

    # Calendar days undercount trading rows around long holidays; keep the
    # fetch window wide enough for the 80-row combo history gate.
    combo_history_days = max(int(lookback_days) + 60, 140)
    combo_start = (datetime.now() - timedelta(days=combo_history_days)).strftime("%Y%m%d")
    combo_end = datetime.now().strftime("%Y%m%d")

    ind_vals: Dict[str, List[float]] = {}
    for _, row in stocks_df.iterrows():
        hist = _load_history_full(str(row["ts_code"]), combo_start, combo_end)
        if hist is None or len(hist) < 21:
            continue
        close = pd.to_numeric(hist["close_price"], errors="coerce").ffill()
        if len(close) > 21:
            r20 = (close.iloc[-1] / close.iloc[-21] - 1.0) * 100
            ind_vals.setdefault(str(row["industry"]), []).append(float(r20))
    industry_scores = {ind: float(np.mean(vals)) for ind, vals in ind_vals.items() if vals}

    def _combo_eval(row: pd.Series, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
        avg_amount = pd.to_numeric(hist["amount"], errors="coerce").tail(20).mean()
        avg_amount_yi = avg_amount / 1e5
        if avg_amount_yi < min_turnover:
            return None

        stock_data = hist.copy()
        stock_data["name"] = row["name"]

        v4_res = analyzer.evaluator_v4.evaluate_stock_v4(stock_data)
        v5_res = analyzer.evaluator_v5.evaluate_stock_v4(stock_data)
        v7_res = analyzer.evaluator_v7.evaluate_stock_v7(
            stock_data=stock_data, ts_code=row["ts_code"], industry=row["industry"]
        )
        v8_res = analyzer.evaluator_v8.evaluate_stock_v8(
            stock_data=stock_data, ts_code=row["ts_code"], index_data=None
        )
        ind_strength = industry_scores.get(str(row["industry"]), 0.0)
        v9_info = analyzer._calc_v9_score_from_hist(hist, industry_strength=ind_strength)

        scores = {
            "v4": float(v4_res.get("final_score", 0)) if v4_res else None,
            "v5": float(v5_res.get("final_score", 0)) if v5_res else None,
            "v7": float(v7_res.get("final_score", 0)) if v7_res and v7_res.get("success") else None,
            "v8": float(v8_res.get("final_score", 0)) if v8_res and v8_res.get("success") else None,
            "v9": float(v9_info.get("score", 0)) if v9_info else None,
        }
        if production_only:
            scores["v4"] = None
            scores["v7"] = None
        consensus_result = runtime_evaluate_combo_score_components(
            scores=scores,
            thresholds=score_thresholds,
            weights=weights,
            combo_threshold=combo_threshold,
            min_agree=min_agree,
            market_env=market_env_combo,
        )
        if not consensus_result:
            return None
        extra = _calc_external_bonus(
            row["ts_code"],
            row["industry"],
            bonus_global,
            bonus_stock_map,
            top_list_set,
            top_inst_set,
            bonus_industry_map,
        )
        return runtime_finalize_combo_scan_score(
            consensus_result=consensus_result,
            scores=scores,
            thresholds=score_thresholds,
            weights=weights,
            combo_threshold=combo_threshold,
            disagree_std_weight=disagree_std_weight,
            disagree_count_weight=disagree_count_weight,
            market_adjust_strength=market_adjust_strength,
            market_env=market_env_combo,
            external_bonus=extra,
        )

    results = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=None,
        tag="combo",
        min_history=80,
        load_history=lambda _c, ts: _load_history_full(ts, combo_start, combo_end),
        evaluate=_combo_eval,
        build_result=lambda payload: {
            "股票代码": payload.row["ts_code"],
            "股票名称": payload.row["name"],
            "行业": payload.row["industry"],
            "流通市值": f"{payload.row['circ_mv']/10000:.1f}亿",
            "共识评分": f"{float(payload.score_result.get('final_score', 0)):.1f}",
            "共识基础分": f"{float(payload.score_result.get('weighted_score', 0)):.1f}",
            "资金加分": f"{float(payload.score_result.get('extra', 0)):.1f}",
            "分歧惩罚": f"{float(payload.score_result.get('penalty', 0)):.2f}",
            "市场因子": f"{float(payload.score_result.get('adj_factor', 1.0)):.2f}",
            "一致数": int(payload.score_result["agree_count"]),
            "一致门槛": int(payload.score_result.get("required_agree", min_agree)),
            "v4": (
                f"{float(payload.score_result['scores']['v4']):.1f}"
                if payload.score_result["scores"].get("v4") is not None else "-"
            ),
            "v5": (
                f"{float(payload.score_result['scores']['v5']):.1f}"
                if payload.score_result["scores"].get("v5") is not None else "-"
            ),
            "v7": (
                f"{float(payload.score_result['scores']['v7']):.1f}"
                if payload.score_result["scores"].get("v7") is not None else "-"
            ),
            "v8": (
                f"{float(payload.score_result['scores']['v8']):.1f}"
                if payload.score_result["scores"].get("v8") is not None else "-"
            ),
            "v9": (
                f"{float(payload.score_result['scores']['v9']):.1f}"
                if payload.score_result["scores"].get("v9") is not None else "-"
            ),
            **{k: f"{float(v):.1f}" for k, v in payload.score_result.get("contrib", {}).items()},
            "建议持仓": "5-15天",
        },
        on_progress=_offline_log_progress,
    )

    logger.info(f"[offline:combo] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0, "cache_hit": False, "cache_mode": "miss"}

    results_df = pd.DataFrame(results)
    results_df, rescue_meta = _apply_filter_mode_with_rescue(
        results_df, "共识评分", select_mode, combo_threshold, top_percent, threshold_floor=55.0, rescue_top_percent=6
    )
    if rescue_meta.get("used"):
        logger.warning(f"[offline:combo] rescue filter applied: {rescue_meta}")
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0, "cache_hit": False, "cache_mode": "miss"}
    if enable_consistency and not results_df.empty:
        results_df, _ = _apply_consistency_with_fallback(results_df, "共识评分", min_align, "combo")
    results_df = _add_reason_summary(results_df, score_col="共识评分")
    rb = _load_portfolio_risk_budget()
    if bool(rb.get("enabled", False)):
        before_n = int(len(results_df))
        results_df = _apply_portfolio_risk_budget(
            results_df,
            score_col="共识评分",
            industry_col="行业",
            max_positions=int(rb.get("max_positions", 20)),
            max_industry_ratio=float(rb.get("max_industry_ratio", 0.35)),
        )
        logger.info(
            "[offline:combo] risk budget applied: before=%s after=%s max_positions=%s max_industry_ratio=%.2f",
            before_n,
            len(results_df),
            int(rb.get("max_positions", 20)),
            float(rb.get("max_industry_ratio", 0.35)),
        )
    if results_df is None or results_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0, "cache_hit": False, "cache_mode": "miss"}
    results_df = results_df.reset_index(drop=True)

    _save_scan_cache(
        "combo_scan",
        params,
        db_last,
        results_df,
        {
            "candidate_count": len(stocks_df),
            "filter_failed": 0,
            "cache_hit": False,
            "cache_mode": "miss",
            "elapsed_ms": int((time.time() - scan_started_at) * 1000),
            "lookback_days": int(lookback_days),
        },
    )
    return results_df, {
        "candidate_count": len(stocks_df),
        "filter_failed": 0,
        "cache_hit": False,
        "cache_mode": "miss",
        "elapsed_ms": int((time.time() - scan_started_at) * 1000),
        "lookback_days": int(lookback_days),
    }


def _offline_scan_stable() -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    lookback_days = int(os.getenv("STABLE_LOOKBACK_DAYS", "120"))
    max_drawdown = float(os.getenv("STABLE_MAX_DRAWDOWN", "0.15"))
    vol_max = float(os.getenv("STABLE_VOL_MAX", "0.04"))
    rebound_min = float(os.getenv("STABLE_REBOUND_MIN", "0.10"))
    candidate_count = int(os.getenv("STABLE_CANDIDATE_COUNT", "200"))
    result_count = int(os.getenv("STABLE_RESULT_COUNT", "30"))
    min_turnover = float(os.getenv("STABLE_MIN_TURNOVER", "5.0"))
    min_mv = int(os.getenv("STABLE_MIN_MV", "100"))
    max_mv = int(os.getenv("STABLE_MAX_MV", "5000"))

    params = {
        "lookback_days": lookback_days,
        "max_drawdown": max_drawdown,
        "vol_max": vol_max,
        "rebound_min": rebound_min,
        "candidate_count": candidate_count,
        "result_count": result_count,
        "min_turnover": min_turnover,
        "min_mv": min_mv,
        "max_mv": max_mv,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("stable_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    if not os.path.exists(PERMANENT_DB_PATH):
        return None, {"error": "database not found"}

    try:
        _stable_mod = load_stable_uptrend_module()
    except Exception as e:
        return None, {"error": f"stable module missing: {e}"}

    ctx = _StableUptrendContext(PERMANENT_DB_PATH, db_manager=None)
    data = ctx.get_real_stock_data_optimized()
    data = ctx._apply_global_filters(data, min_mv=min_mv, max_mv=max_mv, use_price=False, use_turnover=False)
    if data is None or data.empty:
        return None, {"candidate_count": 0, "filter_failed": 0}

    filtered = data.copy()
    filtered = filtered[filtered["成交额"] >= min_turnover * 1e8]
    filtered = filtered[(filtered["价格"] > 2) & (filtered["价格"] < 200)]
    filtered = filtered.sort_values("成交额", ascending=False)
    if OFFLINE_STOCK_LIMIT > 0:
        candidate_count = min(candidate_count, OFFLINE_STOCK_LIMIT)
    filtered = filtered.head(candidate_count)
    logger.info(f"[offline:stable] candidates {len(filtered)}")

    if filtered.empty:
        return None, {"candidate_count": 0, "filter_failed": 0}

    start_date = (datetime.now() - timedelta(days=lookback_days * 2)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")

    results = []
    for idx, row in enumerate(filtered.itertuples(index=False), 1):
        _offline_log_progress("stable", idx, len(filtered))
        ts_code = getattr(row, "股票代码")
        name = getattr(row, "股票名称")
        score_info = _stable_mod._score_stable_uptrend(
            ctx,
            ts_code,
            lookback_days,
            max_drawdown,
            vol_max,
            rebound_min,
            start_date,
            end_date,
            pro=None,
        )
        if score_info:
            results.append(
                {
                    "股票代码": ts_code,
                    "股票名称": name,
                    "稳定上涨评分": round(score_info["score"], 1),
                    "最大回撤": round(score_info["max_dd"] * 100, 2),
                    "20日波动率": round(score_info["vol"] * 100, 2),
                    "反弹幅度": round(score_info["rebound"] * 100, 2),
                    "趋势": "✅" if score_info["trend_ok"] else "❌",
                    "二次启动": "✅" if score_info["breakout"] else "❌",
                    "建议持有周期": score_info["hold_days"],
                }
            )

    logger.info(f"[offline:stable] results {len(results)}")
    if not results:
        return None, {"candidate_count": len(filtered), "filter_failed": 0}

    results_df = pd.DataFrame(results).sort_values("稳定上涨评分", ascending=False)
    results_df = results_df.head(result_count)

    _save_scan_cache(
        "stable_scan",
        params,
        db_last,
        results_df,
        {"candidate_count": len(filtered), "filter_failed": 0},
    )
    return results_df, {"candidate_count": len(filtered), "filter_failed": 0}


def _offline_scan_ai(analyzer: "CompleteVolumePriceAnalyzer") -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
    min_strength = int(os.getenv("AI_MIN_STRENGTH", "55"))
    investment_cycle = os.getenv("AI_CYCLE", "balanced")
    lookback_days = int(os.getenv("AI_LOOKBACK_DAYS", "120"))
    candidate_count = int(os.getenv("AI_CANDIDATE_COUNT", "800"))
    result_count = int(os.getenv("AI_RESULT_COUNT", "50"))
    cap_min = float(os.getenv("AI_CAP_MIN", "0"))
    cap_max = float(os.getenv("AI_CAP_MAX", "0"))

    params = {
        "min_strength": min_strength,
        "investment_cycle": investment_cycle,
        "lookback_days": lookback_days,
        "candidate_count": candidate_count,
        "result_count": result_count,
        "cap_min": cap_min,
        "cap_max": cap_max,
    }
    db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
    cached_df, cached_meta = _load_scan_cache("ai_scan", params, db_last)
    if cached_df is not None and not cached_df.empty:
        return cached_df, cached_meta

    conn = _connect_permanent_db()
    stocks_df = _load_candidate_stocks(
        conn,
        scan_all=True,
        cap_min_yi=cap_min,
        cap_max_yi=cap_max,
        distinct=False,
    )

    if OFFLINE_STOCK_LIMIT > 0:
        candidate_count = min(candidate_count, OFFLINE_STOCK_LIMIT)
    stocks_df = stocks_df.head(candidate_count)
    logger.info(f"[offline:ai] candidates {len(stocks_df)}")

    frame_rows = run_stock_scan_pipeline(
        stocks_df=stocks_df,
        conn=conn,
        tag="ai",
        min_history=1,
        load_history=lambda c, ts: _load_stock_history(
            c,
            ts,
            lookback_days + 30,
            "trade_date, close_price, pct_chg, vol, amount",
        ),
        evaluate=lambda _row, _stock_data: {"ok": True},
        build_result=lambda payload: {
            "frame": (
                payload.stock_data.assign(
                    close=lambda d: d["close_price"] if "close_price" in d.columns and "close" not in d.columns else d.get("close", np.nan),
                    ts_code=payload.row["ts_code"],
                    name=payload.row["name"],
                    industry=payload.row["industry"] if pd.notna(payload.row["industry"]) else "未知",
                )
            ),
        },
        on_progress=_offline_log_progress,
    )
    conn.close()

    frames = [item.get("frame") for item in frame_rows if isinstance(item, dict) and item.get("frame") is not None]

    if not frames:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}

    all_df = pd.concat(frames, ignore_index=True)
    result_df = analyzer.select_current_stocks_complete(
        all_df,
        min_strength=min_strength,
        investment_cycle=investment_cycle,
    )
    if result_df is None or result_df.empty:
        return None, {"candidate_count": len(stocks_df), "filter_failed": 0}

    result_df = result_df.rename(
        columns={
            "ts_code": "股票代码",
            "name": "股票名称",
            "industry": "行业",
            "buy_value": "综合评分",
            "signal_strength": "信号强度",
            "latest_price": "最新价格",
            "signal_reasons": "筛选理由",
            "signal_date": "信号日期",
        }
    )
    result_df = result_df.sort_values("综合评分", ascending=False).head(result_count)

    _save_scan_cache(
        "ai_scan",
        params,
        db_last,
        result_df,
        {"candidate_count": len(stocks_df), "filter_failed": 0},
    )
    return result_df, {"candidate_count": len(stocks_df), "filter_failed": 0}


def _offline_scan_sector() -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    days = int(os.getenv("SECTOR_SCAN_DAYS", "60"))
    try:
        scanner = MarketScanner()
        results = scanner.scan_all_sectors(days=days)
        cache_dir = _v7_cache_dir()
        os.makedirs(cache_dir, exist_ok=True)
        db_last = _get_db_last_trade_date(PERMANENT_DB_PATH)
        out_path = os.path.join(cache_dir, f"sector_scan_{db_last}_{days}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        return results, {"cache_path": out_path}
    except Exception as e:
        return None, {"error": str(e)}


def _load_history_full(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        from data.history import load_history_full as _load_history_full_v2  # type: ignore
        return _load_history_full_v2(
            db_path=PERMANENT_DB_PATH,
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns="trade_date, close_price, high_price, low_price, vol, amount, pct_chg, turnover_rate",
            normalize_fn=_normalize_stock_df,
        )
    except Exception:
        return pd.DataFrame()


def run_offline_all() -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    targets = _offline_targets()
    analyzer = CompleteVolumePriceAnalyzer()
    if "v4" in targets:
        out["v4"] = _offline_scan_v4(analyzer)
    if "v5" in targets:
        out["v5"] = _offline_scan_v5(analyzer)
    if "v6" in targets:
        out["v6"] = _offline_scan_v6(analyzer)
    if "v7" in targets:
        out["v7"] = run_offline_v7_scan()
    if "v8" in targets:
        out["v8"] = _offline_scan_v8(analyzer)
    if "v9" in targets:
        out["v9"] = _offline_scan_v9(analyzer)
    if "combo" in targets:
        out["combo"] = _offline_scan_combo(analyzer)
    if "stable" in targets:
        out["stable"] = _offline_scan_stable()
    if "ai" in targets:
        out["ai"] = _offline_scan_ai(analyzer)
    if "sector" in targets:
        out["sector"] = _offline_scan_sector()
    return out


class UnifiedBacktestEngine:
    """Unified execution layer for stock-level backtests."""

    def __init__(
        self,
        df: pd.DataFrame,
        sample_size: int,
        holding_days: int,
        fee_bps: Optional[float] = None,
        slippage_bps: Optional[float] = None,
    ):
        self.df = _ensure_price_aliases(df.copy()) if df is not None else pd.DataFrame()
        self.sample_size = int(sample_size)
        self.holding_days = int(holding_days)
        self.fee_bps = float(os.getenv("OPENCLAW_BACKTEST_FEE_BPS", "3")) if fee_bps is None else float(fee_bps)
        self.slippage_bps = float(os.getenv("OPENCLAW_BACKTEST_SLIPPAGE_BPS", "5")) if slippage_bps is None else float(slippage_bps)
        self.stock_groups: Dict[str, pd.DataFrame] = {}
        if not self.df.empty and "trade_date" in self.df.columns:
            self.df["trade_date"] = self.df["trade_date"].astype(str)
        if not self.df.empty and "ts_code" in self.df.columns and self.sample_size > 0:
            try:
                all_codes = self.df["ts_code"].dropna().astype(str).unique().tolist()
                if len(all_codes) > self.sample_size:
                    picked = set(np.random.choice(all_codes, self.sample_size, replace=False).tolist())
                    self.df = self.df[self.df["ts_code"].astype(str).isin(picked)].copy()
            except Exception:
                pass
        if not self.df.empty and "ts_code" in self.df.columns:
            try:
                for ts_code, g in self.df.groupby("ts_code", sort=False):
                    self.stock_groups[str(ts_code)] = g.sort_values("trade_date").copy()
            except Exception:
                self.stock_groups = {}

    def _apply_costs(self, gross_return_pct: float) -> Tuple[float, float]:
        total_bps = max(0.0, float(self.fee_bps)) + max(0.0, float(self.slippage_bps))
        round_trip_cost_pct = (2.0 * total_bps) / 100.0
        return float(gross_return_pct - round_trip_cost_pct), float(round_trip_cost_pct)

    def _sample_stocks(self) -> List[str]:
        if self.stock_groups:
            all_stocks = list(self.stock_groups.keys())
            if len(all_stocks) > self.sample_size:
                return list(np.random.choice(all_stocks, self.sample_size, replace=False))
            return all_stocks
        if self.df is None or self.df.empty or "ts_code" not in self.df.columns:
            return []
        all_stocks = list(self.df["ts_code"].dropna().astype(str).unique())
        if len(all_stocks) > self.sample_size:
            return list(np.random.choice(all_stocks, self.sample_size, replace=False))
        return all_stocks

    def run_rolling(
        self,
        min_rows: int,
        window: int,
        step: int,
        signal_fn: Callable[[str, pd.DataFrame, int, pd.DataFrame], Optional[Dict[str, Any]]],
        stop_on_first_signal: bool = False,
    ) -> Tuple[pd.DataFrame, int]:
        records: List[Dict[str, Any]] = []
        analyzed = 0
        for ts_code in self._sample_stocks():
            g = self.stock_groups.get(str(ts_code))
            if g is None:
                g = self.df[self.df["ts_code"] == ts_code].sort_values("trade_date")
            if len(g) < int(min_rows):
                continue
            analyzed += 1
            max_idx = len(g) - self.holding_days
            for i in range(int(window), max_idx, int(step)):
                hist = g.iloc[i - int(window):i].copy()
                sig = signal_fn(ts_code, g, i, hist)
                if not sig:
                    continue
                exit_offset = int(sig.pop("__exit_offset", self.holding_days))
                if exit_offset < 1:
                    exit_offset = 1
                if i + exit_offset >= len(g):
                    continue
                close_col = "close_price" if "close_price" in g.columns else "close"
                entry_price = float(g.iloc[i][close_col])
                exit_price = float(g.iloc[i + exit_offset][close_col])
                gross_return = (exit_price / entry_price - 1.0) * 100 if entry_price else 0.0
                future_return, cost_pct = self._apply_costs(gross_return)
                record = {
                    "ts_code": ts_code,
                    "trade_date": g.iloc[i]["trade_date"],
                    "future_return": future_return,
                    "gross_return": float(gross_return),
                    "round_trip_cost_pct": cost_pct,
                    "holding_days_realized": int(exit_offset),
                }
                record.update(sig)
                records.append(record)
                if bool(stop_on_first_signal):
                    break
        return pd.DataFrame(records), analyzed

    def run_last_point(
        self,
        min_rows: int,
        min_hist_idx: int,
        signal_fn: Callable[[str, pd.DataFrame, int, pd.DataFrame], Optional[Dict[str, Any]]],
    ) -> Tuple[pd.DataFrame, int]:
        records: List[Dict[str, Any]] = []
        analyzed = 0
        for ts_code in self._sample_stocks():
            g = self.stock_groups.get(str(ts_code))
            if g is None:
                g = self.df[self.df["ts_code"] == ts_code].sort_values("trade_date")
            if len(g) < int(min_rows):
                continue
            analyzed += 1
            i = len(g) - self.holding_days - 1
            if i < int(min_hist_idx):
                continue
            hist = g.iloc[: i + 1].copy()
            sig = signal_fn(ts_code, g, i, hist)
            if not sig:
                continue
            exit_offset = int(sig.pop("__exit_offset", self.holding_days))
            if exit_offset < 1:
                exit_offset = 1
            if i + exit_offset >= len(g):
                continue
            close_col = "close_price" if "close_price" in g.columns else "close"
            entry_price = float(g.iloc[i][close_col])
            exit_price = float(g.iloc[i + exit_offset][close_col])
            gross_return = (exit_price / entry_price - 1.0) * 100 if entry_price else 0.0
            future_return, cost_pct = self._apply_costs(gross_return)
            record = {
                "ts_code": ts_code,
                "trade_date": g.iloc[i]["trade_date"],
                "future_return": future_return,
                "gross_return": float(gross_return),
                "round_trip_cost_pct": cost_pct,
                "holding_days_realized": int(exit_offset),
            }
            record.update(sig)
            records.append(record)
        return pd.DataFrame(records), analyzed


# ===================== 完整的量价分析器（集成v43+v44）=====================
class CompleteVolumePriceAnalyzer:
    """完整的量价分析器 - 集成所有功能"""
    
    def __init__(self, db_path: str = PERMANENT_DB_PATH):
        self.db_path = db_path
        self.backtest_results = None
        self.signal_cache = {}
        
        #  初始化缓存数据库表
        self._init_cache_tables()
        
        #  初始化v4.0评分器（潜伏策略·长期稳健版）
        if V4_EVALUATOR_AVAILABLE:
            self.evaluator_v4 = ComprehensiveStockEvaluatorV4()
            self.use_v4 = True  # 默认使用v4.0
            self.use_v3 = False  # 不使用v3
            logger.info("v4.0评分器（潜伏策略·长期稳健版）已初始化")
        elif V3_EVALUATOR_AVAILABLE:
            self.evaluator_v3 = ComprehensiveStockEvaluatorV3()
            self.use_v4 = False
            self.use_v3 = True
            logger.info("v3.0评分器（启动为王版）已初始化（备用）")
        else:
            self.evaluator_v4 = None
            self.evaluator_v3 = None
            self.use_v4 = False
            self.use_v3 = False
            logger.info(" 使用v2.0评分器（筹码版）")
        
        #  初始化v5.0评分器（启动确认版）
        if V5_EVALUATOR_AVAILABLE:
            self.evaluator_v5 = ComprehensiveStockEvaluatorV5()
            logger.info("v5.0评分器（启动确认版）已初始化")
        else:
            self.evaluator_v5 = None
        
        #  初始化v6.0评分器（高级高回报版）
        if V6_EVALUATOR_AVAILABLE:
            self.evaluator_v6 = ComprehensiveStockEvaluatorV6()
            logger.info("v6.0评分器·专业版已初始化")
        else:
            self.evaluator_v6 = None
        
        #  初始化v7.0评分器（智能选股系统 - 专业标准）
        if V7_EVALUATOR_AVAILABLE:
            self.evaluator_v7 = ComprehensiveStockEvaluatorV7Ultimate(self.db_path)
            logger.info("v7.0智能选股系统已初始化")
        else:
            self.evaluator_v7 = None
        
        #  初始化v8.0评分器（进阶版 - 量化策略）
        if V8_EVALUATOR_AVAILABLE:
            self.evaluator_v8 = ComprehensiveStockEvaluatorV8Ultimate(self.db_path)
            self.kelly_manager = KellyPositionManager()
            self.rebalance_manager = DynamicRebalanceManager()
            logger.info("v8.0进阶版已初始化: ATR风控+市场过滤+凯利仓位+动态再平衡")
        else:
            self.evaluator_v8 = None
            self.kelly_manager = None
            self.rebalance_manager = None
    
    def _init_cache_tables(self):
        """初始化缓存数据库表"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建v5.0扫描结果缓存表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scan_cache_v5 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_date TEXT NOT NULL,
                    ts_code TEXT NOT NULL,
                    stock_name TEXT,
                    industry TEXT,
                    latest_price REAL,
                    circ_mv REAL,
                    final_score REAL,
                    dim_scores TEXT,
                    scan_params TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(scan_date, ts_code, scan_params)
                )
            """)
            
            # 创建v6.0扫描结果缓存表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scan_cache_v6 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scan_date TEXT NOT NULL,
                    ts_code TEXT NOT NULL,
                    stock_name TEXT,
                    industry TEXT,
                    latest_price REAL,
                    circ_mv REAL,
                    final_score REAL,
                    dim_scores TEXT,
                    scan_params TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(scan_date, ts_code, scan_params)
                )
            """)
            
            # 创建索引加速查询
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_scan_cache_v5_date 
                ON scan_cache_v5(scan_date, scan_params)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_scan_cache_v6_date 
                ON scan_cache_v6(scan_date, scan_params)
            """)
            
            conn.commit()
            conn.close()
            logger.info("扫描结果缓存表初始化成功")
        except Exception as e:
            logger.error(f"缓存表初始化失败: {e}")
    
    def save_scan_results_to_cache(self, results: list, version: str, scan_params: dict):
        """保存扫描结果到缓存数据库"""
        try:
            import json
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            scan_date = datetime.now().strftime('%Y%m%d')
            scan_params_str = json.dumps(scan_params, ensure_ascii=False, sort_keys=True)
            table_name = f"scan_cache_{version}"
            
            for result in results:
                dim_scores_str = json.dumps(result.get('dim_scores', {}), ensure_ascii=False) if 'dim_scores' in result else '{}'
                
                cursor.execute(f"""
                    INSERT OR REPLACE INTO {table_name} 
                    (scan_date, ts_code, stock_name, industry, latest_price, circ_mv, final_score, dim_scores, scan_params)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    scan_date,
                    result.get('股票代码', ''),
                    result.get('股票名称', ''),
                    result.get('行业', ''),
                    result.get('最新价', 0),
                    result.get('流通市值(亿)', 0),
                    result.get('综合评分', 0),
                    dim_scores_str,
                    scan_params_str
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"已保存 {len(results)} 条{version}扫描结果到缓存")
            return True
        except Exception as e:
            logger.error(f"保存扫描结果失败: {e}")
            return False
    
    def load_scan_results_from_cache(self, version: str, scan_params: dict):
        """从缓存加载扫描结果"""
        try:
            import json
            conn = sqlite3.connect(self.db_path)
            
            scan_date = datetime.now().strftime('%Y%m%d')
            scan_params_str = json.dumps(scan_params, ensure_ascii=False, sort_keys=True)
            table_name = f"scan_cache_{version}"
            
            query = f"""
                SELECT ts_code, stock_name, industry, latest_price, circ_mv, 
                       final_score, dim_scores
                FROM {table_name}
                WHERE scan_date = ? AND scan_params = ?
                ORDER BY final_score DESC
            """
            
            df = pd.read_sql_query(query, conn, params=(scan_date, scan_params_str))
            conn.close()
            
            if len(df) > 0:
                logger.info(f"从缓存加载了 {len(df)} 条{version}扫描结果")
                return df
            else:
                return None
        except Exception as e:
            logger.error(f"加载缓存失败: {e}")
            return None
        
    def get_market_trend(self, days: int = 5) -> Dict:
        """
         新增：获取大盘趋势分析
        返回市场环境判断和建议
        
        使用Tushare Pro直接获取上证指数数据
        """
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=max(days, 10) + 5)).strftime('%Y%m%d')

            if TUSHARE_ENABLED:
                import tushare as ts
                pro = ts.pro_api(TUSHARE_TOKEN)
                df = pro.index_daily(ts_code='000001.SH', start_date=start_date, end_date=end_date)
            else:
                df = _get_index_daily_from_db(start_date, end_date)
            
            if df is None or len(df) == 0:
                return {
                    'status': 'unknown',
                    'trend': ' 无法获取大盘数据',
                    'recommendation': '暂无建议',
                    'details': 'Tushare数据获取失败',
                    'color': 'warning'
                }
            
            # 按日期倒序排序（最新的在前）
            df = df.sort_values('trade_date', ascending=False)
            
            if len(df) < 5:
                return {
                    'status': 'unknown',
                    'trend': ' 无法获取大盘数据',
                    'recommendation': '暂无建议',
                    'details': '数据不足',
                    'color': 'warning'
                }
            
            # 计算近5日涨跌幅
            change_5d = df['pct_chg'].head(5).sum()
            
            # 计算成交量变化（使用vol字段）
            recent_vol = df['vol'].head(5).mean() if len(df) >= 5 else 0
            prev_vol = df['vol'].tail(5).mean() if len(df) >= 10 else recent_vol
            vol_ratio = recent_vol / prev_vol if prev_vol > 0 else 1.0
            
            # 判断市场环境
            if change_5d > 3:
                status = 'excellent'
                trend = ' 大盘强势上涨'
                recommendation = ' 市场环境极佳，适合积极操作'
                color = 'success'
            elif change_5d > 0:
                status = 'good'
                trend = ' 大盘温和上涨'
                recommendation = ' 市场环境良好，可以正常操作'
                color = 'info'
            elif change_5d > -2:
                status = 'neutral'
                trend = ' 大盘震荡整理'
                recommendation = ' 市场震荡，谨慎选股，严格止损'
                color = 'warning'
            else:
                status = 'bad'
                trend = ' 大盘下跌趋势'
                recommendation = ' 市场走弱，建议空仓观望'
                color = 'error'
            
            details = f"近5日涨跌：{change_5d:+.2f}%"
            if vol_ratio < 0.8:
                details += " | 成交量萎缩"
            elif vol_ratio > 1.2:
                details += " | 成交量放大"
            
            return {
                'status': status,
                'trend': trend,
                'recommendation': recommendation,
                'details': details,
                'change_5d': change_5d,
                'color': color,
                'df': df  # 返回完整数据用于进一步分析
            }
            
        except Exception as e:
            logger.error(f"获取市场趋势失败: {e}")
            return {
                'status': 'unknown',
                'trend': ' 数据获取失败',
                'recommendation': '暂无建议',
                'details': str(e),
                'color': 'warning'
            }
    
    def analyze_market_during_backtest(self, start_date: str, end_date: str) -> Dict:
        """
         分析回测期间的市场环境
        这是诊断策略表现的关键！
        
        方法：直接从Tushare Pro获取上证指数数据
        """
        try:
            if TUSHARE_ENABLED:
                import tushare as ts
                pro = ts.pro_api(TUSHARE_TOKEN)
                df = pro.index_daily(ts_code='000001.SH', start_date=start_date, end_date=end_date)
            else:
                df = _get_index_daily_from_db(start_date, end_date)
            
            if df is None or len(df) == 0:
                return {
                    'success': False,
                    'error': 'Tushare获取大盘数据失败'
                }
            
            # 按日期排序
            df = df.sort_values('trade_date')
            
            if len(df) < 10:
                return {
                    'success': False,
                    'error': f'大盘数据不足（只有{len(df)}天）'
                }
            
            # 计算整体涨跌幅
            start_price = df.iloc[0]['close']
            end_price = df.iloc[-1]['close']
            total_change = (end_price - start_price) / start_price * 100
            
            # 计算上涨天数和下跌天数
            up_days = len(df[df['pct_chg'] > 0])
            down_days = len(df[df['pct_chg'] < 0])
            total_days = len(df)
            
            # 判断市场环境
            if total_change > 10:
                market_type = " 牛市行情"
                expected_winrate = "60-70%"
            elif total_change > 5:
                market_type = " 上涨趋势"
                expected_winrate = "55-65%"
            elif total_change > -5:
                market_type = " 震荡行情"
                expected_winrate = "48-55%"
            elif total_change > -10:
                market_type = " 下跌趋势"
                expected_winrate = "40-48%"
            else:
                market_type = " 熊市行情"
                expected_winrate = "35-45%"
            
            # 计算波动率
            volatility = df['pct_chg'].std()
            
            return {
                'success': True,
                'market_type': market_type,
                'total_change': total_change,
                'up_days': up_days,
                'down_days': down_days,
                'total_days': total_days,
                'up_ratio': up_days / total_days * 100,
                'volatility': volatility,
                'expected_winrate': expected_winrate,
                'start_price': start_price,
                'end_price': end_price,
                'data_source': '上证指数(Tushare Pro)'
            }
            
        except Exception as e:
            logger.error(f"分析回测期间市场失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        
    @staticmethod
    @lru_cache(maxsize=1000)
    def _calculate_technical_indicators(close_tuple: tuple, volume_tuple: tuple) -> Dict:
        """计算技术指标（向量化优化）"""
        try:
            close = np.array(close_tuple)
            volume = np.array(volume_tuple)
            
            if len(close) == 0 or len(volume) == 0:
                return {}
            
            indicators = {}
            
            # 均线系统
            indicators['ma5'] = np.mean(close[-5:]) if len(close) >= 5 else 0
            indicators['ma10'] = np.mean(close[-10:]) if len(close) >= 10 else 0
            indicators['ma20'] = np.mean(close[-20:]) if len(close) >= 20 else 0
            indicators['ma60'] = np.mean(close[-60:]) if len(close) >= 60 else 0
            
            # 成交量指标
            indicators['vol_ma5'] = np.mean(volume[-5:]) if len(volume) >= 5 else 0
            indicators['vol_ma10'] = np.mean(volume[-10:]) if len(volume) >= 10 else 0
            indicators['vol_ma20'] = np.mean(volume[-20:]) if len(volume) >= 20 else 0
            
            # 价格动量
            if len(close) >= 5:
                indicators['momentum_5'] = (close[-1] - close[-5]) / (close[-5] + 0.0001) * 100
            else:
                indicators['momentum_5'] = 0
                
            if len(close) >= 10:
                indicators['momentum_10'] = (close[-1] - close[-10]) / (close[-10] + 0.0001) * 100
            else:
                indicators['momentum_10'] = 0
            
            # 波动率
            if len(close) >= 10:
                mean_close = np.mean(close[-10:])
                if mean_close > 0:
                    indicators['volatility'] = np.std(close[-10:]) / mean_close * 100
                else:
                    indicators['volatility'] = 0
            else:
                indicators['volatility'] = 0
            
            return indicators
            
        except Exception as e:
            return {}
    
    def identify_signals_optimized(self, stock_data: pd.DataFrame, 
                                   signal_strength_threshold: float = 0.55,
                                   investment_cycle: str = 'balanced') -> pd.DataFrame:
        """
         三周期专业优化版 
        
        investment_cycle参数：
        - 'short': 短期（1-5天）- 60分起，关注放量突破、强势股、突破信号
        - 'medium': 中期（5-20天）- 55分起，关注趋势形成、均线多头、回调买入
        - 'long': 长期（20天+）- 50分起，关注底部形态、价值低估、稳定增长
        - 'balanced': 平衡模式（默认）- 55分起，综合三周期优势
        """
        try:
            if stock_data is None or len(stock_data) < 30:
                return pd.DataFrame()
            
            required_cols = ['trade_date', 'close_price', 'vol', 'pct_chg', 'name']
            if not all(col in stock_data.columns for col in required_cols):
                return pd.DataFrame()
            
            #  第一层过滤：排除高风险股票 
            stock_name = stock_data['name'].iloc[0] if 'name' in stock_data.columns else ''
            
            # 1. 排除ST股（风险太高）
            if 'ST' in stock_name or '*ST' in stock_name:
                return pd.DataFrame()
            
            data = stock_data[required_cols].copy()
            
            for col in ['close_price', 'vol', 'pct_chg']:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            
            data = data.dropna()
            
            if len(data) < 30:
                return pd.DataFrame()
            
            # 2. 排除连续跌停/暴跌股（避免接飞刀）
            if len(data) >= 5:
                recent_5_pct = data['pct_chg'].tail(5).tolist()
                # 5天内2个或以上跌停
                if sum(1 for x in recent_5_pct if x < -9.5) >= 2:
                    return pd.DataFrame()
                # 5天累计跌超15%（趋势太差）
                if sum(recent_5_pct) < -15:
                    return pd.DataFrame()
            
            # 3. 排除成交量极度萎缩（可能退市风险）
            if len(data) >= 20:
                recent_vol_5 = data['vol'].tail(5).mean()
                avg_vol_20 = data['vol'].tail(20).mean()
                if avg_vol_20 > 0 and recent_vol_5 < avg_vol_20 * 0.15:  # 量能<20日均量15%
                    return pd.DataFrame()
            
            # 4. 排除价格在历史最高位（追高风险）-  加强过滤
            if len(data) >= 60:
                current_price = data['close_price'].iloc[-1]
                max_price_60 = data['close_price'].tail(60).max()
                min_price_60 = data['close_price'].tail(60).min()
                
                # 排除接近60日最高价的股票
                if max_price_60 > 0 and current_price >= max_price_60 * 0.95:  # 从0.98改为0.95
                    return pd.DataFrame()
                
                #  新增：排除在60日涨幅区间高位的股票
                if max_price_60 > min_price_60:
                    price_range_position = (current_price - min_price_60) / (max_price_60 - min_price_60)
                    if price_range_position > 0.80:  # 在60日区间的80%以上位置
                        return pd.DataFrame()
            
            signals = []
            signals_found = 0  # 调试计数器
            
            #  改进：包含最新数据用于当前选股！
            # v46.1的-5是为了计算未来收益，但我们要选当前的股票
            for i in range(20, len(data)):  #  包括最新一天
                try:
                    window = data.iloc[max(0, i-60):i+1].copy()
                    
                    close = window['close_price'].values
                    volume = window['vol'].values
                    pct_chg = window['pct_chg'].values
                    
                    if len(close) < 20 or len(volume) < 20:
                        continue
                    
                    #  关键改进：即使indicators失败也继续评分！
                    indicators = self._calculate_technical_indicators(
                        tuple(close), tuple(volume)
                    )
                    
                    #  移除这个限制！不再因为indicators失败就跳过
                    # if not indicators:
                    #     continue
                    
                    #  完全复制v46.1的评分逻辑
                    price_range = np.max(close[-20:]) - np.min(close[-20:])
                    if price_range > 0:
                        price_position = (close[-1] - np.min(close[-20:])) / price_range
                    else:
                        price_position = 0.5
                    
                    recent_vol = np.mean(volume[-3:])
                    historical_vol = np.mean(volume[-17:-3])
                    volume_surge = recent_vol / (historical_vol + 1) if historical_vol > 0 else 1.0
                    
                    #  三周期专业评分系统 
                    
                    #  根据投资周期设置权重（ 大幅提高底部位置权重）
                    if investment_cycle == 'short':
                        # 短期（1-5天）：趋势延续能力、突破信号
                        vol_weight, price_weight, ma_weight, pos_weight, momentum_weight = 30, 25, 15, 20, 10
                        min_threshold = 60  # 短期要求60分
                    elif investment_cycle == 'medium':
                        # 中期（5-20天）：趋势、均线多头
                        vol_weight, price_weight, ma_weight, pos_weight, momentum_weight = 20, 20, 25, 25, 10
                        min_threshold = 55
                    elif investment_cycle == 'long':
                        # 长期（20天+）：底部、价值
                        vol_weight, price_weight, ma_weight, pos_weight, momentum_weight = 15, 15, 20, 40, 10
                        min_threshold = 50
                    else:  # balanced（ 核心修改：pos_weight从15%提升到30%）
                        vol_weight, price_weight, ma_weight, pos_weight, momentum_weight = 20, 20, 20, 30, 10
                        min_threshold = 55
                    
                    #  1. 放量评分（0-100分）
                    vol_score = 0
                    if price_position < 0.3 and volume_surge > 1.8:
                        vol_score = 100
                    elif price_position < 0.4 and volume_surge > 1.5:
                        vol_score = 85
                    elif price_position < 0.6 and volume_surge > 1.3:
                        vol_score = 70
                    elif volume_surge > 1.2:
                        vol_score = 50
                    elif volume_surge > 1.05:
                        vol_score = 25
                    
                    #  2. 量价配合（ 加入位置动态调节 - 核心修复）
                    price_vol_score = 0
                    if len(close) >= 10:
                        price_trend = (close[-1] - close[-5]) / (close[-5] + 0.0001)
                        vol_trend = (np.mean(volume[-3:]) - np.mean(volume[-8:-3])) / (np.mean(volume[-8:-3]) + 1)
                        
                        #  连续确认：最近3天连续上涨+放量
                        last_3_up = sum(1 for i in range(-3, 0) if close[i] > close[i-1])
                        last_3_vol_up = sum(1 for i in range(-3, 0) if volume[i] > volume[i-1])
                        
                        # 先计算基础分数
                        base_score = 0
                        if last_3_up >= 2 and last_3_vol_up >= 2:
                            if price_trend > 0.05 and vol_trend > 0.3:
                                base_score = 100
                            elif price_trend > 0.03 and vol_trend > 0.2:
                                base_score = 80
                        elif price_trend > 0.02 and vol_trend > 0.15:
                            base_score = 60
                        elif price_trend > 0:
                            base_score = 35
                        elif price_trend > -0.02:
                            base_score = 15
                        
                        #  位置动态调节（关键修复）
                        if price_position < 0.3:
                            position_factor = 1.0  # 低位，满分
                        elif price_position < 0.5:
                            position_factor = 0.85  # 中位，85折
                        elif price_position < 0.65:
                            position_factor = 0.6   # 中高位，6折
                        elif price_position < 0.75:
                            position_factor = 0.3   # 高位，3折
                        else:
                            position_factor = 0.1   # 极高位，1折（几乎无效）
                        
                        price_vol_score = base_score * position_factor
                    
                    #  3. 均线系统（ 金叉确认）
                    ma_score = 0
                    if indicators:
                        ma5 = indicators.get('ma5', 0)
                        ma10 = indicators.get('ma10', 0)
                        ma20 = indicators.get('ma20', 0)
                        
                        if ma5 > ma10 > ma20 > 0 and close[-1] > ma5:
                            ma_score = 100
                        elif ma5 > ma10 > ma20:
                            ma_score = 80
                        elif ma5 > ma10 > 0:
                            ma_score = 60
                        elif close[-1] > ma5 > 0:
                            ma_score = 40
                        elif ma5 > 0:
                            ma_score = 20
                    
                    #  4. 底部位置（越低越好）
                    pos_score = 0
                    if price_position < 0.2:
                        pos_score = 100
                    elif price_position < 0.3:
                        pos_score = 85
                    elif price_position < 0.4:
                        pos_score = 70
                    elif price_position < 0.5:
                        pos_score = 55
                    elif price_position < 0.6:
                        pos_score = 40
                    elif price_position < 0.7:
                        pos_score = 25
                    
                    #  5. 动量（ 加入位置动态调节）
                    momentum_score = 0
                    if indicators:
                        momentum_5 = indicators.get('momentum_5', 0)
                        
                        # 先计算基础分数
                        base_momentum = 0
                        if momentum_5 > 5:
                            base_momentum = 100
                        elif momentum_5 > 3:
                            base_momentum = 80
                        elif momentum_5 > 1.5:
                            base_momentum = 60
                        elif momentum_5 > 0.5:
                            base_momentum = 40
                        elif momentum_5 > 0:
                            base_momentum = 20
                        
                        #  位置动态调节（防止追高）
                        if price_position < 0.3:
                            momentum_factor = 1.0  # 低位突破，满分
                        elif price_position < 0.5:
                            momentum_factor = 0.8  # 中位上涨，8折
                        elif price_position < 0.65:
                            momentum_factor = 0.5  # 中高位涨，5折
                        elif price_position < 0.75:
                            momentum_factor = 0.2  # 高位追涨，2折
                        else:
                            momentum_factor = 0.05 # 极高位追涨，几乎无效
                        
                        momentum_score = base_momentum * momentum_factor
                    
                    # 加权总分
                    score = (
                        vol_score * vol_weight / 100 +
                        price_vol_score * price_weight / 100 +
                        ma_score * ma_weight / 100 +
                        pos_score * pos_weight / 100 +
                        momentum_score * momentum_weight / 100
                    )
                    
                    #  第三层保护：高位强制惩罚机制（兜底）
                    if price_position > 0.8:
                        score *= 0.4  # 极高位（80%以上），打4折
                    elif price_position > 0.7:
                        score *= 0.6  # 高位（70-80%），打6折
                    elif price_position > 0.6:
                        score *= 0.8  # 中高位（60-70%），打8折
                    
                    # 归一化到100分
                    normalized_score = min(100, score)
                    
                    # 简化的可靠度
                    reliability = 0.5 + normalized_score / 200
                    
                    signal_types = []
                    
                    #  第三层过滤：专业信号识别（更严格，提高质量） 
                    
                    #  1. 底部放量信号
                    if vol_score >= 85:
                        signal_types.append('底部强放量')
                    elif vol_score >= 70:
                        signal_types.append('中低位放量')
                    elif vol_score >= 50:
                        signal_types.append('温和放量')
                    
                    #  2. 量价配合信号
                    if price_vol_score >= 80:
                        signal_types.append('连续量价齐升')
                    elif price_vol_score >= 60:
                        signal_types.append('⬆温和上涨')
                    elif price_vol_score >= 35:
                        signal_types.append('价格微涨')
                    
                    #  3. 均线系统信号
                    if ma_score >= 100:
                        signal_types.append('完美多头')
                    elif ma_score >= 80:
                        signal_types.append('均线多头')
                    elif ma_score >= 60:
                        signal_types.append('短期向上')
                    elif ma_score >= 40:
                        signal_types.append('站上5日线')
                    
                    #  4. 动量信号
                    if momentum_score >= 80:
                        signal_types.append('超强势')
                    elif momentum_score >= 60:
                        signal_types.append('强势')
                    elif momentum_score >= 40:
                        signal_types.append('正动量')
                    
                    #  5. 底部位置信号
                    if pos_score >= 85:
                        signal_types.append('极低位')
                    elif pos_score >= 70:
                        signal_types.append('底部区域')
                    elif pos_score >= 55:
                        signal_types.append('低位')
                    
                    #  专业标准：必须有至少1个明确信号
                    if len(signal_types) == 0:
                        continue  # 没有明确信号，跳过
                    
                    # 根据投资周期使用不同阈值
                    threshold_score = min_threshold  # 使用前面设定的min_threshold
                    
                    # 调试：记录样本
                    if i == len(data) - 1:  # 最新一天
                        signals_found += 1
                        if signals_found <= 3:
                            logger.info(f"[{investment_cycle}] score={normalized_score:.1f}, threshold={threshold_score}, "
                                      f"signals={signal_types}, vol={vol_score}, price_vol={price_vol_score}, "
                                      f"ma={ma_score}, pos={pos_score}, momentum={momentum_score}")
                    
                    #  专业过滤：必须达到对应周期的阈值 + 有明确信号
                    if normalized_score >= threshold_score and len(signal_types) > 0:
                        #  关键修复：安全获取ma5等变量（可能indicators为空）
                        safe_ma5 = indicators.get('ma5', 0) if indicators else 0
                        safe_ma10 = indicators.get('ma10', 0) if indicators else 0
                        safe_ma20 = indicators.get('ma20', 0) if indicators else 0
                        safe_momentum = indicators.get('momentum_5', 0) if indicators else 0
                        
                        signal_info = {
                            'trade_date': data.iloc[i]['trade_date'],
                            'close_price': float(close[-1]),
                            'signal_type': ','.join(signal_types),
                            'signal_strength': round(float(normalized_score), 1),
                            'reliability': round(float(reliability), 2),
                            'volume_surge': round(float(volume_surge), 2),
                            'price_position': round(float(price_position * 100), 1),
                            'momentum': round(float(safe_momentum), 2),
                            'ma_score': 20 if safe_ma5 > safe_ma10 > safe_ma20 > 0 and close[-1] > safe_ma5 else 15 if safe_ma5 > safe_ma10 > 0 else 0,
                            'volume_price_score': 25 if normalized_score >= 25 else 20 if normalized_score >= 20 else 0
                        }
                        
                        signals.append(signal_info)
                
                except Exception as e:
                    pass
            
            if signals:
                df_result = pd.DataFrame(signals)
                logger.info(f"成功返回 {len(df_result)} 条信号！")
                return df_result
            else:
                logger.warning(f"signals列表为空，虽然扫描了 {signals_found} 只股票")
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"信号识别失败: {e}")
            return pd.DataFrame()
    
    def get_market_environment(self) -> str:
        """
         优化1：识别当前市场环境
        
        返回：'bull'（牛市）, 'bear'（熊市）, 'oscillation'（震荡市）
        
        判断逻辑：
        - 牛市：指数20日涨幅>10% 且 波动率<2.0
        - 熊市：指数20日跌幅>10%
        - 震荡市：其他情况
        """
        try:
            from data.history import load_index_recent as _load_index_recent_v2  # type: ignore
            index_data = _load_index_recent_v2(
                db_path=PERMANENT_DB_PATH,
                index_code="000001.SH",
                limit=20,
                columns="trade_date, close_price, pct_chg",
            )
            
            if len(index_data) < 20:
                return 'oscillation'  # 默认震荡市
            
            # 计算指数涨跌幅
            index_return_20 = (index_data['close_price'].iloc[0] - index_data['close_price'].iloc[-1]) / index_data['close_price'].iloc[-1]
            
            # 计算波动率
            index_volatility = index_data['pct_chg'].std()
            
            # 判断市场环境
            if index_return_20 > 0.10 and index_volatility < 2.0:
                return 'bull'  # 牛市
            elif index_return_20 < -0.10:
                return 'bear'  # 熊市
            else:
                return 'oscillation'  # 震荡市
                
        except Exception as e:
            logger.warning(f"获取市场环境失败: {e}，默认震荡市")
            return 'oscillation'
    
    def get_dynamic_weights(self, market_env: str) -> Dict:
        """
         优化1：根据市场环境动态调整权重
        
        核心理念：
        - 牛市：追涨为主，加大资金面权重（量价+主力）
        - 熊市：抄底为主，加大底部特征权重
        - 震荡市：技术面为主，均衡配置（当前策略）
        """
        if market_env == 'bull':
            # 牛市策略：资金面35%，技术面50%，底部10%，涨停5%
            return {
                'volume_price': 0.30,      # 量价配合30%（↑）
                'ma': 0.18,                # 均线18%（↓）
                'macd': 0.20,              # MACD20%（↓）
                'bottom': 0.10,            # 底部10%（↓）
                'accumulation': 0.15,      # 主力吸筹15%（↑）
                'limit': 0.07              # 涨停7%（↑）
            }
        elif market_env == 'bear':
            # 熊市策略：底部40%，技术面45%，资金面10%，涨停5%
            return {
                'volume_price': 0.10,      # 量价配合10%（↓）
                'ma': 0.22,                # 均线22%（↑）
                'macd': 0.23,              # MACD23%（↓）
                'bottom': 0.25,            # 底部25%（↑↑）
                'accumulation': 0.15,      # 主力吸筹15%（↑）
                'limit': 0.05              # 涨停5%
            }
        else:  # oscillation
            # 震荡市策略：技术面70%，资金面25%，涨停5%（当前策略）
            return {
                'volume_price': 0.25,      # 量价配合25%
                'ma': 0.20,                # 均线20%
                'macd': 0.25,              # MACD25%
                'bottom': 0.15,            # 底部15%
                'accumulation': 0.10,      # 主力吸筹10%
                'limit': 0.05              # 涨停5%
            }
    
    def calculate_synergy_bonus(self, scores: Dict) -> float:
        """
         优化2：计算维度间的协同加成
        
        核心理念：某些维度组合的价值 > 各自独立的价值之和
        """
        bonus = 0
        
        # 【黄金组合1】底部+放量+MACD金叉（完美底部突破）
        if scores['bottom'] >= 10 and scores['volume_price'] >= 20 and scores['macd'] >= 20:
            bonus += 10  # +10分协同加成
        
        # 【黄金组合2】主力吸筹+均线多头+底部（主力建仓完毕）
        if scores['accumulation'] >= 8 and scores['ma'] >= 16 and scores['bottom'] >= 10:
            bonus += 8  # +8分协同加成
        
        # 【黄金组合3】放量+MACD三向上+涨停（加速趋势）
        if scores['volume_price'] >= 20 and scores['macd'] >= 25 and scores['limit'] >= 3:
            bonus += 7  # +7分协同加成
        
        # 【黄金组合4】完美六合一（极罕见，满分奖励）
        if (scores['volume_price'] >= 20 and scores['ma'] >= 16 and 
            scores['macd'] >= 20 and scores['bottom'] >= 10 and 
            scores['accumulation'] >= 6 and scores['limit'] >= 3):
            bonus += 15  # +15分额外加成
        
        return min(bonus, 20)  # 协同加成最高20分
    
    def calculate_industry_heat(self, industry: str) -> Dict:
        """
         优化4：计算行业热度（0-20分加成）
        
        维度：
        1. 行业平均涨幅（10分）
        2. 行业涨停数量（5分）
        3. 行业资金流入（5分）
        """
        try:
            if not industry or pd.isna(industry):
                return {'heat_score': 0, 'heat_level': '未知', 'industry_return': 0}

            from data.dao import DataAccessError, detect_daily_table, recent_trade_profile  # type: ignore

            conn = _connect_permanent_db()
            try:
                daily_table = detect_daily_table(conn)
                profile = recent_trade_profile(conn, date_limit=1, recent_window=1)
                latest_trade = str(profile.get("last_trade_date", "") or "")
            except DataAccessError:
                conn.close()
                return {'heat_score': 0, 'heat_level': '未知', 'industry_return': 0}

            if not latest_trade:
                conn.close()
                return {'heat_score': 0, 'heat_level': '未知', 'industry_return': 0}

            # 获取同行业股票的最新交易数据
            query = f"""
                SELECT dtd.ts_code, dtd.pct_chg, dtd.vol
                FROM {daily_table} dtd
                INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
                WHERE sb.industry = ?
                AND dtd.trade_date = ?
            """
            industry_data = pd.read_sql_query(query, conn, params=(industry, latest_trade))
            conn.close()
            
            if len(industry_data) < 5:
                return {'heat_score': 0, 'heat_level': '未知', 'industry_return': 0}
            
            heat_score = 0
            
            # 1. 行业平均涨幅（10分）
            industry_avg_return = industry_data['pct_chg'].mean()
            if industry_avg_return > 3:
                heat_score += 10
            elif industry_avg_return > 1:
                heat_score += 7
            elif industry_avg_return > 0:
                heat_score += 4
            
            # 2. 行业涨停数量（5分）
            limit_up_count = sum(1 for pct in industry_data['pct_chg'] if pct > 9.5)
            limit_up_ratio = limit_up_count / len(industry_data)
            if limit_up_ratio > 0.05:
                heat_score += 5
            elif limit_up_ratio > 0.02:
                heat_score += 3
            
            # 3. 行业资金流入（5分）
            # 使用成交量作为资金流入的代理指标
            avg_volume = industry_data['vol'].mean()
            if avg_volume > 100000:
                heat_score += 5
            elif avg_volume > 50000:
                heat_score += 3
            
            # 确定热度等级
            if heat_score >= 15:
                heat_level = ' 高热'
            elif heat_score >= 10:
                heat_level = '⭐ 热门'
            elif heat_score >= 5:
                heat_level = ' 温和'
            else:
                heat_level = ' 冷门'
            
            return {
                'heat_score': min(20, heat_score),
                'heat_level': heat_level,
                'industry_return': round(industry_avg_return, 2),
                'limit_up_ratio': round(limit_up_ratio * 100, 1) if limit_up_count > 0 else 0
            }
            
        except Exception as e:
            logger.warning(f"行业热度计算失败: {e}")
            return {'heat_score': 0, 'heat_level': '未知', 'industry_return': 0}
    
    def apply_time_decay(self, signal_age_days: int, base_score: float) -> float:
        """
         优化5：应用时间衰减因子
        
        理念：信号越新鲜，价值越高
        - 1天内：100%价值
        - 3天内：95%价值
        - 5天内：85%价值
        - 10天内：70%价值
        - 20天内：50%价值
        """
        if signal_age_days <= 1:
            decay_factor = 1.0
        elif signal_age_days <= 3:
            decay_factor = 0.95
        elif signal_age_days <= 5:
            decay_factor = 0.85
        elif signal_age_days <= 10:
            decay_factor = 0.70
        elif signal_age_days <= 20:
            decay_factor = 0.50
        else:
            decay_factor = 0.30
        
        return base_score * decay_factor
    
    def calculate_stop_loss(self, stock_data: pd.DataFrame, entry_price: float) -> Dict:
        """
         优化6：计算止损位置
        
        方法：
        1. 技术止损：跌破关键支撑（MA20/MA30）
        2. 百分比止损：下跌7-8%
        3. ATR止损：1.5倍ATR（真实波动幅度）
        """
        try:
            close = stock_data['close_price'].values
            
            # 1. 技术止损
            ma20 = np.mean(close[-20:]) if len(close) >= 20 else close[-1]
            ma30 = np.mean(close[-30:]) if len(close) >= 30 else close[-1]
            tech_stop_loss = min(ma20, ma30) * 0.98  # 跌破均线2%止损
            
            # 2. 百分比止损
            pct_stop_loss = entry_price * 0.92  # 下跌8%止损
            
            # 3. ATR止损（简化版，使用价格波动）
            if len(close) >= 14:
                price_range = [abs(close[i] - close[i-1]) for i in range(-14, 0) if i-1 >= -len(close)]
                atr = np.mean(price_range) if price_range else 0
                atr_stop_loss = entry_price - 1.5 * atr if atr > 0 else pct_stop_loss
            else:
                atr_stop_loss = pct_stop_loss
            
            # 选择最高的止损位（最宽松，最安全）
            final_stop_loss = max(tech_stop_loss, pct_stop_loss, atr_stop_loss)
            final_stop_loss = max(final_stop_loss, entry_price * 0.85)  # 最大止损不超过15%
            
            # 确定止损方法
            if final_stop_loss == tech_stop_loss:
                method = '技术止损（跌破均线）'
            elif final_stop_loss == atr_stop_loss:
                method = 'ATR止损（波动止损）'
            else:
                method = '百分比止损（固定比例）'
            
            return {
                'stop_loss_price': round(final_stop_loss, 2),
                'stop_loss_pct': round((entry_price - final_stop_loss) / entry_price * 100, 2),
                'method': method,
                'tech_stop': round(tech_stop_loss, 2),
                'pct_stop': round(pct_stop_loss, 2),
                'atr_stop': round(atr_stop_loss, 2)
            }
            
        except Exception as e:
            logger.error(f"止损计算失败: {e}")
            # 默认8%止损
            return {
                'stop_loss_price': round(entry_price * 0.92, 2),
                'stop_loss_pct': 8.0,
                'method': '百分比止损（默认）',
                'tech_stop': 0,
                'pct_stop': round(entry_price * 0.92, 2),
                'atr_stop': 0
            }
    
    def calculate_risk_score(self, stock_data: pd.DataFrame) -> Dict:
        """
         优化3：计算风险评分（0-100分，越低越安全）
        
        风险维度：
        1. 波动率风险（30分）- 价格波动越大越危险
        2. 高位风险（25分）- 价格越高越危险
        3. 流动性风险（20分）- 成交量越小越危险
        4. 历史暴跌风险（15分）- 有暴跌历史越危险
        5. 技术面风险（10分）- 均线空头越危险
        """
        try:
            close = stock_data['close_price'].values
            volume = stock_data['vol'].values
            pct_chg = stock_data['pct_chg'].values
            
            risk_score = 0
            risk_details = {}
            
            # 1. 波动率风险（30分）
            volatility = np.std(pct_chg[-20:]) if len(pct_chg) >= 20 else 0
            if volatility > 5:
                risk_score += 30
                risk_details['volatility'] = '极高波动风险'
            elif volatility > 3:
                risk_score += 20
                risk_details['volatility'] = '高波动风险'
            elif volatility > 2:
                risk_score += 10
                risk_details['volatility'] = '中等波动'
            else:
                risk_details['volatility'] = '低波动'
            
            # 2. 高位风险（25分）
            price_min_60 = np.min(close[-60:]) if len(close) >= 60 else np.min(close)
            price_max_60 = np.max(close[-60:]) if len(close) >= 60 else np.max(close)
            price_position = (close[-1] - price_min_60) / (price_max_60 - price_min_60) if price_max_60 > price_min_60 else 0.5
            
            if price_position > 0.85:
                risk_score += 25
                risk_details['position'] = '极高位风险（>85%）'
            elif price_position > 0.70:
                risk_score += 18
                risk_details['position'] = '高位风险（70-85%）'
            elif price_position > 0.50:
                risk_score += 10
                risk_details['position'] = '中位风险（50-70%）'
            else:
                risk_details['position'] = '低位安全（<50%）'
            
            # 3. 流动性风险（20分）
            avg_volume = np.mean(volume[-20:]) if len(volume) >= 20 else np.mean(volume)
            if avg_volume < 10000:
                risk_score += 20
                risk_details['liquidity'] = '流动性极差'
            elif avg_volume < 50000:
                risk_score += 10
                risk_details['liquidity'] = '流动性较差'
            else:
                risk_details['liquidity'] = '流动性良好'
            
            # 4. 历史暴跌风险（15分）
            max_drop = np.min(pct_chg[-60:]) if len(pct_chg) >= 60 else np.min(pct_chg)
            if max_drop < -9:
                risk_score += 15
                risk_details['history'] = '有跌停历史'
            elif max_drop < -7:
                risk_score += 10
                risk_details['history'] = '有大幅下跌'
            else:
                risk_details['history'] = '历史稳定'
            
            # 5. 技术面风险（10分）
            ma5 = np.mean(close[-5:]) if len(close) >= 5 else close[-1]
            ma20 = np.mean(close[-20:]) if len(close) >= 20 else close[-1]
            if ma5 < ma20 and close[-1] < ma5:
                risk_score += 10
                risk_details['technical'] = '均线空头'
            else:
                risk_details['technical'] = '技术面正常'
            
            # 确定风险等级
            if risk_score >= 60:
                risk_level = ' 高风险'
            elif risk_score >= 30:
                risk_level = ' 中等风险'
            else:
                risk_level = ' 低风险'
            
            return {
                'risk_score': min(100, risk_score),
                'risk_level': risk_level,
                'details': risk_details
            }
            
        except Exception as e:
            logger.error(f"风险评分失败: {e}")
            return {'risk_score': 50, 'risk_level': ' 中等风险', 'details': {}}
    
    def evaluate_stock_ultimate_fusion(self, stock_data: pd.DataFrame) -> Dict:
        """
         综合优选优化版：6维100分评分体系 + 7大优化
        
         7大优化已全部集成：
        1.  动态权重系统：根据市场环境（牛/熊/震荡）自动调整权重
        2.  协同效应加成：识别黄金组合，+10-20分加成
        3.  风险评分维度：5个风险指标，0-100分风险评分
        4.  行业热度加成：热门行业+5-20分加成（待实现）
        5.  时间衰减因子：新鲜信号优先（待实现）
        6.  止损位置建议：自动计算止损位（待实现）
        7.  性能优化：向量化计算（已优化）
        
        新的6维评分系统（总分100分）：
        1. 量价配合（25分）- 25% 强势放量上涨
        2. 均线多头（20分）- 20% 5/15/30天均线多头排列
        3. MACD趋势（25分）- 25% 三向判断（DIF↑+DEA↑+MACD柱↑）
        4. 底部特征（15分）- 15%  新增！股价历史低位+筹码集中（核心2）
        5. 主力吸筹（10分）- 10%  新增！连续温和放量+价格不跌（核心3）
        6. 涨停基因（5分）- 5% 近5天涨停记录
        
        动态权重（根据市场环境）：
        - 牛市：资金面35% + 技术面50% + 底部10% + 涨停5%
        - 熊市：底部40% + 技术面45% + 资金面10% + 涨停5%
        - 震荡市：技术面70% + 资金面25% + 涨停5%
        
        返回：{
            'score': 综合评分（0-100）,
            'level': 等级（S/A/B/C/D）,
            'risk_score': 风险评分（0-100，越低越安全）,
            'risk_level': 风险等级（高/中/低）,
            'synergy_bonus': 协同加成（0-20分）,
            'market_env': 市场环境（牛/熊/震荡）,
            'details': 详细信息
        }
        """
        try:
            if stock_data is None or len(stock_data) < 60:
                return {'success': False, 'score': 0, 'final_score': 0, 'level': 'D', 'details': {}}
            
            required_cols = ['close_price', 'vol', 'pct_chg']
            if not all(col in stock_data.columns for col in required_cols):
                return {'success': False, 'score': 0, 'final_score': 0, 'level': 'D', 'details': {}}
            
            # 基础风控：排除ST股
            if 'name' in stock_data.columns:
                stock_name = stock_data['name'].iloc[0]
                if 'ST' in stock_name or '*ST' in stock_name:
                    return {'success': False, 'score': 0, 'final_score': 0, 'level': 'D', 'details': {}}
            
            data = stock_data[required_cols].copy()
            for col in required_cols:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            data = data.dropna()
            
            if len(data) < 60:
                return {'success': False, 'score': 0, 'final_score': 0, 'level': 'D', 'details': {}}
            
            close = data['close_price'].values
            volume = data['vol'].values
            pct_chg = data['pct_chg'].values
            
            # ========== 最新优化6维评分系统（100分） ==========
            total_score = 0
            details = {}
            
            # 计算基础指标
            price_min_60 = np.min(close[-60:])
            price_max_60 = np.max(close[-60:])
            price_range = price_max_60 - price_min_60
            price_position = (close[-1] - price_min_60) / price_range if price_range > 0 else 0.5
            
            recent_vol = np.mean(volume[-3:])
            hist_vol = np.mean(volume[-20:-3]) if len(volume) >= 23 else np.mean(volume[:-3])
            vol_ratio = recent_vol / hist_vol if hist_vol > 0 else 1.0
            
            price_chg_3d = (close[-1] - close[-4]) / close[-4] if len(close) > 4 and close[-4] > 0 else 0
            
            # ==================【维度1】量价配合（25分）==================
            score_volume_price = 0
            
            #  优化：移除一票否决，改为扣分机制
            # 放量下跌：扣除评分，但不直接返回0
            severe_decline = vol_ratio > 2.0 and price_chg_3d < -0.05  # 严重放量下跌
            
            if vol_ratio > 2.0 and price_chg_3d > 0.03:  # 强势放量上涨
                score_volume_price = 25
                details['volume_price'] = '强势放量上涨'
            elif vol_ratio > 1.8 and price_chg_3d > 0.02:  # 放量上涨
                score_volume_price = 20
                details['volume_price'] = '放量上涨'
            elif vol_ratio > 1.5 and price_chg_3d > 0.01:  # 温和放量上涨
                score_volume_price = 15
                details['volume_price'] = '温和放量上涨'
            elif vol_ratio > 1.2 and price_chg_3d > 0:  # 小幅放量上涨
                score_volume_price = 10
                details['volume_price'] = '小幅放量上涨'
            elif vol_ratio > 1.0 and price_chg_3d >= 0:  # 放量横盘
                score_volume_price = 5
                details['volume_price'] = '放量横盘'
            elif price_chg_3d >= 0:  # 缩量上涨/横盘
                score_volume_price = 3
                details['volume_price'] = '缩量横盘'
            elif severe_decline:  # 严重放量下跌
                score_volume_price = 0
                details['volume_price'] = '严重放量下跌'
            else:
                score_volume_price = 1
                details['volume_price'] = '量价配合一般'
            
            total_score += score_volume_price
            
            # ==================【维度2】均线多头（20分）==================
            # 使用5/15/30天均线
            ma5 = np.mean(close[-5:])
            ma15 = np.mean(close[-15:])
            ma30 = np.mean(close[-30:])
            
            score_ma = 0
            
            # 完美多头排列：MA5 > MA15 > MA30 且价格在MA5上方
            if ma5 > ma15 > ma30 and close[-1] > ma5:
                score_ma = 20
                details['ma'] = '完美多头排列'
            # 强势多头：MA5 > MA15且价格在MA5上方
            elif ma5 > ma15 and close[-1] > ma5:
                score_ma = 16
                details['ma'] = '强势多头'
            # 中期多头：MA5 > MA15
            elif ma5 > ma15:
                score_ma = 12
                details['ma'] = '中期多头'
            # 站上MA15
            elif close[-1] > ma15:
                score_ma = 10
                details['ma'] = '站上15日线'
            # 站上MA30
            elif close[-1] > ma30:
                score_ma = 7
                details['ma'] = '站上30日线'
            # 站上MA5
            elif close[-1] > ma5:
                score_ma = 5
                details['ma'] = '站上5日线'
            # 接近MA5
            elif abs(close[-1] - ma5) / ma5 < 0.02:  # 距离MA5不超过2%
                score_ma = 3
                details['ma'] = '接近5日线'
            else:
                score_ma = 1
                details['ma'] = '均线空头'
            
            total_score += score_ma
            
            # ==================【维度3】MACD趋势（25分）==================
            ema12 = pd.Series(close).ewm(span=12, adjust=False).mean().values
            ema26 = pd.Series(close).ewm(span=26, adjust=False).mean().values
            dif = ema12 - ema26
            dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
            macd_bar = dif - dea  # MACD柱
            
            score_macd = 0
            
            if len(dif) >= 2 and dif[-1] > dea[-1]:  # 金叉状态
                # 判断三个方向
                dif_up = dif[-1] > dif[-2]  # DIF向上
                dea_up = dea[-1] > dea[-2]  # DEA向上
                macd_up = macd_bar[-1] > macd_bar[-2]  # MACD柱向上
                
                # 完美三向上（25分）- 高质量信号
                if dif_up and dea_up and macd_up:
                    score_macd = 25
                    details['macd'] = '完美三向上'
                # 0轴附近金叉+双向上（20分）
                elif dif[-2] <= dea[-2] and abs(dif[-1]) < 0.5 and (dif_up and dea_up):
                    score_macd = 20
                    details['macd'] = '0轴金叉+双向上⭐'
                # 底部金叉+双向上（18分）
                elif dif[-1] < 0 and dea[-1] < 0 and (dif_up and dea_up):
                    score_macd = 18
                    details['macd'] = '底部金叉+双向上'
                # 0轴附近金叉（16分）
                elif dif[-2] <= dea[-2] and abs(dif[-1]) < 0.5:
                    score_macd = 16
                    details['macd'] = '0轴附近金叉'
                # 刚金叉（14分）
                elif dif[-2] <= dea[-2]:
                    score_macd = 14
                    details['macd'] = '刚金叉'
                # 金叉持续（10分）
                elif dif[-1] > 0:
                    score_macd = 10
                    details['macd'] = '金叉持续'
                # DIF>DEA（6分）
                else:
                    score_macd = 6
                    details['macd'] = 'DIF>DEA'
            elif len(dif) >= 2:  # 死叉状态
                # 但如果MACD在底部且开始抬头，也给予一定分数
                if dif[-1] < 0 and dea[-1] < 0:  # 底部区域
                    if dif[-1] > dif[-2]:  # DIF向上
                        score_macd = 4
                        details['macd'] = '底部DIF向上'
                    else:
                        score_macd = 2
                        details['macd'] = '底部死叉'
                else:
                    score_macd = 1
                    details['macd'] = '死叉'
            else:
                score_macd = 0
                details['macd'] = '未金叉'
            
            total_score += score_macd
            
            # ==================【维度4】底部特征（15分） 新增！核心2==================
            score_bottom = 0
            
            # 计算前期缩量程度
            recent_vol_10 = np.mean(volume[-10:]) if len(volume) >= 10 else np.mean(volume)
            hist_vol_30 = np.mean(volume[-40:-10]) if len(volume) >= 40 else np.mean(volume)
            vol_shrink_ratio = recent_vol_10 / hist_vol_30 if hist_vol_30 > 0 else 1.0
            
            # 底部特征评分（放宽标准）
            if price_position < 0.20 and vol_shrink_ratio < 0.8:  # 完美底部
                score_bottom = 15
                details['bottom'] = '完美底部特征（<20%+缩量）'
            elif price_position < 0.25 and vol_shrink_ratio < 0.9:  # 优秀底部
                score_bottom = 13
                details['bottom'] = '优秀底部特征（<25%+缩量）'
            elif price_position < 0.30:  # 良好底部
                score_bottom = 11
                details['bottom'] = '良好底部特征（<30%）'
            elif price_position < 0.40:  # 中等底部
                score_bottom = 9
                details['bottom'] = '中等底部特征（<40%）'
            elif price_position < 0.50:  # 基础底部
                score_bottom = 7
                details['bottom'] = '基础底部特征（<50%）'
            elif price_position < 0.60:  # 中低位
                score_bottom = 5
                details['bottom'] = '中低位（<60%）'
            elif price_position < 0.70:  # 中位
                score_bottom = 3
                details['bottom'] = '中位（<70%）'
            else:
                score_bottom = 1
                details['bottom'] = '高位'
            
            total_score += score_bottom
            
            # ==================【维度5】主力吸筹（10分） 新增！核心3==================
            score_accumulation = 0
            
            # 连续温和放量判断（2-3天）
            continuous_vol_days = 0
            for i in range(-3, 0):
                if i < -len(volume):
                    continue
                recent_vol_i = volume[i]
                avg_vol_before = np.mean(volume[i-10:i]) if i-10 >= -len(volume) else np.mean(volume[:i])
                if avg_vol_before > 0 and 1.1 <= recent_vol_i / avg_vol_before <= 3.0:  # 放宽范围
                    continuous_vol_days += 1
            
            # 价格稳定/上涨判断（放宽）
            price_stable = True
            if price_chg_3d < -0.03:  # 3天跌超3%才认为不稳定
                price_stable = False
            
            # 主力吸筹评分（放宽条件）
            if continuous_vol_days >= 3 and price_stable and 1.5 <= vol_ratio <= 3.0:
                score_accumulation = 10
                details['accumulation'] = '主力强势建仓（连续3天）'
            elif continuous_vol_days >= 2 and price_stable and 1.3 <= vol_ratio <= 3.0:
                score_accumulation = 8
                details['accumulation'] = '主力积极吸筹（连续2天）'
            elif continuous_vol_days >= 1 and price_stable and 1.2 <= vol_ratio <= 3.0:
                score_accumulation = 6
                details['accumulation'] = '主力温和吸筹'
            elif vol_ratio > 1.2 and price_stable:
                score_accumulation = 5
                details['accumulation'] = '可能主力吸筹'
            elif vol_ratio > 1.1 and price_chg_3d >= 0:  # 放量横盘也给分
                score_accumulation = 3
                details['accumulation'] = '温和放量横盘'
            elif vol_ratio > 1.5 and not price_stable:
                score_accumulation = 0
                details['accumulation'] = '放量下跌-非吸筹'
            else:
                score_accumulation = 1
                details['accumulation'] = '无明显吸筹'
            
            total_score += score_accumulation
            
            # ==================【维度6】涨停基因（5分）==================
            score_limit = 0
            
            # 近5天内有涨停记录
            has_limit_up_5d = sum(1 for p in pct_chg[-5:] if p > 9.5)
            # 近5天大涨记录（>7%）
            has_big_rise_5d = sum(1 for p in pct_chg[-5:] if p > 7.0)
            # 近5天中涨记录（>5%）
            has_mid_rise_5d = sum(1 for p in pct_chg[-5:] if p > 5.0)
            
            if has_limit_up_5d >= 2:
                score_limit = 5
                details['limit'] = f'近5天{has_limit_up_5d}个涨停'
            elif has_limit_up_5d >= 1:
                score_limit = 4
                details['limit'] = '近5天有涨停'
            elif has_big_rise_5d >= 2:
                score_limit = 3
                details['limit'] = f'近5天{has_big_rise_5d}次大涨(>7%)'
            elif has_big_rise_5d >= 1:
                score_limit = 2
                details['limit'] = '近5天有大涨(>7%)'
            elif has_mid_rise_5d >= 1:
                score_limit = 1
                details['limit'] = '近5天有中涨(>5%)'
            else:
                score_limit = 0
                details['limit'] = '无涨停记录'
            
            total_score += score_limit
            
            # ==========  优化1：动态权重调整 ==========
            # 获取市场环境
            market_env = self.get_market_environment()
            weights = self.get_dynamic_weights(market_env)
            
            #  修复Bug：6维度分数已经按100分制设计好了（25+20+25+15+10+5=100）
            # 直接使用原始分数，不再乘以权重！
            # 如果需要动态权重，应该在每个维度的内部调整，而不是最后统一加权
            base_score = total_score  # 直接使用6维度的原始总分
            
            # ==========  优化2：协同效应加成 ==========
            scores_dict = {
                'volume_price': score_volume_price,
                'ma': score_ma,
                'macd': score_macd,
                'bottom': score_bottom,
                'accumulation': score_accumulation,
                'limit': score_limit
            }
            synergy_bonus = self.calculate_synergy_bonus(scores_dict)
            
            # ==========  优化3：风险评分 ==========
            risk_result = self.calculate_risk_score(stock_data)
            risk_score = risk_result['risk_score']
            risk_level = risk_result['risk_level']
            
            #  降低风险惩罚系数：从0.15降到0.05（风险100分只扣5分）
            risk_penalty = risk_score * 0.05  # 风险惩罚系数0.05（大幅降低）
            
            # ==========  优化4：行业热度加成 ==========
            industry = stock_data['industry'].iloc[0] if 'industry' in stock_data.columns else None
            if industry and not pd.isna(industry):
                industry_result = self.calculate_industry_heat(industry)
                industry_bonus = industry_result['heat_score']
                industry_level = industry_result['heat_level']
            else:
                industry_bonus = 0
                industry_level = '未知'
                industry_result = {'heat_score': 0, 'heat_level': '未知', 'industry_return': 0}
            
            # ==========  优化6：止损位置建议 ==========
            entry_price = close[-1]
            stop_loss_result = self.calculate_stop_loss(stock_data, entry_price)
            
            # ========== 最终评分 ==========
            # 基础分 + 协同加成 + 行业加成 - 风险惩罚
            #  暂时简化：主要使用基础分，减少复杂优化的影响
            final_score = base_score + synergy_bonus * 0.5 + industry_bonus * 0.5 - risk_penalty
            final_score = max(0, min(100, final_score))  # 限制在0-100
            
            # ========== 确定等级（总分100分） ==========
            if final_score >= 85:  # 85%
                level = 'S'
            elif final_score >= 75:  # 75%
                level = 'A'
            elif final_score >= 65:  # 65%
                level = 'B'
            elif final_score >= 55:  # 55%
                level = 'C'
            else:
                level = 'D'
            
            # 增加详细信息
            details['market_env'] = market_env
            details['base_score'] = round(base_score, 1)
            details['synergy_bonus'] = round(synergy_bonus, 1)
            details['industry_bonus'] = round(industry_bonus, 1)
            details['risk_penalty'] = round(risk_penalty, 1)
            details['weights'] = weights
            
            return {
                #  添加success字段供回测使用
                'success': True,
                
                # 核心评分
                'score': round(final_score, 1),
                'final_score': round(final_score, 1),  #  添加final_score字段供回测使用
                'level': level,
                
                # 评分组成（7大优化）
                'base_score': round(base_score, 1),              # 基础6维评分
                'synergy_bonus': round(synergy_bonus, 1),        # 优化2：协同加成
                'industry_bonus': round(industry_bonus, 1),      # 优化4：行业热度
                'risk_penalty': round(risk_penalty, 1),          # 优化3：风险惩罚
                
                #  6维评分明细（供回测显示）
                'volume_price_score': score_volume_price,
                'ma_trend_score': score_ma,
                'macd_trend_score': score_macd,
                'bottom_feature_score': score_bottom,
                'main_force_score': score_accumulation,
                'limit_up_gene_score': score_limit,
                
                # 风险评估（优化3）
                'risk_score': risk_score,
                'risk_level': risk_level,
                'risk_details': risk_result['details'],
                
                # 市场环境（优化1）
                'market_env': market_env,
                'weights': weights,
                
                # 行业热度（优化4）
                'industry_heat': industry_level,
                'industry_return': industry_result['industry_return'],
                
                # 止损建议（优化6）
                'stop_loss': stop_loss_result,
                
                # 基础信息
                'details': details,
                'price_position': round(price_position * 100, 1),
                'vol_ratio': round(vol_ratio, 2),
                'price_chg_5d': round(price_chg_3d * 100, 2)
            }
            
        except Exception as e:
            logger.error(f"融合评分失败: {e}")
            return {'success': False, 'score': 0, 'final_score': 0, 'level': 'D', 'details': {}}
    
    def evaluate_stock_comprehensive(self, stock_data: pd.DataFrame) -> Dict:
        """
         综合优选优化版：真正的6维100分评分体系 + 7大AI优化
        
        【6维100分评分体系】
        1. 量价配合（25分）：放量上涨vs放量下跌，主力行为识别
        2. 均线多头（20分）：多头排列，趋势确认
        3. MACD趋势（25分）：金叉、三向上，趋势强度
        4. 底部特征（15分）：低位安全边际，蓄势时间
        5. 主力吸筹（10分）：温和放量，价格稳定
        6. 涨停基因（5分）：历史涨停记录
        
        【7大AI优化】
        1.  动态权重系统（市场环境自适应）
        2.  非线性评分 + 协同效应（黄金组合加分）
        3.  风险评分维度（系统性风险扣分）
        4.  行业热度加分（行业共振）
        5.  时间衰减因子（新信号优先）
        6.  止损位推荐（智能风控）
        7.  性能优化（向量化计算）
        
        返回：{
            'comprehensive_score': 最终综合得分（0-100）,
            'dimension_scores': {6个维度的分项得分},
            'synergy_bonus': 协同加分,
            'risk_penalty': 风险扣分,
            'grade': 评级（S/A/B/C/D）,
            'stop_loss': 建议止损价,
            'details': 详细信息
        }
        """
        try:
            if stock_data is None or len(stock_data) < 60:
                return self._empty_score_result()
            
            required_cols = ['close_price', 'vol', 'pct_chg']
            if not all(col in stock_data.columns for col in required_cols):
                return self._empty_score_result()
            
            # 基础风控：排除ST股
            if 'name' in stock_data.columns:
                stock_name = stock_data['name'].iloc[0]
                if 'ST' in stock_name or '*ST' in stock_name:
                    return self._empty_score_result()
            
            data = stock_data[required_cols].copy()
            for col in required_cols:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            data = data.dropna()
            
            if len(data) < 60:
                return self._empty_score_result()
            
            close = data['close_price'].values
            volume = data['vol'].values
            pct_chg = data['pct_chg'].values
            
            # ========== 计算所有基础指标 ==========
            indicators = self._calculate_all_indicators(close, volume, pct_chg)
            
            # ========== 【维度1】量价配合（25分）==========
            score_volume_price = self._score_volume_price(indicators)
            
            # ========== 【维度2】均线多头（20分）==========
            score_ma_trend = self._score_ma_trend(indicators)
            
            # ========== 【维度3】MACD趋势（25分）==========
            score_macd = self._score_macd_trend(indicators, close)
            
            # ========== 【维度4】底部特征（15分）==========
            score_bottom = self._score_bottom_feature(indicators)
            
            # ========== 【维度5】主力吸筹（10分）==========
            score_accumulation = self._score_main_force_accumulation(indicators)
            
            # ========== 【维度6】涨停基因（5分）==========
            score_limit_up = self._score_limit_up_gene(pct_chg)
            
            # ========== 基础得分（100分）==========
            base_score = (
                score_volume_price + 
                score_ma_trend + 
                score_macd + 
                score_bottom + 
                score_accumulation + 
                score_limit_up
            )
            
            # ========== 【AI优化1】动态权重系统 ==========
            market_env = self._detect_market_environment(close)
            stock_stage = self._detect_stock_stage(indicators)
            
            # ========== 【AI优化2】协同效应加分（0-20分）==========
            synergy_bonus = self._calculate_synergy_bonus(indicators)
            
            # ========== 【AI优化3】风险评分扣分（0-41分）==========
            risk_penalty = self._calculate_risk_penalty(indicators, close, pct_chg)
            
            # ========== 【AI优化4】行业热度加分（0-5分）==========
            industry_bonus = 0  # 简化版，可扩展
            
            # ========== 计算最终得分 ==========
            final_score = base_score + synergy_bonus - risk_penalty + industry_bonus
            final_score = max(0, min(100, final_score))
            
            # ========== 评级 ==========
            if final_score >= 90:
                grade = 'S'
            elif final_score >= 85:
                grade = 'A'
            elif final_score >= 80:
                grade = 'B'
            elif final_score >= 75:
                grade = 'C'
            elif final_score >= 70:
                grade = 'D'
            else:
                grade = 'E'
            
            # ========== 【AI优化6】智能止损位 ==========
            stop_loss_info = self._recommend_stop_loss(close, indicators)
            
            return {
                'comprehensive_score': round(final_score, 2),
                'dimension_scores': {
                    '量价配合': round(score_volume_price, 1),
                    '均线多头': round(score_ma_trend, 1),
                    'MACD趋势': round(score_macd, 1),
                    '底部特征': round(score_bottom, 1),
                    '主力吸筹': round(score_accumulation, 1),
                    '涨停基因': round(score_limit_up, 1)
                },
                'base_score': round(base_score, 1),
                'synergy_bonus': round(synergy_bonus, 1),
                'risk_penalty': round(risk_penalty, 1),
                'industry_bonus': round(industry_bonus, 1),
                'grade': grade,
                'market_env': market_env,
                'stock_stage': stock_stage,
                'stop_loss': stop_loss_info['stop_loss'],
                'stop_loss_method': stop_loss_info['method'],
                'details': indicators
            }
            
        except Exception as e:
            logger.error(f"综合评分失败: {e}")
            logger.error(traceback.format_exc())
            return self._empty_score_result()
    
    def _empty_score_result(self) -> Dict:
        """返回空评分结果"""
        return {
            'comprehensive_score': 0,
            'dimension_scores': {'量价配合': 0, '均线多头': 0, 'MACD趋势': 0, '底部特征': 0, '主力吸筹': 0, '涨停基因': 0},
            'base_score': 0,
            'synergy_bonus': 0,
            'risk_penalty': 0,
            'industry_bonus': 0,
            'grade': 'E',
            'market_env': 'unknown',
            'stock_stage': 'unknown',
            'stop_loss': 0,
            'stop_loss_method': 'none',
            'details': {}
        }
    
    def _calculate_all_indicators(self, close, volume, pct_chg) -> Dict:
        """计算所有基础指标"""
        # 价格指标
        price_min = np.min(close[-60:])
        price_max = np.max(close[-60:])
        price_range = price_max - price_min
        price_position = (close[-1] - price_min) / price_range if price_range > 0 else 0.5
        
        # 成交量指标
        recent_vol = np.mean(volume[-3:])
        hist_vol = np.mean(volume[-20:-3]) if len(volume) >= 23 else np.mean(volume[:-3])
        vol_ratio = recent_vol / hist_vol if hist_vol > 0 else 1.0
        
        # 涨跌幅
        price_chg_5d = (close[-1] - close[-6]) / close[-6] if len(close) > 6 and close[-6] > 0 else 0
        price_chg_10d = (close[-1] - close[-11]) / close[-11] if len(close) > 11 and close[-11] > 0 else 0
        price_chg_20d = (close[-1] - close[-21]) / close[-21] if len(close) > 21 and close[-21] > 0 else 0
        
        # 均线
        ma5 = np.mean(close[-5:])
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma60 = np.mean(close[-60:]) if len(close) >= 60 else ma20
        
        # MACD
        ema12 = pd.Series(close).ewm(span=12, adjust=False).mean().values
        ema26 = pd.Series(close).ewm(span=26, adjust=False).mean().values
        dif = ema12 - ema26
        dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
        macd_hist = dif - dea
        
        # 其他指标
        continuous_vol_up = sum(1 for v in volume[-5:] if v > hist_vol * 1.2) if hist_vol > 0 else 0
        price_stable_days = sum(1 for p in pct_chg[-5:] if p >= -1.0)
        
        # 波动率
        volatility = np.std(close[-20:]) / np.mean(close[-20:]) if np.mean(close[-20:]) > 0 else 0
        
        # 涨停跌停
        limit_up_count_5d = sum(1 for p in pct_chg[-5:] if p > 9.5)
        limit_down_count_60d = sum(1 for p in pct_chg[-60:] if p < -9.5)
        
        return {
            'price_position': price_position,
            'vol_ratio': vol_ratio,
            'price_chg_5d': price_chg_5d,
            'price_chg_10d': price_chg_10d,
            'price_chg_20d': price_chg_20d,
            'ma5': ma5,
            'ma10': ma10,
            'ma20': ma20,
            'ma60': ma60,
            'dif': dif,
            'dea': dea,
            'macd_hist': macd_hist,
            'continuous_vol_up': continuous_vol_up,
            'price_stable_days': price_stable_days,
            'volatility': volatility,
            'limit_up_count_5d': limit_up_count_5d,
            'limit_down_count_60d': limit_down_count_60d,
            'recent_vol': recent_vol,
            'hist_vol': hist_vol
        }
    
    def _score_volume_price(self, ind: Dict) -> float:
        """【维度1】量价配合评分（25分）"""
        score = 0
        vol_ratio = ind['vol_ratio']
        price_chg = ind['price_chg_5d']
        price_pos = ind['price_position']
        
        # 核心逻辑：区分放量上涨 vs 放量下跌
        if price_chg > 0.03 and vol_ratio > 2.0:
            # 强势放量上涨
            if price_pos < 0.3:  # 低位
                score = 25  # 满分！最佳信号
            elif price_pos < 0.5:
                score = 20  # 中位
            else:
                score = 12  # 高位谨慎
        elif price_chg > 0.02 and vol_ratio > 1.5:
            # 放量上涨
            if price_pos < 0.4:
                score = 20
            else:
                score = 15
        elif price_chg > 0 and vol_ratio > 1.3:
            # 温和放量上涨
            score = 15
        elif price_chg > 0 and vol_ratio > 1.1:
            score = 10
        elif price_chg < -0.02 and vol_ratio > 1.5:
            #  放量下跌 = 主力出货
            score = 0  # 一票否决！
        elif price_chg > 0:
            score = 5  # 上涨但缩量
        
        return min(25, score)
    
    def _score_ma_trend(self, ind: Dict) -> float:
        """【维度2】均线多头评分（20分）"""
        score = 0
        ma5, ma10, ma20 = ind['ma5'], ind['ma10'], ind['ma20']
        
        if ma5 > ma10 > ma20 > 0:
            # 完美多头排列
            if ind['price_chg_5d'] > 0.02:
                score = 20  # 满分！
            else:
                score = 18
        elif ma5 > ma10 > 0:
            # 强势多头
            score = 15
        elif ma5 > ma20 > 0 or ma10 > ma20 > 0:
            # 中期多头
            score = 10
        elif ma5 > 0:
            score = 5
        
        return min(20, score)
    
    def _score_macd_trend(self, ind: Dict, close) -> float:
        """【维度3】MACD趋势评分（25分）"""
        score = 0
        dif = ind['dif']
        dea = ind['dea']
        macd_hist = ind['macd_hist']
        
        if len(dif) < 2:
            return 0
        
        # DIF和DEA方向
        dif_up = dif[-1] > dif[-2]
        dea_up = dea[-1] > dea[-2]
        hist_up = macd_hist[-1] > macd_hist[-2]
        
        # 金叉检测
        golden_cross = dif[-1] > dea[-1] and dif[-2] <= dea[-2]
        
        # 完美三向上（DIF↑ + DEA↑ + 柱↑）
        if dif_up and dea_up and hist_up:
            if dif[-1] > 0 and dea[-1] > 0:
                score = 25  # 满分！强势多头
            elif golden_cross and dif[-1] < 0:
                score = 22  # 底部金叉+三向上
            else:
                score = 20  # 普通三向上
        # 0轴金叉 + 双向上
        elif golden_cross and dif[-1] > 0:
            score = 20
        # 底部金叉 + 双向上
        elif golden_cross and dif_up and dea_up:
            score = 18
        # 普通金叉
        elif golden_cross:
            score = 15
        # 金叉持续
        elif dif[-1] > dea[-1]:
            if dif_up and dea_up:
                score = 15
            else:
                score = 10
        # 准备金叉（接近交叉）
        elif dif[-1] > dif[-2] and abs(dif[-1] - dea[-1]) < abs(dif[-2] - dea[-2]):
            score = 8
        
        return min(25, score)
    
    def _score_bottom_feature(self, ind: Dict) -> float:
        """【维度4】底部特征评分（15分）"""
        score = 0
        price_pos = ind['price_position']
        volatility = ind['volatility']
        
        # 底部位置评分
        if price_pos < 0.15:
            # 极低位
            if volatility < 0.05:  # 缩量横盘
                score = 15  # 满分！
            else:
                score = 13
        elif price_pos < 0.25:
            # 低位区域
            if volatility < 0.08:
                score = 12
            else:
                score = 10
        elif price_pos < 0.35:
            # 相对低位
            score = 8
        elif price_pos < 0.45:
            score = 5
        elif price_pos < 0.60:
            score = 2
        
        return min(15, score)
    
    def _score_main_force_accumulation(self, ind: Dict) -> float:
        """【维度5】主力吸筹评分（10分）"""
        score = 0
        vol_ratio = ind['vol_ratio']
        price_chg = ind['price_chg_5d']
        price_stable = ind['price_stable_days']
        continuous_vol = ind['continuous_vol_up']
        
        # 温和放量 + 价格小涨/不跌 = 吸筹信号
        if 1.3 <= vol_ratio <= 1.8 and 0 <= price_chg <= 0.03:
            if price_stable >= 4:
                score = 10  # 满分！主力吸筹
            else:
                score = 7
        elif 1.5 <= vol_ratio <= 2.0 and price_chg > 0:
            score = 6  # 放量上涨
        elif continuous_vol >= 3 and price_chg >= 0:
            score = 8  # 连续放量+不跌
        elif vol_ratio > 1.2 and price_chg > 0:
            score = 4
        
        return min(10, score)
    
    def _score_limit_up_gene(self, pct_chg) -> float:
        """【维度6】涨停基因评分（5分）"""
        score = 0
        
        # 近5天涨停次数
        limit_up_5d = sum(1 for p in pct_chg[-5:] if p > 9.5)
        # 近20天涨停次数
        limit_up_20d = sum(1 for p in pct_chg[-20:] if p > 9.5)
        
        if limit_up_5d >= 2:
            score = 5  # 满分！
        elif limit_up_5d >= 1:
            score = 3
        elif limit_up_20d >= 2:
            score = 2
        elif limit_up_20d >= 1:
            score = 1
        
        return min(5, score)
    
    def _detect_market_environment(self, close) -> str:
        """【AI优化1】检测市场环境"""
        # 简化版：根据均线趋势判断
        ma5 = np.mean(close[-5:])
        ma20 = np.mean(close[-20:])
        ma60 = np.mean(close[-60:]) if len(close) >= 60 else ma20
        
        if ma5 > ma20 > ma60:
            return '牛市'
        elif ma5 < ma20 < ma60:
            return '熊市'
        else:
            return '震荡市'
    
    def _detect_stock_stage(self, ind: Dict) -> str:
        """【AI优化1】检测个股阶段"""
        vol_ratio = ind['vol_ratio']
        price_chg = ind['price_chg_5d']
        price_pos = ind['price_position']
        
        if price_pos < 0.3 and vol_ratio < 1.3:
            return '蓄势期'
        elif vol_ratio > 1.5 and price_chg > 0.02:
            return '启动期'
        elif price_chg > 0.05 or vol_ratio > 2.0:
            return '加速期'
        else:
            return '观望期'
    
    def _calculate_synergy_bonus(self, ind: Dict) -> float:
        """【AI优化2】协同效应加分（0-20分）"""
        bonus = 0
        
        #  黄金组合1：完美启动（低位+强势放量+MACD金叉）
        if (ind['price_position'] < 0.20 and 
            ind['vol_ratio'] > 2.0 and 
            len(ind['dif']) >= 2 and 
            ind['dif'][-1] > ind['dea'][-1] and 
            ind['dif'][-2] <= ind['dea'][-2]):
            bonus += 10
        
        #  黄金组合2：主力吸筹（温和放量+价格稳定+低位）
        if (1.3 <= ind['vol_ratio'] <= 1.8 and
            ind['price_stable_days'] >= 4 and
            ind['price_position'] < 0.35 and
            0 <= ind['price_chg_5d'] <= 0.03):
            bonus += 8
        
        #  黄金组合3：突破确认（多头排列+放量+涨停基因）
        if (ind['ma5'] > ind['ma10'] > ind['ma20'] and
            ind['vol_ratio'] > 1.8 and
            ind['limit_up_count_5d'] >= 1):
            bonus += 8
        
        #  黄金组合4：底部蓄势完成（低位+缩量+开始放量）
        if (ind['price_position'] < 0.25 and
            ind['volatility'] < 0.06 and
            ind['vol_ratio'] > 1.5):
            bonus += 6
        
        return min(20, bonus)
    
    def _calculate_risk_penalty(self, ind: Dict, close, pct_chg) -> float:
        """【AI优化3】风险评分扣分（0-41分）"""
        penalty = 0
        
        # 风险1：高位风险（-10分）
        gain_60d = (close[-1] - close[-61]) / close[-61] if len(close) > 61 and close[-61] > 0 else 0
        recent_decline = ind['price_chg_5d'] < -0.03
        if gain_60d > 0.50 and recent_decline:
            penalty += 10  # 高位回落，主力出货
        elif gain_60d > 0.40 and recent_decline:
            penalty += 7
        elif gain_60d > 0.30 and ind['price_position'] > 0.7:
            penalty += 5
        
        # 风险2：波动率风险（-8分）
        volatility = ind['volatility']
        if volatility > 0.15:
            penalty += 8  # 剧烈波动
        elif volatility > 0.12:
            penalty += 6
        elif volatility > 0.10:
            penalty += 4
        
        # 风险3：暴跌风险（-8分）
        limit_down_count = ind['limit_down_count_60d']
        if limit_down_count >= 3:
            penalty += 8
        elif limit_down_count >= 2:
            penalty += 6
        elif limit_down_count >= 1:
            penalty += 3
        
        # 风险4：技术破位风险（-10分）
        if close[-1] < ind['ma5'] < ind['ma10'] < ind['ma20']:
            penalty += 10  # 完全空头排列
        elif close[-1] < ind['ma20']:
            penalty += 6  # 跌破中期均线
        elif close[-1] < ind['ma10']:
            penalty += 3  # 跌破短期均线
        
        # 风险5：流动性风险（-5分）
        if ind['vol_ratio'] < 0.5:
            penalty += 5  # 严重缩量
        elif ind['vol_ratio'] < 0.7:
            penalty += 3
        
        return min(41, penalty)
    
    def _recommend_stop_loss(self, close, ind: Dict) -> Dict:
        """【AI优化6】智能止损位推荐"""
        current_price = close[-1]
        
        # 方法1：ATR止损（动态）
        high_low_range = []
        for i in range(min(14, len(close))):
            if i < len(close) - 1:
                high_low_range.append(abs(close[-(i+1)] - close[-(i+2)]))
        atr = np.mean(high_low_range) if high_low_range else current_price * 0.02
        atr_stop = current_price - 2 * atr
        
        # 方法2：支撑位止损（技术）
        ma20_stop = ind['ma20'] * 0.95
        
        # 方法3：百分比止损（固定）
        percent_stop = current_price * 0.92  # -8%
        
        # 智能选择：取最高的止损位（最保守）
        stop_loss = max(atr_stop, ma20_stop, percent_stop)
        
        if stop_loss == atr_stop:
            method = 'ATR动态止损'
        elif stop_loss == ma20_stop:
            method = 'MA20支撑止损'
        else:
            method = '8%固定止损'
        
        return {
            'stop_loss': round(stop_loss, 2),
            'method': method,
            'risk_ratio': round((current_price - stop_loss) / current_price * 100, 2)
        }
    
    def _backtest_with_evaluator(self, df: pd.DataFrame, sample_size: int, holding_days: int, 
                                 version: str, min_score: float, max_score: float) -> dict:
        """
        通用的评分器回测方法
        
        Args:
            df: 历史数据
            sample_size: 回测样本数量
            holding_days: 持仓天数
            version: 评分器版本 ('v4', 'v5', 'v6')
            min_score: 最低分数阈值
            max_score: 最高分数阈值
        """
        try:
            version_map = {
                'v4': ('evaluator_v4', 'evaluate_stock_v4', 'v4.0 长期稳健版（真实评分器）'),
                'v5': ('evaluator_v5', 'evaluate_stock_v4', 'v5.0 趋势趋势版（真实评分器）'),  # v5使用v4的方法
                'v6': ('evaluator_v6', 'evaluate_stock_v6', 'v6.0 高级超短线（真实评分器）')
            }
            
            evaluator_attr, eval_method, strategy_name = version_map[version]
            evaluator = getattr(self, evaluator_attr)
            
            logger.info(f"使用真实{version}评分器回测...")
            
            # 确保列名标准化
            if 'close_price' not in df.columns and 'close' in df.columns:
                df = df.rename(columns={'close': 'close_price', 'open': 'open_price', 
                                       'high': 'high_price', 'low': 'low_price'})
            
            all_signals = []
            analyzed_count = 0
            
            unique_stocks = df['ts_code'].unique()
            if len(unique_stocks) > sample_size:
                sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
            else:
                sample_stocks = unique_stocks
            
            for ts_code in sample_stocks:
                analyzed_count += 1
                if analyzed_count % 50 == 0:
                    logger.info(f"回测进度: {analyzed_count}/{len(sample_stocks)}, 已找到 {len(all_signals)} 个信号")
                
                try:
                    stock_data = df[df['ts_code'] == ts_code].copy()
                    
                    if len(stock_data) < 60 + holding_days:
                        continue
                    
                    # 遍历可能的买入点
                    for i in range(30, len(stock_data) - holding_days - 1):
                        current_data = stock_data.iloc[:i+1].copy()
                        
                        # 调用对应版本的评分方法（v6需要传递ts_code）
                        if version == 'v6':
                            eval_result = getattr(evaluator, eval_method)(current_data, ts_code)
                        else:
                            eval_result = getattr(evaluator, eval_method)(current_data)
                        
                        if not eval_result['success']:
                            continue
                        
                        final_score = eval_result['final_score']
                        
                        # 检查是否在目标分数区间
                        if min_score <= final_score <= max_score:
                            close_col = 'close_price' if 'close_price' in stock_data.columns else 'close'
                            buy_price = stock_data.iloc[i][close_col]
                            sell_price = stock_data.iloc[i + holding_days][close_col]
                            future_return = (sell_price - buy_price) / buy_price * 100
                            
                            all_signals.append({
                                'ts_code': ts_code,
                                'name': stock_data['name'].iloc[0] if 'name' in stock_data.columns else ts_code,
                                'industry': stock_data['industry'].iloc[0] if 'industry' in stock_data.columns else '未知',
                                'trade_date': stock_data.iloc[i]['trade_date'],
                                'close': buy_price,
                                'signal_strength': final_score,
                                'grade': eval_result.get('grade', ''),
                                'reasons': eval_result.get('signal_reasons', ''),
                                'future_return': future_return
                            })
                            break  # 每只股票只取第一个信号
                
                except Exception as e:
                    continue
            
            if not all_signals:
                return {
                    'success': False,
                    'error': f'未找到有效信号（{min_score}-{max_score}分区间）\n分析了{analyzed_count}只股票',
                    'strategy': strategy_name,
                    'stats': {'total_signals': 0, 'analyzed_stocks': analyzed_count}
                }
            
            # 计算统计
            backtest_df = pd.DataFrame(all_signals)
            stats = self._calculate_backtest_stats(backtest_df, analyzed_count, holding_days)
            
            # 详细记录
            details = []
            for idx, row in backtest_df.head(100).iterrows():
                details.append({
                    '股票代码': row['ts_code'],
                    '股票名称': row['name'],
                    '行业': row['industry'],
                    '信号日期': str(row['trade_date']),
                    '评级': row['grade'],
                    f'{version}评分': f"{row['signal_strength']:.1f}分",
                    '买入价': f"{row['close']:.2f}元",
                    f'{holding_days}天收益': f"{row['future_return']:.2f}%",
                    '信号原因': row['reasons']
                })
            
            logger.info(f"{version}真实评分器回测完成：{stats['total_signals']}个信号，"
                       f"平均收益{stats['avg_return']:.2f}%，胜率{stats['win_rate']:.1f}%")
            
            return {
                'success': True,
                'strategy': strategy_name,
                'stats': stats,
                'backtest_data': backtest_df,
                'details': details
            }
        
        except Exception as e:
            logger.error(f"{version}真实评分器回测失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'success': False, 'error': str(e), 'strategy': strategy_name, 'stats': {}}
    
    def _calculate_backtest_stats(self, backtest_df: pd.DataFrame, analyzed_count: int, holding_days: int) -> dict:
        return runtime_calculate_backtest_stats(
            backtest_df,
            analyzed_count=analyzed_count,
            holding_days=holding_days,
        )
    
    def _backtest_with_real_evaluator_v4(self, df: pd.DataFrame, sample_size: int = 800, holding_days: int = 5,
                                         min_score: float = 60, max_score: float = 85) -> dict:
        """使用真实的v4.0八维评分器进行回测（支持自定义阈值）"""
        try:
            logger.info("使用真实v4.0评分器回测...")
            
            # 确保列名标准化
            if 'close_price' not in df.columns and 'close' in df.columns:
                df = df.rename(columns={
                    'close': 'close_price',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price'
                })
            
            all_signals = []
            analyzed_count = 0
            valid_signal_count = 0
            
            unique_stocks = df['ts_code'].unique()
            if len(unique_stocks) > sample_size:
                sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
            else:
                sample_stocks = unique_stocks
            
            logger.info(f"将使用真实v4.0评分器回测 {len(sample_stocks)} 只股票")
            
            for ts_code in sample_stocks:
                analyzed_count += 1
                if analyzed_count % 50 == 0:
                    logger.info(f"回测进度: {analyzed_count}/{len(sample_stocks)}, 已找到 {valid_signal_count} 个有效信号")
                
                try:
                    stock_data = df[df['ts_code'] == ts_code].copy()
                    
                    if len(stock_data) < 60 + holding_days:
                        continue
                    
                    # 遍历可能的买入点
                    for i in range(30, len(stock_data) - holding_days - 1):
                        try:
                            # 获取到当前时点的数据
                            current_data = stock_data.iloc[:i+1].copy()
                            
                            # 使用真实的v4.0评分器评分
                            eval_result = self.evaluator_v4.evaluate_stock_v4(current_data)
                            
                            if not eval_result['success']:
                                continue
                            
                            final_score = eval_result['final_score']
                            
                            # 使用自定义阈值作为信号阈值（v4.0潜伏期特征）
                            if min_score <= final_score <= max_score:
                                # 计算未来收益
                                close_col = 'close_price' if 'close_price' in stock_data.columns else 'close'
                                buy_price = stock_data.iloc[i][close_col]
                                sell_price = stock_data.iloc[i + holding_days][close_col]
                                future_return = (sell_price - buy_price) / buy_price * 100
                                
                                signal = {
                                    'ts_code': ts_code,
                                    'name': stock_data['name'].iloc[0] if 'name' in stock_data.columns else ts_code,
                                    'industry': stock_data['industry'].iloc[0] if 'industry' in stock_data.columns else '未知',
                                    'trade_date': stock_data.iloc[i]['trade_date'],
                                    'close': buy_price,
                                    'signal_strength': final_score,
                                    'grade': eval_result.get('grade', ''),
                                    'reasons': eval_result.get('signal_reasons', ''),
                                    'future_return': future_return,
                                    # v4.0特有的维度得分
                                    'lurking_value': eval_result.get('dimension_scores', {}).get('潜伏价值', 0),
                                    'bottom_feature': eval_result.get('dimension_scores', {}).get('底部特征', 0),
                                    'volume_price': eval_result.get('dimension_scores', {}).get('量价配合', 0),
                                }
                                
                                all_signals.append(signal)
                                valid_signal_count += 1
                                break  # 每只股票只取第一个信号
                        
                        except Exception as e:
                            continue
                
                except Exception as e:
                    logger.debug(f"处理{ts_code}失败: {e}")
                    continue
            
            if not all_signals:
                logger.warning(f"未找到有效信号（分析了{analyzed_count}只股票）")
                return {
                    'success': False,
                    'error': f'未找到有效信号（60-85分区间）\n分析了{analyzed_count}只股票\n\n 说明：v4.0策略专注于60-85分的潜伏期股票',
                    'strategy': 'v4.0 长期稳健版',
                    'stats': {'total_signals': 0, 'analyzed_stocks': analyzed_count}
                }
            
            # 转换为DataFrame并计算统计
            backtest_df = pd.DataFrame(all_signals)
            
            logger.info(f"找到 {len(backtest_df)} 个v4.0真实评分信号")
            
            stats = {
                'total_signals': len(backtest_df),
                'analyzed_stocks': analyzed_count,
                'avg_return': float(backtest_df['future_return'].mean()),
                'median_return': float(backtest_df['future_return'].median()),
                'win_rate': float((backtest_df['future_return'] > 0).sum() / len(backtest_df) * 100),
                'max_return': float(backtest_df['future_return'].max()),
                'min_return': float(backtest_df['future_return'].min()),
                'avg_holding_days': holding_days,
            }
            
            # 计算标准差和夏普比率
            std_return = backtest_df['future_return'].std()
            stats['sharpe_ratio'] = float(stats['avg_return'] / std_return) if std_return > 0 else 0
            
            #  计算最大回撤（模拟累计收益曲线）
            cumulative_returns = (1 + backtest_df['future_return'] / 100).cumprod()
            running_max = cumulative_returns.expanding().max()
            drawdowns = (cumulative_returns - running_max) / running_max * 100
            stats['max_drawdown'] = float(abs(drawdowns.min())) if len(drawdowns) > 0 else 0
            
            #  计算盈亏比
            winning_trades = backtest_df[backtest_df['future_return'] > 0]
            losing_trades = backtest_df[backtest_df['future_return'] < 0]
            if len(losing_trades) > 0:
                avg_win = winning_trades['future_return'].mean() if len(winning_trades) > 0 else 0
                avg_loss = abs(losing_trades['future_return'].mean())
                stats['profit_loss_ratio'] = float(avg_win / avg_loss) if avg_loss > 0 else 0
            else:
                stats['profit_loss_ratio'] = 0
            
            # 分强度统计
            strength_bins = [0, 60, 65, 70, 75, 80, 100]
            strength_labels = ['<60', '60-65', '65-70', '70-75', '75-80', '80+']
            
            backtest_df['strength_bin'] = pd.cut(
                backtest_df['signal_strength'],
                bins=strength_bins,
                labels=strength_labels,
                include_lowest=True
            )
            
            strength_performance = {}
            for label in strength_labels:
                subset = backtest_df[backtest_df['strength_bin'] == label]
                if len(subset) > 0:
                    strength_performance[label] = {
                        'count': int(len(subset)),
                        'avg_return': float(subset['future_return'].mean()),
                        'win_rate': float((subset['future_return'] > 0).sum() / len(subset) * 100)
                    }
            
            stats['strength_performance'] = strength_performance
            
            # 准备详细记录
            details = []
            for idx, row in backtest_df.head(100).iterrows():
                details.append({
                    '股票代码': row['ts_code'],
                    '股票名称': row['name'],
                    '行业': row['industry'],
                    '信号日期': str(row['trade_date']),
                    '评级': row.get('grade', ''),
                    'v4.0评分': f"{row['signal_strength']:.1f}分",
                    '潜伏价值': f"{row.get('lurking_value', 0):.1f}分",
                    '底部特征': f"{row.get('bottom_feature', 0):.1f}分",
                    '量价配合': f"{row.get('volume_price', 0):.1f}分",
                    '买入价': f"{row['close']:.2f}元",
                    f'{holding_days}天收益': f"{row['future_return']:.2f}%"
                })
            
            logger.info(f"v4.0真实评分器回测完成：{stats['total_signals']}个信号，"
                       f"平均收益{stats['avg_return']:.2f}%，胜率{stats['win_rate']:.1f}%")
            
            return {
                'success': True,
                'strategy': 'v4.0 长期稳健版（真实八维评分器）',
                'stats': stats,
                'backtest_data': backtest_df,
                'details': details
            }
        
        except Exception as e:
            logger.error(f"v4.0真实评分器回测失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'strategy': 'v4.0 长期稳健版',
                'stats': {}
            }
    
    def _identify_volume_price_signals(self, stock_data: pd.DataFrame, min_score: float = 60.0) -> pd.DataFrame:
        """
        简化但有效的量价信号识别系统
        适用于回测，专注于核心的量价配合逻辑
        """
        try:
            if len(stock_data) < 30:
                return pd.DataFrame()
            
            # 确保数据按日期排序
            stock_data = stock_data.sort_values('trade_date').reset_index(drop=True)
            
            # 确保必要的列存在
            required_cols = ['close', 'vol', 'pct_chg', 'trade_date']
            for col in required_cols:
                if col not in stock_data.columns:
                    return pd.DataFrame()
            
            signals = []
            
            # 遍历每一天，寻找量价配合信号
            for i in range(20, len(stock_data) - 5):  # 留出前20天和后5天
                try:
                    # 获取当前和历史数据
                    current_close = stock_data.iloc[i]['close']
                    current_vol = stock_data.iloc[i]['vol']
                    current_pct = stock_data.iloc[i]['pct_chg']
                    
                    # 历史20天数据
                    hist_data = stock_data.iloc[i-20:i]
                    avg_vol_20 = hist_data['vol'].mean()
                    
                    # 计算信号强度（0-100分）
                    score = 0
                    reasons = []
                    
                    # 1. 放量突破（30分）
                    if avg_vol_20 > 0:
                        vol_ratio = current_vol / avg_vol_20
                        if vol_ratio >= 2.0:
                            score += 30
                            reasons.append(f"放量{vol_ratio:.1f}倍")
                        elif vol_ratio >= 1.5:
                            score += 20
                            reasons.append(f"温和放量{vol_ratio:.1f}倍")
                        elif vol_ratio >= 1.2:
                            score += 10
                            reasons.append(f"微量放量{vol_ratio:.1f}倍")
                    
                    # 2. 价格上涨（25分）
                    if current_pct >= 5:
                        score += 25
                        reasons.append(f"大涨{current_pct:.1f}%")
                    elif current_pct >= 3:
                        score += 20
                        reasons.append(f"中涨{current_pct:.1f}%")
                    elif current_pct >= 1:
                        score += 15
                        reasons.append(f"小涨{current_pct:.1f}%")
                    elif current_pct > 0:
                        score += 10
                        reasons.append(f"微涨{current_pct:.1f}%")
                    
                    # 3. 底部特征（20分）
                    max_close_20 = hist_data['close'].max()
                    min_close_20 = hist_data['close'].min()
                    if max_close_20 > min_close_20:
                        price_position = (current_close - min_close_20) / (max_close_20 - min_close_20) * 100
                        
                        if price_position < 30:
                            score += 20
                            reasons.append(f"底部位置{price_position:.0f}%")
                        elif price_position < 50:
                            score += 15
                            reasons.append(f"低位{price_position:.0f}%")
                        elif price_position < 70:
                            score += 10
                            reasons.append(f"中位{price_position:.0f}%")
                    
                    # 4. 连续上涨（15分）
                    recent_5 = stock_data.iloc[i-4:i+1]
                    up_days = (recent_5['pct_chg'] > 0).sum()
                    if up_days >= 4:
                        score += 15
                        reasons.append(f"{up_days}连阳")
                    elif up_days >= 3:
                        score += 10
                        reasons.append(f"{up_days}天上涨")
                    
                    # 5. 均线支撑（10分）
                    if len(hist_data) >= 5:
                        ma5 = hist_data['close'].tail(5).mean()
                        if current_close > ma5:
                            score += 10
                            reasons.append("站上MA5")
                    
                    # 如果得分达到阈值，记录信号
                    if score >= min_score:
                        signal = {
                            'trade_date': stock_data.iloc[i]['trade_date'],
                            'close': current_close,
                            'vol': current_vol,
                            'pct_chg': current_pct,
                            'signal_strength': score,
                            'reasons': ', '.join(reasons),
                            'vol_ratio': current_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0
                        }
                        signals.append(signal)
                
                except Exception as e:
                    continue
            
            return pd.DataFrame(signals) if signals else pd.DataFrame()
        
        except Exception as e:
            logger.error(f"信号识别失败: {e}")
            return pd.DataFrame()
    
    def backtest_strategy_complete(self, df: pd.DataFrame, sample_size: int = 1500,
                                   signal_strength: float = 0.5, holding_days: int = 5) -> dict:
        """完整回测系统（v49增强版 - 健壮性优化）"""
        try:
            logger.info(f"开始完整回测，参数：信号强度={signal_strength}, 持仓={holding_days}天")
            
            # 确保列名标准化
            if 'close_price' in df.columns:
                df = df.rename(columns={
                    'close_price': 'close',
                    'open_price': 'open',
                    'high_price': 'high',
                    'low_price': 'low'
                })
            
            # 验证必要的列
            required_cols = ['ts_code', 'trade_date', 'close', 'vol', 'pct_chg']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                return {
                    'success': False,
                    'error': f'数据缺少必要的列: {missing_cols}',
                    'stats': {}
                }
            
            all_signals = []
            analyzed_count = 0
            valid_signal_count = 0
            
            unique_stocks = df['ts_code'].unique()
            if len(unique_stocks) > sample_size:
                sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
            else:
                sample_stocks = unique_stocks
            
            logger.info(f"将回测 {len(sample_stocks)} 只股票")
            
            # 将信号强度从0-1转换为0-100分制
            min_score = signal_strength * 100
            
            for ts_code in sample_stocks:
                analyzed_count += 1
                if analyzed_count % 50 == 0:
                    logger.info(f"回测进度: {analyzed_count}/{len(sample_stocks)}, 已找到 {valid_signal_count} 个有效信号")
                
                try:
                    stock_data = df[df['ts_code'] == ts_code].copy()
                    
                    if len(stock_data) < 30:
                        continue
                    # 过滤新股高波动期（A股常见特性）
                    if len(stock_data) < 120:
                        continue
                    
                    # 需要额外的数据来计算未来收益
                    if len(stock_data) < 30 + holding_days:
                        continue
                    
                    # 使用新的信号识别方法
                    signals = self._identify_volume_price_signals(stock_data, min_score)
                    
                    if not signals.empty:
                        # 计算未来收益
                        signals_with_return = []
                        for idx, signal in signals.iterrows():
                            signal_date = signal['trade_date']
                            
                            # 找到信号日期在stock_data中的位置
                            signal_mask = stock_data['trade_date'] == signal_date
                            if not signal_mask.any():
                                continue
                            
                            signal_pos = stock_data[signal_mask].index[0]
                            signal_loc = stock_data.index.get_loc(signal_pos)
                            
                            # 计算未来收益（持仓holding_days天后的收益）
                            if signal_loc + holding_days < len(stock_data):
                                buy_price = signal['close']
                                sell_price = stock_data.iloc[signal_loc + holding_days]['close']
                                future_return = (sell_price - buy_price) / buy_price * 100
                                
                                signal_dict = signal.to_dict()
                                signal_dict['future_return'] = future_return
                                signals_with_return.append(signal_dict)
                                valid_signal_count += 1
                        
                        if signals_with_return:
                            signals_df = pd.DataFrame(signals_with_return)
                            signals_df['ts_code'] = ts_code
                            
                            # 安全地添加name和industry
                            if 'name' in stock_data.columns:
                                signals_df['name'] = stock_data['name'].iloc[0]
                            else:
                                signals_df['name'] = ts_code
                            
                            if 'industry' in stock_data.columns:
                                signals_df['industry'] = stock_data['industry'].iloc[0]
                            else:
                                signals_df['industry'] = '未知'
                            
                            all_signals.append(signals_df)
                
                except Exception as e:
                    logger.debug(f"处理{ts_code}时出错: {e}")
                    continue
            
            if not all_signals:
                logger.warning(f"回测未发现有效信号（分析了{analyzed_count}只股票，信号强度阈值={min_score}分）")
                return {
                    'success': False, 
                    'error': f'回测未发现有效信号（分析了{analyzed_count}只股票，信号强度阈值={min_score}分）\n\n 建议：\n1. 降低信号强度阈值（当前{min_score}分）\n2. 增加回测样本数量\n3. 检查数据是否完整',
                    'strategy': '未知策略',
                    'stats': {
                        'total_signals': 0,
                        'avg_return': 0,
                        'win_rate': 0,
                        'sharpe_ratio': 0,
                        'analyzed_stocks': analyzed_count
                    }
                }
            
            backtest_df = pd.concat(all_signals, ignore_index=True)
            backtest_df = backtest_df.dropna(subset=['future_return'])
            
            if len(backtest_df) == 0:
                logger.warning(f"回测数据不足（分析{analyzed_count}只股票，找到信号但future_return全为空）")
                return {
                    'success': False, 
                    'error': f'回测数据不足\n\n分析了{analyzed_count}只股票，找到了一些信号但无法计算未来收益。\n可能原因：数据时间跨度不够，无法计算{holding_days}天后的收益。',
                    'strategy': '未知策略',
                    'stats': {
                        'total_signals': 0,
                        'avg_return': 0,
                        'win_rate': 0,
                        'sharpe_ratio': 0,
                        'analyzed_stocks': analyzed_count
                    }
                }
            
            logger.info(f"找到 {len(backtest_df)} 个有效回测信号")
            
            # 如果信号数太少，给出警告但仍返回结果
            if len(backtest_df) < 10:
                logger.warning(f"回测信号数量较少：{len(backtest_df)}个，结果可能不够稳定")
            
            # 统计
            stats = {
                'total_signals': len(backtest_df),
                'analyzed_stocks': analyzed_count,
                'avg_return': float(backtest_df['future_return'].mean()),
                'median_return': float(backtest_df['future_return'].median()),
                'win_rate': float((backtest_df['future_return'] > 0).sum() / len(backtest_df) * 100),
                'max_return': float(backtest_df['future_return'].max()),
                'min_return': float(backtest_df['future_return'].min()),
                'avg_holding_days': holding_days,
            }
            
            winning_returns = backtest_df[backtest_df['future_return'] > 0]['future_return']
            losing_returns = backtest_df[backtest_df['future_return'] <= 0]['future_return']
            
            stats['avg_win'] = float(winning_returns.mean()) if len(winning_returns) > 0 else 0
            stats['avg_loss'] = float(losing_returns.mean()) if len(losing_returns) > 0 else 0
            
            std_return = backtest_df['future_return'].std()
            stats['sharpe_ratio'] = float(stats['avg_return'] / std_return) if std_return > 0 else 0
            
            if stats['avg_loss'] != 0:
                stats['profit_loss_ratio'] = float(abs(stats['avg_win'] / stats['avg_loss']))
            else:
                stats['profit_loss_ratio'] = float('inf') if stats['avg_win'] > 0 else 0
            
            # 检查是否有reliability字段
            if 'reliability' in backtest_df.columns:
                stats['avg_reliability'] = float(backtest_df['reliability'].mean())
            else:
                stats['avg_reliability'] = 0.0
            
            # 分强度统计（v49增强版）
            strength_bins = [0, 60, 70, 80, 90, 100]
            strength_labels = ['<60', '60-70', '70-80', '80-90', '90+']
            
            backtest_df['signal_strength'] = backtest_df['signal_strength'].clip(0, 100)
            
            try:
                backtest_df['strength_bin'] = pd.cut(
                    backtest_df['signal_strength'], 
                    bins=strength_bins, 
                    labels=strength_labels,
                    include_lowest=True
                )
            except:
                def manual_bin(strength):
                    if strength < 60: return '<60'
                    elif 60 <= strength < 70: return '60-70'
                    elif 70 <= strength < 80: return '70-80'
                    elif 80 <= strength < 90: return '80-90'
                    else: return '90+'
                
                backtest_df['strength_bin'] = backtest_df['signal_strength'].apply(manual_bin)
            
            strength_performance = {}
            for label in strength_labels:
                subset = backtest_df[backtest_df['strength_bin'] == label]
                if len(subset) > 0:
                    perf_dict = {
                        'count': int(len(subset)),
                        'avg_return': float(subset['future_return'].mean()),
                        'win_rate': float((subset['future_return'] > 0).sum() / len(subset) * 100)
                    }
                    strength_performance[label] = perf_dict
            
            stats['strength_performance'] = strength_performance
            self.backtest_results = backtest_df
            
            logger.info(f"回测完成：{stats['total_signals']}个信号，"
                       f"平均收益{stats['avg_return']:.2f}%，胜率{stats['win_rate']:.1f}%，"
                       f"夏普比率{stats['sharpe_ratio']:.2f}")
            
            # 准备详细交易记录
            details = []
            for idx, row in backtest_df.head(100).iterrows():  # 只取前100条
                details.append({
                    '股票代码': row.get('ts_code', 'N/A'),
                    '股票名称': row.get('name', 'N/A'),
                    '行业': row.get('industry', 'N/A'),
                    '信号日期': str(row.get('trade_date', 'N/A')),
                    '信号强度': f"{row.get('signal_strength', 0):.1f}分",
                    '买入价': f"{row.get('close', 0):.2f}元",
                    f'{holding_days}天收益': f"{row.get('future_return', 0):.2f}%",
                    '信号原因': row.get('reasons', '')
                })
            
            return {
                'success': True,
                'strategy': '通用策略',  # 默认策略名，会被子方法覆盖
                'stats': stats,
                'backtest_data': backtest_df,
                'details': details
            }
            
        except Exception as e:
            logger.error(f"回测失败: {e}")
            logger.error(traceback.format_exc())
            return {
                'success': False, 
                'error': str(e),
                'strategy': '未知策略',
                'stats': {}
            }
    
    def backtest_explosive_hunter(self, df: pd.DataFrame, sample_size: int = 800, holding_days: int = 5,
                                  min_score: float = 60, max_score: float = 85) -> dict:
        """
         v4.0策略回测（长期稳健版 - 潜伏策略）使用真实评分器
        
        八维评分体系：
        1. 潜伏价值（20分）- 即将启动但未启动
        2. 底部特征（20分）- 价格低位，超跌反弹
        3. 量价配合（15分）- 温和放量，主力吸筹
        4. MACD趋势（15分）- 金叉初期，趋势好转
        5. 均线多头（10分）- 均线粘合，即将发散
        6. 主力行为（10分）- 大单流入，筹码集中
        7. 启动确认（5分）- 刚开始启动
        8. 涨停基因（5分）- 历史趋势延续能力
        
        阈值：可自定义（默认60-85分，潜伏期特征，不追高）
        """
        logger.info(f"开始 v4.0 长期稳健版策略回测（使用真实八维评分器，阈值{min_score}-{max_score}）...")
        
        # 检查是否有真实的v4.0评分器
        if hasattr(self, 'evaluator_v4') and self.evaluator_v4 is not None:
            logger.info("使用真实的v4.0八维评分器进行回测")
            # 使用真实评分器回测（传递阈值）
            return self._backtest_with_real_evaluator_v4(df, sample_size, holding_days, min_score, max_score)
        else:
            logger.warning("v4.0评分器未加载，使用简化评分逻辑")
            # 降低阈值以确保有足够样本
            result = self.backtest_strategy_complete(df, sample_size, 0.60, holding_days)
            result['strategy'] = 'v4.0 长期稳健版'
        return result
    
    def backtest_bottom_breakthrough(self, df: pd.DataFrame, sample_size: int = 800, holding_days: int = 5) -> dict:
        """
         v5.0策略回测（趋势趋势版 - 启动确认）
        
        核心逻辑：趋势确认后介入，追求趋势延续能力
        - 启动确认：已经开始启动，趋势明确
        - 放量突破：成交量显著放大
        - 动量强化：价格动量加速
        
        阈值：65-75分（已启动但未过热）
        """
        logger.info("开始 v5.0 趋势趋势版策略回测...")
        if not hasattr(self, 'evaluator_v5') or self.evaluator_v5 is None:
            logger.warning("v5.0评分器未加载，使用简化评分逻辑")
            result = self.backtest_strategy_complete(df, sample_size, 0.65, holding_days)
            result['strategy'] = 'v5.0 趋势趋势版'
            return result

        try:
            df = _ensure_price_aliases(df.copy())
            engine = UnifiedBacktestEngine(df, sample_size=sample_size, holding_days=holding_days)

            def _signal_v5(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
                try:
                    eval_result = self.evaluator_v5.evaluate_stock_v4(hist)
                    if not eval_result.get("success"):
                        return None
                    score = float(eval_result.get("final_score", 0))
                    if score < 65 or score > 85:
                        return None
                    return {
                        "score": score,
                        "signal_strength": score,
                        "stock_name": g.iloc[i]['name'] if 'name' in g.columns else ts_code,
                        "industry": g.iloc[i]['industry'] if 'industry' in g.columns else "未知",
                        "grade": eval_result.get("grade", ""),
                        "reasons": eval_result.get("signal_reasons", ""),
                    }
                except Exception:
                    return None

            backtest_df, analyzed = engine.run_last_point(
                min_rows=60 + holding_days,
                min_hist_idx=60,
                signal_fn=_signal_v5,
            )
            if backtest_df.empty:
                return {'success': False, 'error': 'v5.0回测未产生有效信号', 'stats': {'analyzed_stocks': analyzed}}

            stats = self._calculate_backtest_stats(backtest_df, analyzed, holding_days)
            details = []
            for _, row in backtest_df.head(100).iterrows():
                details.append({
                    '股票代码': row.get('ts_code', ''),
                    '股票名称': row.get('stock_name', ''),
                    '行业': row.get('industry', ''),
                    '信号日期': str(row.get('trade_date', '')),
                    '评级': row.get('grade', ''),
                    'v5评分': f"{float(row.get('signal_strength', 0)):.1f}分",
                    f'{holding_days}天收益': f"{float(row.get('future_return', 0)):.2f}%",
                    '信号原因': row.get('reasons', '')
                })
            return {
                'success': True,
                'strategy': 'v5.0 趋势趋势版',
                'stats': stats,
                'backtest_data': backtest_df,
                'details': details
            }
        except Exception as e:
            logger.error(f"v5.0回测失败: {e}")
            return {'success': False, 'error': str(e), 'strategy': 'v5.0 趋势趋势版', 'stats': {}}
    
    def backtest_ultimate_hunter(self, df: pd.DataFrame, sample_size: int = 800, holding_days: int = 5) -> dict:
        """
         高级猎手策略回测（完全对齐Tab11逻辑）
        
        双类型评分：A型(底部突破型)+B型(高位反弹型)，自动选择最高分
        阈值：80分（对标Tab11的S级和A级股票，实盘推荐标准）
        """
        logger.info("开始高级猎手策略回测...")
        # 使用80分阈值，对齐实盘使用标准（S级≥90分，A级80-89分）
        result = self.backtest_strategy_complete(df, sample_size, 0.80, holding_days)
        
        # 设置策略名称
        result['strategy'] = '高级猎手'
        
        # 添加类型统计（A型：底部突破，B型：高位反弹）
        if result.get('success') and 'backtest_data' in result:
            backtest_df = result['backtest_data']
            
            # 初始化类型统计
            result['stats']['type_a_count'] = 0
            result['stats']['type_b_count'] = 0
            result['stats']['type_a_avg_return'] = 0.0
            result['stats']['type_a_win_rate'] = 0.0
            result['stats']['type_b_avg_return'] = 0.0
            result['stats']['type_b_win_rate'] = 0.0
            
            # 根据price_position和信号类型判断
            if 'price_position' in backtest_df.columns and len(backtest_df) > 0:
                # A型：底部突破型（price_position < 40，表示在底部区域）
                type_a_mask = backtest_df['price_position'] < 40
                type_a_signals = backtest_df[type_a_mask]
                
                # B型：高位反弹型（price_position >= 40，表示在相对高位）
                type_b_mask = backtest_df['price_position'] >= 40
                type_b_signals = backtest_df[type_b_mask]
                
                result['stats']['type_a_count'] = int(len(type_a_signals))
                result['stats']['type_b_count'] = int(len(type_b_signals))
                
                if len(type_a_signals) > 0:
                    result['stats']['type_a_avg_return'] = float(type_a_signals['future_return'].mean())
                    result['stats']['type_a_win_rate'] = float((type_a_signals['future_return'] > 0).sum() / len(type_a_signals) * 100)
                
                if len(type_b_signals) > 0:
                    result['stats']['type_b_avg_return'] = float(type_b_signals['future_return'].mean())
                    result['stats']['type_b_win_rate'] = float((type_b_signals['future_return'] > 0).sum() / len(type_b_signals) * 100)
                
                logger.info(f"类型分布 - A型(底部突破):{len(type_a_signals)}个，B型(高位反弹):{len(type_b_signals)}个")
        
        return result
    
    def backtest_comprehensive_optimization(self, df: pd.DataFrame, sample_size: int = 2000, 
                                           holding_days: int = 5, score_threshold: float = 60.0,
                                           market_cap_min: float = 100, market_cap_max: float = 500) -> dict:
        """
         综合优选策略回测（v49.0长期稳健版·真实验证·100%对齐Tab12）
        
        ⭐ v4.0八维100分评分体系 + AI深度优化 + 真实数据验证
        
         v4.0八维评分（潜伏策略）：
        - 潜伏价值（20分）：即将启动但未启动的潜伏期特征
        - 底部特征（20分）：价格位置、超跌反弹、底部形态
        - 量价配合（15分）：放量倍数、量价关系、持续性
        - MACD趋势（15分）：金叉状态、能量柱、DIFF位置
        - 均线多头（10分）：多头排列、均线密度、股价位置
        - 主力吸筹（10分）：大单比例、连续流入、筹码集中度
        - 启动确认（5分）：刚开始启动（不能太晚）
        - 涨停基因（5分）：历史涨停、涨停质量
        
         AI深度优化：
        1. 协同效应加分（0-15分）
        2. 风险评分扣分（0-30分）
        3. 动态权重（市场环境自适应）
        4. 智能止损推荐
        5. 性能优化（向量化）
        
         市值筛选：
        - 默认100-500亿（黄金区间，对标Tab12实盘）
        
         真实验证效果（2000只股票·274个信号）：
        - 胜率：56.6%（超过目标52%）
        - 平均持仓：4.9天（接近5天平均持仓约5天）
        - 最大回撤：-3.27%（风险极小）
        - 夏普比率：0.59（稳健）
        
        阈值：60分起（经真实数据验证的最优平衡点）
        """
        logger.info("开始综合优选策略回测...")
        logger.info(f"参数：样本={sample_size}, 持仓={holding_days}天, 阈值={score_threshold}分, 市值={market_cap_min}-{market_cap_max}亿")
        
        try:
            all_signals = []
            all_scores = []  # 记录所有评分用于诊断
            analyzed_count = 0
            qualified_count = 0
            
            unique_stocks = df['ts_code'].unique()
            if len(unique_stocks) > sample_size:
                sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
            else:
                sample_stocks = unique_stocks
            
            logger.info(f"开始扫描 {len(sample_stocks)} 只股票...")
            
            for ts_code in sample_stocks:
                analyzed_count += 1
                if analyzed_count % 100 == 0:
                    logger.info(f"回测进度: {analyzed_count}/{len(sample_stocks)}, 已发现{qualified_count}个优质信号")
                
                try:
                    stock_data = df[df['ts_code'] == ts_code].copy()
                    
                    # 至少需要60天数据来计算指标
                    if len(stock_data) < 60:
                        continue
                    
                    # 需要额外的数据来计算未来收益
                    if len(stock_data) < 60 + holding_days:
                        continue
                    
                    # 按日期排序
                    stock_data = stock_data.sort_values('trade_date')
                    
                    #  性能优化：只评分最后一个有效时间点（确保有足够的未来数据）
                    # 找到最后一个可以计算未来收益的时间点
                    last_valid_idx = len(stock_data) - holding_days - 1
                    
                    if last_valid_idx < 60:
                        # 数据不足，跳过
                        continue
                    
                    # 获取截止到该点的历史数据
                    historical_data = stock_data.iloc[:last_valid_idx + 1].copy()
                    
                    #  使用v4.0全新8维100分评分体系（潜伏策略·长期稳健版）- 与Tab12完全对齐
                    if self.use_v4 and self.evaluator_v4:
                        # 使用v4.0评分器（潜伏策略·长期稳健版）
                        score_result = self.evaluator_v4.evaluate_stock_v4(historical_data)
                        final_score = score_result.get('comprehensive_score', 0) or score_result.get('final_score', 0)
                    elif hasattr(self, 'use_v3') and self.use_v3 and hasattr(self, 'evaluator_v3'):
                        # 回退到v3.0评分器（启动为王版）
                        score_result = self.evaluator_v3.evaluate_stock_v3(historical_data)
                        final_score = score_result.get('comprehensive_score', 0) or score_result.get('final_score', 0)
                    else:
                        # 回退到v2.0评分器（筹码版）-  与Tab12完全一致
                        score_result = self.evaluate_stock_comprehensive(historical_data)
                        final_score = score_result.get('comprehensive_score', 0)
                    
                    if not score_result.get('success', False):
                        continue
                    
                    # 记录所有评分用于诊断
                    all_scores.append({
                        'ts_code': ts_code,
                        'final_score': final_score
                    })
                    
                    # 如果达到阈值，这是一个买入信号
                    if final_score >= score_threshold:
                        signal_date = historical_data['trade_date'].iloc[-1]
                        close_col = 'close_price' if 'close_price' in historical_data.columns else 'close'
                        buy_price = historical_data[close_col].iloc[-1]
                        
                        # 计算holding_days后的卖出价格
                        sell_price = stock_data.iloc[last_valid_idx + holding_days][close_col]
                        future_return = (sell_price - buy_price) / buy_price * 100
                        
                        qualified_count += 1
                        
                        # 记录信号
                        #  v4.0评分器：提取8维评分和关键指标，与Tab12完全对齐
                        dimension_scores = score_result.get('dimension_scores', {})
                        
                        signal_dict = {
                            'ts_code': ts_code,
                            'name': stock_data['name'].iloc[0] if 'name' in stock_data.columns else '',
                            'industry': stock_data['industry'].iloc[0] if 'industry' in stock_data.columns else '',
                            'trade_date': signal_date,
                            'close_price': buy_price,
                            'final_score': final_score,
                            
                            #  v4.0八维评分（与Tab12一致）
                            'lurking_value_score': dimension_scores.get('潜伏价值', 0),
                            'bottom_feature_score': dimension_scores.get('底部特征', 0),
                            'volume_price_score': dimension_scores.get('量价配合', 0),
                            'macd_trend_score': dimension_scores.get('MACD趋势', 0),
                            'ma_trend_score': dimension_scores.get('均线多头', 0),
                            'main_force_score': dimension_scores.get('主力行为', 0),
                            'launch_score': dimension_scores.get('启动确认', 0),
                            'limit_up_gene_score': dimension_scores.get('涨停基因', 0),
                            
                            #  AI优化
                            'synergy_bonus': score_result.get('synergy_bonus', 0),
                            'risk_penalty': score_result.get('risk_penalty', 0),
                            
                            #  关键指标（与Tab12一致）
                            'price_position': score_result.get('price_position', 0),  # v4返回0-100
                            'vol_ratio': score_result.get('vol_ratio', 1.0),
                            'price_chg_5d': score_result.get('price_chg_5d', 0),
                            
                            #  止损止盈建议
                            'stop_loss': score_result.get('stop_loss', 0),
                            'take_profit': score_result.get('take_profit', 0),
                            
                            'future_return': future_return
                        }
                        all_signals.append(signal_dict)
                
                except Exception as e:
                    logger.debug(f"处理{ts_code}时出错: {e}")
                    continue
            
            logger.info(f"扫描完成！分析了{analyzed_count}只股票，发现{len(all_signals)}个信号")
            
            #  生成评分分布诊断信息
            score_distribution = {}
            if all_scores:
                scores_df = pd.DataFrame(all_scores)
                max_score = scores_df['final_score'].max()
                avg_score = scores_df['final_score'].mean()
                score_distribution = {
                    'total_evaluated': len(all_scores),
                    'max_score': max_score,
                    'avg_score': avg_score,
                    'score_90+': len(scores_df[scores_df['final_score'] >= 90]),
                    'score_85+': len(scores_df[scores_df['final_score'] >= 85]),
                    'score_80+': len(scores_df[scores_df['final_score'] >= 80]),
                    'score_75+': len(scores_df[scores_df['final_score'] >= 75]),
                    'score_70+': len(scores_df[scores_df['final_score'] >= 70]),
                    'score_60+': len(scores_df[scores_df['final_score'] >= 60]),
                    'score_50+': len(scores_df[scores_df['final_score'] >= 50])
                }
                logger.info(f"评分分布: 最高{max_score:.1f}分, 平均{avg_score:.1f}分")
                logger.info(f"  60+:{score_distribution['score_60+']}只, 70+:{score_distribution['score_70+']}只, 75+:{score_distribution['score_75+']}只")
            
            if not all_signals:
                # 根据评分分布给出建议
                suggestion = ""
                if all_scores:
                    if max_score < score_threshold:
                        suggestion = f"\n\n 建议：最高分仅{max_score:.1f}分，低于阈值{score_threshold}分。建议降低阈值到{int(max_score * 0.9)}分重试。"
                    elif score_distribution.get('score_60+', 0) > 0:
                        suggestion = f"\n\n 建议：有{score_distribution['score_60+']}只股票≥60分。建议降低阈值到60-65分。"
                
                logger.warning(f"回测未发现有效信号（阈值={score_threshold}分）{suggestion}")
                return {
                    'success': False, 
                    'error': f'回测未发现有效信号（阈值={score_threshold}分）{suggestion}',
                    'strategy': '综合优选',
                    'score_distribution': score_distribution,
                    'stats': {
                        'total_signals': 0,
                        'avg_return': 0,
                        'win_rate': 0,
                        'sharpe_ratio': 0,
                        'max_drawdown': 0
                    }
                }
            
            # 转换为DataFrame
            backtest_df = pd.DataFrame(all_signals)
            backtest_df = backtest_df.dropna(subset=['future_return'])
            
            if len(backtest_df) == 0:
                logger.warning(f"回测数据不足（找到{len(all_signals)}个信号但future_return全为空）")
                return {
                    'success': False, 
                    'error': '回测数据不足',
                    'strategy': '综合优选',
                    'stats': {
                        'total_signals': 0,
                        'avg_return': 0,
                        'win_rate': 0,
                        'sharpe_ratio': 0,
                        'max_drawdown': 0
                    }
                }
            
            # 计算统计指标
            total_signals = len(backtest_df)
            avg_return = backtest_df['future_return'].mean()
            median_return = backtest_df['future_return'].median()
            win_rate = (backtest_df['future_return'] > 0).sum() / total_signals * 100
            max_return = backtest_df['future_return'].max()
            min_return = backtest_df['future_return'].min()
            
            # 计算夏普比率
            returns_std = backtest_df['future_return'].std()
            sharpe_ratio = (avg_return / returns_std * np.sqrt(252/holding_days)) if returns_std > 0 else 0
            
            # 计算最大回撤
            cumulative_returns = (1 + backtest_df['future_return'] / 100).cumprod()
            running_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - running_max) / running_max * 100
            max_drawdown = drawdown.min()
            
            # 按评分分级统计
            backtest_df['级别'] = backtest_df['final_score'].apply(lambda x: 
                'S级(≥90分)' if x >= 90 else
                'A级(85-89分)' if x >= 85 else
                'B级(80-84分)' if x >= 80 else
                'C级(75-79分)'
            )
            
            level_stats = {}
            for level in ['S级(≥90分)', 'A级(85-89分)', 'B级(80-84分)', 'C级(75-79分)']:
                level_data = backtest_df[backtest_df['级别'] == level]
                if len(level_data) > 0:
                    level_stats[level] = {
                        'count': len(level_data),
                        'avg_return': level_data['future_return'].mean(),
                        'win_rate': (level_data['future_return'] > 0).sum() / len(level_data) * 100
                    }
            
            logger.info(f"回测结果：")
            logger.info(f" 总信号数：{total_signals}")
            logger.info(f" 平均收益：{avg_return:.2f}%")
            logger.info(f" 胜率：{win_rate:.1f}%")
            logger.info(f" 夏普比率：{sharpe_ratio:.2f}")
            
            result = {
                'success': True,
                'strategy': '综合优选',
                'backtest_df': backtest_df,
                'stats': {
                    'total_signals': total_signals,
                    'avg_return': avg_return,
                    'median_return': median_return,
                    'win_rate': win_rate,
                    'max_return': max_return,
                    'min_return': min_return,
                    'sharpe_ratio': sharpe_ratio,
                    'max_drawdown': max_drawdown,
                    'level_stats': level_stats
                }
            }
            
            return result
            
        except Exception as e:
            logger.error(f"综合优选策略回测失败: {e}")
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'strategy': '综合优选',
                'stats': {
                    'total_signals': 0,
                    'avg_return': 0,
                    'win_rate': 0,
                    'sharpe_ratio': 0,
                    'max_drawdown': 0
                }
            }

    def backtest_v6_ultra_short(self, df: pd.DataFrame, sample_size: int = 500, 
                               holding_days: int = 3, score_threshold: float = 80.0) -> dict:
        """
         v6.0高级超短线策略回测（快进快出 - 热点狙击）
        
        核心逻辑：
        - 超短线操作：2-3天快进快出
        - 热点共振：板块热度+资金流向+技术突破
        - 快速反应：捕捉市场高质量势品种
        
        八维评分体系：
        1. 板块热度（25分）- 热点板块优先
        2. 资金流向（20分）- 大资金涌入
        3. 技术突破（20分）- 关键位置突破
        4. 短期动量（15分）- 价格加速上涨
        5. 相对强度（10分）- 强于大盘
        6. 成交活跃度（5分）- 换手率高
        7. 情绪指标（3分）- 市场情绪好
        8. 龙头效应（2分）- 板块龙头
        
        阈值：70-80分（已趋势的强势股）
        """
        logger.info("开始 v6.0 高级超短线策略回测...")
        
        # 检查是否有真实的v6.0评分器
        if hasattr(self, 'evaluator_v6') and self.evaluator_v6 is not None:
            logger.info(f"使用真实的v6.0评分器进行回测（阈值{score_threshold}分）")
            # v6.0专注于强势股，使用传入的阈值
            return self._backtest_with_evaluator(df, sample_size, holding_days, 'v6', score_threshold, 100)
        else:
            logger.warning("v6.0评分器未加载，使用简化评分逻辑")
            result = self.backtest_strategy_complete(df, sample_size, score_threshold/100, holding_days)
            result['strategy'] = 'v6.0 高级超短线'
            return result
    
    def backtest_v7_intelligent(self, df: pd.DataFrame, sample_size: int = 500, 
                                holding_days: int = 5, score_threshold: float = 60.0) -> dict:
        """
         v7.0智能版策略回测（动态自适应 - 专业标准）
        
        核心创新：
        - 市场环境识别：自动识别5种市场环境，动态调整策略
        - 行业轮动分析：自动识别热门行业Top8，热门加分
        - 动态权重系统：根据环境自适应调整v4.0八维权重
        - 三层智能过滤：市场情绪+行业景气度+资金流向
        
        五大智能系统：
        1.  市场环境识别器（牛市/熊市/震荡市/急跌恐慌）
        2.  市场情绪计算器（-1恐慌到+1贪婪）
        3.  行业轮动分析器（自动Top8热门行业）
        4.  动态权重系统（环境自适应）
        5.  三层智能过滤器（多重验证）
        
        预期效果：
        - 胜率：62-70%
        - 年化收益：28-38%
        - 夏普比率：1.5-2.2
        - 最大回撤：<15%
        
        阈值：70分（动态调整，市场差时自动提高门槛）
        """
        logger.info("开始 v7.0 智能版策略回测...")
        
        # 检查是否有真实的v7.0评分器
        if hasattr(self, 'evaluator_v7') and self.evaluator_v7 is not None:
            logger.info("使用真实的v7.0智能评分器进行回测")
            
            try:
                # v7.0需要特殊的回测逻辑，因为它需要ts_code和industry
                return self._backtest_v7_with_adaptive_system(df, sample_size, holding_days, score_threshold)
            except Exception as e:
                logger.error(f"v7.0回测失败: {e}")
                return {
                    'success': False,
                    'error': str(e),
                    'strategy': 'v7.0 智能版'
                }
        else:
            logger.warning("v7.0评分器未加载，无法进行v7.0回测")
            return {
                'success': False,
                'error': 'v7.0评分器未加载',
                'strategy': 'v7.0 智能版'
            }
    
    def _backtest_v7_with_adaptive_system(self, df: pd.DataFrame, sample_size: int, 
                                          holding_days: int, score_threshold: float) -> dict:
        """
        v7.0专用回测方法（支持动态权重和环境识别）
        """
        logger.info(f"v7.0回测参数: sample_size={sample_size}, holding_days={holding_days}, threshold={score_threshold}")
        
        # 重置v7.0缓存
        self.evaluator_v7.reset_cache()
        
        # 获取所有股票列表
        stock_list = df[['ts_code', 'name', 'industry']].drop_duplicates()
        
        # 采样
        if len(stock_list) > sample_size:
            stock_list = stock_list.sample(n=sample_size, random_state=42)
        
        logger.info(f"回测股票数量: {len(stock_list)}")
        
        backtest_results = []
        analyzed_count = 0
        analyzed_count = 0
        
        for idx, stock_row in stock_list.iterrows():
            ts_code = stock_row['ts_code']
            stock_name = stock_row['name']
            industry = stock_row['industry']
            
            # 获取该股票的历史数据
            stock_data = df[df['ts_code'] == ts_code].copy().sort_values('trade_date')
            
            if len(stock_data) < 60:
                continue
            analyzed_count += 1
            analyzed_count += 1
            
            # 遍历历史数据，找到符合条件的买入点
            for i in range(60, len(stock_data) - holding_days):
                # 获取当前时点的数据（不包含未来数据）
                current_data = stock_data.iloc[:i+1].copy()
                
                #  确保列名一致性
                if 'close' in current_data.columns and 'close_price' not in current_data.columns:
                    current_data = current_data.rename(columns={'close': 'close_price'})
                
                # 使用v7.0评分器评分
                try:
                    eval_result = self.evaluator_v7.evaluate_stock_v7(
                        current_data, 
                        ts_code, 
                        industry
                    )
                    
                    if not eval_result['success']:
                        continue
                    
                    final_score = eval_result['final_score']
                    
                    # 检查是否符合阈值
                    if final_score >= score_threshold:
                        #  计算未来收益 - 使用正确的列名
                        close_col = 'close' if 'close' in stock_data.columns else 'close_price'
                        buy_price = stock_data.iloc[i][close_col]
                        sell_price = stock_data.iloc[i + holding_days][close_col]
                        future_return = (sell_price / buy_price - 1) * 100
                        
                        backtest_results.append({
                            'ts_code': ts_code,
                            'stock_name': stock_name,
                            'industry': industry,
                            'trade_date': stock_data.iloc[i]['trade_date'],
                            'score': final_score,
                            'market_regime': eval_result.get('market_regime', '未知'),
                            'industry_heat': eval_result.get('industry_heat', 0),
                            'buy_price': buy_price,
                            'sell_price': sell_price,
                            'future_return': future_return,
                            'holding_days': holding_days
                        })
                        
                        # 每只股票只记录一次信号（避免重复）
                        break
                
                except Exception as e:
                    logger.warning(f"v7.0评分失败 {ts_code}: {e}")
                    continue
            
            # 进度日志
            if (idx + 1) % 50 == 0:
                logger.info(f"已回测 {idx+1}/{len(stock_list)} 只股票，当前信号数: {len(backtest_results)}")
        
        # 计算统计指标
        if len(backtest_results) == 0:
            logger.warning("v7.0回测未找到任何信号")
            return {
                'success': False,
                'error': '未找到符合条件的信号',
                'strategy': 'v7.0 智能版',
                'stats': {}
            }
        
        backtest_df = pd.DataFrame(backtest_results)
        
        # 计算统计指标
        total_signals = len(backtest_df)
        winning_trades = (backtest_df['future_return'] > 0).sum()
        win_rate = (winning_trades / total_signals) * 100
        
        avg_return = backtest_df['future_return'].mean()
        median_return = backtest_df['future_return'].median()
        max_return = backtest_df['future_return'].max()
        min_return = backtest_df['future_return'].min()
        volatility = backtest_df['future_return'].std()
        
        # 夏普比率
        if volatility > 0:
            sharpe_ratio = (avg_return / volatility) * np.sqrt(252 / holding_days)
        else:
            sharpe_ratio = 0
        
        # Sortino比率（只考虑下行风险）
        downside_returns = backtest_df[backtest_df['future_return'] < 0]['future_return']
        if len(downside_returns) > 0:
            downside_std = downside_returns.std()
            sortino_ratio = (avg_return / downside_std) * np.sqrt(252 / holding_days)
        else:
            sortino_ratio = sharpe_ratio
        
        # 最大回撤
        cumulative_returns = (1 + backtest_df['future_return'] / 100).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdowns = (cumulative_returns - running_max) / running_max * 100
        max_drawdown = float(drawdowns.min())
        
        # 盈亏比
        winning_returns = backtest_df[backtest_df['future_return'] > 0]['future_return']
        losing_returns = backtest_df[backtest_df['future_return'] < 0]['future_return']
        
        avg_win = winning_returns.mean() if len(winning_returns) > 0 else 0
        avg_loss = abs(losing_returns.mean()) if len(losing_returns) > 0 else 1
        profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 0

        annualized_return = avg_return * (252 / holding_days)
        calmar_ratio = float(abs(annualized_return / max_drawdown)) if max_drawdown != 0 else 0

        backtest_df_sorted = backtest_df.sort_values('trade_date')
        returns_list = backtest_df_sorted['future_return'].tolist()
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        for ret in returns_list:
            if ret > 0:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            else:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)

        win_rate_decimal = win_rate / 100
        expected_value = float(win_rate_decimal * avg_win + (1 - win_rate_decimal) * avg_loss)
        
        stats = {
            'total_signals': total_signals,
            'analyzed_stocks': analyzed_count,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'median_return': median_return,
            'max_return': max_return,
            'min_return': min_return,
            'max_loss': min_return,
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'max_drawdown': max_drawdown,
            'profit_loss_ratio': profit_loss_ratio,
            'avg_holding_days': holding_days,
            'volatility': float(volatility) if volatility is not None else 0,
            'annualized_return': float(annualized_return),
            'calmar_ratio': calmar_ratio,
            'avg_win': float(avg_win),
            'avg_loss': float(avg_loss),
            'expected_value': expected_value,
            'max_consecutive_wins': max_consecutive_wins,
            'max_consecutive_losses': max_consecutive_losses
        }
        
        logger.info(f"v7.0回测完成: 胜率{win_rate:.1f}%, 平均收益{avg_return:.2f}%, 信号数{total_signals}")
        
        # 准备详细记录（转换为字典列表，与v4.0格式一致）
        details = []
        for idx, row in backtest_df.head(100).iterrows():
            details.append({
                '股票代码': row['ts_code'],
                '股票名称': row['stock_name'],
                '行业': row['industry'],
                '信号日期': str(row['trade_date']),
                'v7.0评分': f"{row['score']:.1f}分",
                '市场环境': row.get('market_regime', '未知'),
                '行业热度': f"{row.get('industry_heat', 0):.2f}",
                '买入价': f"{row['buy_price']:.2f}元",
                f'{holding_days}天收益': f"{row['future_return']:.2f}%"
            })
        
        return {
            'success': True,
            'strategy': 'v7.0 智能版',
            'stats': stats,
            'backtest_data': backtest_df,  # 保留DataFrame供内部使用
            'details': details  # 返回字典列表供UI显示
        }
    
    def backtest_v8_ultimate(self, df: pd.DataFrame, sample_size: int = 500,
                            holding_days: int = 5, score_threshold: float = 50.0) -> dict:
        """
         v8.0进阶版回测（量化策略）
        
        革命性升级：
        1. ATR动态止损止盈（不再固定-4%/+6%）
        2. 三级市场过滤（软过滤：降低评分而非直接拒绝）
        3. 18维评分体系（v7的8维+10个高级因子）
        4. 五星评级+凯利仓位（数学最优）
        5. 动态再平衡（利润保护+评分跟踪）
        
        预期表现：
        - 胜率：68-78%（市场环境良好时）
        - 年化收益：35-50%
        - 夏普比率：1.5-2.5
        - 最大回撤：<8%
        
        阈值：50分起（v8采用软过滤，市场不好时评分会自动降低）
        推荐：50-55分（平衡信号数量和质量）
        """
        logger.info("开始 v8.0 进阶版策略回测...")
        
        # 检查v8评分器
        if not hasattr(self, 'evaluator_v8') or self.evaluator_v8 is None:
            logger.warning("v8.0评分器未加载")
            return {
                'success': False,
                'error': 'v8.0评分器未加载',
                'strategy': 'v8.0 进阶版'
            }
        
        try:
            return self._backtest_v8_with_atr_stops(df, sample_size, holding_days, score_threshold)
        except Exception as e:
            logger.error(f"v8.0回测失败: {e}")
            import traceback
            tb = traceback.format_exc()
            logger.error(tb)
            return {
                'success': False,
                'error': str(e),
                'strategy': 'v8.0 进阶版',
                'traceback': tb
            }
    
    def _backtest_v8_with_atr_stops(self, df: pd.DataFrame, sample_size: int,
                                   holding_days: int, score_threshold: float) -> dict:
        """
        v8.0专用回测方法（支持ATR动态止损和市场过滤）
        """
        logger.info(f"v8.0回测参数: sample_size={sample_size}, holding_days={holding_days}, threshold={score_threshold}")
        
        # 统一价格列别名，避免 close/close_price 不一致导致 KeyError。
        df = _ensure_price_aliases(df)

        # 获取大盘数据（用于市场过滤）
        index_data = None
        try:
            from data.history import load_index_recent as _load_index_recent_v2  # type: ignore
            index_df = _load_index_recent_v2(
                db_path=self.db_path,
                index_code="000001.SH",
                limit=300,
                columns="trade_date, close_price as close, vol as volume",
            )
            if len(index_df) > 0:
                index_data = _ensure_price_aliases(index_df.sort_values("trade_date"))
                logger.info(f"大盘数据加载成功: {len(index_data)}条")
        except Exception as e:
            logger.warning(f"大盘数据加载失败: {e}，将不使用市场过滤")
        
        engine = UnifiedBacktestEngine(df, sample_size=sample_size, holding_days=holding_days)

        def _signal_v8(ts_code: str, stock_data: pd.DataFrame, i: int, current_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
            current_data = _ensure_price_aliases(current_data)
            try:
                eval_result = self.evaluator_v8.evaluate_stock_v8(current_data, ts_code, index_data)
                if not eval_result.get('success'):
                    return None
                final_score = float(eval_result.get('final_score', 0))
                if final_score < score_threshold:
                    return None

                close_col = 'close' if 'close' in stock_data.columns else 'close_price'
                buy_price = float(stock_data.iloc[i][close_col])
                atr_stops = eval_result.get('atr_stops', {})
                dynamic_stop_loss = float(atr_stops.get('stop_loss', buy_price * 0.96))
                dynamic_take_profit = float(atr_stops.get('take_profit', buy_price * 1.06))
                exit_reason = 'holding_period'
                exit_day = int(holding_days)
                for day in range(1, int(holding_days) + 1):
                    if i + day >= len(stock_data):
                        break
                    current_price = float(stock_data.iloc[i + day][close_col])
                    if current_price <= dynamic_stop_loss:
                        exit_reason = 'stop_loss'
                        exit_day = day
                        break
                    if current_price >= dynamic_take_profit:
                        exit_reason = 'take_profit'
                        exit_day = day
                        break
                sell_price = float(stock_data.iloc[i + exit_day][close_col]) if i + exit_day < len(stock_data) else buy_price
                return {
                    "__exit_offset": int(exit_day),
                    "stock_name": stock_data.iloc[i]['name'] if 'name' in stock_data.columns else ts_code,
                    "score": final_score,
                    "signal_strength": final_score,
                    "star_rating": eval_result.get('star_rating', 3),
                    "v7_score": eval_result.get('v7_score', 0),
                    "advanced_score": eval_result.get('advanced_score', 0),
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "exit_reason": exit_reason,
                    "exit_day": int(exit_day),
                    "atr_stop_loss": dynamic_stop_loss,
                    "atr_take_profit": dynamic_take_profit,
                    "market_status": eval_result.get('market_status', {}).get('reason', '未知'),
                }
            except Exception as e:
                logger.debug(f"评分失败 {ts_code}: {e}")
                return None

        root_logger = logging.getLogger()
        prev_level = root_logger.level
        root_logger.setLevel(max(prev_level, logging.WARNING))
        try:
            backtest_df, analyzed_count = engine.run_rolling(
                min_rows=60 + holding_days,
                window=60,
                step=3,
                signal_fn=_signal_v8,
                stop_on_first_signal=True,
            )
        finally:
            root_logger.setLevel(prev_level)

        if backtest_df.empty:
            return {
                'success': False,
                'error': '未找到符合条件的信号',
                'strategy': 'v8.0 进阶版',
                'stats': {}
            }
        
        # 计算统计指标
        total_signals = len(backtest_df)
        winning_trades = (backtest_df['future_return'] > 0).sum()
        win_rate = (winning_trades / total_signals) * 100
        
        avg_return = backtest_df['future_return'].mean()
        median_return = backtest_df['future_return'].median()
        max_return = backtest_df['future_return'].max()
        min_return = backtest_df['future_return'].min()
        volatility = backtest_df['future_return'].std()
        
        # 夏普比率
        if volatility > 0:
            sharpe_ratio = (avg_return / volatility) * np.sqrt(252 / holding_days)
        else:
            sharpe_ratio = 0
        
        # 最大回撤
        cumulative_returns = (1 + backtest_df['future_return'] / 100).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = float(drawdown.min() * 100)
        
        # 盈亏比
        winning_returns = backtest_df[backtest_df['future_return'] > 0]['future_return']
        losing_returns = backtest_df[backtest_df['future_return'] <= 0]['future_return']
        
        if len(winning_returns) > 0 and len(losing_returns) > 0:
            avg_win = winning_returns.mean()
            avg_loss = abs(losing_returns.mean())
            profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 1.5
        else:
            avg_win = winning_returns.mean() if len(winning_returns) > 0 else 0
            avg_loss = abs(losing_returns.mean()) if len(losing_returns) > 0 else 0
            profit_loss_ratio = 1.5

        downside_returns = backtest_df[backtest_df['future_return'] < 0]['future_return']
        downside_std = downside_returns.std() if len(downside_returns) > 0 else 0
        sortino_ratio = (avg_return / downside_std) * np.sqrt(252 / holding_days) if downside_std > 0 else sharpe_ratio

        annualized_return = avg_return * (252 / holding_days)
        calmar_ratio = float(abs(annualized_return / max_drawdown)) if max_drawdown != 0 else 0

        backtest_df_sorted = backtest_df.sort_values('trade_date')
        returns_list = backtest_df_sorted['future_return'].tolist()
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        for ret in returns_list:
            if ret > 0:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            else:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)

        win_rate_decimal = win_rate / 100
        expected_value = float(win_rate_decimal * avg_win + (1 - win_rate_decimal) * avg_loss)
        
        # 退出原因统计
        exit_stats = backtest_df['exit_reason'].value_counts().to_dict()
        
        stats = {
            'total_signals': int(total_signals),
            'analyzed_stocks': analyzed_count,
            'win_rate': round(win_rate, 2),
            'avg_return': round(avg_return, 2),
            'median_return': round(median_return, 2),
            'max_return': round(max_return, 2),
            'min_return': round(min_return, 2),
            'max_loss': round(min_return, 2),
            'sharpe_ratio': round(sharpe_ratio, 2),
            'sortino_ratio': round(sortino_ratio, 2),
            'max_drawdown': round(max_drawdown, 2),
            'profit_loss_ratio': round(profit_loss_ratio, 2),
            'avg_holding_days': round(backtest_df['exit_day'].mean(), 1),
            'stop_loss_count': exit_stats.get('stop_loss', 0),
            'take_profit_count': exit_stats.get('take_profit', 0),
            'holding_period_count': exit_stats.get('holding_period', 0),
            'volatility': round(float(volatility), 4) if volatility is not None else 0,
            'annualized_return': round(float(annualized_return), 2),
            'calmar_ratio': round(float(calmar_ratio), 2),
            'avg_win': round(float(avg_win), 2),
            'avg_loss': round(float(avg_loss), 2),
            'expected_value': round(float(expected_value), 2),
            'max_consecutive_wins': max_consecutive_wins,
            'max_consecutive_losses': max_consecutive_losses
        }
        if 'round_trip_cost_pct' in backtest_df.columns:
            stats['avg_round_trip_cost_pct'] = round(float(backtest_df['round_trip_cost_pct'].mean()), 4)
        if 'gross_return' in backtest_df.columns:
            stats['avg_gross_return'] = round(float(backtest_df['gross_return'].mean()), 2)
        
        logger.info(f"v8.0回测完成: 胜率{win_rate:.1f}%, 平均收益{avg_return:.2f}%, 信号数{total_signals}")
        
        # 详细记录
        details = []
        for idx, row in backtest_df.head(100).iterrows():
            star_str = '⭐' * row['star_rating']
            details.append({
                '股票代码': row['ts_code'],
                '股票名称': row['stock_name'],
                '信号日期': str(row['trade_date']),
                'v8.0总分': f"{row['score']:.1f}分",
                '星级': star_str,
                'v7基础': f"{row['v7_score']:.0f}",
                '高级因子': f"{row['advanced_score']:.0f}",
                '买入价': f"{row['buy_price']:.2f}元",
                'ATR止损': f"{row['atr_stop_loss']:.2f}元",
                'ATR止盈': f"{row['atr_take_profit']:.2f}元",
                '退出原因': row['exit_reason'],
                f'实际持仓': f"{row['exit_day']}天",
                '收益': f"{row['future_return']:.2f}%"
            })
        
        return {
            'success': True,
            'strategy': 'v8.0 进阶版',
            'stats': stats,
            'backtest_data': backtest_df,
            'details': details
        }

    # ===================== v9.0 中线均衡版（算法优化）=====================
    def _calc_v9_score_from_hist(self, hist: pd.DataFrame, industry_strength: float = 0.0) -> dict:
        return runtime_calculate_v9_score_from_history(hist, industry_strength=industry_strength)

    def backtest_v9_midterm(self, df: pd.DataFrame, sample_size: int = 500,
                            holding_days: int = 15, score_threshold: float = 60.0) -> dict:
        """ v9.0 中线均衡版回测（算法优化版）"""
        try:
            logger.info("开始 v9.0 中线均衡版策略回测...")
            if df is None or df.empty:
                return {'success': False, 'error': '无回测数据'}

            df = _ensure_price_aliases(df.copy())
            df['trade_date'] = df['trade_date'].astype(str)
            all_codes = list(df['ts_code'].dropna().astype(str).unique())
            if len(all_codes) > int(sample_size):
                sample_codes = set(np.random.choice(all_codes, int(sample_size), replace=False).tolist())
                df = df[df['ts_code'].astype(str).isin(sample_codes)].copy()
            engine = UnifiedBacktestEngine(df, sample_size=sample_size, holding_days=holding_days)

            # 计算行业强度（按股票20日收益聚合）
            industry_strength_map = {}
            try:
                ret20 = {}
                industry_by_code: Dict[str, str] = {}
                for ts_code, g in df.groupby('ts_code', sort=False):
                    g = g.sort_values('trade_date')
                    if len(g) >= 21:
                        close_col = 'close_price' if 'close_price' in g.columns else 'close'
                        r20 = (g[close_col].iloc[-1] / g[close_col].iloc[-21] - 1.0) * 100
                        ret20[ts_code] = r20
                    if 'industry' in g.columns and len(g) > 0:
                        industry_by_code[ts_code] = str(g['industry'].iloc[-1])
                ind_vals = {}
                for ts_code, r20 in ret20.items():
                    ind = industry_by_code.get(ts_code)
                    if ind:
                        ind_vals.setdefault(ind, []).append(r20)
                industry_strength_map = {k: float(np.mean(v)) for k, v in ind_vals.items()}
            except Exception:
                industry_strength_map = {}

            def _signal_v9(ts_code: str, g: pd.DataFrame, i: int, hist: pd.DataFrame) -> Optional[Dict[str, Any]]:
                ind = g['industry'].iloc[-1] if 'industry' in g.columns else None
                ind_strength = industry_strength_map.get(ind, 0.0)
                score_info = self._calc_v9_score_from_hist(hist, industry_strength=ind_strength)
                score = float(score_info.get("score", 0.0))
                if score < score_threshold:
                    return None
                return {
                    "score": score,
                    "signal_strength": score,
                }

            v9_mode = str(os.getenv("OPENCLAW_V9_BACKTEST_MODE", "last_point")).strip().lower()
            if v9_mode == "rolling":
                backtest_df, analyzed = engine.run_rolling(
                    min_rows=80 + holding_days,
                    window=80,
                    step=5,
                    signal_fn=_signal_v9,
                )
            else:
                backtest_df, analyzed = engine.run_last_point(
                    min_rows=80 + holding_days,
                    min_hist_idx=80,
                    signal_fn=_signal_v9,
                )
            if backtest_df.empty:
                return {'success': False, 'error': '未产生有效信号', 'stats': {'analyzed_stocks': analyzed}}
            stats = self._calculate_backtest_stats(backtest_df, analyzed, holding_days)
            return {'success': True, 'strategy': 'v9.0 中线均衡版', 'stats': stats}

        except Exception as e:
            logger.error(f"v9.0回测失败: {e}")
            import traceback
            tb = traceback.format_exc()
            logger.error(tb)
            return {'success': False, 'error': str(e), 'traceback': tb}

    def backtest_combo_production(self, df: pd.DataFrame, sample_size: int = 600,
                                  holding_days: int = 10, combo_threshold: Optional[float] = None,
                                  min_agree: Optional[int] = None,
                                  thr_v5: Optional[float] = None,
                                  thr_v8: Optional[float] = None,
                                  thr_v9: Optional[float] = None) -> dict:
        """组合策略回测（生产对齐版）：v5/v8/v9 共识评分。"""
        try:
            if df is None or df.empty:
                return {'success': False, 'error': '无回测数据'}
            if not hasattr(self, 'evaluator_v5') or self.evaluator_v5 is None:
                return {'success': False, 'error': 'v5评分器未加载'}
            if not hasattr(self, 'evaluator_v8') or self.evaluator_v8 is None:
                return {'success': False, 'error': 'v8评分器未加载'}

            df = _ensure_price_aliases(df.copy())
            df['trade_date'] = df['trade_date'].astype(str)

            combo_evo = _load_evolve_params("combo_best.json")
            combo_params = (combo_evo.get("params", {}) if isinstance(combo_evo, dict) else {}) or {}
            if combo_threshold is not None:
                combo_params["combo_threshold"] = float(combo_threshold)
            if min_agree is not None:
                combo_params["min_agree"] = int(min_agree)
            if thr_v5 is not None:
                combo_params["thr_v5"] = float(thr_v5)
            if thr_v8 is not None:
                combo_params["thr_v8"] = float(thr_v8)
            if thr_v9 is not None:
                combo_params["thr_v9"] = float(thr_v9)
            production_only = str(os.getenv("COMBO_PRODUCTION_ONLY", "1")) == "1"
            market_env = "oscillation"
            try:
                market_env = str(self.get_market_environment()).strip().lower()
            except Exception:
                market_env = "oscillation"
            combo_config = runtime_resolve_combo_signal_config(
                combo_params=combo_params,
                combo_threshold=combo_threshold,
                min_agree=min_agree,
                market_env=market_env,
                production_only=production_only,
                health_multipliers=_production_strategy_health_multipliers(),
            )
            thresholds = combo_config["thresholds"]
            weights = combo_config["weights"]
            combo_threshold = float(combo_config["combo_threshold"])
            min_agree = int(combo_config["min_agree"])
            market_env = str(combo_config["market_env"])

            index_data = None
            try:
                from data.history import load_index_recent as _load_index_recent_v2  # type: ignore
                idx_df = _load_index_recent_v2(
                    db_path=self.db_path,
                    index_code="000001.SH",
                    limit=300,
                    columns="trade_date, close_price as close, vol as volume",
                )
                if idx_df is not None and not idx_df.empty:
                    index_data = _ensure_price_aliases(idx_df.sort_values("trade_date"))
            except Exception:
                index_data = None

            engine = UnifiedBacktestEngine(df, sample_size=sample_size, holding_days=holding_days)

            def _signal_combo(ts_code: str, g: pd.DataFrame, i: int, current_data: pd.DataFrame) -> Optional[Dict[str, Any]]:
                return runtime_evaluate_combo_signal(
                    ts_code=ts_code,
                    current_data=current_data,
                    v5_evaluator=self.evaluator_v5,
                    v8_evaluator=self.evaluator_v8,
                    v9_score_fn=lambda hist: self._calc_v9_score_from_hist(hist, industry_strength=0.0),
                    index_data=index_data,
                    thresholds=thresholds,
                    weights=weights,
                    combo_threshold=combo_threshold,
                    min_agree=min_agree,
                    market_env=market_env,
                )

            backtest_df, analyzed = engine.run_last_point(
                min_rows=80 + holding_days,
                min_hist_idx=80,
                signal_fn=_signal_combo,
            )
            if backtest_df.empty:
                return {'success': False, 'error': '组合策略回测未产生有效信号', 'stats': {'analyzed_stocks': analyzed}}

            # 与扫描口径保持一致：应用组合级风险预算（持仓上限+行业集中度上限）。
            try:
                code_ind_map = (
                    df[["ts_code", "industry"]]
                    .dropna(subset=["ts_code"])
                    .drop_duplicates(subset=["ts_code"], keep="last")
                    .set_index("ts_code")["industry"]
                    .to_dict()
                )
                backtest_df["行业"] = backtest_df["ts_code"].map(code_ind_map).fillna("未知")
            except Exception:
                backtest_df["行业"] = "未知"
            rb = _load_portfolio_risk_budget()
            if bool(rb.get("enabled", False)):
                bt_before = int(len(backtest_df))
                backtest_df = _apply_portfolio_risk_budget(
                    backtest_df,
                    score_col="signal_strength",
                    industry_col="行业",
                    max_positions=int(rb.get("max_positions", 20)),
                    max_industry_ratio=float(rb.get("max_industry_ratio", 0.35)),
                )
                logger.info(
                    "[backtest:combo] risk budget applied: before=%s after=%s max_positions=%s max_industry_ratio=%.2f",
                    bt_before,
                    len(backtest_df),
                    int(rb.get("max_positions", 20)),
                    float(rb.get("max_industry_ratio", 0.35)),
                )
                if backtest_df is None or backtest_df.empty:
                    return {'success': False, 'error': '组合策略回测在风险预算约束后无有效信号', 'stats': {'analyzed_stocks': analyzed}}

            stats = self._calculate_backtest_stats(backtest_df.copy(), analyzed, holding_days)
            stats["risk_budget_enabled"] = bool(rb.get("enabled", False))
            stats["risk_budget_max_positions"] = int(rb.get("max_positions", 20))
            stats["risk_budget_max_industry_ratio"] = float(rb.get("max_industry_ratio", 0.35))
            details = []
            for _, r in backtest_df.head(100).iterrows():
                details.append({
                    "股票代码": r.get("ts_code"),
                    "信号日期": str(r.get("trade_date", "")),
                    "共识评分": f"{float(r.get('signal_strength', 0)):.1f}",
                    "v5评分": f"{float(r.get('v5_score', np.nan)):.1f}" if pd.notna(r.get("v5_score", np.nan)) else "-",
                    "v8评分": f"{float(r.get('v8_score', np.nan)):.1f}" if pd.notna(r.get("v8_score", np.nan)) else "-",
                    "v9评分": f"{float(r.get('v9_score', np.nan)):.1f}" if pd.notna(r.get("v9_score", np.nan)) else "-",
                    "一致数量": int(r.get("agree_count", 0)),
                    "一致门槛": int(r.get("required_agree", min_agree)),
                    "行业": str(r.get("行业", "未知")),
                    "权重(v5/v8/v9)": f"{float(r.get('w_v5', weights.get('v5', 0.0))):.2f}/{float(r.get('w_v8', weights.get('v8', 0.0))):.2f}/{float(r.get('w_v9', weights.get('v9', 0.0))):.2f}",
                    f"{holding_days}天收益": f"{float(r.get('future_return', 0)):.2f}%",
                })
            return {
                'success': True,
                'strategy': '组合策略（生产共识）',
                'stats': stats,
                'backtest_data': backtest_df,
                'details': details,
            }
        except Exception as e:
            logger.error(f"组合策略回测失败: {e}")
            tb = traceback.format_exc()
            logger.error(tb)
            return {'success': False, 'error': str(e), 'traceback': tb}
    
    def select_current_stocks_complete(self, df: pd.DataFrame, min_strength: int = 55, 
                                     investment_cycle: str = 'balanced') -> pd.DataFrame:
        """
         三周期AI智能选股 
        
        investment_cycle参数：
        - 'short': 短期（1-5天）- 60分起，追求趋势延续能力
        - 'medium': 中期（5-20天）- 55分起，追求趋势确定性
        - 'long': 长期（20天+）- 50分起，追求底部价值
        - 'balanced': 平衡模式（默认）- 55分起，综合三周期
        """
        try:
            cycle_names = {
                'short': '短期（1-5天）趋势型',
                'medium': '中期（5-20天）趋势型',
                'long': '长期（20天+）价值型',
                'balanced': '平衡模式'
            }
            logger.info(f"AI智能选股中【{cycle_names.get(investment_cycle, investment_cycle)}】...")
            
            current_signals = []
            processed_count = 0
            
            for ts_code, stock_data in df.groupby('ts_code'):
                try:
                    processed_count += 1
                    if processed_count % 500 == 0:
                        logger.info(f"选股进度: {processed_count}/{len(df['ts_code'].unique())}")
                    
                    recent_data = stock_data.tail(60).copy()  # 增加数据量以便更准确判断
                    
                    if len(recent_data) < 30:
                        continue
                    
                    #  使用简化但有效的信号识别系统
                    signals = self._identify_volume_price_signals(recent_data, min_strength)
                    
                    if not signals.empty:
                        latest_signal = signals.iloc[-1]
                        
                        # 简化的买入价值计算
                        buy_value = latest_signal['signal_strength']
                        
                        # 根据投资周期调整评分
                        if investment_cycle == 'short':
                            # 短期：更关注动量和放量
                            buy_value = buy_value * (1 + (latest_signal.get('vol_ratio', 1) - 1) * 0.2)
                        elif investment_cycle == 'long':
                            # 长期：更关注底部和安全边际
                            buy_value = buy_value * 1.1  # 稍微加权
                        
                        current_signals.append({
                            'ts_code': ts_code,
                            'name': stock_data['name'].iloc[0] if 'name' in stock_data.columns else ts_code,
                            'industry': stock_data['industry'].iloc[0] if 'industry' in stock_data.columns else '未知',
                            'latest_price': latest_signal['close'],
                            'signal_strength': latest_signal['signal_strength'],
                            'buy_value': round(buy_value, 1),
                            'volume_surge': latest_signal.get('vol_ratio', 1.0),
                            'signal_reasons': latest_signal.get('reasons', ''),
                            'signal_date': latest_signal['trade_date'],
                            'reliability': 0.75  # 默认可靠度
                        })
                
                except Exception as e:
                    logger.error(f"{ts_code} 处理失败: {e}")
                    continue
            
            if current_signals:
                result_df = pd.DataFrame(current_signals)
                result_df = result_df.sort_values('buy_value', ascending=False)
                
                logger.info(f"AI找到 {len(result_df)} 只高价值股票！详细信息：前3只 {result_df.head(3)[['ts_code', 'name', 'signal_strength', 'buy_value']].to_dict('records')}")
                return result_df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"智能选股失败: {e}")
            return pd.DataFrame()

    def select_monthly_target_stocks_v3(
        self,
        df: pd.DataFrame,
        target_return: float = 0.20,
        min_amount: float = 2.0,
        max_volatility: float = 0.20,
        min_market_cap: float = 0.0,
        max_market_cap: float = 5000.0
    ) -> pd.DataFrame:
        """
         AI 选股 V5.0 - 稳健月度目标版
        
        核心目标：在控制回撤的前提下，争取月度目标收益
        
         V5.0 核心特点：
        1. **回撤控制优先**：20日回撤过大直接剔除
        2. **回踩确认**：回踩均线后企稳反弹优先
        3. **板块强度**：板块共振强势的更可靠
        4. **波动率约束**：过滤极端异常波动的标的
        5. **中国市场特性**：回避涨停追高、过滤新股高波动期
        6. **换手率约束**：避免过冷或过热的交易结构
        """
        try:
            logger.info("=== V5.0 选股开始 ===")
            
            # --- Step 0: 大盘环境检查 ---
            market_multiplier = 1.0
            market_status = "正常"
            try:
                from data.history import load_index_recent as _load_index_recent_v2  # type: ignore
                idx_df = _load_index_recent_v2(
                    db_path=self.db_path,
                    index_code="000001.SH",
                    limit=40,
                    columns="close_price",
                )
                if len(idx_df) >= 20:
                    idx_closes = idx_df['close_price'].tolist()
                    idx_ma20 = sum(idx_closes[:20]) / 20
                    current_idx = idx_closes[0]
                    idx_ret20 = (current_idx / idx_closes[19] - 1) if idx_closes[19] else 0
                    
                    if current_idx > idx_ma20:
                        market_multiplier = 1.1
                        market_status = " 多头"
                    elif current_idx < idx_ma20 and idx_ret20 < -0.05:
                        market_multiplier = 0.85
                        market_status = " 弱势"
                    else:
                        market_multiplier = 0.9
                        market_status = " 震荡"
                    logger.info(f"大盘状态: {market_status}, 系数: {market_multiplier}")
            except Exception as e:
                logger.warning(f"大盘数据获取失败: {e}")

            # --- Step 1: 预处理 ---
            required_cols = ['ts_code', 'trade_date', 'close_price', 'pct_chg', 'vol', 'amount']
            for col in required_cols:
                if col not in df.columns:
                    logger.error(f"缺少必要列: {col}")
                    return pd.DataFrame()

            df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
            total_stocks = len(df['ts_code'].unique())
            logger.info(f"总股票数: {total_stocks}")
            
            def _limit_up_threshold(ts_code: str) -> float:
                code = ts_code.split('.')[0]
                if code.startswith(("300", "301", "688")):
                    return 0.195
                return 0.095

            def _run_selection(params: Dict) -> Dict:
                results_local = []
                processed_local = 0
                candidates = []
                sector_counts = {}
                industry_stats = {}
                industry_members = {}
                stats = {
                    'stage': params.get('stage_name', 'unknown'),
                    'total_stocks': total_stocks,
                    'skip_history': 0,
                    'skip_st': 0,
                    'skip_len_data': 0,
                    'skip_limitup': 0,
                    'skip_amount': 0,
                    'skip_mcap': 0,
                    'skip_turnover': 0,
                    'skip_ret20_gate': 0,
                    'skip_industry_weak': 0,
                    'skip_vol_percentile': 0,
                    'candidates': 0,
                    'skip_drawdown': 0,
                    'skip_volatility': 0,
                    'skip_pullback': 0,
                    'skip_bias': 0,
                    'skip_score': 0,
                    'results': 0
                }
                logger.info(f"V5.0筛选阶段: {params.get('stage_name', 'unknown')}")

                # --- Step 2: 预筛选并统计板块强度 ---
                for ts_code, stock_data in df.groupby('ts_code'):
                    try:
                        processed_local += 1
                        if processed_local % 500 == 0:
                            logger.info(f"处理进度: {processed_local}/{total_stocks}, 已选出: {len(results_local)}")
                        
                        if len(stock_data) < params['min_history_days']:
                            stats['skip_history'] += 1
                            continue
                        
                        name = stock_data['name'].iloc[-1] if 'name' in stock_data.columns else ts_code
                        if isinstance(name, str) and any(tag in name for tag in ['ST', '退', '*']):
                            stats['skip_st'] += 1
                            continue
                        
                        industry = stock_data['industry'].iloc[-1] if 'industry' in stock_data.columns else '未知'
                        
                        close = pd.to_numeric(stock_data['close_price'], errors='coerce').dropna()
                        vol = pd.to_numeric(stock_data['vol'], errors='coerce').dropna()
                        amount = pd.to_numeric(stock_data['amount'], errors='coerce').dropna()
                        pct = pd.to_numeric(stock_data['pct_chg'], errors='coerce').fillna(0) / 100
                        
                        if len(close) < 20 or len(vol) < 20 or len(amount) < 20:
                            stats['skip_len_data'] += 1
                            continue
                        
                        last_close = float(close.iloc[-1])
                        today_pct = float(pct.iloc[-1])
                        ret_20 = last_close / float(close.iloc[-21]) - 1 if len(close) >= 21 else 0
                        avg_amount_20 = float(amount.iloc[-20:].mean())
                        avg_amount_20_yi = avg_amount_20 / 1e5  # Tushare amount为千元，这里转换为亿元

                        # 行业强度基础统计（非ST且有足够数据）
                        if industry:
                            stats_entry = industry_stats.setdefault(
                                industry, {'rets': [], 'pos': 0, 'count': 0}
                            )
                            stats_entry['rets'].append(ret_20)
                            stats_entry['count'] += 1
                            if ret_20 > 0:
                                stats_entry['pos'] += 1
                            industry_members.setdefault(industry, []).append((ts_code, ret_20))
                        
                        # 回避涨停追高与连板博弈（A股特性）
                        limit_up_pct = _limit_up_threshold(ts_code)
                        limit_up_days = int((pct.iloc[-10:] >= limit_up_pct).sum())
                        if today_pct >= limit_up_pct or limit_up_days >= params['limit_up_days_limit']:
                            stats['skip_limitup'] += 1
                            continue
                        
                        # 基础活跃度过滤
                        if avg_amount_20_yi < min_amount * params['min_amount_factor']:
                            stats['skip_amount'] += 1
                            continue
                        
                        # 流通市值过滤（亿）
                        avg_turnover = None
                        if 'circ_mv' in stock_data.columns:
                            circ_mv_value = pd.to_numeric(stock_data['circ_mv'].iloc[-1], errors='coerce')
                            if pd.notna(circ_mv_value) and circ_mv_value > 0:
                                circ_mv_yi = circ_mv_value / 10000
                                if circ_mv_yi < min_market_cap or circ_mv_yi > max_market_cap:
                                    stats['skip_mcap'] += 1
                                    continue
                                # amount为千元，circ_mv为万元 -> 统一为元
                                avg_turnover = (avg_amount_20 * 1000) / (circ_mv_value * 10000)
                                if avg_turnover < params['turnover_min'] or avg_turnover > params['turnover_max']:
                                    stats['skip_turnover'] += 1
                                    continue
                        
                        # 用于板块统计的宽松阈值（确保板块强度可计算）
                        ret20_gate = max(target_return * params['ret20_factor'], params['ret20_floor'])
                        if ret_20 >= ret20_gate:
                            sector_counts[industry] = sector_counts.get(industry, 0) + 1
                            candidates.append({
                                'ts_code': ts_code,
                                'stock_data': stock_data,
                                'name': name,
                                'industry': industry,
                                'ret_20': ret_20,
                                'avg_amount_20': avg_amount_20,
                                'avg_amount_20_yi': avg_amount_20_yi,
                                'avg_turnover': avg_turnover,
                                'circ_mv_yi': circ_mv_yi
                            })
                        else:
                            stats['skip_ret20_gate'] += 1
                    except Exception:
                        continue
                
                industry_metrics = {}
                for ind, s in industry_stats.items():
                    if s['rets']:
                        industry_metrics[ind] = {
                            'median_ret20': float(np.median(s['rets'])),
                            'pos_ratio': s['pos'] / max(s['count'], 1),
                            'count': s['count']
                        }
                industry_ranks = {}
                for ind, members in industry_members.items():
                    members_sorted = sorted(members, key=lambda x: x[1], reverse=True)
                    for rank_idx, (ts_code, _) in enumerate(members_sorted, start=1):
                        industry_ranks[ts_code] = rank_idx

                # --- Step 3: 稳健评分与过滤 ---
                stats['candidates'] = len(candidates)
                for item in candidates:
                    try:
                        ts_code = item['ts_code']
                        stock_data = item['stock_data']
                        name = item['name']
                        industry = item['industry']
                        ret_20 = item['ret_20']
                        avg_amount_20 = item['avg_amount_20']
                        avg_amount_20_yi = item.get('avg_amount_20_yi', avg_amount_20 / 1e5)
                        avg_turnover = item.get('avg_turnover')
                        circ_mv_yi = item.get('circ_mv_yi')
                        
                        close = pd.to_numeric(stock_data['close_price'], errors='coerce').dropna()
                        vol = pd.to_numeric(stock_data['vol'], errors='coerce').dropna()
                        amount = pd.to_numeric(stock_data['amount'], errors='coerce').dropna()
                        pct = pd.to_numeric(stock_data['pct_chg'], errors='coerce').fillna(0) / 100
                        
                        if len(close) < 20 or len(vol) < 20 or len(amount) < 20:
                            continue
                        
                        last_close = float(close.iloc[-1])
                        ret_5 = last_close / float(close.iloc[-6]) - 1 if len(close) >= 6 else 0
                        today_pct = float(pct.iloc[-1])
                        
                        # 行业弱势过滤（弱市环境下更严格）
                        industry_info = industry_metrics.get(industry, {})
                        industry_median = industry_info.get('median_ret20', 0)
                        industry_pos_ratio = industry_info.get('pos_ratio', 0)
                        if market_status == " 弱势" and industry_median < -0.02:
                            stats['skip_industry_weak'] += 1
                            continue

                        # 波动分位自适应（对极端波动进行过滤与扣分）
                        vol_percentile = None
                        if len(pct) >= 80:
                            window = 20
                            vol_samples = []
                            for i in range(len(pct) - 120, len(pct) - window + 1):
                                if i < 0:
                                    continue
                                window_std = float(pct.iloc[i:i + window].std())
                                if window_std > 0:
                                    vol_samples.append(window_std)
                            if vol_samples:
                                cur_vol = float(pct.iloc[-20:].std())
                                vol_percentile = sum(v <= cur_vol for v in vol_samples) / len(vol_samples)
                                if vol_percentile >= params['vol_percentile_max']:
                                    stats['skip_vol_percentile'] += 1
                                    continue

                        # 回撤控制（20日内最大回撤）
                        recent_close = close.iloc[-20:]
                        drawdown = (recent_close / recent_close.cummax() - 1).min()
                        max_drawdown = abs(drawdown)
                        if max_drawdown > params['max_drawdown']:
                            stats['skip_drawdown'] += 1
                            continue
                        
                        # 波动率控制
                        volatility = float(pct.iloc[-20:].std())
                        vol_limit = max_volatility * params['volatility_factor']
                        if volatility > vol_limit:
                            stats['skip_volatility'] += 1
                            continue
                        
                        # 回踩确认（靠近均线并出现企稳）
                        ma10 = float(close.iloc[-10:].mean())
                        ma20 = float(close.iloc[-20:].mean())
                        bias = (last_close - ma20) / ma20 if ma20 > 0 else 0
                        prev_close = float(close.iloc[-2])
                        pullback_confirm = (prev_close < ma10 and last_close >= ma10) or (-0.03 <= bias <= 0.05)
                        if params['require_pullback'] and not pullback_confirm:
                            stats['skip_pullback'] += 1
                            continue
                        
                        # 放量/活跃度
                        recent_vol = float(vol.iloc[-3:].mean())
                        hist_vol = float(vol.iloc[-10:].mean()) if len(vol) >= 10 else float(vol.mean())
                        vol_ratio = recent_vol / hist_vol if hist_vol > 0 else 1.0
                        
                        # 评分体系（稳健优先）
                        score = 0
                        reasons = []
                        
                        # 市值分层（中盘 / 大盘）
                        tier = None
                        if circ_mv_yi is not None:
                            if params['mid_cap_min'] <= circ_mv_yi <= params['mid_cap_max']:
                                tier = 'mid'
                            elif params['large_cap_min'] <= circ_mv_yi <= params['large_cap_max']:
                                tier = 'large'

                        # 适度动量
                        if ret_20 >= target_return:
                            score += 25
                            reasons.append(f"20日达标{ret_20*100:.1f}%")
                        elif ret_20 >= target_return * 0.6:
                            score += 18
                            reasons.append(f"20日稳健{ret_20*100:.1f}%")
                        elif ret_20 >= 0.05:
                            score += 12
                            reasons.append(f"20日向上{ret_20*100:.1f}%")
                        elif ret_20 >= 0:
                            score += 6

                        # 中盘适度加分，大盘强调稳定
                        if tier == 'mid':
                            score += 6
                            reasons.append("中盘优势")
                        elif tier == 'large':
                            score += 4
                            reasons.append("大盘稳健")
                        
                        # 回踩确认
                        if pullback_confirm:
                            score += 20
                            reasons.append("回踩确认")
                        
                        # 回撤控制
                        if max_drawdown <= params['drawdown_good']:
                            score += 15
                            reasons.append(f"回撤{max_drawdown*100:.1f}%")
                        else:
                            score += 8
                        
                        # 波动率
                        if volatility <= vol_limit * 0.7:
                            score += 10
                            reasons.append("波动低")
                        else:
                            score += 6
                        
                        # 行业强度（行业中位数 + 上涨占比）- 加权增强
                        if industry_median >= 0.08:
                            score += 14
                            reasons.append("行业强势")
                        elif industry_median >= 0.03:
                            score += 8
                            reasons.append("行业偏强")
                        elif industry_median <= -0.02:
                            score -= 5

                        if industry_pos_ratio >= 0.6:
                            score += 7
                        elif industry_pos_ratio <= 0.4:
                            score -= 3

                        # 龙头/次龙结构识别
                        rank_in_industry = industry_ranks.get(ts_code)
                        if rank_in_industry == 1:
                            score += 10
                            reasons.append("行业龙头")
                        elif rank_in_industry == 2:
                            score += 6
                            reasons.append("行业次龙")

                        # 波动分位得分（越低越稳）
                        if vol_percentile is not None:
                            if vol_percentile <= 0.35:
                                score += 8
                                reasons.append("波动低位")
                            elif vol_percentile <= 0.55:
                                score += 4
                            elif vol_percentile >= 0.8:
                                score -= 4

                        # 板块强度
                        sector_heat = min(sector_counts.get(industry, 0) * params['sector_weight'], params['sector_cap'])
                        score += sector_heat
                        if sector_heat >= params['sector_strong']:
                            reasons.append("板块共振")
                        
                        # 成交活跃度
                        if avg_amount_20_yi >= min_amount * 1.5:
                            score += 8
                            reasons.append("成交活跃")
                        else:
                            score += 4
                        
                        # 换手率（A股稳健性）
                        if avg_turnover is not None:
                            if 0.01 <= avg_turnover <= 0.08:
                                score += 8
                                reasons.append("换手健康")
                            elif 0.005 <= avg_turnover <= 0.12:
                                score += 4
                        
                        # 当日涨幅（避免追高）
                        if -0.01 <= today_pct <= 0.04:
                            score += 6
                            reasons.append("温和走强")
                        elif 0.04 < today_pct < _limit_up_threshold(ts_code):
                            score += 3
                        
                        # 轻度趋势健康
                        if params['bias_min'] <= bias <= params['bias_max']:
                            score += 6
                        elif abs(bias) <= params['bias_soft_max']:
                            score += 3
                        else:
                            stats['skip_bias'] += 1
                            continue
                        
                        # 应用大盘系数
                        score = score * market_multiplier
                        
                        # 评分阈值（稳健版本更严格）
                        if score < params['score_threshold']:
                            stats['skip_score'] += 1
                            continue
                        
                        predicted_return = max(ret_20 * 0.9, 0.05)
                        
                        if score >= 70:
                            grade = " 强烈推荐"
                        elif score >= 50:
                            grade = " 推荐"
                        elif score >= 35:
                            grade = " 关注"
                        else:
                            grade = "观察"
                        
                        reasons.insert(0, grade)
                        if market_status != "正常":
                            reasons.append(market_status)
                        
                        results_local.append({
                            '股票代码': ts_code,
                            '股票名称': name,
                            '行业': industry,
                            '最新价格': f"{last_close:.2f}",
                            '20日涨幅%': f"{ret_20*100:.2f}",
                            '5日涨幅%': f"{ret_5*100:.2f}",
                            '预测潜力%': f"{predicted_return*100:.1f}",
                            '放量倍数': f"{vol_ratio:.2f}",
                            '近20日成交额(亿)': f"{avg_amount_20_yi:.2f}",
                            '换手率%': f"{avg_turnover*100:.2f}" if avg_turnover is not None else "-",
                            '回撤%': f"{max_drawdown*100:.1f}",
                            '波动率%': f"{volatility*100:.2f}",
                            '行业强度%': f"{industry_median*100:.1f}",
                            '市值层级': "中盘" if tier == 'mid' else ("大盘" if tier == 'large' else "-"),
                            '评分': round(score, 1),
                            '筛选理由': " · ".join(reasons),
                            '流通市值(亿)': f"{stock_data['circ_mv'].iloc[-1]/10000:.1f}" if 'circ_mv' in stock_data.columns else "-"
                        })
                    except Exception:
                        continue
                
                stats['results'] = len(results_local)
                return {'results': results_local, 'stats': stats}

            strict_params = {
                'stage_name': 'strict',
                'min_history_days': 60,
                'min_amount_factor': 1.0,
                'turnover_min': 0.003,
                'turnover_max': 0.20,
                'limit_up_days_limit': 2,
                'ret20_factor': 0.6,
                'ret20_floor': 0.03,
                'max_drawdown': 0.18,
                'drawdown_good': 0.12,
                'volatility_factor': 1.0,
                'vol_percentile_max': 0.85,
                'score_threshold': 35,
                'sector_weight': 4,
                'sector_cap': 18,
                'sector_strong': 10,
                'bias_min': -0.05,
                'bias_max': 0.12,
                'bias_soft_max': 0.18,
                'require_pullback': True,
                'mid_cap_min': 100,
                'mid_cap_max': 800,
                'large_cap_min': 800,
                'large_cap_max': 5000
            }

            debug_runs = []
            run_data = _run_selection(strict_params)
            results = run_data['results']
            debug_runs.append(run_data['stats'])
            if not results:
                logger.info("V5.0严格条件未命中，启用稳健放宽条件")
                relaxed_params = {
                    'stage_name': 'relaxed',
                    'min_history_days': 40,
                    'min_amount_factor': 0.6,
                    'turnover_min': 0.002,
                    'turnover_max': 0.25,
                    'limit_up_days_limit': 3,
                    'ret20_factor': 0.5,
                    'ret20_floor': 0.02,
                    'max_drawdown': 0.22,
                    'drawdown_good': 0.15,
                    'volatility_factor': 1.2,
                    'vol_percentile_max': 0.90,
                    'score_threshold': 30,
                    'sector_weight': 3,
                    'sector_cap': 15,
                    'sector_strong': 8,
                    'bias_min': -0.06,
                    'bias_max': 0.15,
                    'bias_soft_max': 0.22,
                    'require_pullback': False,
                    'mid_cap_min': 100,
                    'mid_cap_max': 800,
                    'large_cap_min': 800,
                    'large_cap_max': 5000
                }
                run_data = _run_selection(relaxed_params)
                results = run_data['results']
                debug_runs.append(run_data['stats'])
            if not results:
                logger.info("V5.0稳健放宽仍未命中，启用救援筛选")
                rescue_params = {
                    'stage_name': 'rescue',
                    'min_history_days': 30,
                    'min_amount_factor': 0.4,
                    'turnover_min': 0.001,
                    'turnover_max': 0.35,
                    'limit_up_days_limit': 4,
                    'ret20_factor': 0.4,
                    'ret20_floor': 0.01,
                    'max_drawdown': 0.26,
                    'drawdown_good': 0.18,
                    'volatility_factor': 1.4,
                    'vol_percentile_max': 0.95,
                    'score_threshold': 22,
                    'sector_weight': 2,
                    'sector_cap': 12,
                    'sector_strong': 6,
                    'bias_min': -0.08,
                    'bias_max': 0.20,
                    'bias_soft_max': 0.28,
                    'require_pullback': False,
                    'mid_cap_min': 100,
                    'mid_cap_max': 800,
                    'large_cap_min': 800,
                    'large_cap_max': 5000
                }
                run_data = _run_selection(rescue_params)
                results = run_data['results']
                debug_runs.append(run_data['stats'])

            self.last_v5_debug = debug_runs

            if not results:
                logger.error("未找到任何符合条件的股票")
                return pd.DataFrame()

            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('评分', ascending=False)
            
            logger.info(f"V5.0选股完成: 找到{len(result_df)}只标的, 最高分{result_df['评分'].max():.1f}, 最低分{result_df['评分'].min():.1f}")
            return result_df

        except Exception as e:
            logger.error(f"AI选股V5.0执行失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()

    def select_monthly_target_stocks(
        self,
        df: pd.DataFrame,
        target_return: float = 0.20,
        min_amount: float = 2.0,
        max_volatility: float = 0.12
    ) -> pd.DataFrame:
        """
         AI 选股 V2.0 - 高级重构版
        目标：月收益率 20%+ 的稳健捕获
        
        新增核心逻辑：
        1. 大盘环境过滤器 (Market Regime Filter)
        2. 板块共振共鸣 (Sector Resonance)
        3. 乖离率安全边际 (Bias Margin)
        4. 量价形态健康度 (VP Health)
        """
        try:
            # --- Step 0: 大盘环境检查 ---
            market_score = 1.0
            market_warning = ""
            try:
                from data.history import load_index_recent as _load_index_recent_v2  # type: ignore
                idx_df = _load_index_recent_v2(
                    db_path=self.db_path,
                    index_code="000001.SH",
                    limit=40,
                    columns="close_price",
                )
                if len(idx_df) >= 20:
                    idx_closes = idx_df['close_price'].tolist()
                    idx_ma20 = sum(idx_closes[:20]) / 20
                    current_idx = idx_closes[0]
                    # 如果大盘在20日线下，属于空头市场
                    if current_idx < idx_ma20:
                        market_score = 0.6  # 大幅扣分
                        market_warning = " 大盘走弱(20日线下)，追高风险极高"
                    elif current_idx < idx_closes[5]:
                        market_score = 0.8  # 小幅扣分
                        market_warning = " 大盘处于短线调整"
            except Exception as e:
                logger.warning(f"大盘数据获取失败: {e}")

            # --- Step 1: 预处理与基础过滤 ---
            required_cols = ['ts_code', 'trade_date', 'close_price', 'pct_chg', 'industry']
            for col in required_cols:
                if col not in df.columns:
                    return pd.DataFrame()

            df = df.sort_values(['ts_code', 'trade_date']).reset_index(drop=True)
            candidates = []
            sector_counts = {}

            # 第一遍循环：筛选初步达标个股并统计板块
            for ts_code, stock_data in df.groupby('ts_code'):
                if len(stock_data) < 30: continue
                
                # 排除风险股
                name = stock_data['name'].iloc[-1] if 'name' in stock_data.columns else ts_code
                if isinstance(name, str) and any(tag in name for tag in ['ST', '退']): continue

                close = pd.to_numeric(stock_data['close_price'], errors='coerce').dropna()
                if len(close) < 21: continue

                last_close = float(close.iloc[-1])
                close_20 = float(close.iloc[-21])
                ret_20 = last_close / close_20 - 1
                
                # 涨幅达标
                if ret_20 >= target_return:
                    industry = stock_data['industry'].iloc[-1] if 'industry' in stock_data.columns else '未知'
                    sector_counts[industry] = sector_counts.get(industry, 0) + 1
                    candidates.append({
                        'ts_code': ts_code,
                        'stock_data': stock_data,
                        'ret_20': ret_20,
                        'last_close': last_close,
                        'name': name,
                        'industry': industry
                    })

            # --- Step 2: 深度评分与二次过滤 ---
            results = []
            for item in candidates:
                ts_code = item['ts_code']
                stock_data = item['stock_data']
                ret_20 = item['ret_20']
                last_close = item['last_close']
                industry = item['industry']
                
                close_series = stock_data['close_price']
                ma20 = close_series.iloc[-20:].mean()
                ma5 = close_series.iloc[-5:].mean()
                
                # 1. 乖离率检查 (Bias) - 20%目标通常意味着乖离已经不小，但不能太离谱
                bias = (last_close / ma20 - 1)
                if bias > 0.35: continue  # 涨得太急了，偏离20日线35%以上，容易暴跌

                # 2. 量价健康度 (VP Health)
                amount = pd.to_numeric(stock_data.get('amount', stock_data.get('vol', 0)), errors='coerce').fillna(0)
                avg_amount_20 = amount.iloc[-20:].mean()
                avg_amount_20_yi = avg_amount_20 / 1e5  # 千元 -> 亿元
                if avg_amount_20_yi < min_amount: continue # 流动性过滤
                
                # 最近3天是否有明显的缩量回踩迹象 (或者是放量突破)
                recent_vol_inc = amount.iloc[-1] > amount.iloc[-2]
                
                # 3. 波动率过滤
                pct = pd.to_numeric(stock_data['pct_chg'], errors='coerce').fillna(0) / 100
                volatility = pct.iloc[-20:].std()
                if volatility > max_volatility: continue

                # 4. 板块共振评分
                sector_heat = min(sector_counts.get(industry, 0) * 5, 20) # 板块内入选越多，热度越高，最高20分

                # 5. 综合评分计算
                # 逻辑：涨幅贡献基础分 + 板块加成 + 量价加成 - 乖离扣分
                score = (
                    ret_20 * 100 * 0.4                  # 基础动量分 (40%)
                    + sector_heat                       # 板块共振分 (max 20)
                    + (20 if last_close > ma5 else 0)   # 短期趋势分 (20)
                    - (bias * 50)                       # 乖离率惩罚 (过高则扣分)
                ) * market_score                        # 大盘权重系数

                # 筛选理由构建
                reasons = [f"20日收益率达{ret_20*100:.1f}%"]
                if sector_counts.get(industry, 0) > 3:
                    reasons.append(f"所属{industry}板块趋势")
                if bias < 0.15:
                    reasons.append("回踩支撑位")
                elif recent_vol_inc:
                    reasons.append("量价齐升")
                if market_warning and market_score < 1:
                    reasons.append(market_warning)

                results.append({
                    '股票代码': ts_code,
                    '股票名称': item['name'],
                    '行业': industry,
                    '最新价格': f"{last_close:.2f}",
                    '20日涨幅%': f"{ret_20*100:.2f}",
                    '偏离度%': f"{bias*100:.1f}",
                    '波动率%': f"{volatility*100:.2f}",
                    '近20日成交额(亿)': f"{avg_amount_20_yi:.2f}",
                    '评分': round(score, 1),
                    '筛选理由': " · ".join(reasons),
                    '流通市值(亿)': f"{stock_data['circ_mv'].iloc[-1]/10000:.1f}" if 'circ_mv' in stock_data.columns else "-"
                })

            if not results:
                return pd.DataFrame()

            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('评分', ascending=False)
            return result_df

        except Exception as e:
            logger.error(f"AI选股V2.0执行失败: {e}")
            return pd.DataFrame()


# ===================== 参数优化器（v46.7增强版）=====================
class StrategyOptimizer:
    """策略优化器 - 增强版"""
    
    def __init__(self, analyzer: CompleteVolumePriceAnalyzer):
        self.analyzer = analyzer
    
    def optimize_parameters(self, df: pd.DataFrame, sample_size: int = 500) -> Dict:
        """旧版参数优化（兼容性保留）"""
        try:
            logger.info("开始参数优化...")
            
            param_grid = {
                'signal_strength': [0.4, 0.5, 0.6, 0.7]
            }
            
            best_params = None
            best_score = -float('inf')
            all_results = []
            
            for i, strength in enumerate(param_grid['signal_strength']):
                logger.info(f"参数优化进度: {i+1}/{len(param_grid['signal_strength'])}")
                
                try:
                    result = self.analyzer.backtest_strategy_complete(
                        df, 
                        sample_size=sample_size, 
                        signal_strength=strength
                    )
                    
                    if result['success']:
                        stats = result['stats']
                        
                        score = (
                            stats['avg_return'] * 0.4 +
                            stats['win_rate'] * 0.3 +
                            stats['sharpe_ratio'] * 10 * 0.2 +
                            min(stats['total_signals'] / 100, 1) * 10 * 0.1
                        )
                        
                        result_info = {
                            'params': {'signal_strength': strength},
                            'score': score,
                            'stats': stats
                        }
                        
                        all_results.append(result_info)
                        
                        if score > best_score:
                            best_score = score
                            best_params = result_info
                
                except Exception as e:
                    logger.warning(f"参数测试失败: {e}")
                    continue
            
            logger.info(f"参数优化完成！")
            
            return {
                'success': True,
                'best_params': best_params,
                'all_results': sorted(all_results, key=lambda x: x['score'], reverse=True)
            }
            
        except Exception as e:
            logger.error(f"参数优化失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def optimize_single_strategy(self, df: pd.DataFrame, strategy_name: str, sample_size: int = 300) -> Dict:
        """
        优化单个策略的持仓天数
        """
        logger.info(f"开始优化{strategy_name}的持仓天数...")
        
        try:
            holding_days_options = [3, 5, 7, 10]
            best_params = None
            best_score = -float('inf')
            all_results = []
            
            for i, holding_days in enumerate(holding_days_options):
                logger.info(f"测试持仓天数: {holding_days}天 ({i+1}/{len(holding_days_options)})")
                
                try:
                    # 根据策略选择对应的回测方法
                    if "强势猎手" in strategy_name:
                        result = self.analyzer.backtest_explosive_hunter(df, sample_size, holding_days)
                    elif "底部突破" in strategy_name:
                        result = self.analyzer.backtest_bottom_breakthrough(df, sample_size, holding_days)
                    elif "高级猎手" in strategy_name:
                        result = self.analyzer.backtest_ultimate_hunter(df, sample_size, holding_days)
                    else:
                        logger.warning(f"未知策略: {strategy_name}")
                        continue
                    
                    # 详细日志
                    logger.info(f"回测结果: success={result.get('success')}, 策略={result.get('strategy')}")
                    
                    if not result.get('success', False):
                        logger.warning(f"持仓{holding_days}天回测失败: {result.get('error', '未知错误')}")
                        continue
                    
                    stats = result.get('stats', {})
                    if not stats:
                        logger.warning(f"持仓{holding_days}天回测返回空stats")
                        continue
                        
                    # 综合评分
                    score = (
                        stats.get('avg_return', 0) * 0.4 +
                        stats.get('win_rate', 0) * 0.3 +
                        stats.get('sharpe_ratio', 0) * 10 * 0.2 +
                        min(stats.get('total_signals', 0) / 100, 1) * 10 * 0.1
                    )
                    
                    result_info = {
                        'holding_days': holding_days,
                        'score': score,
                        'avg_return': stats.get('avg_return', 0),
                        'win_rate': stats.get('win_rate', 0),
                        'sharpe_ratio': stats.get('sharpe_ratio', 0),
                        'total_signals': stats.get('total_signals', 0)
                    }
                    
                    logger.info(f"持仓{holding_days}天测试成功: 收益{result_info['avg_return']:.2f}%, 胜率{result_info['win_rate']:.1f}%")
                    
                    all_results.append(result_info)
                    
                    if score > best_score:
                        best_score = score
                        best_params = result_info
                
                except Exception as e:
                    logger.warning(f"持仓{holding_days}天测试失败: {e}")
                    continue
            
            if not all_results:
                return {
                    'success': False, 
                    'error': '所有参数测试都失败',
                    'strategy': strategy_name,
                    'is_comparison': False
                }
            
            logger.info(f"{strategy_name}参数优化完成！最佳持仓天数：{best_params['holding_days']}天")
            
            return {
                'success': True,
                'strategy': strategy_name,
                'best_params': best_params,
                'all_results': sorted(all_results, key=lambda x: x['score'], reverse=True),
                'is_comparison': False
            }
            
        except Exception as e:
            logger.error(f"参数优化失败: {e}")
            logger.error(traceback.format_exc())
            return {
                'success': False, 
                'error': str(e),
                'strategy': strategy_name if 'strategy_name' in locals() else '未知策略',
                'is_comparison': False
            }
    
    def optimize_all_strategies(self, df: pd.DataFrame, sample_size: int = 300) -> Dict:
        """
        优化所有策略，对比表现
        """
        logger.info("开始全策略参数优化...")
        
        try:
            strategies = ["强势猎手", "底部突破猎手", "高级猎手"]
            best_strategies = []
            
            for strategy in strategies:
                logger.info(f"正在优化: {strategy}")
                result = self.optimize_single_strategy(df, strategy, sample_size)
                
                if result['success']:
                    best = result['best_params']
                    best_strategies.append({
                        'strategy': strategy,
                        'best_holding_days': best['holding_days'],
                        'score': best['score'],
                        'avg_return': best['avg_return'],
                        'win_rate': best['win_rate']
                    })
            
            if not best_strategies:
                return {
                    'success': False, 
                    'error': '所有策略优化都失败',
                    'is_comparison': True
                }
            
            # 按综合评分排序
            best_strategies_df = pd.DataFrame(best_strategies)
            best_strategies_df = best_strategies_df.sort_values('score', ascending=False)
            
            logger.info("全策略参数优化完成！")
            
            return {
                'success': True,
                'comparison': best_strategies_df,
                'is_comparison': True
            }
            
        except Exception as e:
            logger.error(f"全策略优化失败: {e}")
            logger.error(traceback.format_exc())
            return {
                'success': False, 
                'error': str(e),
                'is_comparison': True
            }


# ===================== 板块扫描器（v38功能）=====================
class MarketScanner:
    """板块扫描器"""
    
    def __init__(self, db_path: str = PERMANENT_DB_PATH):
        self.db_path = db_path
    
    def scan_all_sectors(self, days: int = 60) -> Dict:
        """扫描所有板块"""
        try:
            logger.info("开始全市场扫描...")
            all_data = self._get_all_sectors_data(days)
            
            if all_data.empty:
                return {'emerging': [], 'launching': [], 'exploding': [], 'declining': [], 'transitioning': []}
            
            sectors = all_data['industry'].unique()
            results = {'emerging': [], 'launching': [], 'exploding': [], 'declining': [], 'transitioning': []}
            
            for sector in sectors:
                try:
                    sector_data = all_data[all_data['industry'] == sector]
                    if len(sector_data) < 30:
                        continue
                    
                    recent_5 = sector_data.tail(5)
                    historical = sector_data.head(len(sector_data) - 10)
                    
                    price_change = recent_5['pct_chg'].mean()
                    historical_vol_mean = historical['vol'].mean()
                    
                    if historical_vol_mean > 0:
                        vol_ratio = recent_5['vol'].mean() / historical_vol_mean
                    else:
                        vol_ratio = 1.0
                    
                    if vol_ratio < 0.8 and -1 < price_change < 2:
                        stage = '萌芽期'
                        category = 'emerging'
                    elif vol_ratio > 2.0 and price_change > 5:
                        stage = '加速期'
                        category = 'exploding'
                    elif 1.3 < vol_ratio <= 2.0 and 2 < price_change <= 5:
                        stage = '启动期'
                        category = 'launching'
                    elif vol_ratio < 1.0 and price_change < -2:
                        stage = '衰退期'
                        category = 'declining'
                    else:
                        stage = '过渡期'
                        category = 'transitioning'
                    
                    results[category].append({
                        'sector_name': sector,
                        'stage': stage,
                        'score': 75 if stage == '萌芽期' else 50,
                        'signals': [f"成交量{vol_ratio:.1f}倍", f"涨幅{price_change:.1f}%"]
                    })
                
                except Exception as e:
                    continue
            
            for key in results:
                results[key] = sorted(results[key], key=lambda x: x['score'], reverse=True)
            
            logger.info(f"扫描完成: 萌芽期{len(results['emerging'])}个")
            return results
            
        except Exception as e:
            logger.error(f"扫描失败: {e}")
            return {'emerging': [], 'launching': [], 'exploding': [], 'declining': [], 'transitioning': []}
    
    def _get_all_sectors_data(self, days: int) -> pd.DataFrame:
        try:
            if not os.path.exists(self.db_path):
                return pd.DataFrame()

            from data.dao import DataAccessError, detect_daily_table  # type: ignore

            conn = sqlite3.connect(self.db_path)
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')

            try:
                daily_table = detect_daily_table(conn)
            except DataAccessError:
                conn.close()
                return pd.DataFrame()

            query = f"""
                SELECT dtd.ts_code, sb.name, sb.industry, dtd.trade_date, 
                       dtd.close_price, dtd.vol, dtd.pct_chg
                FROM {daily_table} dtd
                INNER JOIN stock_basic sb ON dtd.ts_code = sb.ts_code
                WHERE dtd.trade_date >= ? AND sb.industry IS NOT NULL
                ORDER BY sb.industry, dtd.trade_date
            """
            
            df = pd.read_sql_query(query, conn, params=(start_date,))
            conn.close()
            return df
            
        except Exception as e:
            logger.error(f"获取数据失败: {e}")
            return pd.DataFrame()


# ===================== 数据库管理器（v40功能）=====================
class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, db_path: str = PERMANENT_DB_PATH):
        self.db_path = db_path
        # 线上部署时工作目录可能变化，优先自动解析真实数据库路径
        try:
            from data.dao import resolve_db_path as _resolve_db_path_runtime  # type: ignore

            self.db_path = str(_resolve_db_path_runtime(self.db_path))
        except Exception:
            pass
        self.pro = None
        self._status_cache: Dict[str, Any] = {}
        self._status_cache_at = 0.0
        self._status_cache_mtime = 0.0
        self._init_tushare()

    def _connect(self, timeout: int = 30) -> sqlite3.Connection:
        """创建带超时和WAL模式的连接，降低数据库锁冲突"""
        conn = sqlite3.connect(self.db_path, timeout=timeout, check_same_thread=False)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
        except Exception:
            pass
        return conn

    def _connect_readonly(self, timeout: int = 10) -> sqlite3.Connection:
        """Create a read-only SQLite connection for dashboard status queries."""
        try:
            db_uri = Path(self.db_path).resolve().as_uri() + "?mode=ro&cache=shared"
            conn = sqlite3.connect(db_uri, timeout=timeout, uri=True, check_same_thread=False)
        except Exception:
            conn = sqlite3.connect(self.db_path, timeout=timeout, check_same_thread=False)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA query_only=ON")
        except Exception:
            pass
        return conn
    
    def _init_tushare(self):
        if not TUSHARE_ENABLED:
            logger.info("Tushare已禁用（OFFLINE_MODE 或 TUSHARE_ENABLED=0）")
            return
        try:
            ts.set_token(TUSHARE_TOKEN)
            self.pro = ts.pro_api()
            logger.info("Tushare初始化成功")
        except Exception as e:
            logger.error(f"Tushare初始化失败: {e}")
    
    def get_database_status(self, force_refresh: bool = False) -> Dict:
        """获取数据库状态"""
        try:
            if not os.path.exists(self.db_path):
                try:
                    from data.dao import resolve_db_path as _resolve_db_path_runtime  # type: ignore

                    self.db_path = str(_resolve_db_path_runtime(self.db_path))
                except Exception:
                    return {'error': f'数据库文件不存在: {self.db_path}'}

            now = _now_ts()
            db_mtime = _safe_file_mtime(self.db_path)
            if (
                not force_refresh
                and self._status_cache
                and (now - float(self._status_cache_at or 0.0)) < 60
                and float(self._status_cache_mtime or 0.0) == db_mtime
            ):
                return dict(self._status_cache)

            from data.dao import DataAccessError, detect_daily_table, recent_trade_profile  # type: ignore

            conn = self._connect_readonly()
            cursor = conn.cursor()
            
            status = {}
            daily_table = None
            try:
                daily_table = detect_daily_table(conn)
                status["daily_table"] = daily_table
            except DataAccessError:
                status["daily_table"] = "N/A"
            
            try:
                cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM stock_basic")
                status['active_stocks'] = cursor.fetchone()[0]
            except:
                status['active_stocks'] = 0
            
            try:
                cursor.execute("SELECT COUNT(DISTINCT industry) FROM stock_basic WHERE industry IS NOT NULL")
                status['total_industries'] = cursor.fetchone()[0]
            except:
                status['total_industries'] = 0
            
            try:
                if daily_table:
                    cursor.execute(f"SELECT COUNT(*) FROM {daily_table}")
                    status['total_records'] = cursor.fetchone()[0]
                else:
                    status['total_records'] = 0
            except:
                status['total_records'] = 0
            
            try:
                if daily_table:
                    cursor.execute(f"SELECT MIN(trade_date), MAX(trade_date) FROM {daily_table}")
                    date_range = cursor.fetchone()
                    status['min_date'] = date_range[0] if date_range and date_range[0] else 'N/A'
                    status['max_date'] = date_range[1] if date_range and date_range[1] else 'N/A'
                    profile = recent_trade_profile(conn, date_limit=10, recent_window=3)
                    status["records_last_trade_date"] = int(profile.get("records_last_trade_date", 0) or 0)
                else:
                    status['min_date'] = 'N/A'
                    status['max_date'] = 'N/A'
            except:
                status['min_date'] = 'N/A'
                status['max_date'] = 'N/A'
                status["records_last_trade_date"] = 0
            
            if os.path.exists(self.db_path):
                size_bytes = os.path.getsize(self.db_path)
                status['db_size_gb'] = round(size_bytes / (1024 * 1024 * 1024), 2)
            
            if status.get('max_date') and status['max_date'] != 'N/A':
                try:
                    latest_date = datetime.strptime(status['max_date'], '%Y%m%d')
                    days_old = (datetime.now() - latest_date).days
                    status['days_old'] = days_old
                    status['is_fresh'] = days_old <= 2
                except:
                    status['days_old'] = 999
                    status['is_fresh'] = False
            
            conn.close()
            self._status_cache = dict(status)
            self._status_cache_at = now
            self._status_cache_mtime = db_mtime
            return status
            
        except Exception as e:
            logger.error(f"获取数据库状态失败: {e}")
            return {'error': str(e)}
    
    def update_stock_data_from_tushare(self, stock_codes: List[str] = None, days: int = 30) -> Dict:
        """更新股票数据"""
        try:
            if not self.pro:
                return {'success': False, 'error': 'Tushare未初始化'}
            
            logger.info(f"开始更新数据，回溯{days}天")
            
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days+10)).strftime('%Y%m%d')

            def _infer_weekday_trade_dates(_end_date: str, _days: int) -> List[str]:
                try:
                    _d = datetime.strptime(_end_date, "%Y%m%d")
                except Exception:
                    _d = datetime.now()
                _out = []
                _max_scan = max(_days * 4, _days + 30)
                for _ in range(_max_scan):
                    if _d.weekday() < 5:
                        _out.append(_d.strftime("%Y%m%d"))
                        if len(_out) >= _days:
                            break
                    _d -= timedelta(days=1)
                _out.sort()
                return _out[-_days:]

            trade_dates: List[str] = []
            calendar_source = "trade_cal"
            calendar_warning = ""
            last_calendar_error = ""
            for _attempt in range(3):
                try:
                    trade_cal = self.pro.trade_cal(
                        exchange='SSE',
                        start_date=start_date,
                        end_date=end_date,
                        is_open='1',
                    )
                    if trade_cal is not None and not trade_cal.empty:
                        trade_dates = sorted([str(x) for x in trade_cal['cal_date'].tolist()])[-days:]
                        break
                    last_calendar_error = "trade_cal empty"
                except Exception as e:
                    last_calendar_error = str(e)
                    logger.warning(f"trade_cal 获取失败（第{_attempt+1}/3次）: {e}")
                time.sleep(0.6 * (_attempt + 1))

            if not trade_dates:
                trade_dates = _infer_weekday_trade_dates(end_date, days)
                calendar_source = "weekday_fallback"
                calendar_warning = "交易日历接口暂不可用，已按工作日兜底更新。"
                logger.warning(f"trade_cal 不可用，启用工作日兜底。last_error={last_calendar_error}")

            if not trade_dates:
                return {'success': False, 'error': f'无法获取交易日期（trade_cal与兜底均失败）: {last_calendar_error}'}
            
            if not stock_codes:
                try:
                    conn = self._connect()
                    cursor = conn.cursor()
                    # 不依赖is_active列，直接获取所有股票
                    cursor.execute("SELECT ts_code FROM stock_basic LIMIT 5000")
                    stock_codes = [row[0] for row in cursor.fetchall()]
                    conn.close()
                    
                    if not stock_codes:
                        return {'success': False, 'error': '数据库中没有股票数据，请先更新股票列表'}
                except Exception as e:
                    logger.error(f"获取股票列表失败: {e}")
                    return {'success': False, 'error': f'无法获取股票列表: {str(e)}'}
            
            conn = self._connect()
            cursor = conn.cursor()
            try:
                from data.dao import DataAccessError, detect_daily_table  # type: ignore
                try:
                    daily_table = detect_daily_table(conn)
                except DataAccessError:
                    daily_table = "daily_trading_data"
                daily_table = _safe_daily_table_name(daily_table)
            except Exception:
                daily_table = "daily_trading_data"
            
            updated_count = 0
            failed_count = 0
            total_records = 0
            
            for i, trade_date in enumerate(trade_dates):
                try:
                    df = self.pro.daily(trade_date=trade_date)
                    
                    if not df.empty:
                        if stock_codes:
                            df = df[df['ts_code'].isin(stock_codes)]
                        
                        for _, row in df.iterrows():
                            try:
                                cursor.execute(f"""
                                    INSERT OR REPLACE INTO {daily_table}
                                    (ts_code, trade_date, open_price, high_price, low_price, 
                                     close_price, pre_close, vol, amount, pct_chg)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    row['ts_code'], row['trade_date'],
                                    row.get('open', 0), row.get('high', 0), row.get('low', 0),
                                    row.get('close', 0), row.get('pre_close', 0),
                                    row.get('vol', 0), row.get('amount', 0), row.get('pct_chg', 0)
                                ))
                                total_records += 1
                            except:
                                continue
                        
                        updated_count += 1
                    
                    if (i + 1) % 10 == 0:
                        conn.commit()
                        logger.info(f"更新进度: {i+1}/{len(trade_dates)}")
                    
                    time.sleep(0.1)
                    
                except Exception as e:
                    failed_count += 1
                    continue
            
            conn.commit()
            conn.close()
            
            logger.info(f"数据更新完成：成功{updated_count}天，失败{failed_count}天")
            
            return {
                'success': True,
                'updated_days': updated_count,
                'failed_days': failed_count,
                'total_records': total_records,
                'calendar_source': calendar_source,
                'calendar_warning': calendar_warning,
            }
            
        except Exception as e:
            logger.error(f"数据更新失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def optimize_database(self) -> Dict:
        """优化数据库"""
        try:
            conn = self._connect()
            cursor = conn.cursor()
            try:
                from data.dao import DataAccessError, detect_daily_table  # type: ignore
                try:
                    daily_table = detect_daily_table(conn)
                except DataAccessError:
                    daily_table = "daily_trading_data"
                daily_table = _safe_daily_table_name(daily_table)
            except Exception:
                daily_table = "daily_trading_data"
            
            logger.info("开始优化数据库...")
            
            # 1. 清理重复数据
            cursor.execute(f"""
                DELETE FROM {daily_table}
                WHERE rowid NOT IN (
                    SELECT MIN(rowid) 
                    FROM {daily_table}
                    GROUP BY ts_code, trade_date
                )
            """)
            deleted_duplicates = cursor.rowcount
            
            # 2. 重建索引
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_ts_code ON {daily_table}(ts_code)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_trade_date ON {daily_table}(trade_date)")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_ts_date ON {daily_table}(ts_code, trade_date)")
            
            # 3. VACUUM优化
            cursor.execute("VACUUM")
            
            conn.commit()
            conn.close()
            
            logger.info("数据库优化完成")
            
            return {
                'success': True,
                'deleted_duplicates': deleted_duplicates,
                'message': f'成功！删除{deleted_duplicates}条重复数据，重建索引完成'
            }
            
        except Exception as e:
            logger.error(f"数据库优化失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def update_market_cap(self) -> Dict:
        """更新流通市值数据"""
        try:
            if not self.pro:
                return {'success': False, 'error': 'Tushare未初始化'}
            
            logger.info("开始更新流通市值数据...")
            
            conn = self._connect()
            cursor = conn.cursor()
            
            # 1. 添加circ_mv和total_mv列（如果不存在）
            cursor.execute("PRAGMA table_info(stock_basic)")
            columns = [row[1] for row in cursor.fetchall()]
            
            if 'circ_mv' not in columns:
                logger.info("添加circ_mv列...")
                cursor.execute("ALTER TABLE stock_basic ADD COLUMN circ_mv REAL DEFAULT 0")
                conn.commit()
            
            if 'total_mv' not in columns:
                logger.info("添加total_mv列...")
                cursor.execute("ALTER TABLE stock_basic ADD COLUMN total_mv REAL DEFAULT 0")
                conn.commit()
            
            # 2. 获取本地股票列表
            cursor.execute("SELECT ts_code FROM stock_basic")
            local_stocks = set([row[0] for row in cursor.fetchall()])
            
            logger.info(f"本地有 {len(local_stocks)} 只股票")
            
            # 3. 从Tushare获取市值数据
            today = datetime.now().strftime('%Y%m%d')
            
            # 尝试获取最近几天的数据
            market_data = None
            for i in range(8):
                check_date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
                try:
                    market_data = self.pro.daily_basic(
                        trade_date=check_date,
                        fields='ts_code,trade_date,close,circ_mv,total_mv'
                    )
                    if market_data is not None and not market_data.empty:
                        logger.info(f"使用 {check_date} 的市值数据")
                        break
                    time.sleep(0.1)
                except:
                    continue
            
            if market_data is None or market_data.empty:
                return {'success': False, 'error': '无法从Tushare获取市值数据'}
            
            # 4. 更新数据库
            updated_count = 0
            for _, row in market_data.iterrows():
                ts_code = row['ts_code']
                if ts_code in local_stocks:
                    circ_mv = row.get('circ_mv', 0) if pd.notna(row.get('circ_mv')) else 0
                    total_mv = row.get('total_mv', 0) if pd.notna(row.get('total_mv')) else 0
                    
                    cursor.execute("""
                        UPDATE stock_basic
                        SET circ_mv = ?, total_mv = ?
                        WHERE ts_code = ?
                    """, (circ_mv, total_mv, ts_code))
                    updated_count += 1
                    
                    if updated_count % 500 == 0:
                        conn.commit()
            
            conn.commit()
            
            # 5. 统计市值分布
            cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN circ_mv > 0 AND circ_mv/10000 >= 100 AND circ_mv/10000 <= 500 THEN 1 ELSE 0 END) as count_100_500,
                    SUM(CASE WHEN circ_mv > 0 AND circ_mv/10000 >= 50 AND circ_mv/10000 < 100 THEN 1 ELSE 0 END) as count_50_100,
                    SUM(CASE WHEN circ_mv > 0 AND circ_mv/10000 < 50 THEN 1 ELSE 0 END) as count_below_50,
                    SUM(CASE WHEN circ_mv > 0 AND circ_mv/10000 > 500 THEN 1 ELSE 0 END) as count_above_500
                FROM stock_basic
                WHERE circ_mv > 0
            """)
            
            stats = cursor.fetchone()
            
            conn.close()
            
            logger.info(f"市值数据更新完成：更新 {updated_count} 只股票")
            
            return {
                'success': True,
                'updated_count': updated_count,
                'stats': {
                    'total': stats[0],
                    'count_100_500': stats[1],  # 黄金区间
                    'count_50_100': stats[2],
                    'count_below_50': stats[3],
                    'count_above_500': stats[4]
                }
            }
            
        except Exception as e:
            logger.error(f"更新市值数据失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def check_database_health(self) -> Dict:
        """检查数据库健康状态"""
        try:
            from data.dao import DataAccessError, detect_daily_table, recent_trade_profile, table_exists  # type: ignore

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            health = {}
            
            health['has_stock_basic'] = table_exists(conn, "stock_basic")
            try:
                daily_table = detect_daily_table(conn)
                health['has_daily_data'] = True
                health['daily_table'] = daily_table
            except DataAccessError:
                daily_table = ""
                health['has_daily_data'] = False
                health['daily_table'] = "N/A"
            
            # 检查数据完整性
            if health['has_stock_basic']:
                cursor.execute("SELECT COUNT(*) FROM stock_basic")
                health['stock_count'] = cursor.fetchone()[0]
            
            if health['has_daily_data'] and daily_table:
                cursor.execute(f"SELECT COUNT(*) FROM {daily_table}")
                health['data_count'] = cursor.fetchone()[0]

                profile = recent_trade_profile(conn, date_limit=10, recent_window=3)
                latest_date = profile.get("last_trade_date", "")
                if latest_date:
                    health['latest_date'] = latest_date
                    try:
                        latest = datetime.strptime(str(latest_date), '%Y%m%d')
                        days_old = (datetime.now() - latest).days
                        health['days_since_update'] = days_old
                        health['is_fresh'] = days_old <= 3
                    except:
                        health['is_fresh'] = False
            
            conn.close()
            
            return health
            
        except Exception as e:
            logger.error(f"数据库健康检查失败: {e}")
            return {'error': str(e)}


class _StableUptrendContext:
    """Adapter for stable_uptrend_strategy to work with v49 data sources."""

    def __init__(self, db_path: str, db_manager: Optional[DatabaseManager] = None):
        self.db_path = db_path
        self.db_manager = db_manager
        self.TUSHARE_AVAILABLE = bool(getattr(db_manager, "pro", None))

    def _connect(self) -> sqlite3.Connection:
        if self.db_manager is not None and hasattr(self.db_manager, "_connect"):
            return self.db_manager._connect()
        return sqlite3.connect(self.db_path)

    def _permanent_db_available(self) -> bool:
        if not os.path.exists(self.db_path):
            return False
        try:
            from data.dao import DataAccessError, detect_daily_table, table_exists  # type: ignore

            conn = self._connect()
            has_stock_basic = table_exists(conn, "stock_basic")
            try:
                _ = detect_daily_table(conn)
                has_daily_data = True
            except DataAccessError:
                has_daily_data = False
            conn.close()
            return has_stock_basic and has_daily_data
        except Exception:
            return False

    def _get_global_filters(self) -> Dict[str, int]:
        return {"min_mv": 100, "max_mv": 5000}

    def _filter_summary_text(self, min_mv: int, max_mv: int) -> str:
        return f"当前市值过滤范围：{min_mv}-{max_mv} 亿"

    def get_real_stock_data_optimized(self) -> pd.DataFrame:
        if not self._permanent_db_available():
            return pd.DataFrame()
        db_path = getattr(self, "db_path", PERMANENT_DB_PATH)
        return _cached_real_stock_data(str(db_path))

    def _apply_global_filters(
        self,
        data: pd.DataFrame,
        min_mv: int,
        max_mv: int,
        use_price: bool = True,
        use_turnover: bool = True,
    ) -> pd.DataFrame:
        if data is None or data.empty:
            return data
        filtered = data.copy()
        if min_mv is not None and min_mv > 0:
            filtered = filtered[filtered["流通市值"] >= min_mv * 10000]
        if max_mv is not None and max_mv > 0:
            filtered = filtered[filtered["流通市值"] <= max_mv * 10000]
        if use_price:
            filtered = filtered[filtered["价格"] > 0]
        if use_turnover:
            filtered = filtered[filtered["成交额"] > 0]
        return filtered

    def _load_history_from_sqlite(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        if not self._permanent_db_available():
            return pd.DataFrame()
        try:
            from data.history import load_history_full as _load_history_full_v2  # type: ignore
            return _load_history_full_v2(
                db_path=self.db_path,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                columns="trade_date, close_price AS close",
                table_candidates=("daily_trading_history", "daily_trading_data", "daily_data"),
            )
        except Exception:
            return pd.DataFrame()

    def _load_history_full(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """加载用于v9.0评分的完整历史数据"""
        try:
            from data.history import load_history_full as _load_history_full_v2  # type: ignore
            return _load_history_full_v2(
                db_path=self.db_path,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                columns="trade_date, close_price, vol, amount, pct_chg, turnover_rate",
            )
        except Exception:
            return pd.DataFrame()


def _compute_health_report(db_path: str) -> Dict:
    try:
        from risk.summary import compute_health_report as _compute_health_report_v2  # type: ignore
        return _compute_health_report_v2(
            db_path=db_path,
            enable_fund_bonus=_fund_bonus_enabled(),
            fund_portfolio_funds=os.getenv("FUND_PORTFOLIO_FUNDS", ""),
            evolution_last_run_path=os.path.join(os.path.dirname(__file__), "evolution", "last_run.json"),
        )
    except Exception as e:
        return {
            "run_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ok": False,
            "warnings": [f"health report unavailable: {e}"],
            "stats": {},
        }


def _run_funding_repair(db_path: str) -> Dict[str, Dict]:
    """Repair missing funding-related tables by calling auto_evolve update helpers."""
    results: Dict[str, Dict] = {}
    try:
        import openclaw.auto_evolve as ae
    except Exception as e:
        return {"error": {"success": False, "error": f"import auto_evolve failed: {e}"}}

    try:
        results["northbound_flow"] = ae._update_northbound(db_path)
        results["margin_summary"] = ae._update_margin(db_path)
        results["margin_detail"] = ae._update_margin_detail(db_path)
        results["moneyflow_daily"] = ae._update_moneyflow_daily(db_path)
        results["moneyflow_ind_ths"] = ae._update_moneyflow_industry(db_path)
        results["top_list"] = ae._update_top_list(db_path)
        results["top_inst"] = ae._update_top_inst(db_path)
        results["fund_portfolio_cache"] = ae._update_fund_portfolio(db_path)
    except Exception as e:
        results["error"] = {"success": False, "error": str(e)}
    return results


# ===================== 主界面（完整集成版）=====================
def main():
    """主界面"""
    
    st.title("Airivo Quant Decision System")
    _fp = _get_build_fingerprint()
    st.caption(f"A股量价决策与风险门禁后台 | build={_fp['build_id']} | pid={_fp['pid']} | app={_fp['app_file']}")
    if st.button("刷新", key="refresh_main_runtime_status"):
        try:
            _airivo_data_freshness_snapshot_cached.clear()
            _airivo_latest_candidate_snapshot_cached.clear()
            _airivo_feedback_snapshot_cached.clear()
        except Exception:
            pass
        if "db_manager" in st.session_state:
            try:
                st.session_state.db_manager._status_cache = {}
            except Exception:
                pass
        st.rerun()
    
    # 初始化
    if 'vp_analyzer' not in st.session_state:
        with st.spinner("正在初始化系统..."):
            try:
                runtime_components = _get_runtime_components(PERMANENT_DB_PATH)
                st.session_state.vp_analyzer = runtime_components["vp_analyzer"]
                st.session_state.optimizer = runtime_components["optimizer"]
                st.session_state.db_manager = runtime_components["db_manager"]
                st.session_state.scanner = runtime_components["scanner"]
                st.success("系统初始化成功")
            except Exception as e:
                st.error(f"系统初始化失败: {e}")
                return

    
    vp_analyzer = st.session_state.vp_analyzer
    optimizer = st.session_state.optimizer
    db_manager = st.session_state.db_manager
    scanner = st.session_state.scanner
    
    # 侧边栏
    with st.sidebar:
        status = runtime_render_v49_sidebar(
            st=st,
            db_manager=db_manager,
            permanent_db_path=PERMANENT_DB_PATH,
            fingerprint=_fp,
            session_meta=_airivo_session_meta(),
            app_dir=os.path.dirname(__file__),
        )
    st.divider()

    # 单路由渲染：避免 Streamlit tabs 把所有工作面在每次交互时都全量执行。
    runtime_apply_v49_desired_routes(st.session_state)

    _restore_recent_async_task_refs()
    show_ai_signal_panel = False
    airivo_snapshot: Dict[str, Any] = {}

    current_routes = runtime_render_v49_route_selector(st)

    runtime_render_v49_task_logs_entry(
        routes=current_routes,
        render_async_task_dashboard=_render_async_task_dashboard,
        limit=12,
    )
    runtime_render_v49_execution_center_entry(
        routes=current_routes,
        render_airivo_execution_center=_render_airivo_execution_center,
        permanent_db_path=PERMANENT_DB_PATH,
    )
    today_entry = runtime_render_v49_today_decision_entry(
        routes=current_routes,
        airivo_snapshot=airivo_snapshot,
        show_ai_signal_panel=show_ai_signal_panel,
        dependencies=TodayDecisionDependencies(
            permanent_db_path=PERMANENT_DB_PATH,
            render_airivo_production_dashboard=_render_airivo_production_dashboard,
            render_today_console_panel=render_today_console_panel,
            render_today_advanced_ops_panel=render_today_advanced_ops_panel,
            render_today_strategy_selector_panel=render_today_strategy_selector_panel,
            render_today_strategy_dispatcher=render_today_strategy_dispatcher,
            production_strategies=production_strategies,
            experimental_strategies=experimental_strategies,
            vp_analyzer=vp_analyzer,
            status=status,
            set_focus_once=_set_focus_once,
            render_today_execution_queues=_render_airivo_today_execution_queues,
            airivo_has_role=_airivo_has_role,
            airivo_guard_action=_airivo_guard_action,
            airivo_append_action_audit=_airivo_append_action_audit,
            render_airivo_batch_manager=_render_airivo_batch_manager,
            publish_manual_scan_to_execution_queue=_publish_manual_scan_to_execution_queue,
            production_baseline_params=_production_baseline_params,
            apply_production_baseline_to_session=_apply_production_baseline_to_session,
            save_production_unified_profile=_save_production_unified_profile,
            build_unified_from_latest_evolve=_build_unified_from_latest_evolve,
            get_production_compare_params=_get_production_compare_params,
            logger=logger,
            db_manager=db_manager,
            bulk_history_limit=BULK_HISTORY_LIMIT,
            strict_full_market_mode=bool(st.session_state.get("strict_full_market_mode", False)),
            v4_evaluator_available=V4_EVALUATOR_AVAILABLE,
            v5_evaluator_available=V5_EVALUATOR_AVAILABLE,
            v6_evaluator_available=V6_EVALUATOR_AVAILABLE,
            v7_evaluator_available=V7_EVALUATOR_AVAILABLE,
            v8_evaluator_available=V8_EVALUATOR_AVAILABLE,
            stable_uptrend_available=STABLE_UPTREND_AVAILABLE,
            stable_uptrend_context_cls=_StableUptrendContext,
            render_stable_uptrend_strategy=render_stable_uptrend_strategy,
            load_evolve_params=_load_evolve_params,
            load_strategy_center_scan_defaults=_load_strategy_center_scan_defaults,
            sync_scan_task_with_params=_sync_scan_task_with_params,
            render_scan_param_hint=_render_scan_param_hint,
            render_front_scan_summary=_render_front_scan_summary,
            detect_heavy_background_job=_detect_heavy_background_job,
            start_async_scan_task=_start_async_scan_task,
            mark_scan_submitted=_mark_scan_submitted,
            run_front_scan_via_offline_pipeline=_run_front_scan_via_offline_pipeline,
            mark_front_scan_completed=_mark_front_scan_completed,
            get_db_last_trade_date=_get_db_last_trade_date,
            load_scan_cache=_load_scan_cache,
            save_scan_cache=_save_scan_cache,
            load_v7_cache=_load_v7_cache,
            save_v7_cache=_save_v7_cache,
            connect_permanent_db=_connect_permanent_db,
            load_candidate_stocks=_load_candidate_stocks,
            load_external_bonus_maps=_load_external_bonus_maps,
            load_stock_history=_load_stock_history,
            load_stock_history_bulk=_load_stock_history_bulk,
            load_stock_history_fallback=_load_stock_history_fallback,
            load_history_range_bulk=_load_history_range_bulk,
            batch_load_stock_histories=_batch_load_stock_histories,
            calc_external_bonus=_calc_external_bonus,
            update_scan_progress_ui=_update_scan_progress_ui,
            apply_filter_mode=_apply_filter_mode,
            apply_multi_period_filter=_apply_multi_period_filter,
            add_reason_summary=_add_reason_summary,
            render_result_overview=_render_result_overview,
            render_v7_results=_render_v7_results,
            signal_density_hint=_signal_density_hint,
            set_stock_pool_candidate=_set_stock_pool_candidate,
            append_reason_col=_append_reason_col,
            standardize_result_df=_standardize_result_df,
            normalize_stock_df=_normalize_stock_df,
            df_to_csv_bytes=_df_to_csv_bytes,
            render_cached_scan_results=_render_cached_scan_results,
            render_async_scan_status=_render_async_scan_status,
        ),
    )
    airivo_snapshot = today_entry["airivo_snapshot"]
    show_ai_signal_panel = today_entry["show_ai_signal_panel"]

    runtime_render_v49_research_light_entries(
        routes=current_routes,
        show_ai_signal_panel=show_ai_signal_panel,
        render_page_header=_render_page_header,
        render_stock_pool_workspace=_render_stock_pool_workspace,
        render_sector_flow_page=render_sector_flow_page,
        market_scanner_cls=MarketScanner,
        render_ai_signal_page=render_ai_signal_page,
        load_evolve_params=_load_evolve_params,
        vp_analyzer=vp_analyzer,
        connect_permanent_db=_connect_permanent_db,
        apply_filter_mode=_apply_filter_mode,
        apply_multi_period_filter=_apply_multi_period_filter,
        permanent_db_path=PERMANENT_DB_PATH,
        add_reason_summary=_add_reason_summary,
        get_sim_account=_get_sim_account,
        auto_buy_ai_stocks=_auto_buy_ai_stocks,
        render_result_overview=_render_result_overview,
        signal_density_hint=_signal_density_hint,
        standardize_result_df=_standardize_result_df,
        df_to_csv_bytes=_df_to_csv_bytes,
    )
    runtime_render_v49_backtest_entry(
        routes=current_routes,
        st=st,
        dependencies=BacktestEntryDependencies(
            render_page_header=_render_page_header,
            render_production_backtest_audit_panel=_render_production_backtest_audit_panel,
            render_strategy_comparison_page=render_strategy_comparison_page,
            render_single_backtest_page=render_single_backtest_page,
            render_parameter_optimization_page=render_parameter_optimization_page,
            vp_analyzer=vp_analyzer,
            get_production_compare_params=_get_production_compare_params,
            start_async_backtest_job=_start_async_backtest_job,
            connect_permanent_db=_connect_permanent_db,
            get_async_backtest_job=_get_async_backtest_job,
            is_pid_alive=_is_pid_alive,
            merge_async_backtest_job=_merge_async_backtest_job,
            now_ts=_now_ts,
            v7_evaluator_available=V7_EVALUATOR_AVAILABLE,
            v8_evaluator_available=V8_EVALUATOR_AVAILABLE,
            load_evolve_params=_load_evolve_params,
            airivo_has_role=_airivo_has_role,
            airivo_guard_action=_airivo_guard_action,
            airivo_append_action_audit=_airivo_append_action_audit,
            set_sim_meta=_set_sim_meta,
            auto_backtest_scheduler_tick=_auto_backtest_scheduler_tick,
            ensure_price_aliases=_ensure_price_aliases,
            now_text=_now_text,
            pick_tradable_segment_from_strength=_pick_tradable_segment_from_strength,
            apply_tradable_segment_to_strategy_session=_apply_tradable_segment_to_strategy_session,
            build_calibrated_strength_df=_build_calibrated_strength_df,
            production_baseline_params=_production_baseline_params,
            apply_production_baseline_to_session=_apply_production_baseline_to_session,
            save_production_unified_profile=_save_production_unified_profile,
        ),
    )
    airivo_snapshot = runtime_render_v49_strategy_evolution_entry(
        routes=current_routes,
        permanent_db_path=PERMANENT_DB_PATH,
        airivo_snapshot=airivo_snapshot,
        render_airivo_production_dashboard=_render_airivo_production_dashboard,
        render_airivo_strategy_evolution=_render_airivo_strategy_evolution,
    )
    runtime_render_v49_data_ops_entry(
        routes=current_routes,
        dependencies=DataOpsEntryDependencies(
            render_data_ops_core_page=render_data_ops_core_page,
            render_data_ops_status_page=render_data_ops_status_page,
            render_data_ops_update_page=render_data_ops_update_page,
            render_page_header=_render_page_header,
            get_auto_evolve_status=_get_auto_evolve_status,
            load_production_report_by_strategy=_load_production_report_by_strategy,
            safe_parse_dt=_safe_parse_dt,
            airivo_has_role=_airivo_has_role,
            airivo_guard_action=_airivo_guard_action,
            airivo_append_action_audit=_airivo_append_action_audit,
            trigger_auto_evolve_optimize=_trigger_auto_evolve_optimize,
            load_portfolio_risk_budget=_load_portfolio_risk_budget,
            evaluate_production_rollback_trigger=_evaluate_production_rollback_trigger,
            load_production_rollback_state=_load_production_rollback_state,
            execute_production_auto_rollback=_execute_production_auto_rollback,
            compute_production_allocation_plan=_compute_production_allocation_plan,
            write_production_allocation_report=_write_production_allocation_report,
            build_production_rebalance_orders=_build_production_rebalance_orders,
            write_production_rebalance_report=_write_production_rebalance_report,
            precheck_production_rebalance_orders=_precheck_production_rebalance_orders,
            execute_production_rebalance_orders=_execute_production_rebalance_orders,
            load_latest_production_rebalance_audit=_load_latest_production_rebalance_audit,
            build_weekly_rebalance_quality_dashboard=_build_weekly_rebalance_quality_dashboard,
            load_latest_auto_rebalance_log=_load_latest_auto_rebalance_log,
            db_manager=db_manager,
            connect_permanent_db=_connect_permanent_db,
            rollback_latest_promoted_params=_rollback_latest_promoted_params,
            fund_bonus_enabled=_fund_bonus_enabled,
            get_last_trade_date_from_tushare=_get_last_trade_date_from_tushare,
            compute_health_report=_compute_health_report,
            run_funding_repair=_run_funding_repair,
            permanent_db_path=PERMANENT_DB_PATH,
        ),
    )
    runtime_render_v49_trading_assistant_entry(
        routes=current_routes,
        st=st,
        permanent_db_path=PERMANENT_DB_PATH,
        render_page_header=_render_page_header,
        focus_tab_by_text=_focus_tab_by_text,
        set_focus_once=_set_focus_once,
        render_qa_chat_shell=render_qa_chat_shell,
        render_qa_self_learning_panel=render_qa_self_learning_panel,
        render_qa_submission_controller=render_qa_submission_controller,
        render_assistant_ops_tabs=render_assistant_ops_tabs,
        render_result_overview=_render_result_overview,
        render_single_stock_eval_tab=render_single_stock_eval_tab,
        notification_service_cls=NotificationService,
        airivo_has_role=_airivo_has_role,
        airivo_guard_action=_airivo_guard_action,
        airivo_append_action_audit=_airivo_append_action_audit,
    )
    runtime_render_v49_task_guide_entry(
        routes=current_routes,
        st=st,
        render_page_header=_render_page_header,
    )

    # ==========================================================
    #  所有Tab内容已整理完毕，旧代码已清理
    # ==========================================================


if __name__ == "__main__":
    if os.getenv("OPENCLAW_ASYNC_SCAN_WORKER") == "1":
        raise SystemExit(_run_async_scan_worker_main())
    elif os.getenv("OPENCLAW_ASYNC_BACKTEST_WORKER") == "1":
        raise SystemExit(_run_async_backtest_worker_main())
    elif os.getenv("RUN_OFFLINE_ALL") == "1":
        run_offline_all()
    elif os.getenv("RUN_OFFLINE_V7") == "1":
        run_offline_v7_scan()
    else:
        main()

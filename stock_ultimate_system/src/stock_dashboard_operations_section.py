from __future__ import annotations

from typing import Any, Callable

from src.dashboard_operations import build_operations_render_contract, render_operations_section


def _display_status_label(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "待确认"
    normalized = text.lower()
    mapping = {
        "manual_review": "人工复核",
        "blocked": "已阻断",
        "conditional": "受控放行",
        "running": "运行中",
        "running_daily_research": "每日研究运行中",
        "completed": "已完成",
        "done": "已完成",
        "up_to_date": "已更新",
        "partial_success": "待补齐",
        "pending_window": "受控等待",
        "ready_for_data_check": "等待数据门检查",
        "unknown": "待确认",
        "pass": "通过",
        "failed": "失败",
        "yellow": "黄色观察",
    }
    return mapping.get(normalized, text)


def build_stock_operations_section(
    *,
    visible: bool,
    effective_update_status: dict[str, Any],
    automation_health: dict[str, Any],
    update_health: dict[str, Any],
    update_timeline_panel: str,
    update_alerts_panel: str,
    daily_research_runtime: dict[str, Any],
    research_topology: dict[str, Any],
    research_batch_status: dict[str, Any],
    evolution_status: dict[str, Any],
    grid_backtest_status: dict[str, Any],
    progress_pct_label: str,
    server_sync_preflight: dict[str, Any] | None,
    status_label: Callable[[object], str] = _display_status_label,
) -> str:
    operations_effective_update_status = dict(effective_update_status)
    operations_effective_update_status["status"] = status_label(effective_update_status.get("status"))
    operations_effective_update_status["stage"] = status_label(effective_update_status.get("stage"))
    operations_daily_research_runtime = dict(daily_research_runtime)
    operations_daily_research_runtime["state"] = status_label(daily_research_runtime.get("state"))
    operations_daily_research_runtime["stage"] = status_label(daily_research_runtime.get("stage"))
    operations_research_batch_status = dict(research_batch_status)
    operations_research_batch_status["status"] = status_label(research_batch_status.get("status"))

    return render_operations_section(
        build_operations_render_contract(
            visible=visible,
            effective_update_status=operations_effective_update_status,
            automation_health=automation_health,
            update_health=update_health,
            update_timeline_panel=update_timeline_panel,
            update_alerts_panel=update_alerts_panel,
            daily_research_runtime=operations_daily_research_runtime,
            research_topology=research_topology,
            research_batch_status=operations_research_batch_status,
            evolution_status=evolution_status,
            grid_backtest_status=grid_backtest_status,
            progress_pct_label=progress_pct_label,
            server_sync_preflight=server_sync_preflight or {},
        )
    )

from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from src.dashboard_support import public_profile_list_text, public_profile_text, public_search_mode_text


def _surface_operation_note(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "当前研究口径已固化"
    lowered = text.lower()
    if lowered in {"current", "current research semantics fixed"}:
        return "当前研究口径已固化"
    return "当前研究口径已固化"


def _surface_failed_step(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "无"
    mapping = {
        "grid_backtest": "长窗验证",
        "validation": "样本验证",
        "candidate_generation": "候选生成",
        "candidate_scan": "候选扫描",
        "research_pool": "研究池构建",
    }
    return mapping.get(text, text)


def _surface_reviewability(
    *,
    effective_update_status: dict[str, str],
    daily_research_runtime: dict[str, str],
    research_batch_status: dict[str, str],
) -> str:
    values = " ".join(
        [
            str(effective_update_status.get("status", "")).lower(),
            str(daily_research_runtime.get("state", "")).lower(),
            str(research_batch_status.get("status", "")).lower(),
        ]
    )
    if any(token in values for token in ("failed", "blocked")):
        return "暂停复核"
    if any(token in values for token in ("partial_success", "running")):
        return "待补齐"
    return "可继续复核"


def _surface_pending_stage(
    *,
    effective_update_status: dict[str, str],
    daily_research_runtime: dict[str, str],
    research_batch_status: dict[str, str],
) -> str:
    failed_step = _surface_failed_step(research_batch_status.get("failed_step"))
    if failed_step != "无":
        return failed_step
    if str(effective_update_status.get("post_candidates", "")).strip() not in {"成功", "-", ""}:
        return "候选生成"
    if str(effective_update_status.get("post_daily_research", "")).strip() not in {"成功", "-", ""}:
        return "每日研究"
    failed_profiles = public_profile_list_text(daily_research_runtime.get("failed_profiles"))
    if failed_profiles != "-":
        return "待复核批次"
    return "等待新增样本"


def _surface_review_blocker(
    *,
    effective_update_status: dict[str, str],
    daily_research_runtime: dict[str, str],
    research_batch_status: dict[str, str],
) -> str:
    failed_step = _surface_failed_step(research_batch_status.get("failed_step"))
    if failed_step != "无":
        return f"{failed_step} 尚未闭合，先不推进正式判断"
    if str(effective_update_status.get("post_candidates", "")).strip() not in {"成功", "-", ""}:
        return "候选链尚未稳定闭合，先不推进正式判断"
    if str(effective_update_status.get("post_daily_research", "")).strip() not in {"成功", "-", ""}:
        return "每日研究产物尚未闭合，先不推进正式判断"
    if str(daily_research_runtime.get("state", "")).strip().lower() == "running":
        return "每日研究仍在运行，先等待正式产物完成"
    if str(effective_update_status.get("status", "")).strip().lower() == "partial_success":
        return "运行产物已生成，但仍有环节待补齐"
    return "当前链路可复核，但不直接构成收益保证"


def _surface_review_materials(
    *,
    daily_research_runtime: dict[str, str],
    grid_backtest_status: dict[str, str],
) -> str:
    materials: list[str] = ["当前判断", "每日研究原文"]
    if str(grid_backtest_status.get("latest_csv", "")).strip() not in {"", "-"}:
        materials.append("最新回测报告")
    if str(daily_research_runtime.get("health_score", "")).strip() not in {"", "-"}:
        materials.append("健康趋势")
    return " / ".join(materials)


def _surface_chain_health_label(value: object) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "completed": "已完成",
        "up_to_date": "已对齐",
        "partial_success": "待补齐",
        "failed": "待复核",
        "blocked": "待复核",
        "running": "运行中",
        "success": "已完成",
    }
    return mapping.get(text, str(value or "").strip() or "待确认")


def _surface_rows_written(value: object) -> str:
    text = str(value or "").strip()
    if text in {"", "-"}:
        return "待补齐"
    if text in {"0", "0 行"}:
        return "待补齐"
    return text


def _surface_candidate_chain_result(value: object) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "success": "已完成",
        "成功": "已完成",
        "failed": "待复核",
        "失败": "待复核",
        "blocked": "待补齐",
        "partial_success": "待补齐",
        "-": "待确认",
        "": "待确认",
    }
    return mapping.get(text, str(value or "").strip() or "待确认")


def _surface_candidate_mode(value: object) -> str:
    text = str(value or "").strip().lower()
    mapping = {
        "quick": "快批",
        "nightly": "夜间批次",
        "-": "待确认",
        "": "待确认",
    }
    return mapping.get(text, str(value or "").strip() or "待确认")


def _surface_server_sync_summary(preflight: dict[str, Any]) -> dict[str, str]:
    if preflight.get("preflight_version") != "server_sync_preflight.v1":
        return {
            "status": "待预检",
            "next_action": "先运行服务器同步预检，再生成同步文件清单",
            "blocking": "预检证据缺失",
            "source": "未发现有效预检报告",
        }
    decision = preflight.get("sync_decision")
    manifest_summary = preflight.get("manifest_summary") if isinstance(preflight.get("manifest_summary"), dict) else {}
    if not isinstance(decision, dict):
        return {
            "status": "待复核",
            "next_action": "预检报告缺少同步决策对象，先重新生成预检",
            "blocking": "sync_decision 缺失",
            "source": str(preflight.get("_source_path", "预检报告")),
        }
    blocking_checks = decision.get("blocking_checks") or []
    blocking_label = "无" if not blocking_checks else " / ".join(str(item) for item in blocking_checks)
    allowed_total = str(manifest_summary.get("allowed_total", "-"))
    denied_total = str(manifest_summary.get("denied_total", "-"))
    return {
        "status": "可同步" if decision.get("allowed_to_sync") is True else "阻断同步",
        "next_action": str(decision.get("next_action", "下一步待确认")),
        "blocking": blocking_label,
        "source": f'允许 {allowed_total} 个文件，阻断运行态 {denied_total} 个文件',
    }


def _surface_failed_profiles(value: object) -> str:
    surfaced = public_profile_list_text(value)
    if surfaced == "-":
        return "无"
    return surfaced


def _surface_alert_panel_html(panel_html: str) -> str:
    text = panel_html
    replacements = {
        "partial_success": "待补齐",
        "failed": "待复核",
        "blocked": "待复核",
        "quick": "快批",
        "写入行数 0": "写入仍待补齐",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def build_operations_render_contract(
    *,
    visible: bool,
    effective_update_status: dict[str, str],
    automation_health: dict[str, str],
    update_health: dict[str, str],
    update_timeline_panel: str,
    update_alerts_panel: str,
    daily_research_runtime: dict[str, str],
    research_topology: dict[str, str],
    research_batch_status: dict[str, str],
    evolution_status: dict[str, str | list[dict[str, str]]],
    grid_backtest_status: dict[str, str],
    progress_pct_label: str,
    server_sync_preflight: dict[str, Any] | None = None,
) -> dict[str, Any]:
    latest_csv = str(grid_backtest_status["latest_csv"])
    latest_csv_name = Path(latest_csv).name if latest_csv != "-" else "-"
    return {
        "visible": visible,
        "effective_update_status": effective_update_status,
        "automation_health": automation_health,
        "update_health": update_health,
        "update_timeline_panel": update_timeline_panel,
        "update_alerts_panel": update_alerts_panel,
        "daily_research_runtime": daily_research_runtime,
        "research_topology": research_topology,
        "research_batch_status": research_batch_status,
        "evolution_status": evolution_status,
        "grid_backtest_status": grid_backtest_status,
        "progress_pct_label": progress_pct_label,
        "latest_csv_name": latest_csv_name,
        "server_sync_preflight": server_sync_preflight or {},
    }


def render_operations_section(operations_render_contract: dict[str, Any]) -> str:
    if not bool(operations_render_contract.get("visible")):
        return ""
    effective_update_status = operations_render_contract["effective_update_status"]
    automation_health = operations_render_contract["automation_health"]
    update_health = operations_render_contract["update_health"]
    update_timeline_panel = str(operations_render_contract["update_timeline_panel"])
    update_alerts_panel = _surface_alert_panel_html(str(operations_render_contract["update_alerts_panel"]))
    daily_research_runtime = operations_render_contract["daily_research_runtime"]
    research_topology = operations_render_contract["research_topology"]
    research_batch_status = operations_render_contract["research_batch_status"]
    evolution_status = operations_render_contract["evolution_status"]
    grid_backtest_status = operations_render_contract["grid_backtest_status"]
    progress_pct_label = str(operations_render_contract["progress_pct_label"])
    latest_csv_name = str(operations_render_contract["latest_csv_name"])
    server_sync = _surface_server_sync_summary(dict(operations_render_contract.get("server_sync_preflight") or {}))
    reviewability = _surface_reviewability(
        effective_update_status=effective_update_status,
        daily_research_runtime=daily_research_runtime,
        research_batch_status=research_batch_status,
    )
    pending_stage = _surface_pending_stage(
        effective_update_status=effective_update_status,
        daily_research_runtime=daily_research_runtime,
        research_batch_status=research_batch_status,
    )
    review_blocker = _surface_review_blocker(
        effective_update_status=effective_update_status,
        daily_research_runtime=daily_research_runtime,
        research_batch_status=research_batch_status,
    )
    review_materials = _surface_review_materials(
        daily_research_runtime=daily_research_runtime,
        grid_backtest_status=grid_backtest_status,
    )
    return (
        '<div class="card" id="ops">'
        '<div class="section-title">'
        '<div>'
        '<div class="eyebrow">内部复核</div>'
        '<h3>当前链路复核</h3>'
        '</div>'
        '<div class="muted">先回答当前链路是否可复核、缺哪一步、为什么不能推进，以及先看哪份复核材料。</div>'
        '</div>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">当前是否可复核</div><div class="value">{html.escape(reviewability)}</div><div class="sub">先判定链路是否已经具备复核条件</div></div>'
        f'<div class="kpi"><div class="label">待补齐环节</div><div class="value">{html.escape(pending_stage)}</div><div class="sub">缺口补齐前，正式判断继续等待</div></div>'
        f'<div class="kpi"><div class="label">当前不能推进的原因</div><div class="value">{html.escape(review_blocker)}</div><div class="sub">只给公开判断，不把原始工程枚举直接端给首页</div></div>'
        '</div>'
        '<div class="grid3" style="margin-top:12px;">'
        f'<div class="kpi"><div class="label">先看复核材料</div><div class="value">{html.escape(review_materials)}</div><div class="sub">优先从公开研究材料判断，不先翻后台字段</div></div>'
        f'<div class="kpi"><div class="label">自动链路健康</div><div class="value">{html.escape(_surface_chain_health_label(automation_health["label"]))}</div><div class="sub">{html.escape(automation_health["detail"])}</div></div>'
        f'<div class="kpi"><div class="label">长窗样本推进</div><div class="value">{html.escape(progress_pct_label)}</div><div class="sub">近7次更新成功率 {html.escape(update_health["success_rate_7d"])}</div></div>'
        '</div>'
        '<div class="grid3" style="margin-top:12px;">'
        f'<div class="kpi"><div class="label">服务器同步门禁</div><div class="value">{html.escape(server_sync["status"])}</div><div class="sub">{html.escape(server_sync["source"])}</div></div>'
        f'<div class="kpi"><div class="label">同步阻断项</div><div class="value">{html.escape(server_sync["blocking"])}</div><div class="sub">阻断项未清零前，不生成 rsync 执行面</div></div>'
        f'<div class="kpi"><div class="label">同步下一步</div><div class="value">{html.escape(server_sync["next_action"])}</div><div class="sub">同步结论来自 preflight，不由 UI 手工判断</div></div>'
        '</div>'
        f'<div class="footer-note">链路完成时间 {html.escape(effective_update_status["last_run"])} ｜ 最新交易日 {html.escape(effective_update_status["db_latest"])} ｜ 写入状态 {_surface_rows_written(effective_update_status["written_rows"])}</div>'
        f'<div class="footer-note">候选生成 {html.escape(_surface_candidate_chain_result(effective_update_status["post_candidates"]))} ｜ 每日研究 {html.escape(_surface_candidate_chain_result(effective_update_status["post_daily_research"]))} ｜ 耗时 {html.escape(effective_update_status["duration"])}</div>'
        '<div class="ops-subgrid">'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">复核材料</div>'
        '<h4>最近执行时间线</h4>'
        f'{update_timeline_panel}'
        '</div>'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">链路提醒</div>'
        '<h4>当前复核提醒</h4>'
        f'{update_alerts_panel}'
        '</div>'
        '</div>'
        '<div class="ops-subgrid" style="margin-top:12px;">'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">研究运行</div>'
        '<h4>每日研究运行明细</h4>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">状态 / 阶段</div><div class="value">{html.escape(daily_research_runtime["state"])} / {html.escape(daily_research_runtime["stage"])}</div></div>'
        f'<div class="kpi"><div class="label">启动时间 / 耗时</div><div class="value">{html.escape(daily_research_runtime["started_at"])} / {html.escape(daily_research_runtime["duration"])}</div></div>'
        f'<div class="kpi"><div class="label">健康分 / 告警数</div><div class="value">{html.escape(daily_research_runtime["health_score"])} / {html.escape(daily_research_runtime["alert_count"])}</div></div>'
        '</div>'
        f'<div class="footer-note">当前批次：{html.escape(public_profile_text(daily_research_runtime["active_profile"]))} ｜ 当前进度：{html.escape(daily_research_runtime["active_progress"])}</div>'
        f'<div class="footer-note">已完成批次：{html.escape(public_profile_list_text(daily_research_runtime["completed_profiles"]))} ｜ 待复核批次：{html.escape(_surface_failed_profiles(daily_research_runtime["failed_profiles"]))}</div>'
        f'<div class="footer-note">执行口径：{html.escape(public_search_mode_text(daily_research_runtime["search_mode"]))} ｜ 研究配置：{html.escape(_surface_operation_note(daily_research_runtime["experiment"]))} ｜ 研究池流动性剔除：{html.escape(daily_research_runtime["liquidity_filtered_out"])} ｜ 候选模式 {html.escape(_surface_candidate_mode(effective_update_status["post_candidates_mode"]))} ｜ 候选耗时 {html.escape(effective_update_status["post_candidates_elapsed_sec"])}s</div>'
        '</div>'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">样本与口径</div>'
        '<h4>研究规模与样本边界</h4>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">每日候选扫描 / TopN</div><div class="value">{html.escape(research_topology["candidate_scan_scope"])} / {html.escape(research_topology["candidate_top_n"])}</div></div>'
        f'<div class="kpi"><div class="label">正式研究池规则 / 规模</div><div class="value">{html.escape(research_topology["formal_research_pool_rule"])} / {html.escape(research_topology["formal_research_pool_size"])}</div></div>'
        f'<div class="kpi"><div class="label">夜间扫描规模 / 周级长回测池</div><div class="value">{html.escape(research_topology["nightly_universe_size"])} / {html.escape(research_topology["weekly_long_pool_size"])}</div></div>'
        '</div>'
        '<div class="footer-note">当前线上策略：候选扫描走全A；正式研究池放开流动性硬过滤、改为软约束；周末长回测扩大样本验证。</div>'
        '</div>'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">候选与验证</div>'
        '<h4>夜间候选与验证批次</h4>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">状态 / 最近完成</div><div class="value">{html.escape(research_batch_status["status"])} / {html.escape(research_batch_status["last_run"])}</div></div>'
        f'<div class="kpi"><div class="label">候选扫描规模 / TopN</div><div class="value">{html.escape(research_batch_status["candidate_universe_size"])} / {html.escape(research_batch_status["candidate_top_n"])}</div></div>'
        f'<div class="kpi"><div class="label">研究池规模 / 待复核步骤</div><div class="value">{html.escape(research_batch_status["stock_pool_size"])} / {html.escape(_surface_failed_step(research_batch_status["failed_step"]))}</div></div>'
        '</div>'
        f'<div class="footer-note">每日批次：{html.escape(public_profile_list_text(research_batch_status["daily_profiles"]))} ｜ 长窗验证批次：{html.escape(public_profile_text(research_batch_status["backtest_profile"]))}</div>'
        f'<div class="footer-note">执行口径：{html.escape(public_search_mode_text(research_batch_status["search_mode"]))} ｜ 研究配置：{html.escape(_surface_operation_note(research_batch_status["experiment"]))} ｜ 流动性阈值：{html.escape(research_batch_status["liquidity_min_turnover"])} ｜ 流动性剔除：{html.escape(research_batch_status["liquidity_filtered_out"])} ｜ 回退池 {html.escape(_surface_rows_written(effective_update_status["post_candidates_used_attempt"]))}</div>'
        '</div>'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">机制迭代</div>'
        '<h4>机制迭代摘要</h4>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">冠军版本 / 最近动作</div><div class="value">{html.escape(str(evolution_status.get("champion_version", "-")))} / {html.escape(str(evolution_status.get("latest_action", "-")))}</div></div>'
        f'<div class="kpi"><div class="label">walk-forward / 稳定性</div><div class="value">{html.escape(str(evolution_status.get("champion_walk_forward_score", "-")))} / {html.escape(str(evolution_status.get("champion_stability", "-")))}</div></div>'
        f'<div class="kpi"><div class="label">冠军模型集</div><div class="value">{html.escape(str(evolution_status.get("champion_models", "-")))}</div></div>'
        '</div>'
        f'<div class="footer-note">最近治理原因：{html.escape(str(evolution_status.get("latest_reason", "-")))}</div>'
        '</div>'
        '<div class="ops-block">'
        '<div class="eyebrow-inline">长窗复核</div>'
        '<h4>长窗验证材料</h4>'
        '<div class="grid3">'
        f'<div class="kpi"><div class="label">最近产物时间</div><div class="value">{html.escape(grid_backtest_status["last_run"])}</div></div>'
        f'<div class="kpi"><div class="label">排名记录数</div><div class="value">{html.escape(grid_backtest_status["rows"])}</div></div>'
        f'<div class="kpi"><div class="label">最新 CSV</div><div class="value">{html.escape(latest_csv_name)}</div></div>'
        '</div>'
        '<div class="grid3" style="margin-top:12px;">'
        f'<div class="kpi"><div class="label">验证窗口 / Replay 次数</div><div class="value">{html.escape(grid_backtest_status["validation_window"])} / {html.escape(grid_backtest_status["replay_runs"])}</div></div>'
        f'<div class="kpi"><div class="label">Regime 覆盖 / 参数敏感度</div><div class="value">{html.escape(grid_backtest_status["regime_coverage_score"])} / {html.escape(grid_backtest_status["parameter_sensitivity_score"])}</div></div>'
        f'<div class="kpi"><div class="label">采样模式 / 已观测 Regime</div><div class="value">{html.escape(grid_backtest_status["sampling_mode"])} / {html.escape(grid_backtest_status["observed_regimes"])}</div></div>'
        '</div>'
        '</div>'
        '</div>'
        '</div>'
    )

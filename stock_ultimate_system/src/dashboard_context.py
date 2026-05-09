from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from src.dashboard_support import (
    SIGNAL_ZH,
    RISK_ZH,
    automation_health_summary,
    backtest_diagnosis,
    backtest_drawdown_area_html,
    backtest_equity_curve_html,
    backtest_metric_chart_html,
    backtest_return_drawdown_chart_html,
    candidate_brief_cards,
    candidate_detail_panel_html,
    candidate_market_snapshot,
    candidate_risk_reward_chart_html,
    candidate_score_chart_html,
    download_href,
    extract_health_metrics,
    file_href,
    fmt_progress_pct,
    health_trend_chart_html,
    latest_backtest_report,
    load_candidate_artifact_status,
    load_csv_rows,
    load_daily_research_runtime_status,
    load_evolution_status,
    load_grid_backtest_status,
    load_prefilter_artifact_status,
    load_recent_update_events,
    load_recent_update_health,
    load_research_batch_status,
    load_research_topology,
    load_update_status,
    read_json,
    read_text,
    resolve_automation_status,
    stock_primary_result_runtime_metadata,
    status_by_score,
    stock_primary_result_card_html,
    summarize_backtest_scope,
    top_candidate_brief,
    translate_generation_mode,
    translate_md_line,
    update_alerts_html,
    update_timeline_html,
)
from src.first_place_evidence_cockpit import (
    build_first_place_evidence_cockpit_view_model,
    render_first_place_evidence_cockpit,
)
from src.primary_result_query_service import build_namespace_home_semantics, build_primary_result_query_view
from src.stock_ai_explainer import build_stock_ai_explainer_result
from src.t12_governance_summary import extract_governance_summary_facts
from src.t12_overview_card import build_t12_overview_minimal_facts
from src.unified_result_builder import build_primary_result
from src.utils.project_paths import (
    resolve_artifacts_path,
    resolve_experiments_path,
    resolve_project_path,
    resolve_reports_path,
)


_DEFAULT_RESOLVE_PROJECT_PATH = resolve_project_path


def _resolve_dashboard_artifacts_path(root: Path) -> Path:
    if resolve_project_path is not _DEFAULT_RESOLVE_PROJECT_PATH:
        try:
            return resolve_project_path("artifacts")
        except KeyError:
            return root / "artifacts"
    return resolve_artifacts_path()


def _resolve_dashboard_experiments_path() -> Path:
    if resolve_project_path is not _DEFAULT_RESOLVE_PROJECT_PATH:
        return resolve_project_path("data/experiments")
    return resolve_experiments_path()


def _resolve_dashboard_reports_path() -> Path:
    if resolve_project_path is not _DEFAULT_RESOLVE_PROJECT_PATH:
        return resolve_project_path("data/reports")
    return resolve_reports_path()


def _latest_json_by_mtime(paths: list[Path]) -> dict[str, Any]:
    existing = [path for path in paths if path.exists() and path.is_file()]
    if not existing:
        return {}
    latest = max(existing, key=lambda path: path.stat().st_mtime)
    payload = read_json(latest)
    if payload:
        payload["_source_path"] = latest.as_posix()
    return payload


def _load_latest_server_sync_preflight(root: Path) -> dict[str, Any]:
    artifact_paths = list((root / "artifacts").glob("server_sync_preflight*.json"))
    local_paths = [
        root / "tmp" / "stock_server_sync_preflight.json",
        Path("/tmp/stock_ultimate_server_sync_preflight.json"),
        Path("/tmp/stock_server_sync_preflight.json"),
    ]
    payload = _latest_json_by_mtime([*artifact_paths, *local_paths])
    if payload.get("preflight_version") != "server_sync_preflight.v1":
        return {}
    return payload


def _resolve_candidate_display_files(exp_dir: Path) -> tuple[Path, Path, str]:
    formal_csv = exp_dir / "candidates_top_latest.csv"
    formal_md = exp_dir / "candidates_top_latest.md"
    if load_csv_rows(formal_csv, limit=1):
        return formal_csv, formal_md, "formal"
    interim_csv = exp_dir / "candidates_top_interim_latest.csv"
    interim_md = exp_dir / "candidates_top_interim_latest.md"
    if load_csv_rows(interim_csv, limit=1):
        return interim_csv, interim_md, "interim"
    return formal_csv, formal_md, "empty"


def _risk_state_label(value: object) -> str:
    mapping = {
        "watch": "正常观察",
        "review": "等待复核",
        "degrade": "降级复核",
        "invalid": "已失效",
        "closed": "已闭环",
    }
    return mapping.get(str(value or "").strip().lower(), str(value or "待确认"))


def _public_explanation_cards(public_explanation: dict[str, Any], *, top_n: int = 5) -> list[dict[str, str]]:
    if public_explanation.get("schema_version") != "candidate_public_explanation.v1":
        return []
    cards: list[dict[str, str]] = []
    for idx, item in enumerate((public_explanation.get("items", []) or [])[:top_n]):
        cards.append(
            {
                "index": str(idx),
                "ts_code": str(item.get("ts_code", "")),
                "stock_name": str(item.get("stock_name", "")),
                "signal": _risk_state_label(item.get("risk_state")),
                "risk_level": "不可正常展示" if not item.get("external_display_allowed") else "只读观察",
                "final_score": "0.0",
                "pred_return": "-",
                "why_watch": str(item.get("why_watch", "")),
                "main_risk": str(item.get("main_risk", "")),
                "invalid_when": str(item.get("invalid_when", "")),
                "next_observation": str(item.get("next_observation", "")),
            }
        )
    return cards


def _public_top_candidate(public_cards: list[dict[str, str]], index: int) -> dict[str, str]:
    if not public_cards or index >= len(public_cards):
        return {"ts_code": "暂无", "stock_name": "", "signal": "-", "final_score": "-", "risk_level": "-"}
    card = public_cards[index]
    return {
        "ts_code": card.get("ts_code", "暂无"),
        "stock_name": card.get("stock_name", ""),
        "signal": card.get("signal", "-"),
        "final_score": card.get("final_score", "-"),
        "risk_level": card.get("risk_level", "-"),
    }


def _public_candidate_detail_html(public_cards: list[dict[str, str]], index: int) -> str:
    if not public_cards:
        return '<div class="chart-empty">暂无外部解释层候选。</div>'
    card = public_cards[min(max(index, 0), len(public_cards) - 1)]
    return (
        '<div class="candidate-detail-card candidate-detail-public">'
        f'<div class="candidate-detail-title">{html.escape(card.get("ts_code", ""))} {html.escape(card.get("stock_name", ""))}</div>'
        f'<div class="muted">状态：{html.escape(card.get("signal", ""))} ｜ 边界：{html.escape(card.get("risk_level", ""))}</div>'
        '<div class="candidate-detail-grid">'
        f'<div><strong>为什么关注</strong><p>{html.escape(card.get("why_watch", ""))}</p></div>'
        f'<div><strong>主要风险</strong><p>{html.escape(card.get("main_risk", ""))}</p></div>'
        f'<div><strong>何时失效</strong><p>{html.escape(card.get("invalid_when", ""))}</p></div>'
        f'<div><strong>下一步观察</strong><p>{html.escape(card.get("next_observation", ""))}</p></div>'
        '</div>'
        '</div>'
    )


def build_dashboard_context(root: Path, candidate_index: int = 0, base_path: str = "") -> dict[str, Any]:
    exp_dir = _resolve_dashboard_experiments_path()
    rep_dir = _resolve_dashboard_reports_path()
    daily_md = exp_dir / "daily_research_latest.md"
    health_csv = exp_dir / "daily_health_trend_latest.csv"
    leaderboard_csv = exp_dir / "backtest_leaderboard.csv"
    candidates_csv, candidates_md, candidate_source_kind = _resolve_candidate_display_files(exp_dir)
    candidate_source_label = {
        "formal": "已确认观察名单",
        "interim": "待确认观察名单",
        "empty": "暂无观察名单",
    }[candidate_source_kind]
    candidates_basket_summary_json = exp_dir / "candidates_basket_summary_latest.json"
    candidates_basket_validation_json = exp_dir / "candidates_basket_validation_latest.json"
    governance_cycle_json = exp_dir / "governance_cycle_latest.json"

    latest_report = latest_backtest_report(rep_dir)
    daily_md_text = read_text(daily_md)
    translated_daily_md_text = "\n".join(translate_md_line(line) for line in daily_md_text.splitlines())
    latest_report_text = read_text(latest_report, "暂无回测报告。")
    health = extract_health_metrics(daily_md_text)
    health_status, health_tag = status_by_score(health["score"])
    candidate_public_explanation = read_json(exp_dir / "candidate_public_explanation_latest.json")
    candidate_risk_state = read_json(exp_dir / "candidate_risk_state_latest.json")
    public_candidate_cards = _public_explanation_cards(candidate_public_explanation, top_n=5)
    candidate_cards = public_candidate_cards or candidate_brief_cards(candidates_csv, top_n=5)
    candidate_external_surface_mode = "public_explanation" if public_candidate_cards else "candidate_csv"
    if public_candidate_cards:
        candidate_source_label = "解释层观察状态"
    if candidate_index < 0 or candidate_index >= max(len(candidate_cards), 1):
        candidate_index = 0
    top1 = (
        _public_top_candidate(public_candidate_cards, candidate_index)
        if public_candidate_cards
        else top_candidate_brief(candidates_csv, index=candidate_index)
    )
    top1_signal = SIGNAL_ZH.get(top1.get("signal", "-"), top1.get("signal", "-"))
    top1_risk = RISK_ZH.get(top1.get("risk_level", "-"), top1.get("risk_level", "-"))
    bt_diag = backtest_diagnosis(leaderboard_csv)
    top1_label = f"{top1['ts_code']}{(' ' + top1.get('stock_name', '')) if top1.get('stock_name') else ''}"
    update_status = load_update_status(exp_dir)
    update_health = load_recent_update_health(root)
    update_events = load_recent_update_events(root, limit=8)

    daily_md_href = file_href(root, daily_md, base_path)
    daily_md_download_href = download_href(root, daily_md, base_path)
    health_csv_href = file_href(root, health_csv, base_path)
    health_csv_download_href = download_href(root, health_csv, base_path)
    leaderboard_href = file_href(root, leaderboard_csv, base_path)
    leaderboard_download_href = download_href(root, leaderboard_csv, base_path)
    candidates_md_href = file_href(root, candidates_md, base_path)
    candidates_md_download_href = download_href(root, candidates_md, base_path)
    candidates_csv_href = file_href(root, candidates_csv, base_path)
    candidates_csv_download_href = download_href(root, candidates_csv, base_path)
    latest_report_href = file_href(root, latest_report, base_path)
    latest_report_download_href = download_href(root, latest_report, base_path)

    health_chart_html = health_trend_chart_html(health_csv)
    backtest_equity_html = backtest_equity_curve_html(leaderboard_csv)
    backtest_drawdown_html = backtest_drawdown_area_html(leaderboard_csv)
    backtest_chart_html = backtest_metric_chart_html(leaderboard_csv)
    backtest_map_chart_html = backtest_return_drawdown_chart_html(leaderboard_csv)
    candidate_chart_html = candidate_score_chart_html(candidates_csv)
    candidate_map_chart_html = candidate_risk_reward_chart_html(candidates_csv)
    candidate_detail_html = (
        _public_candidate_detail_html(public_candidate_cards, candidate_index)
        if public_candidate_cards
        else candidate_detail_panel_html(candidates_csv, index=candidate_index)
    )
    market_snapshot = candidate_market_snapshot(candidates_csv, top_n=10)
    basket_summary = read_json(candidates_basket_summary_json)
    basket_validation = read_json(candidates_basket_validation_json)
    candidates_audit = read_json(exp_dir / "candidates_audit_latest.json")
    governance_cycle = read_json(governance_cycle_json)
    research_batch_status = load_research_batch_status(exp_dir)
    daily_research_runtime = load_daily_research_runtime_status(exp_dir)
    effective_update_status = resolve_automation_status(update_status, daily_research_runtime)
    automation_health = automation_health_summary(effective_update_status)
    candidate_artifact_status = load_candidate_artifact_status(exp_dir)
    prefilter_artifact_status = load_prefilter_artifact_status(exp_dir, effective_update_status)
    research_topology = load_research_topology(root)
    grid_backtest_status = load_grid_backtest_status(exp_dir)
    evolution_status = load_evolution_status(exp_dir)
    update_timeline_panel = update_timeline_html(update_events)
    update_alerts_panel = update_alerts_html(update_status, update_events, daily_research_runtime)
    backtest_scope = summarize_backtest_scope(leaderboard_csv)
    evolution_registry = read_json(exp_dir / "evolution_registry_latest.json")
    primary_result_record = build_primary_result(
        exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=True,
    )
    primary_result = primary_result_record.as_dict()
    primary_result_card_html = stock_primary_result_card_html(primary_result)
    primary_result_runtime_metadata = stock_primary_result_runtime_metadata(primary_result)
    artifacts_dir = _resolve_dashboard_artifacts_path(root)
    server_sync_preflight = _load_latest_server_sync_preflight(root)
    candidate_basket_feedback = read_json(artifacts_dir / "primary_result_candidate_baskets" / "feedback_latest.json")
    first_place_evidence_cockpit = build_first_place_evidence_cockpit_view_model(
        artifacts_dir=artifacts_dir,
        exp_dir=exp_dir,
    )
    first_place_evidence_cockpit_html = render_first_place_evidence_cockpit(first_place_evidence_cockpit)
    t12_minimal_facts = build_t12_overview_minimal_facts(exp_dir)
    t12_governance_source_facts = extract_governance_summary_facts(primary_result)
    governance_cycle_state = str(governance_cycle.get("cycle_state", "unknown") or "unknown")
    governance_recommended_action = str(governance_cycle.get("recommended_action", "manual_review") or "manual_review")
    governance_operator_message = str(governance_cycle.get("operator_message", "治理主链状态暂缺。") or "治理主链状态暂缺。")
    governance_release_readiness = governance_cycle.get("release_readiness", {}) or {}
    governance_fully_release_ready = bool(governance_release_readiness.get("fully_release_ready", False))

    report_state = "已生成" if latest_report else "缺失"
    update_stage = effective_update_status.get("stage", "-")
    run_freshness = effective_update_status.get("last_run", "-")
    progress_pct_label = fmt_progress_pct(str(effective_update_status.get("progress_pct", "-")))
    candidate_score_raw = str(top1.get("final_score", "-") or "-").strip()
    candidate_score = html.escape(candidate_score_raw) if candidate_score_raw and candidate_score_raw != "-" else ""
    candidate_name_raw = str(top1.get("stock_name", "") or "").strip()
    candidate_name = html.escape(candidate_name_raw)
    candidate_count = max(len(candidate_cards), 1)
    generation_mode_label = translate_generation_mode(str(candidate_artifact_status.get("generation_mode", "-")))
    current_basket_pointer_status = str(candidate_artifact_status.get("current_basket_pointer_status", "-") or "-")
    current_basket_pointer_updated_at = str(
        candidate_artifact_status.get("current_basket_pointer_updated_at", "-") or "-"
    )
    current_basket_pointer_basket_id = str(
        candidate_artifact_status.get("current_basket_pointer_basket_id", "-") or "-"
    )
    latest_basket_attempt_status = str(candidate_artifact_status.get("latest_basket_attempt_status", "-") or "-")
    latest_basket_attempt_generated_at = str(
        candidate_artifact_status.get("latest_basket_attempt_generated_at", "-") or "-"
    )
    latest_basket_attempt_blocking_reason = str(
        candidate_artifact_status.get("latest_basket_attempt_blocking_reason", "-") or "-"
    )
    primary_result_query = build_primary_result_query_view(
        primary_result=primary_result,
        health_status=health_status,
        backtest_conclusion=str(bt_diag.get("结论", "未评估") or "未评估"),
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        current_basket_pointer_updated_at=current_basket_pointer_updated_at,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
    )
    summary_lines = list(primary_result_query["summary_lines"])
    headline_tone = str(primary_result_query["headline_tone"])
    headline_detail = str(primary_result_query["headline_detail"])

    liquidity_capacity_state = "可放大"
    weighted_liquidity_score = float(basket_summary.get("weighted_liquidity_score", 0.0) or 0.0)
    liquidity_capacity_weight = float(basket_summary.get("liquidity_capacity_weight", 0.0) or 0.0)
    if liquidity_capacity_weight >= 0.18 or weighted_liquidity_score < 0.58:
        liquidity_capacity_state = "放大量受限"
    elif liquidity_capacity_weight > 0 or weighted_liquidity_score < 0.68:
        liquidity_capacity_state = "放大需观察"

    namespace_home_semantics = build_namespace_home_semantics(
        primary_result=primary_result,
        candidate_artifact_status=candidate_artifact_status,
        prefilter_artifact_status=prefilter_artifact_status,
        governance_release_readiness=governance_release_readiness,
        market_snapshot=market_snapshot,
        bt_diag=bt_diag,
        basket_summary=basket_summary,
        candidates_audit=candidates_audit,
        evolution_status=evolution_status,
        evolution_registry=evolution_registry,
        candidate_cards=candidate_cards,
        liquidity_capacity_state=liquidity_capacity_state,
        current_basket_pointer_status=current_basket_pointer_status,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        db_latest_trade_date=str(effective_update_status.get("db_latest", "-") or "-"),
    )
    stock_ai_explainer = build_stock_ai_explainer_result(
        base_path=base_path,
        primary_result=primary_result,
        primary_result_query=primary_result_query,
        decision_semantics=namespace_home_semantics["decision"],
        blocker_semantics=namespace_home_semantics["blocker"],
        execution_semantics=namespace_home_semantics["execution"],
        evidence_semantics=namespace_home_semantics["evidence"],
        governance_semantics=namespace_home_semantics["governance"],
        current_basket_pointer_status=current_basket_pointer_status,
        current_basket_pointer_basket_id=current_basket_pointer_basket_id,
        current_basket_pointer_updated_at=current_basket_pointer_updated_at,
        latest_basket_attempt_status=latest_basket_attempt_status,
        latest_basket_attempt_generated_at=latest_basket_attempt_generated_at,
        latest_basket_attempt_blocking_reason=latest_basket_attempt_blocking_reason,
        observation_wait_status={},
        storage_dir=artifacts_dir / "stock_ai_runner",
    )

    context = {
        "exp_dir": exp_dir,
        "rep_dir": rep_dir,
        "candidate_index": candidate_index,
        "daily_md": daily_md,
        "health_csv": health_csv,
        "leaderboard_csv": leaderboard_csv,
        "candidates_csv": candidates_csv,
        "candidates_md": candidates_md,
        "candidate_source_kind": candidate_source_kind,
        "candidate_source_label": candidate_source_label,
        "latest_report": latest_report,
        "daily_md_text": daily_md_text,
        "translated_daily_md_text": translated_daily_md_text,
        "latest_report_text": latest_report_text,
        "health": health,
        "health_status": health_status,
        "health_tag": health_tag,
        "candidate_cards": candidate_cards,
        "candidate_public_explanation": candidate_public_explanation,
        "candidate_risk_state": candidate_risk_state,
        "candidate_external_surface_mode": candidate_external_surface_mode,
        "top1": top1,
        "top1_signal": top1_signal,
        "top1_risk": top1_risk,
        "bt_diag": bt_diag,
        "top1_label": top1_label,
        "summary_lines": summary_lines,
        "update_status": update_status,
        "update_health": update_health,
        "update_events": update_events,
        "daily_md_href": daily_md_href,
        "daily_md_download_href": daily_md_download_href,
        "health_csv_href": health_csv_href,
        "health_csv_download_href": health_csv_download_href,
        "leaderboard_href": leaderboard_href,
        "leaderboard_download_href": leaderboard_download_href,
        "candidates_md_href": candidates_md_href,
        "candidates_md_download_href": candidates_md_download_href,
        "candidates_csv_href": candidates_csv_href,
        "candidates_csv_download_href": candidates_csv_download_href,
        "latest_report_href": latest_report_href,
        "latest_report_download_href": latest_report_download_href,
        "health_chart_html": health_chart_html,
        "backtest_equity_html": backtest_equity_html,
        "backtest_drawdown_html": backtest_drawdown_html,
        "backtest_chart_html": backtest_chart_html,
        "backtest_map_chart_html": backtest_map_chart_html,
        "candidate_chart_html": candidate_chart_html,
        "candidate_map_chart_html": candidate_map_chart_html,
        "candidate_detail_html": candidate_detail_html,
        "market_snapshot": market_snapshot,
        "basket_summary": basket_summary,
        "basket_validation": basket_validation,
        "candidates_audit": candidates_audit,
        "governance_cycle": governance_cycle,
        "governance_cycle_state": governance_cycle_state,
        "governance_recommended_action": governance_recommended_action,
        "governance_operator_message": governance_operator_message,
        "governance_release_readiness": governance_release_readiness,
        "governance_fully_release_ready": governance_fully_release_ready,
        "research_batch_status": research_batch_status,
        "daily_research_runtime": daily_research_runtime,
        "effective_update_status": effective_update_status,
        "automation_health": automation_health,
        "candidate_artifact_status": candidate_artifact_status,
        "prefilter_artifact_status": prefilter_artifact_status,
        "research_topology": research_topology,
        "grid_backtest_status": grid_backtest_status,
        "evolution_status": evolution_status,
        "evolution_registry": evolution_registry,
        "server_sync_preflight": server_sync_preflight,
        "update_timeline_panel": update_timeline_panel,
        "update_alerts_panel": update_alerts_panel,
        "backtest_scope": backtest_scope,
        "report_state": report_state,
        "update_stage": update_stage,
        "run_freshness": run_freshness,
        "progress_pct_label": progress_pct_label,
        "candidate_score": candidate_score,
        "candidate_name": candidate_name,
        "candidate_count": candidate_count,
        "generation_mode_label": generation_mode_label,
        "liquidity_capacity_state": liquidity_capacity_state,
        "current_basket_pointer_status": current_basket_pointer_status,
        "current_basket_pointer_updated_at": current_basket_pointer_updated_at,
        "current_basket_pointer_basket_id": current_basket_pointer_basket_id,
        "latest_basket_attempt_status": latest_basket_attempt_status,
        "latest_basket_attempt_generated_at": latest_basket_attempt_generated_at,
        "latest_basket_attempt_blocking_reason": latest_basket_attempt_blocking_reason,
        "headline_tone": headline_tone,
        "headline_detail": headline_detail,
        "primary_result_query": primary_result_query,
        "stock_ai_explainer": stock_ai_explainer,
        "primary_result": primary_result,
        "primary_result_card_html": primary_result_card_html,
        "primary_result_runtime_metadata": primary_result_runtime_metadata,
        "candidate_basket_feedback": candidate_basket_feedback,
        "first_place_evidence_cockpit": first_place_evidence_cockpit,
        "first_place_evidence_cockpit_html": first_place_evidence_cockpit_html,
        "t12_minimal_facts": t12_minimal_facts,
        "t12_governance_source_facts": t12_governance_source_facts,
        "namespace_home_semantics": namespace_home_semantics,
    }
    return context

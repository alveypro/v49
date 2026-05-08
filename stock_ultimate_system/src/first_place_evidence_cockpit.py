from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from src.utils.project_paths import resolve_artifacts_path, resolve_experiments_path


FIRST_PLACE_EVIDENCE_COCKPIT_VERSION = "first_place_evidence_cockpit.v1"
FIRST_PLACE_EVIDENCE_FLOOR = 20
WAIT_STATUS_COMMAND = "python stock_ultimate_system/scripts/inspect_primary_result_observation_wait_status.py --json"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_generated_at(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _normalize_text(value: object, fallback: str = "-") -> str:
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def _display_status_label(value: object) -> str:
    text = _normalize_text(value)
    mapping = {
        "pending_window": "受控等待",
        "ready_for_data_check": "等待数据门检查",
        "blocked": "已阻断",
        "conditional": "受控放行",
        "locked": "已锁定",
        "gate check required": "需要数据门检查",
        "yellow": "黄色观察",
        "unknown": "待确认",
    }
    return mapping.get(text.lower(), text)


def _display_promotion_label(value: object) -> str:
    text = _normalize_text(value)
    if text.lower() == "blocked":
        return "晋级锁定"
    return _display_status_label(text)


def _display_reason(value: object) -> str:
    text = _normalize_text(value)
    mapping = {
        "current date must be on or after observation window start before closure checks": "当前日期尚未到观察窗口开始日，不能执行闭合检查",
        "performance evidence must be ready before promotion review": "表现证据尚未满足晋级复核条件",
        "requires at least 20 ledger entries": "至少需要 20 条账本样本",
    }
    return mapping.get(text.lower(), text)


def _stream_entry_total(performance_evidence: dict[str, Any], stream_id: str) -> int:
    streams = performance_evidence.get("streams")
    if not isinstance(streams, list):
        return 0
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        if stream.get("stream_id") != stream_id:
            continue
        try:
            return int(stream.get("entry_total") or 0)
        except Exception:
            return 0
    return 0


def _stream_first_floor_blocking_reasons(performance_evidence: dict[str, Any], stream_id: str) -> tuple[str, ...]:
    streams = performance_evidence.get("streams")
    if not isinstance(streams, list):
        return tuple()
    for stream in streams:
        if not isinstance(stream, dict) or stream.get("stream_id") != stream_id:
            continue
        windows = stream.get("windows")
        if not isinstance(windows, list) or not windows:
            return tuple()
        first_window = windows[0] if isinstance(windows[0], dict) else {}
        reasons = first_window.get("blocking_reasons")
        if not isinstance(reasons, list):
            return tuple()
        return tuple(_normalize_text(reason) for reason in reasons if _normalize_text(reason))
    return tuple()


def _blocking_reasons(payload: dict[str, Any]) -> tuple[str, ...]:
    reasons = payload.get("blocking_reasons")
    if not isinstance(reasons, list):
        return tuple()
    return tuple(_normalize_text(reason) for reason in reasons if _normalize_text(reason))


def _needed(entry_total: int, floor: int = FIRST_PLACE_EVIDENCE_FLOOR) -> int:
    return max(floor - max(entry_total, 0), 0)


def _window_start_date(window_start: str) -> str:
    if "T" in window_start:
        return window_start.split("T", 1)[0]
    return window_start[:10] if len(window_start) >= 10 else window_start


def _closure_window_start(closure_payload: dict[str, Any]) -> str:
    return _normalize_text(closure_payload.get("window_start"))


def _should_prefer_closure_wait_state(wait_status: dict[str, Any], daily_closure: dict[str, Any]) -> bool:
    if not daily_closure:
        return False
    closure_status = _normalize_text(daily_closure.get("status"), "")
    if closure_status != "pending_window":
        return False
    if not _closure_window_start(daily_closure) or not daily_closure.get("generated_at"):
        return False
    wait_generated_at = _parse_generated_at(wait_status.get("generated_at"))
    closure_generated_at = _parse_generated_at(daily_closure.get("generated_at"))
    if closure_generated_at is None:
        return False
    if wait_generated_at is None:
        return True
    return closure_generated_at >= wait_generated_at


def _decision_copy(status: str, window_start: str) -> dict[str, str]:
    next_check = _window_start_date(window_start)
    if status == "pending_window":
        return {
            "decision_status": "受控等待",
            "decision_reason": "观察窗口尚未开始",
            "next_check": next_check,
            "decision_summary": "所有样本闭合动作已锁定",
        }
    if status == "ready_for_data_check":
        return {
            "decision_status": "等待数据门检查",
            "decision_reason": "观察窗口已开始，仍需真实市场数据覆盖",
            "next_check": "现在检查数据门",
            "decision_summary": "只有数据门通过后才允许闭合",
        }
    return {
        "decision_status": "状态待复核",
        "decision_reason": "等待状态证据不完整或异常",
        "next_check": next_check,
        "decision_summary": "闭合动作保持锁定",
    }


def _progress_label(entry_total: int, floor: int = FIRST_PLACE_EVIDENCE_FLOOR) -> str:
    return f"{max(entry_total, 0)}/{floor}"


def _progress_pct(entry_total: int, floor: int = FIRST_PLACE_EVIDENCE_FLOOR) -> float:
    if floor <= 0:
        return 0.0
    return max(0.0, min(100.0, (float(entry_total) / float(floor)) * 100.0))


def build_first_place_evidence_cockpit_view_model(
    *,
    artifacts_dir: str | Path | None = None,
    exp_dir: str | Path | None = None,
) -> dict[str, Any]:
    resolved_artifacts_dir = resolve_artifacts_path() if artifacts_dir is None else Path(artifacts_dir)
    resolved_exp_dir = resolve_experiments_path() if exp_dir is None else Path(exp_dir)
    current_result_pointer = _read_json(resolved_artifacts_dir / "current_result_pointer" / "current.json")
    wait_status = _read_json(resolved_artifacts_dir / "primary_result_observation_wait_status_latest.json")
    daily_closure = _read_json(resolved_exp_dir / "primary_result_daily_closure_latest.json")
    performance_evidence = _read_json(resolved_artifacts_dir / "primary_result_performance_evidence_latest.json")
    promotion_gate = _read_json(resolved_artifacts_dir / "primary_result_promotion_readiness_gate_latest.json")
    scoreboard = _read_json(resolved_artifacts_dir / "primary_result_daily_operations_scoreboard_latest.json")

    primary_entries = _stream_entry_total(performance_evidence, "primary_result")
    basket_entries = _stream_entry_total(performance_evidence, "candidate_basket")
    window = wait_status.get("observation_window") if isinstance(wait_status.get("observation_window"), dict) else {}
    promotion_decision = _normalize_text(promotion_gate.get("decision") or promotion_gate.get("status"), "blocked")
    wait_blocking_reasons = _blocking_reasons(wait_status)
    promotion_blocking_reasons = _blocking_reasons(promotion_gate)
    primary_blocking_reasons = _stream_first_floor_blocking_reasons(performance_evidence, "primary_result")
    basket_blocking_reasons = _stream_first_floor_blocking_reasons(performance_evidence, "candidate_basket")

    primary_needed = _needed(primary_entries)
    basket_needed = _needed(basket_entries)
    closure_preferred = _should_prefer_closure_wait_state(wait_status, daily_closure)
    observation_window_start = _normalize_text(window.get("started_at") if isinstance(window, dict) else None)
    status = _normalize_text(wait_status.get("status"), "unknown")
    generated_at = _normalize_text(wait_status.get("generated_at"))
    current_date = _normalize_text(wait_status.get("current_date"))
    if closure_preferred:
        observation_window_start = _closure_window_start(daily_closure)
        status = _normalize_text(daily_closure.get("status"), status)
        generated_at = _normalize_text(daily_closure.get("generated_at"), generated_at)
    decision_copy = _decision_copy(status, observation_window_start)
    action_locks = (
        {
            "action": "闭合 primary_result",
            "state": "已锁定" if status != "ready_for_data_check" else "需要数据门检查",
            "reason": _display_reason(wait_blocking_reasons[0]) if wait_blocking_reasons else "等待真实市场数据门检查",
        },
        {
            "action": "闭合 candidate_basket",
            "state": "已锁定" if status != "ready_for_data_check" else "需要数据门检查",
            "reason": f"观察窗口开始于 {observation_window_start}",
        },
        {
            "action": "写入 performance ledger",
            "state": "已锁定",
            "reason": "没有通过闭合门的真实样本",
        },
        {
            "action": "晋级",
            "state": "已锁定",
            "reason": _display_reason(promotion_blocking_reasons[0]) if promotion_blocking_reasons else "表现证据仍在积累",
        },
        {
            "action": "策略变更",
            "state": "已锁定",
            "reason": "当前 sprint 只允许补证据链缺口",
        },
    )

    return {
        "cockpit_version": FIRST_PLACE_EVIDENCE_COCKPIT_VERSION,
        "fact_source_role": "evidence_display_only",
        "fact_source_boundary": "cockpit does not determine primary result truth; it only displays evidence and action locks around the current governed result",
        "status": status,
        **decision_copy,
        "generated_at": generated_at,
        "result_id": _normalize_text(wait_status.get("result_id")),
        "ts_code": _normalize_text(wait_status.get("ts_code")),
        "stock_name": _normalize_text(wait_status.get("stock_name")),
        "current_result_pointer_result_id": _normalize_text(current_result_pointer.get("result_id")),
        "current_result_pointer_updated_at": _normalize_text(current_result_pointer.get("updated_at")),
        "current_date": current_date,
        "observation_window_start": observation_window_start,
        "observation_window_has_started": bool(window.get("has_started")) if isinstance(window, dict) else False,
        "primary_progress": _progress_label(primary_entries),
        "primary_progress_pct": _progress_pct(primary_entries),
        "primary_entries": primary_entries,
        "primary_needed": primary_needed,
        "primary_blocking_reasons": primary_blocking_reasons,
        "basket_progress": _progress_label(basket_entries),
        "basket_progress_pct": _progress_pct(basket_entries),
        "basket_entries": basket_entries,
        "basket_needed": basket_needed,
        "basket_blocking_reasons": basket_blocking_reasons,
        "promotion_decision": promotion_decision,
        "promotion_decision_label": _display_promotion_label(promotion_decision),
        "promotion_generated_at": _normalize_text(promotion_gate.get("generated_at")),
        "promotion_blocking_reasons": promotion_blocking_reasons,
        "scoreboard_status": _normalize_text(scoreboard.get("overall_status"), "unknown"),
        "scoreboard_score": _normalize_text(scoreboard.get("score"), "-"),
        "scoreboard_generated_at": _normalize_text(scoreboard.get("generated_at")),
        "performance_generated_at": _normalize_text(performance_evidence.get("generated_at")),
        "wait_blocking_reasons": wait_blocking_reasons,
        "allowed_action": "重新检查等待状态",
        "next_command": WAIT_STATUS_COMMAND,
        "action_locks": action_locks,
        "evidence_sources": (
            {
                "label": "当前主结果指针",
                "artifact": "current_result_pointer/current.json",
                "generated_at": _normalize_text(current_result_pointer.get("updated_at")),
            },
            {
                "label": "等待状态",
                "artifact": "primary_result_daily_closure_latest.json" if closure_preferred else "primary_result_observation_wait_status_latest.json",
                "generated_at": generated_at,
            },
            {
                "label": "运营评分",
                "artifact": "primary_result_daily_operations_scoreboard_latest.json",
                "generated_at": _normalize_text(scoreboard.get("generated_at")),
            },
            {
                "label": "表现证据",
                "artifact": "primary_result_performance_evidence_latest.json",
                "generated_at": _normalize_text(performance_evidence.get("generated_at")),
            },
            {
                "label": "晋级门禁",
                "artifact": "primary_result_promotion_readiness_gate_latest.json",
                "generated_at": _normalize_text(promotion_gate.get("generated_at")),
            },
        ),
        "boundary": "只读证据驾驶舱，不决定主结果真相，不闭合样本、不写 ledger、不交易、不促晋级。",
    }


def render_first_place_evidence_cockpit(view_model: dict[str, Any]) -> str:
    status = _normalize_text(view_model.get("status"), "unknown")
    status_label = _display_status_label(status)
    primary_pct = float(view_model.get("primary_progress_pct") or 0.0)
    basket_pct = float(view_model.get("basket_progress_pct") or 0.0)
    wait_reasons = view_model.get("wait_blocking_reasons")
    if not isinstance(wait_reasons, (list, tuple)):
        wait_reasons = ()
    promotion_reasons = view_model.get("promotion_blocking_reasons")
    if not isinstance(promotion_reasons, (list, tuple)):
        promotion_reasons = ()
    primary_reasons = view_model.get("primary_blocking_reasons")
    if not isinstance(primary_reasons, (list, tuple)):
        primary_reasons = ()
    basket_reasons = view_model.get("basket_blocking_reasons")
    if not isinstance(basket_reasons, (list, tuple)):
        basket_reasons = ()
    blocking_summary = (
        [_display_reason(reason) for reason in wait_reasons[:1]]
        + [f"主结果证据还差 {view_model.get('primary_needed', 0)} 个干净样本"]
        + [f"候选篮证据还差 {view_model.get('basket_needed', 0)} 个干净样本"]
        + [_display_reason(reason) for reason in promotion_reasons[:1]]
    )
    blocking_html = "".join(
        f'<li>{html.escape(_normalize_text(reason))}</li>' for reason in blocking_summary if _normalize_text(reason)
    )
    action_locks = view_model.get("action_locks")
    if not isinstance(action_locks, (list, tuple)):
        action_locks = ()
    action_lock_rows = "".join(
        '<tr>'
        f'<td>{html.escape(_normalize_text(lock.get("action") if isinstance(lock, dict) else None))}</td>'
        f'<td><span class="evidence-lock-state">{html.escape(_normalize_text(lock.get("state") if isinstance(lock, dict) else None))}</span></td>'
        f'<td>{html.escape(_normalize_text(lock.get("reason") if isinstance(lock, dict) else None))}</td>'
        '</tr>'
        for lock in action_locks
    )
    evidence_sources = view_model.get("evidence_sources")
    if not isinstance(evidence_sources, (list, tuple)):
        evidence_sources = ()
    evidence_source_html = "".join(
        '<div class="evidence-source-item">'
        f'<span>{html.escape(_normalize_text(source.get("label") if isinstance(source, dict) else None))}</span>'
        f'<strong>{html.escape(_normalize_text(source.get("artifact") if isinstance(source, dict) else None))}</strong>'
        f'<small>{html.escape(_normalize_text(source.get("generated_at") if isinstance(source, dict) else None))}</small>'
        '</div>'
        for source in evidence_sources
    )
    return (
        '<section class="card first-place-evidence-cockpit" id="first-place-evidence-cockpit" '
        'aria-label="第一名证据驾驶舱">'
        '<div class="section-title">'
        '<div><div class="eyebrow">证据路径</div><h3>判断、证据、边界、下一步</h3></div>'
        '<div class="muted">等待期只呈现制度事实、证据缺口、行动边界和下一次检查。</div>'
        '</div>'
        '<div class="evidence-decision-layer">'
        '<div class="evidence-decision-main">'
        '<div class="evidence-status-label">当前状态</div>'
        f'<div class="evidence-decision-title">{html.escape(_normalize_text(view_model.get("decision_status")))}</div>'
        f'<div class="evidence-decision-reason">{html.escape(_normalize_text(view_model.get("decision_reason")))}</div>'
        '</div>'
        '<div class="evidence-decision-card">'
        '<div class="evidence-status-label">下一次检查</div>'
        f'<strong>{html.escape(_normalize_text(view_model.get("next_check")))}</strong>'
        '</div>'
        '<div class="evidence-decision-card evidence-decision-card-lock">'
        '<div class="evidence-status-label">当前结论</div>'
        f'<strong>{html.escape(_normalize_text(view_model.get("decision_summary")))}</strong>'
        '</div>'
        '</div>'
        '<div class="evidence-cockpit-hero">'
        '<div class="evidence-status-block">'
        '<div class="evidence-status-label">系统状态</div>'
        f'<div class="evidence-status-value">{html.escape(status_label)}</div>'
        f'<div class="evidence-status-sub">当前日期 {html.escape(_normalize_text(view_model.get("current_date")))} · '
        f'窗口开始 {html.escape(_normalize_text(view_model.get("observation_window_start")))}</div>'
        '</div>'
        '<div class="evidence-command-block">'
        '<div class="evidence-status-label">当前唯一允许动作</div>'
        f'<div class="evidence-command-title">{html.escape(_normalize_text(view_model.get("allowed_action")))}</div>'
        f'<code>{html.escape(_normalize_text(view_model.get("next_command")))}</code>'
        '</div>'
        '</div>'
        '<div class="evidence-progress-grid">'
        '<div class="evidence-progress-card">'
        '<div class="evidence-progress-top"><span>主结果证据</span>'
        f'<strong>{html.escape(_normalize_text(view_model.get("primary_progress")))}</strong></div>'
        '<div class="evidence-progress-track">'
        f'<div class="evidence-progress-fill" style="width:{primary_pct:.2f}%"></div>'
        '</div><div class="evidence-progress-sub">20 个干净样本前不讨论晋级。</div></div>'
        '<div class="evidence-progress-card">'
        '<div class="evidence-progress-top"><span>候选篮证据</span>'
        f'<strong>{html.escape(_normalize_text(view_model.get("basket_progress")))}</strong></div>'
        '<div class="evidence-progress-track">'
        f'<div class="evidence-progress-fill evidence-progress-fill-basket" style="width:{basket_pct:.2f}%"></div>'
        '</div><div class="evidence-progress-sub">篮子证据必须独立闭合，不能用单票替代。</div></div>'
        '<div class="evidence-progress-card evidence-promotion-card">'
        '<div class="evidence-progress-top"><span>晋级门禁</span>'
        f'<strong>{html.escape(_normalize_text(view_model.get("promotion_decision_label")))}</strong></div>'
        f'<div class="evidence-progress-sub">运营评分 {html.escape(_display_status_label(view_model.get("scoreboard_status")))} · '
        f'Score {html.escape(_normalize_text(view_model.get("scoreboard_score")))}</div></div>'
        '</div>'
        '<div class="evidence-sample-strip">'
        f'<div><span>主结果已闭合</span><strong>{html.escape(_normalize_text(view_model.get("primary_entries")))}</strong><small>还需 {html.escape(_normalize_text(view_model.get("primary_needed")))}</small></div>'
        f'<div><span>篮子已闭合</span><strong>{html.escape(_normalize_text(view_model.get("basket_entries")))}</strong><small>还需 {html.escape(_normalize_text(view_model.get("basket_needed")))}</small></div>'
        '<div><span>无效样本</span><strong>0</strong><small>不伪造无效样本</small></div>'
        '<div><span>重复样本</span><strong>0</strong><small>重复不计新样本</small></div>'
        '<div><span>失败样本</span><strong>0</strong><small>失败进入复核</small></div>'
        '</div>'
        '<div class="evidence-ops-grid">'
        '<div class="evidence-blocking-panel">'
        '<div class="evidence-panel-title">阻断原因</div>'
        f'<ul>{blocking_html}</ul>'
        '</div>'
        '<div class="evidence-source-panel">'
        '<div class="evidence-panel-title">证据来源</div>'
        f'<div class="evidence-source-grid">{evidence_source_html}</div>'
        '</div>'
        '</div>'
        '<div class="evidence-lock-matrix">'
        '<div class="evidence-panel-title">行动锁矩阵</div>'
        '<div class="table-shell evidence-lock-table-shell">'
        '<table class="evidence-lock-table"><thead><tr><th>动作</th><th>状态</th><th>原因</th></tr></thead>'
        f'<tbody>{action_lock_rows}</tbody></table>'
        '</div>'
        '</div>'
        f'<div class="footer-note">{html.escape(_normalize_text(view_model.get("boundary")))}</div>'
        '</section>'
    )

from __future__ import annotations

import csv
import html
import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path

import yaml
from src.stock_primary_result import (
    adapt_stock_primary_result_input,
    build_stock_primary_result_view_model,
    render_stock_primary_result,
)


CSV_HEADER_ZH = {
    "generated_at": "生成时间",
    "score": "健康分",
    "success_rate": "成功率",
    "failed_count": "失败次数",
    "alerts_count": "告警数",
    "run_id": "运行ID",
    "created_at": "创建时间",
    "source_type": "来源类型",
    "stock_pool": "股票池",
    "start_date": "开始日期",
    "end_date": "结束日期",
    "total_return": "总收益",
    "sharpe_ratio": "夏普比率",
    "max_drawdown": "最大回撤",
    "total_trades": "交易次数",
    "win_rate": "胜率",
    "rule_block_total": "规则拦截数",
    "report_path": "报告路径",
}

SIGNAL_ZH = {
    "strong_buy": "强烈看多",
    "buy": "看多",
    "watch": "观望",
    "sell": "减仓/回避",
}

RISK_ZH = {
    "low": "低风险",
    "medium": "中风险",
    "high": "高风险",
}

GENERATION_MODE_ZH = {
    "final": "正式结果",
    "interim": "临时结果",
}

STRATEGY_MODE_ZH = {
    "diversified": "分散篮子",
    "raw": "原始排序",
    "top1": "只取第一名",
}

STRATEGY_STRICTNESS_ZH = {
    "loose": "宽松",
    "medium": "中等",
    "tight": "严格",
}

WEAK_MARKET_ACTION_ZH = {
    "normal": "正常执行",
    "top3_only": "只取前三",
    "top1_only": "只取第一名",
    "cash_preferred": "优先空仓",
}

STOCK_LIFECYCLE_STAGE_ZH = {
    "L1": "研究中",
    "L2": "候选结果",
    "L3": "审核阶段结果",
    "L4": "执行阶段结果",
    "L5": "已沉淀",
}

GUARDRAIL_MODE_ZH = {
    "normal": "常规",
    "defensive": "防守",
    "interim": "临时结果",
}

MD_LINE_TRANSLATIONS = {
    "Daily Research Summary": "每日研究总结",
    "Total duration": "总耗时",
    "Daily Health Score": "每日健康评分",
    "Health Trend (Recent)": "健康趋势（近期）",
    "Failure Category Stats": "失败分类统计",
    "Alerts": "告警信息",
    "Failed Profile Recovery": "失败策略恢复",
    "status": "状态",
    "success_rate": "成功率",
    "success_component": "成功项得分",
    "failure_penalty": "失败惩罚",
    "category_penalty": "分类惩罚",
    "score_delta_vs_previous": "相较上次评分变化",
    "trend_alert": "趋势告警",
}

PUBLIC_STATUS_ZH = {
    "completed": "已完成",
    "up_to_date": "已完成",
    "partial_success": "待补齐",
    "running": "进行中",
    "failed": "待复核",
    "blocked": "待复核",
    "done": "已完成",
    "replaying_failed_profiles": "失败批次复跑",
}

PROFILE_ZH = {
    "short": "快批",
    "medium": "标准批",
    "long": "长窗批",
}

STEP_ZH = {
    "candidate_scan": "候选扫描",
    "candidate_generation": "候选生成",
    "research_pool": "研究池构建",
    "grid_backtest": "长窗验证",
    "validation": "样本验证",
}

SEARCH_MODE_ZH = {
    "focused": "聚焦研究",
    "broad": "全域研究",
}


@dataclass
class PrimaryResultCardViewModel:
    ts_code: str
    stock_name: str
    stage_label: str
    stage_combined_label: str
    result_type_label: str
    risk_label: str
    sync_note: str
    source_timestamp: str
    history_visible: bool
    history_slot_a: str | None
    history_slot_b: str | None
    history_slot_c: str | None
    disabled_visible: bool
    disabled_text: str | None
    invalid_visible: bool
    invalid_text: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


STOCK_PRIMARY_RESULT_FALLBACK_REASONS = {
    "none",
}
STOCK_PRIMARY_RESULT_PROBLEM_FALLBACK_REASONS = set()
STOCK_PRIMARY_RESULT_RENDER_CONTRACT_VERSION = "v1"
STOCK_PRIMARY_RESULT_RUNTIME_METADATA_KEYS = (
    "stock_primary_result_source",
    "stock_primary_result_runtime_mode",
    "stock_primary_result_fallback_reason",
    "stock_primary_result_has_problem_fallback",
    "stock_primary_result_is_canonical",
    "stock_primary_result_render_contract_version",
)

def stock_primary_result_runtime_mode() -> str:
    return "canonical"


def stock_primary_result_canonical_render_enabled() -> bool:
    return True


def stock_primary_result_runtime_metadata(primary_result: dict[str, object] | None = None) -> dict[str, object]:
    _ = primary_result
    return {
        "stock_primary_result_source": "canonical",
        "stock_primary_result_runtime_mode": "canonical",
        "stock_primary_result_fallback_reason": "none",
        "stock_primary_result_has_problem_fallback": False,
        "stock_primary_result_is_canonical": True,
        "stock_primary_result_render_contract_version": STOCK_PRIMARY_RESULT_RENDER_CONTRACT_VERSION,
    }


def _file_cache_key(path: Path | None) -> tuple[str, int, int] | None:
    if path is None:
        return None
    try:
        resolved = path.resolve()
        stat = resolved.stat()
    except Exception:
        return None
    return (str(resolved), stat.st_mtime_ns, stat.st_size)


@lru_cache(maxsize=128)
def _read_text_cached(cache_key: tuple[str, int, int]) -> str:
    path = Path(cache_key[0])
    return path.read_text(encoding="utf-8")


def read_text(path: Path | None, fallback: str = "暂无数据。") -> str:
    cache_key = _file_cache_key(path)
    if cache_key is None:
        return fallback
    try:
        return _read_text_cached(cache_key)
    except Exception as e:
        return f"读取失败: {e}"


@lru_cache(maxsize=128)
def _read_json_cached(cache_key: tuple[str, int, int]) -> dict:
    path = Path(cache_key[0])
    return json.loads(path.read_text(encoding="utf-8"))


def read_json(path: Path | None) -> dict:
    cache_key = _file_cache_key(path)
    if cache_key is None:
        return {}
    try:
        payload = _read_json_cached(cache_key)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


@lru_cache(maxsize=128)
def _read_csv_matrix_cached(cache_key: tuple[str, int, int]) -> tuple[tuple[str, ...], ...]:
    path = Path(cache_key[0])
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.reader(f))
    return tuple(tuple(cell for cell in row) for row in rows)


def _read_csv_matrix(path: Path | None) -> list[list[str]]:
    cache_key = _file_cache_key(path)
    if cache_key is None:
        return []
    try:
        return [list(row) for row in _read_csv_matrix_cached(cache_key)]
    except Exception:
        return []


@lru_cache(maxsize=128)
def _load_csv_dict_rows_cached(cache_key: tuple[str, int, int]) -> tuple[dict[str, str], ...]:
    path = Path(cache_key[0])
    with path.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    return tuple(dict(row) for row in rows)


def translate_generation_mode(value: str) -> str:
    key = str(value or "-").strip()
    return GENERATION_MODE_ZH.get(key, key or "-")


def translate_strategy_mode(value: str) -> str:
    key = str(value or "-").strip()
    return STRATEGY_MODE_ZH.get(key, key or "-")


def translate_strategy_strictness(value: str) -> str:
    key = str(value or "-").strip()
    return STRATEGY_STRICTNESS_ZH.get(key, key or "-")


def translate_weak_market_action(value: str) -> str:
    key = str(value or "-").strip()
    return WEAK_MARKET_ACTION_ZH.get(key, key or "-")


def translate_guardrail_mode(value: str) -> str:
    key = str(value or "-").strip()
    return GUARDRAIL_MODE_ZH.get(key, key or "-")


def stock_primary_stage_display(result_lifecycle_stage: str | None) -> str:
    key = str(result_lifecycle_stage or "").strip().upper()
    return STOCK_LIFECYCLE_STAGE_ZH.get(key, "阶段信息暂缺")


def stock_primary_combined_name(primary_result: dict[str, object]) -> str:
    primary_name = stock_primary_stage_display(str(primary_result.get("result_lifecycle_stage", "") or ""))
    status_suffix = stock_primary_status_suffix(primary_result)
    if not status_suffix:
        return primary_name
    return f"{primary_name}（{status_suffix}）"


def build_primary_result_card_view_model(primary_result: dict[str, object]) -> PrimaryResultCardViewModel:
    history_vm = stock_primary_history_view_model(primary_result)
    disabled_text = stock_primary_disabled_reason(primary_result)
    invalid_text = stock_primary_invalid_explanation(primary_result)
    return PrimaryResultCardViewModel(
        ts_code=str(primary_result.get("ts_code", "对象信息暂缺") or "对象信息暂缺"),
        stock_name=str(primary_result.get("stock_name", "名称信息暂缺") or "名称信息暂缺"),
        stage_label=stock_primary_stage_display(str(primary_result.get("result_lifecycle_stage", "") or "")),
        stage_combined_label=stock_primary_combined_name(primary_result),
        result_type_label=str(primary_result.get("result_type", "-") or "-"),
        risk_label=stock_primary_risk_text(_normalize_optional_text(primary_result.get("risk_level"))),
        sync_note=stock_primary_sync_note(primary_result),
        source_timestamp=stock_primary_source_timestamp(primary_result),
        history_visible=bool(history_vm["visible"]),
        history_slot_a=str(history_vm["slot_a"]) if history_vm["slot_a"] is not None else None,
        history_slot_b=str(history_vm["slot_b"]) if history_vm["slot_b"] is not None else None,
        history_slot_c=str(history_vm["slot_c"]) if history_vm["slot_c"] is not None else None,
        disabled_visible=disabled_text is not None,
        disabled_text=disabled_text,
        invalid_visible=invalid_text is not None,
        invalid_text=invalid_text,
    )


def stock_primary_card_view_model(primary_result: dict[str, object]) -> dict[str, object]:
    return build_stock_primary_result_view_model(adapt_stock_primary_result_input(primary_result)).as_dict()


def _normalize_optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    if not text or text in {"-", "None", "null"}:
        return None
    return text


def public_status_text(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    return PUBLIC_STATUS_ZH.get(text, text)


def public_profile_text(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    return PROFILE_ZH.get(text, text)


def public_profile_list_text(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "无"
    parts = [public_profile_text(part.strip(), part.strip()) for part in text.split(",") if part.strip()]
    return "、".join(parts) if parts else "无"


def public_step_text(value: object, fallback: str = "待复核") -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return fallback
    return STEP_ZH.get(text, text)


def public_search_mode_text(value: object, fallback: str = "-") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    return SEARCH_MODE_ZH.get(text, text)


def public_experiment_text(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "当前研究口径已固化"
    return "当前研究口径已固化"


def stock_primary_status_suffix(primary_result: dict[str, object]) -> str | None:
    stage = str(primary_result.get("result_lifecycle_stage", "") or "").strip().upper()
    if stage == "L4":
        execution_status = _normalize_optional_text(primary_result.get("execution_status"))
        mapping = {
            "queued": "待执行",
            "ready": "待执行准备",
            "running": "运行中",
            "completed": "已完成",
            "failed": "执行失败",
            "cancelled": "已取消",
        }
        if execution_status is not None:
            return mapping.get(execution_status.lower(), execution_status)
    if stage == "L3":
        audit_status = _normalize_optional_text(primary_result.get("audit_status"))
        mapping = {
            "in_review": "审核中",
            "passed": "已通过",
            "failed": "未通过",
            "waived": "已豁免",
        }
        if audit_status is not None:
            return mapping.get(audit_status.lower(), audit_status)
    candidate_status = _normalize_optional_text(primary_result.get("candidate_status"))
    candidate_mapping = {
        "candidate": "候选中",
        "shortlisted": "已入池",
        "rejected": "已驳回",
        "expired": "已失效",
    }
    if candidate_status is not None:
        return candidate_mapping.get(candidate_status.lower(), candidate_status)
    research_status = _normalize_optional_text(primary_result.get("research_status"))
    research_mapping = {
        "in_progress": "研究中",
        "completed": "研究完成",
        "suspended": "研究暂停",
        "abandoned": "研究终止",
    }
    if research_status is not None:
        return research_mapping.get(research_status.lower(), research_status)
    return None


def stock_primary_risk_text(risk_level: str | None) -> str:
    key = str(risk_level or "").strip().lower()
    if not key:
        return "风险信息暂缺"
    return RISK_ZH.get(key, key)


def stock_primary_disabled_reason(primary_result: dict[str, object]) -> str | None:
    explicit_reason = _normalize_optional_text(primary_result.get("disabled_reason"))
    if explicit_reason is not None:
        return explicit_reason
    terminal_outcome = _normalize_optional_text(primary_result.get("terminal_outcome"))
    if terminal_outcome == "rejected":
        return "当前对象已被驳回，不能继续推进。"
    if terminal_outcome == "cancelled":
        return "当前对象已被取消，不能继续推进。"
    if terminal_outcome == "expired":
        return "当前对象已过有效窗口，不能继续推进。"
    if terminal_outcome == "superseded":
        return "当前对象已被新版结果替代，不能继续推进。"
    audit_status = _normalize_optional_text(primary_result.get("audit_status"))
    if audit_status == "failed":
        return "审核未通过，当前不能继续推进。"
    promotion_status = _normalize_optional_text(primary_result.get("promotion_status"))
    if promotion_status == "rejected":
        return "晋升未通过，当前不能继续推进。"
    risk_level = _normalize_optional_text(primary_result.get("risk_level"))
    if risk_level in {"high", "critical"}:
        return "当前风险较高，暂不建议继续推进。"
    return None


def stock_primary_invalid_explanation(primary_result: dict[str, object]) -> str | None:
    terminal_outcome = _normalize_optional_text(primary_result.get("terminal_outcome"))
    mapping = {
        "success": "该结果已完成并验证。",
        "expired": "该结果已过有效窗口，当前视为失效。",
        "superseded": "该结果已被新版结果替代。",
        "rejected": "该结果已被制度驳回。",
        "failed": "该结果对应执行路径已失败。",
        "cancelled": "该结果对应流程已取消。",
        "archived": "该结果已归档保存，不再作为当前推进对象。",
    }
    if terminal_outcome is None:
        return None
    base = mapping.get(terminal_outcome.lower(), terminal_outcome)
    explicit_reason = _normalize_optional_text(primary_result.get("invalid_reason"))
    if explicit_reason is not None:
        return explicit_reason
    return base


def _history_record_label(kind: str, raw_value: str) -> str:
    value = raw_value.lower()
    mappings = {
        "execution": {
            "queued": "待执行",
            "ready": "待执行准备",
            "running": "运行中",
            "completed": "已完成",
            "failed": "执行失败",
            "cancelled": "已取消",
        },
        "audit": {
            "in_review": "审核中",
            "passed": "已通过",
            "failed": "未通过",
            "waived": "已豁免",
        },
        "candidate": {
            "candidate": "候选中",
            "shortlisted": "已入池",
            "rejected": "已驳回",
            "expired": "已失效",
        },
        "research": {
            "in_progress": "研究中",
            "completed": "研究完成",
            "suspended": "研究暂停",
            "abandoned": "研究终止",
        },
    }
    return mappings.get(kind, {}).get(value, raw_value)


def stock_primary_history_record(primary_result: dict[str, object]) -> str | None:
    explicit_summary = _normalize_optional_text(primary_result.get("history_summary"))
    if explicit_summary is not None:
        replacements = {
            "执行记录 ready": "执行记录 待执行准备",
            "执行记录 completed": "执行记录 已完成",
            "审核记录 passed": "审核记录 已通过",
            "研究记录 completed": "研究记录 已完成",
        }
        segments = [segment.strip() for segment in explicit_summary.split("；") if segment.strip()]
        segments = [replacements.get(segment, segment) for segment in segments]
        for prefix in ("执行记录", "审核记录", "候选记录", "研究记录"):
            for segment in segments:
                if segment.startswith(prefix):
                    return segment
        return segments[0] if segments else None
    execution_status = _normalize_optional_text(primary_result.get("execution_status"))
    if execution_status is not None:
        return f"执行记录：{_history_record_label('execution', execution_status)}"
    audit_status = _normalize_optional_text(primary_result.get("audit_status"))
    if audit_status is not None:
        return f"审核记录：{_history_record_label('audit', audit_status)}"
    candidate_status = _normalize_optional_text(primary_result.get("candidate_status"))
    if candidate_status is not None:
        return f"候选记录：{_history_record_label('candidate', candidate_status)}"
    research_status = _normalize_optional_text(primary_result.get("research_status"))
    if research_status is not None:
        return f"研究记录：{_history_record_label('research', research_status)}"
    return None


def stock_primary_history_source(primary_result: dict[str, object]) -> tuple[str | None, str | None, str | None]:
    source_file = _normalize_optional_text(primary_result.get("history_source_file"))
    source_timestamp = _normalize_optional_text(primary_result.get("history_source_timestamp"))
    generation_mode = _normalize_optional_text(primary_result.get("history_generation_mode"))
    if source_file is not None and source_timestamp is not None:
        return source_file, source_timestamp, generation_mode or "degraded"
    timestamps = primary_result.get("source_timestamps")
    if not isinstance(timestamps, dict):
        return None, None, None
    priority: list[str] = []
    if _normalize_optional_text(primary_result.get("terminal_outcome")) is not None:
        priority.append("primary_result_terminal_latest.json")
    if _normalize_optional_text(primary_result.get("observation_status")) is not None:
        priority.append("primary_result_observation_latest.json")
    if _normalize_optional_text(primary_result.get("rollback_status")) is not None:
        priority.extend(["primary_result_rollback_latest.json", "t12_rollback_drill_latest.json"])
    if _normalize_optional_text(primary_result.get("execution_status")) is not None:
        priority.extend(["primary_result_execution_latest.json", "t1_execution_checklist_latest.json"])
    if _normalize_optional_text(primary_result.get("audit_status")) is not None:
        priority.extend(["primary_result_audit_latest.json", "governance_audit_latest.json"])
    if _normalize_optional_text(primary_result.get("candidate_status")) is not None:
        priority.extend(["buylist_latest.json", "candidates_top_latest.csv"])
    if _normalize_optional_text(primary_result.get("research_status")) is not None:
        priority.append("daily_research_status_latest.json")
    priority.extend(
        [
            "candidates_top_latest.csv",
            "daily_research_status_latest.json",
            "primary_result_audit_latest.json",
            "primary_result_execution_latest.json",
            "primary_result_observation_latest.json",
            "primary_result_rollback_latest.json",
            "primary_result_terminal_latest.json",
            "governance_audit_latest.json",
            "t1_execution_checklist_latest.json",
            "t12_rollback_drill_latest.json",
            "buylist_latest.json",
        ]
    )
    seen: set[str] = set()
    for candidate in priority:
        if candidate in seen:
            continue
        seen.add(candidate)
        ts = _normalize_optional_text(timestamps.get(candidate))
        if ts is not None:
            return candidate, ts, "degraded"
    return None, None, None


def stock_primary_history_hint(primary_result: dict[str, object], *, has_record: bool, has_source: bool) -> str | None:
    _ = primary_result
    if has_record:
        return "仅供参考，当前主阶段仍以制度主字段为准。"
    if has_source:
        return "当前仅展示可确认的历史痕迹，不代表已通过或可执行。"
    return None


def stock_primary_history_view_model(primary_result: dict[str, object]) -> dict[str, str | bool | None]:
    slot_a = stock_primary_history_record(primary_result)
    source_file, source_timestamp, generation_mode = stock_primary_history_source(primary_result)
    has_source = source_file is not None and source_timestamp is not None
    if slot_a is None and has_source:
        slot_a = "历史记录暂缺"
    slot_b = None
    if has_source:
        mode_label = "直接事实" if generation_mode == "direct" else "降级生成"
        slot_b = f"来源 {source_file} · 同步 {source_timestamp} · {mode_label}"
    slot_c = stock_primary_history_hint(primary_result, has_record=slot_a not in {None, '历史记录暂缺'}, has_source=has_source)
    visible = slot_a is not None or slot_b is not None
    return {
        "visible": visible,
        "slot_a": slot_a,
        "slot_b": slot_b,
        "slot_c": slot_c,
    }


def stock_primary_sync_note(primary_result: dict[str, object]) -> str:
    note = _normalize_optional_text(primary_result.get("data_sync_note"))
    if note is None:
        return "同步信息暂缺。"
    lowered = note.lower()
    if note.startswith("降级显示："):
        return "降级说明"
    if "batch_prediction_timeout" in lowered:
        return "当前结果仍带补证痕迹"
    if "validation_skipped" in lowered:
        return "验证样本仍在补齐"
    return note


def stock_primary_source_timestamp(primary_result: dict[str, object]) -> str:
    timestamps = primary_result.get("source_timestamps")
    if not isinstance(timestamps, dict):
        return "-"
    values = [str(value).strip() for value in timestamps.values() if str(value).strip() and str(value).strip() != "-"]
    return max(values) if values else "-"


def render_primary_result_card_template(view_model: PrimaryResultCardViewModel) -> str:
    ts_code = html.escape(view_model.ts_code or "对象信息暂缺")
    stock_name = html.escape(view_model.stock_name or "名称信息暂缺")
    combined_name = html.escape(view_model.stage_combined_label or "阶段信息暂缺")
    risk_text = html.escape(view_model.risk_label or "风险信息暂缺")
    sync_note = html.escape(view_model.sync_note or "同步信息暂缺。")
    source_ts = html.escape(view_model.source_timestamp or "-")
    result_type = html.escape(view_model.result_type_label or "-")
    history_display = "" if view_model.history_visible else "none"
    history_slot_a = html.escape(view_model.history_slot_a or "")
    history_slot_b = html.escape(view_model.history_slot_b or "")
    history_slot_c = html.escape(view_model.history_slot_c or "")
    disabled_reason = view_model.disabled_text
    invalid_reason = view_model.invalid_text
    history_html = (
        f'<div class="callout primary-result-callout primary-result-callout-history" id="primary-result-history" style="margin-top:12px;display:{history_display};">'
        '<div class="primary-result-callout-head">'
        '<strong class="primary-result-callout-title" id="primary-result-history-title">研究闭环：</strong>'
        '<button type="button" class="primary-result-action-btn" id="primary-result-history-toggle" aria-label="折叠或展开历史验证区，仅改变阅读方式，不改变制度事实。" aria-describedby="primary-result-history-hint" aria-controls="primary-result-history-body" aria-expanded="true" title="仅改变阅读方式，不改变制度事实">折叠</button>'
        '</div>'
        '<div class="muted primary-result-hint" id="primary-result-history-hint" style="margin-top:6px;">仅作参考解释，不替代当前制度判断。</div>'
        '<div class="primary-result-collapsible-body" id="primary-result-history-body" role="region" aria-labelledby="primary-result-history-toggle" aria-hidden="false">'
        f'<div id="primary-result-history-slot-a" class="primary-result-history-slot primary-result-history-slot-main" style="margin-top:8px;">{history_slot_a}</div>'
        f'<div id="primary-result-history-slot-b" class="muted primary-result-history-slot primary-result-history-slot-meta" style="margin-top:6px;" aria-label="仅显示主来源、最近同步和生成方式。" title="仅显示主来源、最近同步和生成方式。">{history_slot_b}</div>'
        f'<div id="primary-result-history-slot-c" class="muted primary-result-history-slot primary-result-history-slot-hint" style="margin-top:6px;" aria-label="仅供参考，不代表当前通过或可执行。" title="仅供参考，不代表当前通过或可执行。">{history_slot_c}</div>'
        '</div>'
        "</div>"
    )
    disabled_display = "" if disabled_reason else "none"
    disabled_html = (
        f'<div class="callout primary-result-callout primary-result-callout-warning" id="primary-result-disabled" style="margin-top:12px;display:{disabled_display};">'
        '<div class="primary-result-callout-head">'
        '<strong class="primary-result-callout-title" id="primary-result-disabled-title">禁用场景：</strong>'
        '<button type="button" class="primary-result-action-btn" id="primary-result-disabled-toggle" aria-label="折叠或展开禁用场景区，仅改变阅读方式，不改变制度事实。" aria-controls="primary-result-disabled-body" aria-expanded="true" title="仅改变阅读方式，不改变制度事实">折叠</button>'
        '</div>'
        '<div class="primary-result-collapsible-body" id="primary-result-disabled-body" role="region" aria-labelledby="primary-result-disabled-toggle" aria-hidden="false">'
        f'<div id="primary-result-disabled-text">{html.escape(disabled_reason) if disabled_reason else ""}</div>'
        + "</div></div>"
        )
    invalid_display = "" if invalid_reason else "none"
    invalid_html = (
        f'<div class="callout primary-result-callout primary-result-callout-critical" id="primary-result-invalid" style="margin-top:12px;display:{invalid_display};">'
        '<div class="primary-result-callout-head">'
        '<strong class="primary-result-callout-title" id="primary-result-invalid-title">失效解释：</strong>'
        '<button type="button" class="primary-result-action-btn" id="primary-result-invalid-toggle" aria-label="折叠或展开失效解释区，仅改变阅读方式，不改变制度事实。" aria-controls="primary-result-invalid-body" aria-expanded="true" title="仅改变阅读方式，不改变制度事实">折叠</button>'
        '</div>'
        '<div class="primary-result-collapsible-body" id="primary-result-invalid-body" role="region" aria-labelledby="primary-result-invalid-toggle" aria-hidden="false">'
        f'<div id="primary-result-invalid-text">{html.escape(invalid_reason) if invalid_reason else ""}</div>'
        + "</div></div>"
    )
    return (
        '<div class="card primary-result-card" id="primary-result-card">'
        '<div class="section-title primary-result-header">'
        '<div><div class="eyebrow">制度主结果</div><h3>统一结果对象</h3></div>'
        '<div class="muted">主阶段只由 result_lifecycle_stage 决定；状态仅做附加显示。</div>'
        '</div>'
        '<div class="primary-result-toolbar">'
        '<button type="button" class="primary-result-action-btn" id="primary-result-copy-summary" aria-label="复制制度事实摘要" title="复制制度事实摘要，不包含前端推断。">复制摘要</button>'
        '<button type="button" class="primary-result-action-btn" id="primary-result-copy-json" aria-label="复制当前已生效的事实 JSON" title="复制当前已生效的事实 JSON。">复制 JSON</button>'
        '</div>'
        '<div class="muted primary-result-live-status" id="primary-result-live-status" aria-live="polite" aria-atomic="true"></div>'
        '<div class="grid3 primary-result-grid">'
        f'<div class="kpi primary-result-kpi primary-result-kpi-identity"><div class="label">当前对象</div><div class="value" id="primary-result-code">{ts_code}</div><div class="sub" id="primary-result-name">{stock_name}</div></div>'
        f'<div class="kpi primary-result-kpi primary-result-kpi-stage"><div class="label">主阶段</div><div class="value" id="primary-result-stage" aria-label="主阶段始终由 result_lifecycle_stage 决定。" title="主阶段始终由 result_lifecycle_stage 决定。">{combined_name}</div><div class="sub" id="primary-result-type">分类 {result_type}</div></div>'
        f'<div class="kpi primary-result-kpi primary-result-kpi-risk"><div class="label">风险提示</div><div class="value" id="primary-result-risk">{risk_text}</div><div class="sub">来源于风险状态字段，不参与主阶段判断</div></div>'
        '</div>'
        '<div class="metric-ribbon primary-result-sync" id="primary-result-sync">'
        f'<div class="metric-chip primary-result-sync-chip" id="primary-result-source-ts">最近来源时间 {source_ts}</div>'
        f'<div class="metric-chip primary-result-sync-chip" id="primary-result-sync-note">{sync_note}</div>'
        '</div>'
        f"{history_html}"
        f"{disabled_html}"
        f"{invalid_html}"
        '</div>'
    )


def _build_stock_primary_result_runtime_bundle(
    primary_result: dict[str, object],
) -> tuple[dict[str, object], object]:
    canonical_fact = adapt_stock_primary_result_input(primary_result)
    canonical_vm = build_stock_primary_result_view_model(canonical_fact)
    return canonical_fact, canonical_vm


def stock_primary_result_card_html(primary_result: dict[str, object]) -> str:
    canonical_fact, canonical_vm = _build_stock_primary_result_runtime_bundle(primary_result)
    _ = canonical_fact
    return render_stock_primary_result(canonical_vm)


def stock_primary_result_bridge_shell_html(primary_result: dict[str, object]) -> str:
    canonical_fact, canonical_vm = _build_stock_primary_result_runtime_bundle(primary_result)
    _ = canonical_vm
    view_model = build_primary_result_card_view_model(canonical_fact)
    legacy_html = render_primary_result_card_template(view_model).replace(
        'id="primary-result-card"',
        'id="primary-result-bridge-shell"',
        1,
    )
    return (
        '<div hidden aria-hidden="true" data-primary-result-bridge-shell="true">'
        f"{legacy_html}"
        "</div>"
    )


def _dashboard_settings_path(root: Path) -> Path | None:
    candidates = [
        root / "config" / "server" / "settings.yaml",
        root / "config" / "settings.yaml",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def tail_csv_as_html(path: Path, max_rows: int = 20) -> str:
    rows = _read_csv_matrix(path)
    if not rows:
        return "<p>暂无数据。</p>"
    try:
        return csv_rows_to_html(rows[0], rows[1:][-max_rows:])
    except Exception as e:
        return f"<p>CSV 解析失败: {html.escape(str(e))}</p>"


def csv_rows_to_html(header_row: list[str], data_rows: list[list[str]]) -> str:
    header = [CSV_HEADER_ZH.get(c, c) for c in header_row]
    th = "".join(f"<th>{html.escape(c)}</th>" for c in header)
    body_rows = []
    for row in data_rows:
        tds = "".join(f"<td>{html.escape(v)}</td>" for v in row)
        body_rows.append(f"<tr>{tds}</tr>")
    return (
        "<table><thead><tr>"
        + th
        + "</tr></thead><tbody>"
        + "".join(body_rows)
        + "</tbody></table>"
    )


def latest_backtest_report(reports_dir: Path) -> Path | None:
    files = sorted(reports_dir.glob("backtest_report_*.md"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _prefix_base_path(base_path: str, suffix: str) -> str:
    base = (base_path or "").strip()
    if not base or base == "/":
        return suffix
    base = "/" + base.strip("/")
    return f"{base}{suffix}"


def file_href(root: Path, target: Path | None, base_path: str = "") -> str:
    if target is None:
        return ''
    try:
        rel = target.resolve().relative_to(root.resolve())
    except Exception:
        return ''
    return _prefix_base_path(base_path, f"/file/{rel.as_posix()}")


def download_href(root: Path, target: Path | None, base_path: str = "") -> str:
    if target is None:
        return ''
    try:
        rel = target.resolve().relative_to(root.resolve())
    except Exception:
        return ''
    return _prefix_base_path(base_path, f"/download/{rel.as_posix()}")


def translate_md_line(line: str) -> str:
    translated = line
    for en, zh in MD_LINE_TRANSLATIONS.items():
        translated = translated.replace(en, zh)
    return translated


def markdown_to_html_basic(md_text: str) -> str:
    lines = md_text.splitlines()
    out: list[str] = []
    in_list = False
    for raw in lines:
        line = raw.rstrip()
        if line.startswith("### "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h4>{html.escape(line[4:])}</h4>")
            continue
        if line.startswith("## "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h3>{html.escape(line[3:])}</h3>")
            continue
        if line.startswith("# "):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<h2>{html.escape(line[2:])}</h2>")
            continue
        if line.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{html.escape(line[2:])}</li>")
            continue
        if re.match(r"^\|.*\|$", line):
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append(f"<pre>{html.escape(line)}</pre>")
            continue
        if not line.strip():
            if in_list:
                out.append("</ul>")
                in_list = False
            out.append("<div style='height:6px'></div>")
            continue
        if in_list:
            out.append("</ul>")
            in_list = False
        out.append(f"<p>{html.escape(line)}</p>")
    if in_list:
        out.append("</ul>")
    return "".join(out)


def extract_health_metrics(daily_md_text: str) -> dict[str, str]:
    metrics = {
        "score": "N/A",
        "success_rate": "N/A",
        "failure_penalty": "N/A",
        "category_penalty": "N/A",
    }
    patterns = {
        "score": r"-\s*score:\s*([0-9.]+/100)",
        "success_rate": r"-\s*success_rate:\s*([0-9.]+%)",
        "failure_penalty": r"-\s*failure_penalty:\s*(-?[0-9.]+)",
        "category_penalty": r"-\s*category_penalty:\s*(-?[0-9.]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, daily_md_text, flags=re.IGNORECASE)
        if match:
            metrics[key] = match.group(1)
    return metrics


def status_by_score(score_text: str) -> tuple[str, str]:
    try:
        score = float(score_text.split("/")[0])
    except Exception:
        return "未评估", "tag-neutral"
    if score >= 85:
        return "稳健", "tag-good"
    if score >= 70:
        return "观察", "tag-warn"
    return "告警", "tag-bad"


def top_candidate_brief(candidates_csv: Path, index: int = 0) -> dict[str, str]:
    rows = load_csv_rows(candidates_csv, limit=max(index + 1, 1))
    if not rows or index >= len(rows):
        return {"ts_code": "暂无", "stock_name": "", "signal": "-", "final_score": "-", "risk_level": "-"}
    row = rows[index]
    return {
        "ts_code": row.get("ts_code", "暂无"),
        "stock_name": row.get("stock_name", ""),
        "signal": row.get("signal", "-"),
        "final_score": row.get("final_score", "-"),
        "risk_level": row.get("risk_level", "-"),
    }


def safe_float(v: str, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def load_csv_rows(path: Path, limit: int = 100) -> list[dict[str, str]]:
    cache_key = _file_cache_key(path)
    if cache_key is None:
        return []
    try:
        rows = _load_csv_dict_rows_cached(cache_key)
        return [dict(row) for row in rows[:limit]]
    except Exception:
        return []


def _stock_pool_size(stock_pool_text: str) -> int:
    text = str(stock_pool_text or "").strip()
    if not text:
        return 0
    if "|" in text:
        return len([part for part in text.split("|") if part.strip()])
    if "," in text:
        return len([part for part in text.split(",") if part.strip()])
    return 1


def _is_official_backtest_source(source_type: str) -> bool:
    return str(source_type or "").strip() == "official_research"


def _load_all_csv_rows(path: Path) -> list[dict[str, str]]:
    return load_csv_rows(path, limit=10**9)


def _backtest_scope_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    official = sum(1 for row in rows if _is_official_backtest_source(row.get("source_type", "")))
    single = sum(1 for row in rows if _stock_pool_size(row.get("stock_pool", "")) <= 1)
    multi = sum(1 for row in rows if _stock_pool_size(row.get("stock_pool", "")) > 1)
    trade_qualified = sum(1 for row in rows if safe_float(row.get("total_trades", "0")) >= 20)
    return {
        "total": len(rows),
        "official": official,
        "single": single,
        "multi": multi,
        "trade_qualified": trade_qualified,
    }


def _sample_gap_text(actual: int, required: int, label: str) -> str:
    if actual >= required:
        return ""
    return f"{label}{actual}/{required}"


def assess_research_sample_coverage(rows: list[dict[str, str]]) -> dict[str, str | int]:
    counts = _backtest_scope_counts(rows)
    if not rows:
        return {
            "stage": "暂无回测样本",
            "label": "暂无回测样本",
            "detail": "当前还没有可用于解释口径的回测记录。",
            **counts,
        }

    research_gate = {
        "official": 300,
        "multi": 150,
        "single": 30,
        "trade_qualified": 100,
    }
    launch_gate = {
        "official": 1000,
        "multi": 500,
        "single": 100,
        "trade_qualified": 300,
    }

    def _meets_gate(gate: dict[str, int]) -> bool:
        return all(counts[key] >= required for key, required in gate.items())

    if _meets_gate(launch_gate):
        stage = "可上线"
        detail = (
            "样本结构已覆盖正式研究、组合回测和单票验证，且有效交易事件充足，可进入上线观察阶段。"
        )
    elif _meets_gate(research_gate):
        stage = "够研究"
        launch_gaps = [
            _sample_gap_text(counts["official"], launch_gate["official"], "正式研究"),
            _sample_gap_text(counts["multi"], launch_gate["multi"], "多股票池"),
            _sample_gap_text(counts["single"], launch_gate["single"], "单票"),
            _sample_gap_text(counts["trade_qualified"], launch_gate["trade_qualified"], "有效交易"),
        ]
        detail = (
            "样本已达到正式研究门槛，但距离上线观察仍有缺口："
            + "、".join(part for part in launch_gaps if part)
            + "。"
        )
    else:
        stage = "需降级"
        research_gaps = [
            _sample_gap_text(counts["official"], research_gate["official"], "正式研究"),
            _sample_gap_text(counts["multi"], research_gate["multi"], "多股票池"),
            _sample_gap_text(counts["single"], research_gate["single"], "单票"),
            _sample_gap_text(counts["trade_qualified"], research_gate["trade_qualified"], "有效交易"),
        ]
        detail = (
            "当前样本还没达到正式研究门槛，建议先扩样本再做参数结论："
            + "、".join(part for part in research_gaps if part)
            + "。"
        )

    label = (
        f"{stage} · 正式研究样本 {counts['official']} 条 / "
        f"多股票池样本 {counts['multi']} 条 / 单票样本 {counts['single']} 条 / "
        f"有效交易样本 {counts['trade_qualified']} 条"
    )
    return {
        "stage": stage,
        "label": label,
        "detail": detail,
        **counts,
    }


def summarize_backtest_scope(leaderboard_csv: Path) -> dict[str, str]:
    rows = _load_all_csv_rows(leaderboard_csv)
    coverage = assess_research_sample_coverage(rows)
    return {
        "label": str(coverage["label"]),
        "detail": str(coverage["detail"]),
    }


def preferred_backtest_table_html(leaderboard_csv: Path, max_rows: int = 20) -> str:
    rows = _read_csv_matrix(leaderboard_csv)
    if not rows:
        return "<p>暂无数据。</p>"
    try:
        header_row = rows[0]
        data_rows = rows[1:]
        stock_pool_idx = header_row.index("stock_pool") if "stock_pool" in header_row else -1
        source_type_idx = header_row.index("source_type") if "source_type" in header_row else -1
        if source_type_idx >= 0:
            official_rows = [row for row in data_rows if _is_official_backtest_source(row[source_type_idx])]
        else:
            official_rows = []
        if official_rows:
            picked = official_rows[:max_rows]
        elif stock_pool_idx >= 0:
            multi_rows = [row for row in data_rows if _stock_pool_size(row[stock_pool_idx]) > 1]
            picked = multi_rows[:max_rows] if multi_rows else data_rows[:max_rows]
        else:
            picked = data_rows[:max_rows]
        return csv_rows_to_html(header_row, picked)
    except Exception as e:
        return f"<p>CSV 解析失败: {html.escape(str(e))}</p>"


def backtest_diagnosis(leaderboard_csv: Path) -> dict[str, str | list[str]]:
    all_rows = _load_all_csv_rows(leaderboard_csv)
    if not all_rows:
        return {
            "结论": "暂无回测数据",
            "总收益": "-",
            "夏普": "-",
            "最大回撤": "-",
            "Calmar": "-",
            "盈亏比": "-",
            "胜率": "-",
            "交易次数": "-",
            "低流动性拦截": "-",
            "佣金": "-",
            "滑点": "-",
            "诊断": ["请先运行回测或每日研究流程。"],
        }
    sample_coverage = assess_research_sample_coverage(all_rows)
    row = all_rows[0]
    total_return = safe_float(row.get("total_return", "0"))
    sharpe = safe_float(row.get("sharpe_ratio", "0"))
    max_drawdown = safe_float(row.get("max_drawdown", "0"))
    calmar = safe_float(row.get("calmar_ratio", "0"))
    reward_risk = safe_float(row.get("reward_risk_ratio", "0"))
    win_rate = safe_float(row.get("win_rate", "0"))
    trades = int(safe_float(row.get("total_trades", "0")))
    low_liquidity_blocks = int(safe_float(row.get("low_liquidity_blocks", "0")))
    total_commission = safe_float(row.get("total_commission", "0"))
    total_slippage_cost = safe_float(row.get("total_slippage_cost", "0"))
    tips: list[str] = []
    if sample_coverage["stage"] == "需降级":
        tips.append(str(sample_coverage["detail"]))
    if sharpe < 0:
        tips.append("风险调整后收益为负，当前参数需继续优化。")
    if max_drawdown < -0.10:
        tips.append("最大回撤偏大，建议提高风控阈值或降低仓位。")
    if low_liquidity_blocks > 0:
        tips.append("存在 low_liquidity 拦截，研究池前置过滤仍可继续收紧。")
    if total_commission + total_slippage_cost > 0 and trades > 0:
        tips.append("当前已纳入成本口径，可继续观察佣金和滑点对收益质量的影响。")
    if trades < 20:
        tips.append("交易样本较少，统计稳定性不足。")
    if win_rate < 0.45:
        tips.append("胜率偏低，建议结合信号阈值与止损参数联调。")
    if not tips:
        tips.append("整体表现可接受，建议持续观察近7日稳定性。")
    if sample_coverage["stage"] == "需降级":
        conclusion = "样本不足，需降级"
    elif sample_coverage["stage"] == "可上线" and sharpe >= 0.8 and max_drawdown > -0.08 and trades >= 20:
        conclusion = "可上线观察"
    elif sharpe >= 0 and max_drawdown > -0.12:
        conclusion = "可继续观察"
    else:
        conclusion = "谨慎优化中"
    return {
        "结论": conclusion,
        "总收益": f"{total_return:.2%}",
        "夏普": f"{sharpe:.3f}",
        "最大回撤": f"{max_drawdown:.2%}",
        "Calmar": f"{calmar:.3f}",
        "盈亏比": f"{reward_risk:.3f}",
        "胜率": f"{win_rate:.2%}",
        "交易次数": f"{trades}",
        "低流动性拦截": f"{low_liquidity_blocks}",
        "佣金": f"{total_commission:.2f}",
        "滑点": f"{total_slippage_cost:.2f}",
        "诊断": tips,
    }


def candidate_action_rows(candidates_csv: Path, top_n: int = 10) -> list[dict[str, str]]:
    rows = load_csv_rows(candidates_csv, limit=top_n)
    out: list[dict[str, str]] = []
    for row in rows:
        signal = row.get("signal", "-")
        risk = row.get("risk_level", "-")
        score = safe_float(row.get("final_score", "0"))
        if signal == "strong_buy" and risk == "low" and score >= 150:
            action = "优先关注（可进入候选观察池）"
        elif signal in {"strong_buy", "buy"} and risk in {"low", "medium"}:
            action = "跟踪观察（等待更优入场点）"
        else:
            action = "暂不参与（风险收益比不足）"
        prob_up = safe_float(row.get("direction_prob_up", "0"))
        pred_ret = safe_float(row.get("pred_return", "0"))
        confidence = safe_float(row.get("confidence", "0"))
        reason = (
            row.get("reason", "")
            .replace("model_bullish", "模型看多")
            .replace("factor_strong", "因子强势")
            .replace("%", "%")
        )
        out.append({
            "代码": row.get("ts_code", ""),
            "名称": row.get("stock_name", ""),
            "行业": row.get("industry", ""),
            "信号": SIGNAL_ZH.get(signal, signal),
            "风险": RISK_ZH.get(risk, risk),
            "综合分": f"{score:.2f}",
            "上涨概率": f"{prob_up:.2%}",
            "预测收益": f"{pred_ret:.2%}",
            "置信度": f"{confidence:.2%}",
            "建议仓位": f"{safe_float(row.get('position_pct', '0')):.1%}",
            "止损价": f"{safe_float(row.get('stop_loss', '0')):.2f}",
            "止盈价": f"{safe_float(row.get('take_profit', '0')):.2f}",
            "建议动作": action,
            "依据": reason,
        })
    return out


def candidate_actions_html(candidates_csv: Path, top_n: int = 10) -> str:
    rows = candidate_action_rows(candidates_csv, top_n=top_n)
    if not rows:
        return "<p>暂无候选建议，请先运行候选生成脚本。</p>"
    head = ["代码", "名称", "行业", "信号", "风险", "上涨概率", "预测收益", "置信度", "综合分", "建议仓位", "建议动作"]
    th = "".join(f"<th>{h}</th>" for h in head)
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td class='cell-code'>{html.escape(row['代码'])}</td>"
            f"<td class='cell-name'>{html.escape(row['名称'])}</td>"
            f"<td class='cell-industry'>{html.escape(row['行业'])}</td>"
            f"<td><span class='signal-chip'>{html.escape(row['信号'])}</span></td>"
            f"<td><span class='risk-chip'>{html.escape(row['风险'])}</span></td>"
            f"<td>{html.escape(row['上涨概率'])}</td>"
            f"<td>{html.escape(row['预测收益'])}</td>"
            f"<td>{html.escape(row['置信度'])}</td>"
            f"<td class='cell-score'>{html.escape(row['综合分'])}</td>"
            f"<td>{html.escape(row['建议仓位'])}</td>"
            f"<td class='cell-action'>{html.escape(row['建议动作'])}</td>"
            "</tr>"
        )
    return "<div class='table-shell'><table class='data-table data-table-candidates'><thead><tr>" + th + "</tr></thead><tbody>" + "".join(body) + "</tbody></table></div>"


def candidate_action_cards_html(candidates_csv: Path, top_n: int = 5) -> str:
    rows = candidate_action_rows(candidates_csv, top_n=top_n)
    if not rows:
        return '<div class="chart-empty">暂无候选建议，请先运行候选生成脚本。</div>'
    cards = []
    for idx, row in enumerate(rows, start=1):
        cards.append(
            '<div class="action-card">'
            f'<div class="action-card-rank">#{idx}</div>'
            '<div class="action-card-top">'
            f'<div><h4>{html.escape(row["代码"])}</h4><div class="action-card-name">{html.escape(row["名称"])} · {html.escape(row["行业"] or "未知行业")}</div></div>'
            f'<div class="action-card-score">{html.escape(row["综合分"])}</div>'
            '</div>'
            '<div class="action-chip-row">'
            f'<span class="signal-chip">{html.escape(row["信号"])}</span>'
            f'<span class="risk-chip">{html.escape(row["风险"])}</span>'
            f'<span class="action-chip">{html.escape(row["建议动作"])}</span>'
            '</div>'
            '<div class="action-metric-grid">'
            f'<div><span>上涨概率</span><strong>{html.escape(row["上涨概率"])}</strong></div>'
            f'<div><span>预测收益</span><strong>{html.escape(row["预测收益"])}</strong></div>'
            f'<div><span>置信度</span><strong>{html.escape(row["置信度"])}</strong></div>'
            f'<div><span>建议仓位</span><strong>{html.escape(row["建议仓位"])}</strong></div>'
            '</div>'
            f'<div class="action-card-note">{html.escape(row["依据"] or "暂无依据拆解")}</div>'
            '</div>'
        )
    return '<div class="action-card-grid">' + "".join(cards) + '</div>'


def build_candidate_actions_render_contract(
    candidates_csv: Path,
    *,
    card_top_n: int = 5,
    table_top_n: int = 10,
    card_hrefs: list[str] | None = None,
    table_title: str = "候选股操作建议",
    table_id: str = "candidate-actions-table",
    export_filename: str = "candidate_actions_view.csv",
) -> dict[str, object]:
    card_rows = candidate_action_rows(candidates_csv, top_n=card_top_n)
    if card_hrefs:
        for idx, row in enumerate(card_rows):
            if idx < len(card_hrefs):
                row["href"] = card_hrefs[idx]
    return {
        "card_rows": card_rows,
        "table_rows": candidate_action_rows(candidates_csv, top_n=table_top_n),
        "table_title": table_title,
        "table_id": table_id,
        "export_filename": export_filename,
    }


def executive_summary(
    health_status: str,
    bt_diag: dict[str, str | list[str]],
    top1_signal: str,
    top1_risk: str,
    top1_code: str,
) -> list[str]:
    lines = [f"系统当前状态为“{health_status}”，建议先看健康评分与告警。"]
    lines.append(
        f"策略综合结论为“{bt_diag.get('结论', '未评估')}”，"
        f"最新回测：总收益 {bt_diag.get('总收益', '-')}, 夏普 {bt_diag.get('夏普', '-')}, 最大回撤 {bt_diag.get('最大回撤', '-')}"
    )
    lines.append(f"今日候选Top1是 {top1_code}，信号“{top1_signal}”，风险“{top1_risk}”。")
    if top1_signal in {"强烈看多", "看多"} and top1_risk == "低风险":
        lines.append("操作建议：可优先纳入观察池，分批小仓位试探，严格执行止损。")
    else:
        lines.append("操作建议：暂以观察为主，等待更强信号或风险下降后再考虑。")
    return lines


def load_update_status(exp_dir: Path) -> dict[str, str]:
    path = exp_dir / "update_status_latest.json"
    base = {
        "status": "暂无",
        "stage": "-",
        "last_run": "-",
        "duration": "-",
        "db_latest": "-",
        "written_rows": "-",
        "post_candidates": "-",
        "post_daily_research": "-",
        "post_candidates_mode": "-",
        "post_candidates_elapsed_sec": "-",
        "post_candidates_effective_universe_size": "-",
        "post_candidates_used_attempt": "-",
        "manual_candidates_last_run": "-",
        "manual_evolution_last_run": "-",
        "manual_evolution_action": "-",
        "manual_evolution_version": "-",
    }
    if not path.exists():
        return base
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        base["status"] = "读取失败"
        return base
    started = str(obj.get("started_at", "-"))
    ended = str(obj.get("ended_at", "-"))
    duration = "-"
    try:
        if started != "-" and ended != "-":
            from datetime import datetime

            duration = f"{(datetime.fromisoformat(ended) - datetime.fromisoformat(started)).total_seconds():.1f}秒"
    except Exception:
        duration = "-"
    update_summary = obj.get("update_summary", {}) or {}
    post_candidates = obj.get("post_candidates", {}) or {}
    post_candidates_meta = obj.get("post_candidates_meta", {}) or {}
    post_daily_research = obj.get("post_daily_research", {}) or {}
    manual_runs = obj.get("manual_runs", {}) or {}
    manual_candidates = manual_runs.get("candidates", {}) if isinstance(manual_runs, dict) else {}
    manual_evolution = obj.get("manual_evolution", {}) or (manual_runs.get("evolution", {}) if isinstance(manual_runs, dict) else {})
    if isinstance(post_candidates, bool):
        post_candidates = {"ok": post_candidates}
    elif not isinstance(post_candidates, dict):
        post_candidates = {}
    if not isinstance(post_candidates_meta, dict):
        post_candidates_meta = {}
    if isinstance(post_daily_research, bool):
        post_daily_research = {"ok": post_daily_research}
    elif not isinstance(post_daily_research, dict):
        post_daily_research = {}
    candidate_last_run = str(manual_candidates.get("ended_at", "-") or "-")
    evolution_last_run = str(manual_evolution.get("ended_at", "-") or "-")
    effective_last_run = ended if ended not in {"-", "None"} else started
    for candidate in [candidate_last_run, evolution_last_run]:
        if candidate not in {"-", "", "None"} and str(candidate) > str(effective_last_run):
            effective_last_run = str(candidate)
    result = {
        "status": str(obj.get("status", "-")),
        "stage": str(obj.get("stage", "-")),
        "last_run": effective_last_run,
        "duration": duration,
        "db_latest": str(update_summary.get("db_latest_after", update_summary.get("db_latest_before", "-"))),
        "written_rows": str(update_summary.get("written_rows", "-")),
        "progress_pct": str(update_summary.get("progress_pct", "-")),
        "post_candidates": "成功" if post_candidates.get("ok") is True else ("失败" if post_candidates.get("ok") is False else "-"),
        "post_daily_research": "成功" if post_daily_research.get("ok") is True else ("失败" if post_daily_research.get("ok") is False else "-"),
        "post_candidates_mode": str(post_candidates_meta.get("mode", "-") or "-"),
        "post_candidates_elapsed_sec": str(post_candidates_meta.get("elapsed_sec", "-") or "-"),
        "post_candidates_effective_universe_size": str(post_candidates_meta.get("effective_universe_size", "-") or "-"),
        "post_candidates_used_attempt": str(post_candidates_meta.get("used_attempt", "-") or "-"),
        "manual_candidates_last_run": candidate_last_run,
        "manual_evolution_last_run": evolution_last_run,
        "manual_evolution_action": str((manual_evolution.get("meta", {}) or {}).get("action", "-") or "-"),
        "manual_evolution_version": str((manual_evolution.get("meta", {}) or {}).get("champion_version", "-") or "-"),
    }
    if (
        result["status"] == "partial_success"
        and result["post_candidates"] == "成功"
        and result["post_daily_research"] == "成功"
    ):
        result["status"] = "completed"
    return result

def load_recent_update_health(root: Path) -> dict[str, str]:
    settings_path = _dashboard_settings_path(root)
    if settings_path is None:
        return {"success_rate_7d": "-", "runs_7d": "-", "fail_7d": "-"}
    try:
        settings = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
        data_cfg = settings.get("data", {})
        raw_db = str(data_cfg.get("sqlite_db_path", "")).strip()
        db_path = (root / raw_db).resolve() if raw_db else None
        if db_path is None or not db_path.exists():
            return {"success_rate_7d": "-", "runs_7d": "-", "fail_7d": "-"}
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, created_at
            FROM data_update_log
            WHERE update_type='daily_trading_update'
            ORDER BY id DESC
            LIMIT 14
            """
        )
        rows = cur.fetchall()
        conn.close()
        if not rows:
            return {"success_rate_7d": "-", "runs_7d": "0", "fail_7d": "0"}
        rows = rows[:7]
        total = len(rows)
        success = sum(1 for row in rows if str(row[0]) in {"completed", "up_to_date"})
        failed = total - success
        rate = (success / total * 100.0) if total > 0 else 0.0
        return {
            "success_rate_7d": f"{rate:.1f}%",
            "runs_7d": str(total),
            "fail_7d": str(failed),
        }
    except Exception:
        return {"success_rate_7d": "-", "runs_7d": "-", "fail_7d": "-"}


def load_recent_update_events(root: Path, limit: int = 8) -> list[dict[str, str]]:
    settings_path = _dashboard_settings_path(root)
    if settings_path is None:
        return []
    try:
        settings = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
        data_cfg = settings.get("data", {})
        raw_db = str(data_cfg.get("sqlite_db_path", "")).strip()
        db_path = (root / raw_db).resolve() if raw_db else None
        if db_path is None or not db_path.exists():
            return []
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, update_type, created_at
            FROM data_update_log
            WHERE update_type='daily_trading_update'
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cur.fetchall()
        conn.close()
        return [
            {
                "status": str(status or "-"),
                "update_type": str(update_type or "-"),
                "created_at": str(created_at or "-"),
            }
            for status, update_type, created_at in rows
        ]
    except Exception:
        return []


def load_research_batch_status(exp_dir: Path) -> dict[str, str]:
    path = exp_dir / "research_batch_latest.json"
    base = {
        "status": "暂无",
        "last_run": "-",
        "candidate_universe_size": "-",
        "candidate_top_n": "-",
        "daily_profiles": "-",
        "backtest_profile": "-",
        "stock_pool_size": "-",
        "failed_step": "-",
        "search_mode": "-",
        "experiment": "-",
        "liquidity_min_turnover": "-",
        "liquidity_filtered_out": "-",
    }
    if not path.exists():
        return base
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        base["status"] = "读取失败"
        return base
    steps = obj.get("steps", {}) or {}
    failed_step = "-"
    for key, step in steps.items():
        if int(step.get("returncode", 0) or 0) != 0:
            failed_step = key
            break
    return {
        "status": str(obj.get("status", "-")),
        "last_run": str(obj.get("ended_at", obj.get("started_at", "-"))),
        "candidate_universe_size": str(obj.get("candidate_universe_size", "-")),
        "candidate_top_n": str(obj.get("candidate_top_n", "-")),
        "daily_profiles": ", ".join(obj.get("daily_profiles", []) or []) or "-",
        "backtest_profile": str(obj.get("backtest_profile", "-")),
        "stock_pool_size": str(obj.get("research_pool_size", len(obj.get("stock_pool", []) or []))),
        "failed_step": failed_step,
        "search_mode": str(obj.get("search_mode", "-")),
        "experiment": str(obj.get("experiment", "-")),
        "liquidity_min_turnover": str(
            (obj.get("research_pool_meta", {}) or {}).get(
                "effective_liquidity_min_turnover",
                (obj.get("research_pool_meta", {}) or {}).get("liquidity_min_turnover", "-"),
            )
        ),
        "liquidity_filtered_out": str((obj.get("research_pool_meta", {}) or {}).get("liquidity_filtered_out", "-")),
    }


def load_daily_research_runtime_status(exp_dir: Path) -> dict[str, str]:
    path = exp_dir / "daily_research_status_latest.json"
    base = {
        "state": "暂无",
        "stage": "-",
        "started_at": "-",
        "ended_at": "-",
        "completed_profiles": "-",
        "failed_profiles": "-",
        "alert_count": "-",
        "health_score": "-",
        "duration": "-",
        "active_profile": "-",
        "active_progress": "-",
        "search_mode": "-",
        "experiment": "-",
        "liquidity_filtered_out": "-",
    }
    if not path.exists():
        return base
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        base["state"] = "读取失败"
        return base

    started = str(obj.get("started_at", "-"))
    ended = str(obj.get("ended_at", "-"))
    duration = "-"
    try:
        if started != "-" and ended not in {"-", "None", ""}:
            from datetime import datetime

            duration = f"{(datetime.fromisoformat(ended) - datetime.fromisoformat(started)).total_seconds():.1f}秒"
    except Exception:
        duration = "-"

    completed_profiles = obj.get("completed_profiles", []) or []
    failed_profiles = obj.get("failed_profiles", []) or []
    health_score = obj.get("health_score", {}) or {}
    active_profile = str(obj.get("active_profile", "-"))
    progress = obj.get("active_progress", {}) or {}
    active_progress = "-"
    if isinstance(progress, dict) and progress:
        phase = str(progress.get("phase", "-"))
        executed = progress.get("executed_runs")
        planned = progress.get("planned_runs")
        if executed is not None and planned is not None:
            active_progress = f"{phase} {executed}/{planned}"
        else:
            active_progress = phase
    return {
        "state": str(obj.get("state", "-")),
        "stage": str(obj.get("stage", "-")),
        "started_at": started,
        "ended_at": ended,
        "completed_profiles": ", ".join(str(item) for item in completed_profiles) or "-",
        "failed_profiles": ", ".join(str(item) for item in failed_profiles) or "-",
        "alert_count": str(obj.get("alert_count", "-")),
        "health_score": f"{float(health_score.get('score', 0.0)):.2f}" if health_score else "-",
        "duration": duration,
        "active_profile": active_profile,
        "active_progress": active_progress,
        "search_mode": str(obj.get("search_mode", "-")),
        "experiment": str(obj.get("experiment", "-")),
        "liquidity_filtered_out": str((obj.get("research_pool_meta", {}) or {}).get("liquidity_filtered_out", "-")),
    }


def resolve_automation_status(update_status: dict[str, str], daily_research_status: dict[str, str]) -> dict[str, str]:
    resolved = dict(update_status)
    update_state = str(update_status.get("status", "暂无"))
    daily_state = str(daily_research_status.get("state", "暂无"))
    post_candidates = str(update_status.get("post_candidates", "-"))
    post_daily_research = str(update_status.get("post_daily_research", "-"))
    completed_profiles = str(daily_research_status.get("completed_profiles", "-"))
    failed_profiles = str(daily_research_status.get("failed_profiles", "-"))

    if update_state == "running" and daily_state == "completed":
        resolved["status"] = "completed"
        resolved["stage"] = "done"
        if daily_research_status.get("ended_at", "-") not in {"-", "", "None"}:
            resolved["last_run"] = str(daily_research_status.get("ended_at"))
        if daily_research_status.get("duration", "-") not in {"-", ""}:
            resolved["duration"] = str(daily_research_status.get("duration"))
        if update_status.get("post_daily_research") in {"-", "失败"}:
            resolved["post_daily_research"] = "成功"
        resolved["status_note"] = "更新链旧状态未收尾，已按最新每日研究完成结果修正展示。"
        return resolved

    if (
        update_state == "partial_success"
        and post_candidates == "成功"
        and daily_state == "completed"
        and completed_profiles not in {"", "-"}
        and failed_profiles in {"", "-"}
        and post_daily_research in {"-", "成功"}
    ):
        resolved["status"] = "completed"
        resolved["stage"] = "done"
        if daily_research_status.get("ended_at", "-") not in {"-", "", "None"}:
            resolved["last_run"] = str(daily_research_status.get("ended_at"))
        if daily_research_status.get("duration", "-") not in {"-", ""}:
            resolved["duration"] = str(daily_research_status.get("duration"))
        resolved["post_daily_research"] = "成功"
        resolved["status_note"] = "旧更新链记录为待补齐，但候选与独立每日研究均已成功完成，已按最终结果修正展示。"
        return resolved

    resolved["status_note"] = "-"
    return resolved


def automation_health_summary(update_status: dict[str, str]) -> dict[str, str]:
    status = str(update_status.get("status", "暂无") or "暂无")
    post_candidates = str(update_status.get("post_candidates", "-") or "-")
    mode = str(update_status.get("post_candidates_mode", "-") or "-")
    elapsed = str(update_status.get("post_candidates_elapsed_sec", "-") or "-")
    effective_size = str(update_status.get("post_candidates_effective_universe_size", "-") or "-")

    if status in {"failed", "partial_success"} or post_candidates == "失败":
        return {
            "health": "failed",
            "label": "失败",
            "detail": "自动链路未完整收尾，建议优先检查候选后处理。",
        }
    if status in {"completed", "up_to_date"} and post_candidates == "成功":
        if mode == "quick":
            return {
                "health": "healthy",
                "label": "健康",
                "detail": f"Quick path 正常，候选 {elapsed}s，池规模 {effective_size}。",
            }
        return {
            "health": "healthy",
            "label": "健康",
            "detail": "自动链路正常完成。",
        }
    return {
        "health": "unknown",
        "label": "待确认",
        "detail": "状态文件已生成，但当前缺少足够证据确认自动链路健康度。",
    }


def load_research_topology(root: Path) -> dict[str, str]:
    settings_path = _dashboard_settings_path(root)
    deploy_root = root / "deploy" / "aliyun"
    nightly_service_path = deploy_root / "stock-ultimate-nightly-research.service"
    weekly_service_path = deploy_root / "stock-ultimate-weekly-long.service"
    update_service_path = deploy_root / "stock-ultimate-update.service"
    base = {
        "candidate_scan_scope": "-",
        "candidate_top_n": "-",
        "formal_research_pool_rule": "-",
        "formal_research_pool_size": "-",
        "weekly_long_pool_size": "-",
        "nightly_universe_size": "-",
    }
    try:
        if settings_path is not None:
            settings = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
            data_cfg = settings.get("data", {}) or {}
            research_cfg = data_cfg.get("research_pool", {}) or {}
            min_mv = research_cfg.get("min_total_mv_yi")
            max_mv = research_cfg.get("max_total_mv_yi")
            size = research_cfg.get("size")
            if min_mv is not None and max_mv is not None:
                base["formal_research_pool_rule"] = f"{min_mv}亿-{max_mv}亿市值"
            if size is not None:
                base["formal_research_pool_size"] = str(size)
    except Exception:
        pass

    def _extract_flag_value(path: Path, flag: str) -> str | None:
        if not path.exists():
            return None
        text = path.read_text(encoding="utf-8")
        tokens = text.replace("\n", " ").split()
        for idx, token in enumerate(tokens):
            if token == flag and idx + 1 < len(tokens):
                return tokens[idx + 1]
        return None

    post_universe_size = _extract_flag_value(update_service_path, "--post-universe-size")
    if post_universe_size == "0":
        base["candidate_scan_scope"] = "全A"
    elif post_universe_size:
        base["candidate_scan_scope"] = f"{post_universe_size}只"

    post_top_n = _extract_flag_value(update_service_path, "--post-top-n")
    if post_top_n:
        base["candidate_top_n"] = post_top_n

    nightly_universe = _extract_flag_value(nightly_service_path, "--candidate-universe-size")
    if nightly_universe:
        base["nightly_universe_size"] = nightly_universe

    weekly_size = _extract_flag_value(weekly_service_path, "--research-pool-size")
    if weekly_size:
        base["weekly_long_pool_size"] = weekly_size

    return base


def load_grid_backtest_status(exp_dir: Path) -> dict[str, str]:
    grid_dir = exp_dir / "grid_search"
    latest_csv = grid_dir / "grid_backtest_latest.csv"
    latest_md = grid_dir / "grid_backtest_latest.md"
    governance_json = grid_dir / "grid_backtest_governance_latest.json"
    base = {
        "last_run": "-",
        "rows": "-",
        "latest_csv": str(latest_csv) if latest_csv.exists() else "-",
        "latest_md": str(latest_md) if latest_md.exists() else "-",
        "validation_window": "-",
        "replay_runs": "-",
        "regime_coverage_score": "-",
        "parameter_sensitivity_score": "-",
        "observed_regimes": "-",
        "sampling_mode": "-",
    }
    if not latest_csv.exists():
        if not governance_json.exists():
            return base
    if latest_csv.exists():
        try:
            stat = latest_csv.stat()
            rows = sum(1 for _ in latest_csv.open("r", encoding="utf-8")) - 1
            base["last_run"] = str(
                __import__("datetime").datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            )
            base["rows"] = str(max(rows, 0))
        except Exception:
            pass
    if governance_json.exists():
        payload = read_json(governance_json)
        validation_window = payload.get("validation_window", {}) or {}
        validation_start = str(validation_window.get("start_date", "") or "")
        validation_end = str(validation_window.get("end_date", "") or "")
        if validation_start and validation_end:
            base["validation_window"] = f"{validation_start} ~ {validation_end}"
        base["replay_runs"] = str(payload.get("replay_runs", "-"))
        base["regime_coverage_score"] = f'{float(payload.get("regime_coverage_score", 0.0)):.4f}'
        base["parameter_sensitivity_score"] = f'{float(payload.get("parameter_sensitivity_score", 0.0)):.4f}'
        observed = payload.get("observed_regimes", []) or []
        if isinstance(observed, list) and observed:
            base["observed_regimes"] = ", ".join(str(item) for item in observed)
        base["sampling_mode"] = str(payload.get("sampling_mode", "-") or "-")
    return base


def load_evolution_status(exp_dir: Path) -> dict[str, str | list[dict[str, str]]]:
    base: dict[str, str | list[dict[str, str]]] = {
        "champion_version": "-",
        "champion_walk_forward_score": "-",
        "champion_stability": "-",
        "champion_models": "-",
        "latest_action": "-",
        "latest_reason": "-",
        "latest_feedback_level": "-",
        "latest_feedback_window": "-",
        "latest_feedback_summary": "-",
        "latest_feedback_change_total": "0",
        "latest_feedback_gate_status": "-",
        "latest_capacity_state": "-",
        "latest_capacity_profile": "-",
        "latest_capacity_gate_status": "-",
        "latest_capacity_stress_score": "0.0",
        "history": [],
    }
    registry_path = exp_dir / "evolution_registry_latest.json"
    if not registry_path.exists():
        return base
    try:
        obj = json.loads(registry_path.read_text(encoding="utf-8"))
    except Exception:
        base["latest_action"] = "读取失败"
        return base
    champion_summary = obj.get("champion_summary", {}) or {}
    champion_payload = obj.get("champion_payload", {}) or {}
    champion_models = (
        (champion_payload.get("model_evolution", {}) or {}).get("selected_models", []) or []
    )
    history_rows = obj.get("history", []) or []
    latest = history_rows[-1] if history_rows else {}
    latest_gates = latest.get("gates", {}) or {}
    latest_execution_feedback = latest_gates.get("execution_feedback", {}) or {}
    latest_capacity_pressure = latest_gates.get("capacity_pressure", {}) or {}
    base["champion_version"] = str(obj.get("champion_version", "-") or "-")
    if champion_summary:
        base["champion_walk_forward_score"] = f'{float(champion_summary.get("walk_forward_score", 0.0)):.4f}'
        base["champion_stability"] = f'{float(champion_summary.get("trade_objective_stability", 0.0)):.4f}'
    if champion_models:
        base["champion_models"] = " / ".join(str(name) for name in champion_models)
    base["latest_action"] = str(latest.get("action", "-") or "-")
    base["latest_reason"] = str(latest.get("reason", "-") or "-")
    if latest_execution_feedback:
        feedback_level = str(latest_execution_feedback.get("feedback_level", "-") or "-")
        review_only = bool(latest_execution_feedback.get("review_only", False))
        passed = bool(latest_execution_feedback.get("passed", False))
        gate_status = "通过"
        if feedback_level == "tighten":
            gate_status = "收紧阻断"
        elif review_only:
            gate_status = "复核观察"
        elif not passed:
            gate_status = "未通过"
        base["latest_feedback_level"] = feedback_level
        base["latest_feedback_window"] = str(latest_execution_feedback.get("window_label", "-") or "-")
        base["latest_feedback_summary"] = str(latest_execution_feedback.get("summary_note", "-") or "-")
        base["latest_feedback_change_total"] = str(int(latest_execution_feedback.get("change_total", 0) or 0))
        base["latest_feedback_gate_status"] = gate_status
    if latest_capacity_pressure:
        capacity_state = str(latest_capacity_pressure.get("capacity_state", "-") or "-")
        watch_only = bool(latest_capacity_pressure.get("watch_only", False))
        passed = bool(latest_capacity_pressure.get("passed", False))
        capacity_gate_status = "通过"
        if capacity_state == "stretched":
            capacity_gate_status = "容量阻断"
        elif watch_only:
            capacity_gate_status = "容量观察"
        elif not passed:
            capacity_gate_status = "未通过"
        base["latest_capacity_state"] = capacity_state
        base["latest_capacity_profile"] = str(latest_capacity_pressure.get("recommended_scale_profile", "-") or "-")
        base["latest_capacity_gate_status"] = capacity_gate_status
        base["latest_capacity_stress_score"] = f'{float(latest_capacity_pressure.get("worst_stress_score", 0.0) or 0.0):.1f}'
    normalized: list[dict[str, str]] = []
    for row in reversed(history_rows[-5:]):
        row_execution_feedback = ((row.get("gates", {}) or {}).get("execution_feedback", {}) or {})
        row_capacity_pressure = ((row.get("gates", {}) or {}).get("capacity_pressure", {}) or {})
        normalized.append({
            "version": str(row.get("version", "") or ""),
            "action": str(row.get("action", "") or ""),
            "reason": str(row.get("reason", "") or ""),
            "created_at": str(row.get("created_at", "") or ""),
            "feedback_level": str(row_execution_feedback.get("feedback_level", "") or ""),
            "feedback_gate_status": (
                "收紧阻断"
                if str(row_execution_feedback.get("feedback_level", "") or "") == "tighten"
                else "复核观察"
                if bool(row_execution_feedback.get("review_only", False))
                else "通过"
                if row_execution_feedback
                else ""
            ),
            "capacity_state": str(row_capacity_pressure.get("capacity_state", "") or ""),
            "capacity_gate_status": (
                "容量阻断"
                if str(row_capacity_pressure.get("capacity_state", "") or "") == "stretched"
                else "容量观察"
                if bool(row_capacity_pressure.get("watch_only", False))
                else "通过"
                if row_capacity_pressure
                else ""
            ),
        })
    base["history"] = normalized
    return base


def load_candidate_artifact_status(exp_dir: Path) -> dict[str, str]:
    base = {
        "generated_at": "-",
        "basket_generated_at": "-",
        "rows": "-",
        "top1": "-",
        "generation_mode": "-",
        "generation_reason": "-",
        "strategy_mode": "-",
        "strategy_strictness": "-",
        "strategy_weak_market_action": "-",
        "current_basket_pointer_updated_at": "-",
        "current_basket_pointer_status": "-",
        "current_basket_pointer_basket_id": "-",
        "latest_basket_attempt_generated_at": "-",
        "latest_basket_attempt_status": "-",
        "latest_basket_attempt_basket_id": "-",
        "latest_basket_attempt_blocking_reason": "-",
        "runtime_status": "-",
        "runtime_stage": "-",
        "runtime_stage_label": "-",
        "runtime_detail": "-",
        "runtime_updated_at": "-",
        "runtime_elapsed_sec": "-",
        "runtime_results_ready": "-",
        "runtime_skipped_count": "-",
    }
    csv_path = exp_dir / "candidates_top_latest.csv"
    basket_path = exp_dir / "candidates_basket_summary_latest.json"
    runtime_path = exp_dir / "candidates_run_status_latest.json"
    strategy_path = exp_dir / "candidate_strategy_profile_latest.json"
    current_pointer_path = exp_dir.parent.parent / "artifacts" / "primary_result_candidate_baskets" / "current.json"
    latest_attempt_path = exp_dir.parent.parent / "artifacts" / "primary_result_candidate_baskets" / "latest_attempt.json"
    if not csv_path.exists():
        return base
    try:
        stat = csv_path.stat()
        base["generated_at"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        rows = load_csv_rows(csv_path, limit=50)
        base["rows"] = str(len(rows))
        if rows:
            base["top1"] = str(rows[0].get("ts_code", "-"))
    except Exception:
        pass
    if basket_path.exists():
        try:
            basket = json.loads(basket_path.read_text(encoding="utf-8"))
            stat = basket_path.stat()
            base["basket_generated_at"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            degraded = bool(basket.get("generation_degraded", False))
            base["generation_mode"] = "interim" if degraded else "final"
            base["generation_reason"] = str(basket.get("generation_reason", "") or ("完整候选结果" if not degraded else "-"))
            base["strategy_mode"] = str(basket.get("strategy_mode", "-") or "-")
            base["strategy_strictness"] = str(basket.get("strategy_strictness", "-") or "-")
            base["strategy_weak_market_action"] = str(basket.get("strategy_weak_market_action", "-") or "-")
        except Exception:
            pass
    if strategy_path.exists() and (
        base["strategy_mode"] == "-" or base["strategy_strictness"] == "-" or base["strategy_weak_market_action"] == "-"
    ):
        try:
            strategy = json.loads(strategy_path.read_text(encoding="utf-8"))
            base["strategy_mode"] = str(strategy.get("selection_mode", base["strategy_mode"]) or base["strategy_mode"])
            base["strategy_strictness"] = str(strategy.get("strictness", base["strategy_strictness"]) or base["strategy_strictness"])
            base["strategy_weak_market_action"] = str(
                strategy.get("weak_market_action", base["strategy_weak_market_action"]) or base["strategy_weak_market_action"]
            )
        except Exception:
            pass
    if current_pointer_path.exists():
        try:
            current_pointer = json.loads(current_pointer_path.read_text(encoding="utf-8"))
            base["current_basket_pointer_updated_at"] = str(current_pointer.get("updated_at", "-") or "-")
            base["current_basket_pointer_status"] = str(current_pointer.get("status", "-") or "-")
            base["current_basket_pointer_basket_id"] = str(current_pointer.get("basket_id", "-") or "-")
        except Exception:
            pass
    if latest_attempt_path.exists():
        try:
            latest_attempt = json.loads(latest_attempt_path.read_text(encoding="utf-8"))
            stat = latest_attempt_path.stat()
            base["latest_basket_attempt_generated_at"] = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            base["latest_basket_attempt_status"] = str(latest_attempt.get("status", "-") or "-")
            base["latest_basket_attempt_basket_id"] = str(latest_attempt.get("basket_id", "-") or "-")
            blocking_reasons = latest_attempt.get("blocking_reasons", [])
            if isinstance(blocking_reasons, list) and blocking_reasons:
                base["latest_basket_attempt_blocking_reason"] = str(blocking_reasons[0] or "-")
        except Exception:
            pass
    if runtime_path.exists():
        try:
            runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
            base["runtime_status"] = str(runtime.get("status", "-") or "-")
            base["runtime_stage"] = str(runtime.get("stage", "-") or "-")
            base["runtime_stage_label"] = str(runtime.get("stage_label", "-") or "-")
            base["runtime_detail"] = str(runtime.get("detail", "-") or "-")
            base["runtime_updated_at"] = str(runtime.get("updated_at", "-") or "-")
            elapsed = runtime.get("elapsed_sec", "-")
            if isinstance(elapsed, (int, float)):
                base["runtime_elapsed_sec"] = f"{float(elapsed):.1f}"
            else:
                base["runtime_elapsed_sec"] = str(elapsed or "-")
            base["runtime_results_ready"] = str(runtime.get("results_ready", "-") or "-")
            base["runtime_skipped_count"] = str(runtime.get("skipped_count", "-") or "-")
        except Exception:
            pass
    return base


def _compact_trade_date(value: object) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    if len(text) >= 8 and text[:8].isdigit():
        return text[:8]
    return text


def load_prefilter_artifact_status(
    exp_dir: Path,
    update_status: dict[str, str] | None = None,
) -> dict[str, str | list[dict[str, str]]]:
    base: dict[str, str | list[dict[str, str]]] = {
        "generated_at": "-",
        "trade_date": "-",
        "expected_trade_date": "-",
        "freshness_status": "unknown",
        "freshness_note": "预筛产物状态待确认",
        "row_count": "0",
        "market_symbol_count": "0",
        "excluded_count": "0",
        "pass_rate_pct": "0.0%",
        "excluded_rate_pct": "0.0%",
        "configured_liquidity_min_turnover": "0",
        "effective_liquidity_min_turnover": "0",
        "top1": "-",
        "top1_name": "-",
        "top1_reason": "-",
        "top10_count": "0",
        "top_exclusion_reason": "-",
        "top_exclusion_reason_count": "0",
        "top_candidates": [],
        "exclusion_summary": [],
        "top_exclusions": [],
    }
    json_path = exp_dir / "candidate_prefilter_universe_latest.json"
    if not json_path.exists():
        return base
    payload = read_json(json_path)
    if not payload:
        return base
    base["generated_at"] = str(payload.get("generated_at", "") or "-")
    base["trade_date"] = str(payload.get("trade_date", "") or "-")
    trade_date_key = _compact_trade_date(base["trade_date"])
    update_status = update_status or {}
    expected_trade_date = _compact_trade_date(update_status.get("db_latest", "-"))
    base["expected_trade_date"] = expected_trade_date or "-"
    post_candidates = str(update_status.get("post_candidates", "-") or "-")
    update_state = str(update_status.get("status", "-") or "-")
    if expected_trade_date and trade_date_key and expected_trade_date > trade_date_key:
        base["freshness_status"] = "stale"
        base["freshness_note"] = (
            f"预筛仍停留在 {trade_date_key}，数据库最新交易日为 {expected_trade_date}，"
            "不得按今日结果使用。"
        )
    elif post_candidates == "失败":
        base["freshness_status"] = "blocked"
        base["freshness_note"] = "候选后处理失败，预筛结果不得直接作为今日候选依据。"
    elif update_state == "partial_success":
        base["freshness_status"] = "degraded"
        base["freshness_note"] = "自动更新链路部分成功，预筛结果需要人工复核。"
    else:
        base["freshness_status"] = "fresh"
        base["freshness_note"] = "预筛交易日与自动更新状态一致。"
    base["row_count"] = str(payload.get("row_count", 0) or 0)
    base["market_symbol_count"] = str(payload.get("market_symbol_count", 0) or 0)
    base["excluded_count"] = str(payload.get("excluded_count", 0) or 0)
    try:
        market_symbol_count = float(base["market_symbol_count"])
        row_count = float(base["row_count"])
        excluded_count = float(base["excluded_count"])
        if market_symbol_count > 0:
            base["pass_rate_pct"] = f"{row_count / market_symbol_count * 100.0:.1f}%"
            base["excluded_rate_pct"] = f"{excluded_count / market_symbol_count * 100.0:.1f}%"
    except Exception:
        pass
    base["configured_liquidity_min_turnover"] = str(payload.get("configured_liquidity_min_turnover", 0) or 0)
    base["effective_liquidity_min_turnover"] = str(payload.get("effective_liquidity_min_turnover", 0) or 0)
    top_candidates = payload.get("top_candidates") or []
    top_exclusions = payload.get("top_exclusions") or []
    name_index: dict[str, str] = {}
    for csv_path in sorted(exp_dir.glob("candidates_top_*.csv")) + [exp_dir / "candidates_top_latest.csv"]:
        if not csv_path.exists():
            continue
        try:
            rows = load_csv_rows(csv_path, limit=200)
        except Exception:
            continue
        for csv_row in rows:
            code = str(csv_row.get("ts_code", "") or "")
            name = str(csv_row.get("stock_name", "") or "")
            if code and name and code not in name_index:
                name_index[code] = name

    missing_codes: list[str] = []
    for row in list(top_candidates[:10]) + list(top_exclusions[:10]):
        if not isinstance(row, dict):
            continue
        ts_code = str(row.get("ts_code", "") or "").strip()
        if ts_code and ts_code not in name_index and ts_code not in missing_codes:
            missing_codes.append(ts_code)
    if missing_codes:
        try:
            settings_path = None
            for candidate_root in [exp_dir] + list(exp_dir.parents):
                settings_path = _dashboard_settings_path(candidate_root)
                if settings_path is not None and settings_path.exists():
                    break
            if settings_path is not None and settings_path.exists():
                settings = yaml.safe_load(settings_path.read_text(encoding="utf-8")) or {}
                data_cfg = settings.get("data", {})
                raw_db = str(data_cfg.get("sqlite_db_path", "")).strip()
                config_root = settings_path.parent.parent
                db_path = (config_root / raw_db).resolve() if raw_db else None
                if db_path is not None and db_path.exists():
                    conn = sqlite3.connect(str(db_path))
                    cur = conn.cursor()
                    chunk_size = 800
                    for start in range(0, len(missing_codes), chunk_size):
                        chunk = missing_codes[start:start + chunk_size]
                        if not chunk:
                            continue
                        placeholders = ",".join(["?"] * len(chunk))
                        cur.execute(
                            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
                            chunk,
                        )
                        for code, name in cur.fetchall():
                            code_text = str(code or "").strip()
                            name_text = str(name or "").strip()
                            if code_text and name_text and code_text not in name_index:
                                name_index[code_text] = name_text
                    conn.close()
        except Exception:
            pass

    normalized: list[dict[str, str]] = []
    for row in top_candidates[:10]:
        if not isinstance(row, dict):
            continue
        ts_code = str(row.get("ts_code", "") or "")
        normalized.append(
            {
                "ts_code": ts_code,
                "stock_name": str(row.get("stock_name", "") or name_index.get(ts_code, "") or ""),
                "prefilter_score": str(row.get("prefilter_score", "") or ""),
                "prefilter_reason": str(row.get("prefilter_reason", "") or ""),
            }
        )
    base["top_candidates"] = normalized
    base["top10_count"] = str(len(normalized))
    if normalized:
        base["top1"] = normalized[0].get("ts_code", "-") or "-"
        base["top1_name"] = normalized[0].get("stock_name", "-") or "-"
        base["top1_reason"] = normalized[0].get("prefilter_reason", "-") or "-"
    normalized_exclusions: list[dict[str, str]] = []
    for row in top_exclusions[:10]:
        if not isinstance(row, dict):
            continue
        ts_code = str(row.get("ts_code", "") or "")
        normalized_exclusions.append(
            {
                "ts_code": ts_code,
                "stock_name": str(row.get("stock_name", "") or name_index.get(ts_code, "") or ""),
                "exclusion_reason": str(row.get("exclusion_reason", "") or ""),
                "exclusion_reason_zh": str(row.get("exclusion_reason_zh", "") or ""),
            }
        )
    base["top_exclusions"] = normalized_exclusions
    exclusion_summary = payload.get("exclusion_summary") or []
    if exclusion_summary and isinstance(exclusion_summary, list):
        normalized_summary: list[dict[str, str]] = []
        total_excluded = max(1, int(float(base["excluded_count"])))
        for row in exclusion_summary[:5]:
            if not isinstance(row, dict):
                continue
            count = int(row.get("count", 0) or 0)
            normalized_summary.append(
                {
                    "reason": str(row.get("reason", "") or "-"),
                    "count": str(count),
                    "share_pct": f"{count / total_excluded * 100.0:.1f}%",
                }
            )
        base["exclusion_summary"] = normalized_summary
        first = exclusion_summary[0] if isinstance(exclusion_summary[0], dict) else {}
        base["top_exclusion_reason"] = str(first.get("reason", "") or base["top_exclusion_reason"])
        base["top_exclusion_reason_count"] = str(first.get("count", 0) or 0)
    elif normalized_exclusions:
        base["top_exclusion_reason"] = normalized_exclusions[0].get("exclusion_reason_zh", "-") or "-"
    return base


def update_timeline_html(events: list[dict[str, str]]) -> str:
    if not events:
        return '<div class="chart-empty">暂无近期任务历史</div>'
    items = []
    for event in events:
        status = event.get("status", "-")
        if status in {"completed", "up_to_date"}:
            tone = "timeline-good"
            status_zh = "完成"
        elif status in {"partial_success", "running"}:
            tone = "timeline-warn"
            status_zh = "待补齐" if status == "partial_success" else "进行中"
        else:
            tone = "timeline-bad"
            status_zh = status
        items.append(
            f"""
            <div class="timeline-item">
              <div class="timeline-dot {tone}"></div>
              <div class="timeline-content">
                <div class="timeline-top">
                  <strong>{html.escape(status_zh)}</strong>
                  <span>{html.escape(event.get("created_at", "-"))}</span>
                </div>
                <div class="timeline-sub">{html.escape(event.get("update_type", "-"))}</div>
              </div>
            </div>
            """
        )
    return '<div class="timeline-list">' + ''.join(items) + '</div>'


def update_alerts_html(
    update_status: dict[str, str],
    events: list[dict[str, str]],
    daily_research_status: dict[str, str] | None = None,
) -> str:
    effective_status = (
        resolve_automation_status(update_status, daily_research_status or {})
        if daily_research_status is not None
        else update_status
    )
    alerts: list[tuple[str, str]] = []
    if effective_status.get("status") not in {"completed", "up_to_date", "暂无"}:
        alerts.append(("alert-bad", f"当前更新状态待复核：{effective_status.get('status', '-')}"))
    if effective_status.get("post_candidates") == "失败":
        alerts.append(("alert-bad", "数据库更新后候选生成失败，建议检查后处理链路。"))
    if effective_status.get("post_daily_research") == "失败":
        alerts.append(("alert-bad", "数据库更新后每日研究失败，健康评分和研究摘要可能不是最新。"))
    status_note = str(effective_status.get("status_note", "-"))
    if status_note not in {"", "-"}:
        alerts.append(("alert-good", status_note))
    partial_count = sum(1 for event in events if event.get("status") == "partial_success")
    if partial_count >= 2:
        alerts.append(("alert-warn", f"最近 {partial_count} 次更新出现待补齐状态，建议检查数据源稳定性。"))
    if not alerts:
        alerts.append(("alert-good", "最近更新链路稳定，未发现需要立即处理的运维告警。"))
    return ''.join(
        f'<div class="alert-strip {tone}">{html.escape(message)}</div>'
        for tone, message in alerts
    )


def fmt_progress_pct(v: str) -> str:
    try:
        return f"{float(v):.1f}%"
    except Exception:
        return "-"


def _svg_polyline(values: list[float], width: int = 520, height: int = 180, color: str = "#0f4c81") -> str:
    if not values:
        return '<div class="chart-empty">暂无可绘制数据</div>'
    lo = min(values)
    hi = max(values)
    span = (hi - lo) or 1.0
    step_x = width / max(1, len(values) - 1)
    points = []
    for idx, value in enumerate(values):
        x = idx * step_x
        y = height - ((value - lo) / span) * (height - 24) - 12
        points.append(f'{x:.1f},{y:.1f}')
    area_points = f'0,{height} ' + ' '.join(points) + f' {width},{height}'
    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart-svg" preserveAspectRatio="none">
      <defs>
        <linearGradient id="fill-{color.strip('#')}" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stop-color="{color}" stop-opacity="0.30"/>
          <stop offset="100%" stop-color="{color}" stop-opacity="0.03"/>
        </linearGradient>
      </defs>
      <polygon points="{area_points}" fill="url(#fill-{color.strip('#')})"></polygon>
      <polyline points="{' '.join(points)}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>
    </svg>
    """


def _svg_area(values: list[float], width: int = 520, height: int = 180, color: str = "#b91c1c") -> str:
    if not values:
        return '<div class="chart-empty">暂无可绘制数据</div>'
    lo = min(values + [0.0])
    hi = max(values + [0.0])
    span = (hi - lo) or 1.0
    step_x = width / max(1, len(values) - 1)
    line_points = []
    for idx, value in enumerate(values):
        x = idx * step_x
        y = height - ((value - lo) / span) * (height - 24) - 12
        line_points.append(f'{x:.1f},{y:.1f}')
    zero_y = height - ((0.0 - lo) / span) * (height - 24) - 12
    area_points = f'0,{zero_y:.1f} ' + ' '.join(line_points) + f' {width},{zero_y:.1f}'
    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart-svg" preserveAspectRatio="none">
      <defs>
        <linearGradient id="fill-area-{color.strip('#')}" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stop-color="{color}" stop-opacity="0.24"/>
          <stop offset="100%" stop-color="{color}" stop-opacity="0.02"/>
        </linearGradient>
      </defs>
      <line x1="0" y1="{zero_y:.1f}" x2="{width}" y2="{zero_y:.1f}" stroke="#cbd5e1" stroke-width="1"></line>
      <polygon points="{area_points}" fill="url(#fill-area-{color.strip('#')})"></polygon>
      <polyline points="{' '.join(line_points)}" fill="none" stroke="{color}" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round"></polyline>
    </svg>
    """


def health_trend_chart_html(health_csv: Path, max_points: int = 20) -> str:
    rows = load_csv_rows(health_csv, limit=max_points)
    if not rows:
        return '<div class="chart-empty">暂无健康趋势数据</div>'
    values = [safe_float(row.get('score', '0')) for row in rows]
    labels = ''.join(f'<span>{html.escape(row.get("generated_at", "")[:10])}</span>' for row in rows[-4:])
    return (
        '<div class="chart-card">'
        '<div class="chart-title">健康评分趋势</div>'
        + _svg_polyline(values, color="#0f766e")
        + f'<div class="chart-axis">{labels}</div>'
        '</div>'
    )


def backtest_metric_chart_html(leaderboard_csv: Path, max_points: int = 12) -> str:
    rows = load_csv_rows(leaderboard_csv, limit=max_points)
    if not rows:
        return '<div class="chart-empty">暂无回测记录</div>'
    values = [safe_float(row.get('sharpe_ratio', '0')) for row in rows]
    labels = ''.join(f'<span>{html.escape(row.get("run_id", "")[-6:])}</span>' for row in rows[:4])
    return (
        '<div class="chart-card">'
        '<div class="chart-title">最近回测 Sharpe 序列</div>'
        + _svg_polyline(values, color="#0f4c81")
        + f'<div class="chart-axis">{labels}</div>'
        '</div>'
    )


def backtest_equity_curve_html(leaderboard_csv: Path, max_points: int = 10) -> str:
    rows = load_csv_rows(leaderboard_csv, limit=max_points)
    if not rows:
        return '<div class="chart-empty">暂无研究净值数据</div>'
    ordered = list(reversed(rows))
    equity = 1.0
    values = []
    for row in ordered:
        equity *= 1.0 + safe_float(row.get('total_return', '0'))
        values.append(equity)
    labels = ''.join(f'<span>{html.escape(row.get("run_id", "")[-6:])}</span>' for row in ordered[-4:])
    return (
        '<div class="chart-card">'
        '<div class="chart-title">研究净值曲线</div>'
        + _svg_polyline(values, color="#0f766e")
        + f'<div class="chart-axis">{labels}</div>'
        '</div>'
    )


def backtest_drawdown_area_html(leaderboard_csv: Path, max_points: int = 10) -> str:
    rows = load_csv_rows(leaderboard_csv, limit=max_points)
    if not rows:
        return '<div class="chart-empty">暂无历史回撤数据</div>'
    ordered = list(reversed(rows))
    values = [-abs(safe_float(row.get('max_drawdown', '0'))) * 100.0 for row in ordered]
    labels = ''.join(f'<span>{html.escape(row.get("run_id", "")[-6:])}</span>' for row in ordered[-4:])
    return (
        '<div class="chart-card">'
        '<div class="chart-title">历史回撤带</div>'
        + _svg_area(values, color="#b91c1c")
        + f'<div class="chart-axis">{labels}</div>'
        '</div>'
    )


def candidate_score_chart_html(candidates_csv: Path, top_n: int = 8) -> str:
    rows = load_csv_rows(candidates_csv, limit=top_n)
    if not rows:
        return '<div class="chart-empty">暂无候选分数数据</div>'
    scores = [safe_float(row.get('final_score', '0')) for row in rows]
    max_score = max(scores) or 1.0
    bars = []
    for row, score in zip(rows, scores):
        width_pct = max(6.0, (score / max_score) * 100.0)
        bars.append(
            f"""
            <div class="bar-row">
              <div class="bar-label">{html.escape(row.get('ts_code', ''))}</div>
              <div class="bar-track"><div class="bar-fill" style="width:{width_pct:.2f}%"></div></div>
              <div class="bar-value">{score:.2f}</div>
            </div>
            """
        )
    return '<div class="chart-card"><div class="chart-title">候选股综合分</div>' + ''.join(bars) + '</div>'


def _svg_scatter(
    points: list[dict[str, str | float]],
    width: int = 520,
    height: int = 220,
    x_key: str = "x",
    y_key: str = "y",
) -> str:
    if not points:
        return '<div class="chart-empty">暂无可绘制数据</div>'
    xs = [float(point[x_key]) for point in points]
    ys = [float(point[y_key]) for point in points]
    x_min, x_max = min(xs), max(xs)
    y_min, y_max = min(ys), max(ys)
    x_span = (x_max - x_min) or 1.0
    y_span = (y_max - y_min) or 1.0
    dots = []
    labels = []
    for point in points:
        x_val = float(point[x_key])
        y_val = float(point[y_key])
        cx = 28 + ((x_val - x_min) / x_span) * (width - 56)
        cy = height - 28 - ((y_val - y_min) / y_span) * (height - 56)
        label = html.escape(str(point.get("label", "")))
        color = html.escape(str(point.get("color", "#0f4c81")))
        dots.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="6.5" fill="{color}" fill-opacity="0.84"></circle>')
        labels.append(f'<text x="{cx + 8:.1f}" y="{cy - 8:.1f}" class="chart-point-label">{label}</text>')
    return f"""
    <svg viewBox="0 0 {width} {height}" class="chart-svg chart-scatter" preserveAspectRatio="none">
      <line x1="28" y1="{height - 28}" x2="{width - 12}" y2="{height - 28}" stroke="#cbd5e1" stroke-width="1.2"></line>
      <line x1="28" y1="12" x2="28" y2="{height - 28}" stroke="#cbd5e1" stroke-width="1.2"></line>
      {''.join(dots)}
      {''.join(labels)}
    </svg>
    """


def candidate_brief_cards(candidates_csv: Path, top_n: int = 5) -> list[dict[str, str]]:
    rows = load_csv_rows(candidates_csv, limit=top_n)
    cards: list[dict[str, str]] = []
    for idx, row in enumerate(rows):
        cards.append(
            {
                "index": str(idx),
                "ts_code": row.get("ts_code", ""),
                "stock_name": row.get("stock_name", ""),
                "signal": SIGNAL_ZH.get(row.get("signal", "-"), row.get("signal", "-")),
                "risk_level": RISK_ZH.get(row.get("risk_level", "-"), row.get("risk_level", "-")),
                "final_score": f"{safe_float(row.get('final_score', '0')):.1f}",
                "pred_return": f"{safe_float(row.get('pred_return', '0')):.1%}",
            }
        )
    return cards


def candidate_market_snapshot(candidates_csv: Path, top_n: int = 10) -> dict[str, str]:
    rows = load_csv_rows(candidates_csv, limit=top_n)
    base = {
        "candidate_count": "0",
        "dominant_regime": "未识别",
        "risk_preference": "中性",
        "style_bias": "均衡",
        "avg_position_pct": "0.0%",
        "avg_risk_pressure": "0.0",
        "guardrail_mode": "-",
        "guardrail_reason": "-",
        "risk_flag_count": "0",
    }
    if not rows:
        return base
    regime_map = {
        "trend": "趋势市",
        "range": "震荡市",
        "volatile": "高波动",
        "range_volatile": "震荡高波",
        "trend_volatile": "趋势高波",
        "extreme": "极端市况",
        "unknown": "未识别",
    }
    regime_counts: dict[str, int] = {}
    total_vol = 0.0
    total_strength = 0.0
    total_position = 0.0
    total_risk_pressure = 0.0
    risk_flag_count = 0
    for row in rows:
        regime = str(row.get("regime", "") or "").strip() or "unknown"
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        total_vol += safe_float(row.get("style_volatility", "0"))
        total_strength += safe_float(row.get("style_relative_strength", "0"))
        total_position += safe_float(row.get("position_pct", "0"))
        total_risk_pressure += safe_float(row.get("basket_risk_pressure_score", "0"))
        if str(row.get("basket_risk_flag", "ok") or "ok").strip() not in {"", "ok"}:
            risk_flag_count += 1
    count = max(len(rows), 1)
    dominant_regime_key = max(regime_counts.items(), key=lambda item: item[1])[0]
    avg_position = total_position / count
    avg_risk_pressure = total_risk_pressure / count
    avg_vol = total_vol / count
    avg_strength = total_strength / count
    guardrail_mode = str(rows[0].get("basket_guardrail_mode", "") or "-")
    guardrail_reason = str(rows[0].get("basket_guardrail_reason", "") or "-")

    if guardrail_mode in {"defensive", "interim"} or avg_position <= 0.11 or avg_risk_pressure >= 55.0:
        risk_preference = "防守优先"
    elif avg_position >= 0.18 and avg_risk_pressure < 42.0:
        risk_preference = "可适度进攻"
    else:
        risk_preference = "均衡推进"

    if avg_strength >= 0.03 and avg_vol >= 0.025:
        style_bias = "高弹性趋势"
    elif avg_strength >= 0.02:
        style_bias = "偏强趋势"
    elif avg_vol >= 0.03:
        style_bias = "高波动承压"
    else:
        style_bias = "均衡"

    base.update(
        {
            "candidate_count": str(len(rows)),
            "dominant_regime": regime_map.get(dominant_regime_key, dominant_regime_key or "未识别"),
            "risk_preference": risk_preference,
            "style_bias": style_bias,
            "avg_position_pct": f"{avg_position:.1%}",
            "avg_risk_pressure": f"{avg_risk_pressure:.1f}",
            "guardrail_mode": translate_guardrail_mode(guardrail_mode),
            "guardrail_reason": guardrail_reason,
            "risk_flag_count": str(risk_flag_count),
        }
    )
    return base


def candidate_detail_panel_html(candidates_csv: Path, index: int = 0) -> str:
    rows = load_csv_rows(candidates_csv, limit=max(index + 1, 1))
    if not rows or index >= len(rows):
        return '<div class="chart-empty">暂无候选股详情</div>'
    row = rows[index]
    reason = row.get("reason", "")
    reason_chips = []
    for item in reason.split(","):
        item = item.strip()
        if not item:
            continue
        item = item.replace("model_bullish", "模型看多").replace("factor_strong", "因子强势")
        reason_chips.append(f'<span class="detail-chip">{html.escape(item)}</span>')
    if not reason_chips:
        reason_chips.append('<span class="detail-chip">暂无理由拆解</span>')
    score = safe_float(row.get("final_score", "0"))
    prob_up = safe_float(row.get("direction_prob_up", "0"))
    pred_ret = safe_float(row.get("pred_return", "0"))
    confidence = safe_float(row.get("confidence", "0"))
    position_pct = safe_float(row.get("position_pct", "0"))
    stop_loss = safe_float(row.get("stop_loss", "0"))
    take_profit = safe_float(row.get("take_profit", "0"))
    risk_reward_ratio = 0.0
    if stop_loss > 0 and take_profit > stop_loss:
        risk_reward_ratio = max((take_profit - stop_loss) / max(stop_loss, 0.01), 0.0)
    signal_zh = SIGNAL_ZH.get(row.get("signal", "-"), row.get("signal", "-"))
    risk_zh = RISK_ZH.get(row.get("risk_level", "-"), row.get("risk_level", "-"))
    signal_strength_map = {
        "strong_buy": 0.92,
        "buy": 0.72,
        "watch": 0.42,
        "sell": 0.16,
    }
    risk_drag_map = {
        "low": 0.22,
        "medium": 0.48,
        "high": 0.78,
    }
    signal_strength = signal_strength_map.get(row.get("signal", ""), 0.35)
    upside_strength = min(max(prob_up, 0.0), 1.0)
    return_strength = min(max(pred_ret / 0.05, 0.0), 1.0)
    confidence_strength = min(max(confidence, 0.0), 1.0)
    risk_drag = risk_drag_map.get(row.get("risk_level", ""), 0.35)
    stop_buffer = 0.0
    if stop_loss > 0 and take_profit > stop_loss:
        stop_buffer = min(max((take_profit - stop_loss) / max(take_profit, 0.01), 0.0), 1.0)
    thesis_parts = []
    if prob_up >= 0.65:
        thesis_parts.append("模型方向偏多")
    if pred_ret >= 0.03:
        thesis_parts.append("预期收益具备进攻性")
    if confidence >= 0.75:
        thesis_parts.append("信号一致性较高")
    if position_pct <= 0.10:
        thesis_parts.append("仓位建议偏保守")
    thesis = "，".join(thesis_parts) if thesis_parts else "当前更像观察型候选，需结合后续价格结构继续确认。"
    execution_note = (
        f"信号 {signal_zh}，风险 {risk_zh}，建议仓位 {position_pct:.1%}。"
        f" 若进入跟踪，先看止损 {stop_loss:.2f} 与止盈 {take_profit:.2f} 的执行边界。"
    )
    score_bars = [
        ("信号强度", signal_strength, signal_zh),
        ("方向概率", upside_strength, f"{prob_up:.1%}"),
        ("收益弹性", return_strength, f"{pred_ret:.1%}"),
        ("模型一致性", confidence_strength, f"{confidence:.1%}"),
    ]
    risk_bars = [
        ("风险拖累", risk_drag, risk_zh),
        ("仓位保守度", 1.0 - min(max(position_pct / 0.25, 0.0), 1.0), f"{position_pct:.1%}"),
        ("止盈止损缓冲", stop_buffer, f"{risk_reward_ratio:.2f}"),
    ]
    score_bar_html = "".join(
        '<div class="lens-row">'
        f'<div class="lens-head"><span>{html.escape(label)}</span><strong>{html.escape(detail)}</strong></div>'
        f'<div class="lens-track"><div class="lens-fill" style="width:{max(6.0, value * 100.0):.1f}%"></div></div>'
        '</div>'
        for label, value, detail in score_bars
    )
    risk_bar_html = "".join(
        '<div class="lens-row">'
        f'<div class="lens-head"><span>{html.escape(label)}</span><strong>{html.escape(detail)}</strong></div>'
        f'<div class="lens-track"><div class="lens-fill lens-fill-warn" style="width:{max(6.0, value * 100.0):.1f}%"></div></div>'
        '</div>'
        for label, value, detail in risk_bars
    )
    return (
        '<div class="detail-panel">'
        '<div class="detail-panel-top">'
        f'<div><div class="eyebrow-inline">候选详情</div><h4>{html.escape(row.get("ts_code", "暂无"))} · {html.escape(row.get("stock_name", ""))}</h4>'
        f'<div class="detail-subtitle">{html.escape(row.get("industry", "未知行业"))} · {html.escape(signal_zh)} · {html.escape(risk_zh)}</div></div>'
        f'<div class="detail-score">{score:.1f}</div>'
        '</div>'
        '<div class="detail-research-grid">'
        f'<div class="research-card"><span>研究判断</span><strong>{html.escape(thesis)}</strong><small>这是系统对当前候选最核心的方向性概括，不替代交易决策。</small></div>'
        f'<div class="research-card"><span>执行提示</span><strong>{html.escape(execution_note)}</strong><small>先看信号、风险和仓位是否一致，再决定是否进入观察池。</small></div>'
        '</div>'
        '<div class="detail-lens-grid">'
        '<div class="lens-card">'
        '<div class="subsection-title">分数拆解</div>'
        f'{score_bar_html}'
        '<div class="lens-note">综合分不是黑盒，它主要来自信号方向、上涨概率、收益弹性和模型一致性。</div>'
        '</div>'
        '<div class="lens-card">'
        '<div class="subsection-title">风险拆解</div>'
        f'{risk_bar_html}'
        '<div class="lens-note">风险不只看标签，还要结合仓位建议、止盈止损边界和风险收益比一起看。</div>'
        '</div>'
        '</div>'
        '<div class="detail-metrics">'
        f'<div><span>上涨概率</span><strong>{prob_up:.1%}</strong></div>'
        f'<div><span>预测收益</span><strong>{pred_ret:.1%}</strong></div>'
        f'<div><span>置信度</span><strong>{confidence:.1%}</strong></div>'
        f'<div><span>建议仓位</span><strong>{position_pct:.1%}</strong></div>'
        f'<div><span>止损价</span><strong>{stop_loss:.2f}</strong></div>'
        f'<div><span>止盈价</span><strong>{take_profit:.2f}</strong></div>'
        f'<div><span>风险收益比</span><strong>{risk_reward_ratio:.2f}</strong></div>'
        f'<div><span>当前信号</span><strong>{html.escape(signal_zh)}</strong></div>'
        f'<div><span>当前风险</span><strong>{html.escape(risk_zh)}</strong></div>'
        '</div>'
        '<div class="subsection-title" style="margin-top:14px;">入选原因</div>'
        f'<div class="detail-chip-row">{"".join(reason_chips)}</div>'
        '</div>'
    )


def candidate_risk_reward_chart_html(candidates_csv: Path, top_n: int = 8) -> str:
    rows = load_csv_rows(candidates_csv, limit=top_n)
    if not rows:
        return '<div class="chart-empty">暂无候选风险收益数据</div>'
    color_map = {"low": "#0f766e", "medium": "#b45309", "high": "#b91c1c"}
    points = []
    for row in rows:
        points.append(
            {
                "x": safe_float(row.get("confidence", "0")) * 100.0,
                "y": safe_float(row.get("pred_return", "0")) * 100.0,
                "label": row.get("ts_code", ""),
                "color": color_map.get(row.get("risk_level", ""), "#0f4c81"),
            }
        )
    return (
        '<div class="chart-card">'
        '<div class="chart-title">候选股风险收益散点</div>'
        + _svg_scatter(points)
        + '<div class="chart-axis"><span>X: 置信度</span><span>Y: 预测收益</span></div>'
        '</div>'
    )


def backtest_return_drawdown_chart_html(leaderboard_csv: Path, max_points: int = 10) -> str:
    rows = load_csv_rows(leaderboard_csv, limit=max_points)
    if not rows:
        return '<div class="chart-empty">暂无回测收益回撤数据</div>'
    points = []
    for row in rows:
        total_return = safe_float(row.get("total_return", "0")) * 100.0
        drawdown = abs(safe_float(row.get("max_drawdown", "0"))) * 100.0
        sharpe = safe_float(row.get("sharpe_ratio", "0"))
        color = "#0f766e" if sharpe >= 1 else ("#0d4f8c" if sharpe >= 0 else "#b91c1c")
        points.append(
            {
                "x": drawdown,
                "y": total_return,
                "label": row.get("run_id", "")[-6:],
                "color": color,
            }
        )
    return (
        '<div class="chart-card">'
        '<div class="chart-title">回测收益 / 回撤分布</div>'
        + _svg_scatter(points)
        + '<div class="chart-axis"><span>X: 最大回撤</span><span>Y: 总收益</span></div>'
        '</div>'
    )

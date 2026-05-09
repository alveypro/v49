from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.artifact_registry import ArtifactRegistry
from src.artifact_source_guard import is_rejected_temp_source_path
from src.current_result_pointer import CurrentResultPointerStore
from src.dashboard_support import load_csv_rows, read_json
from src.primary_result_audit import extract_primary_result_audit_status
from src.primary_result_execution import extract_primary_result_execution_status
from src.primary_result_observation import extract_primary_result_observation_status
from src.result_registry import ResultRegistry
from src.primary_result_rollback import extract_primary_result_rollback_status
from src.primary_result_terminal import extract_primary_result_terminal_outcome
from src.unified_result_record import UnifiedResultRecord


_RESEARCH_STATUS_MAP = {
    "completed": "completed",
    "done": "completed",
    "running": "in_progress",
    "in_progress": "in_progress",
    "partial_success": "suspended",
    "failed": "abandoned",
    "error": "abandoned",
}

_SIGNAL_LEVEL_MAP = {
    "strong_buy": "high",
    "buy": "medium",
    "watch": "low",
    "sell": "none",
}

_RESULT_TYPE_BY_STAGE = {
    "L1": "research",
    "L2": "candidate",
    "L3": "audit",
    "L4": "execution",
    "L5": "archive",
}

_REQUIRED_CHAIN_ARTIFACT_TYPES_BY_STAGE = {
    "L3": ("primary_result_audit",),
    "L4": (
        "primary_result_audit",
        "primary_result_execution",
        "primary_result_rollback",
        "primary_result_observation",
    ),
    "L5": (
        "primary_result_audit",
        "primary_result_execution",
        "primary_result_rollback",
        "primary_result_observation",
        "primary_result_terminal",
    ),
}


def _format_mtime(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _normalize_optional_status(value: object, allowed: set[str] | None = None) -> str | None:
    text = str(value or "").strip().lower()
    if not text or text in {"-", "none", "null", "unknown", "n/a"}:
        return None
    if allowed is not None and text not in allowed:
        return None
    return text


def _derive_research_status(daily_research_status: dict) -> str | None:
    state = _normalize_optional_status(daily_research_status.get("state"))
    if state is None:
        return None
    return _RESEARCH_STATUS_MAP.get(state, state)


def _derive_candidate_status(top_candidate: dict[str, str], buylist: dict) -> str | None:
    ts_code = str(top_candidate.get("ts_code", "") or "").strip()
    if not ts_code:
        return None
    items = buylist.get("items") or []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("ts_code", "") or "").strip() == ts_code:
                return "shortlisted"
    return "candidate"


def _derive_signal_level(top_candidate: dict[str, str]) -> str | None:
    signal = _normalize_optional_status(top_candidate.get("signal"))
    if signal is None:
        return None
    return _SIGNAL_LEVEL_MAP.get(signal, "low")


def _derive_risk_level(top_candidate: dict[str, str]) -> str | None:
    return _normalize_optional_status(top_candidate.get("risk_level"), {"low", "medium", "high", "critical"})


def _derive_audit_status(governance_audit: dict, selected_ts_code: str) -> str | None:
    if not selected_ts_code:
        return None
    summary = governance_audit.get("summary") or {}
    overall = _normalize_optional_status(summary.get("overall_status"))
    if overall == "pass":
        # System governance passed, but this is not a per-result audit fact.
        return None
    if overall == "fail":
        return "failed"
    if overall == "warn":
        return "in_review"
    return None


def _derive_primary_result_audit_status(primary_result_audit: dict, result_id: str, selected_ts_code: str) -> str | None:
    return extract_primary_result_audit_status(
        primary_result_audit,
        result_id=result_id,
        ts_code=selected_ts_code,
    )


def _derive_execution_status(t1_checklist: dict, buylist: dict, selected_ts_code: str) -> str | None:
    if not selected_ts_code:
        return None
    items = buylist.get("items") or []
    in_buylist = any(
        isinstance(item, dict) and str(item.get("ts_code", "") or "").strip() == selected_ts_code
        for item in items
    )
    if not in_buylist:
        return None
    summary = t1_checklist.get("summary") or {}
    overall = _normalize_optional_status(summary.get("overall_status"))
    if overall in {"pass", "warn"}:
        # The checklist signals execution readiness, not that execution has started.
        return None
    return None


def _derive_primary_result_execution_status(
    primary_result_execution: dict,
    result_id: str,
    selected_ts_code: str,
) -> str | None:
    return extract_primary_result_execution_status(
        primary_result_execution,
        result_id=result_id,
        ts_code=selected_ts_code,
    )


def _derive_primary_result_observation_status(
    primary_result_observation: dict,
    result_id: str,
    selected_ts_code: str,
) -> str | None:
    return extract_primary_result_observation_status(
        primary_result_observation,
        result_id=result_id,
        ts_code=selected_ts_code,
    )


def _derive_rollback_status(rollback_drill: dict) -> str | None:
    triggered = rollback_drill.get("triggered")
    if triggered is True:
        return "pending"
    return None


def _derive_primary_result_rollback_status(primary_result_rollback: dict, result_id: str, selected_ts_code: str) -> str | None:
    return extract_primary_result_rollback_status(
        primary_result_rollback,
        result_id=result_id,
        ts_code=selected_ts_code,
    )


def _derive_terminal_outcome(top_candidate: dict[str, str]) -> str | None:
    # First cut intentionally avoids guessing terminal outcomes from broad system files.
    _ = top_candidate
    return None


def _derive_primary_result_terminal_outcome(primary_result_terminal: dict, result_id: str, selected_ts_code: str) -> str | None:
    return extract_primary_result_terminal_outcome(
        primary_result_terminal,
        result_id=result_id,
        ts_code=selected_ts_code,
    )


def _build_history_summary(
    *,
    research_status: str | None,
    candidate_status: str | None,
    audit_status: str | None,
    execution_status: str | None,
    observation_status: str | None = None,
    rollback_status: str | None = None,
    terminal_outcome: str | None = None,
) -> str | None:
    parts: list[str] = []
    if research_status is not None:
        parts.append(f"研究记录 {research_status}")
    if candidate_status is not None:
        parts.append(f"候选记录 {candidate_status}")
    if audit_status is not None:
        parts.append(f"审核记录 {audit_status}")
    if execution_status is not None:
        parts.append(f"执行记录 {execution_status}")
    if observation_status is not None:
        parts.append(f"观察记录 {observation_status}")
    if rollback_status is not None:
        parts.append(f"回滚记录 {rollback_status}")
    if terminal_outcome is not None:
        parts.append(f"终局记录 {terminal_outcome}")
    if not parts:
        return None
    return "；".join(parts)


def _select_history_source(
    *,
    source_timestamps: dict[str, str],
    research_status: str | None,
    candidate_status: str | None,
    audit_status: str | None,
    execution_status: str | None,
    observation_status: str | None = None,
    rollback_status: str | None = None,
    terminal_outcome: str | None = None,
) -> tuple[str | None, str | None, str | None]:
    priority: list[str] = []
    if terminal_outcome is not None:
        priority.append("primary_result_terminal_latest.json")
    if observation_status is not None:
        priority.append("primary_result_observation_latest.json")
    if rollback_status is not None:
        priority.extend(["primary_result_rollback_latest.json", "t12_rollback_drill_latest.json"])
    if execution_status is not None:
        priority.extend(["primary_result_execution_latest.json", "t1_execution_checklist_latest.json"])
    if audit_status is not None:
        priority.extend(["primary_result_audit_latest.json", "governance_audit_latest.json"])
    if candidate_status is not None:
        priority.extend(["buylist_latest.json", "candidates_top_latest.csv"])
    if research_status is not None:
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
    for source_name in priority:
        if source_name in seen:
            continue
        seen.add(source_name)
        source_ts = source_timestamps.get(source_name)
        if source_ts and source_ts != "-":
            return source_name, source_ts, "degraded"
    return None, None, None


def _build_disabled_reason(
    *,
    explicit_reason: str | None = None,
    audit_status: str | None,
    promotion_status: str | None,
    risk_level: str | None,
    terminal_outcome: str | None,
) -> str | None:
    if explicit_reason:
        return explicit_reason
    if terminal_outcome == "rejected":
        return "当前对象已被驳回，不能继续推进。"
    if terminal_outcome == "cancelled":
        return "当前对象已被取消，不能继续推进。"
    if terminal_outcome == "expired":
        return "当前对象已过有效窗口，不能继续推进。"
    if terminal_outcome == "superseded":
        return "当前对象已被新版结果替代，不能继续推进。"
    if audit_status == "failed":
        return "审核未通过，当前不能继续推进。"
    if promotion_status == "rejected":
        return "晋升未通过，当前不能继续推进。"
    if risk_level in {"high", "critical"}:
        return "当前风险较高，暂不建议继续推进。"
    return None


def _build_invalid_reason(*, terminal_outcome: str | None, explicit_reason: str | None = None) -> str | None:
    mapping = {
        "success": "该结果已完成并验证。",
        "expired": "该结果已过有效窗口。",
        "superseded": "该结果已被新版结果替代。",
        "rejected": "该结果已被制度驳回。",
        "failed": "该结果在执行过程中失败。",
        "cancelled": "该结果已取消，不再继续推进。",
        "archived": "该结果已归档留存。",
    }
    if terminal_outcome is None:
        return None
    base = mapping.get(terminal_outcome, terminal_outcome)
    if explicit_reason:
        return f"{base} 补充说明：{explicit_reason}"
    return base


def resolve_result_lifecycle_stage(
    *,
    research_status: str | None,
    candidate_status: str | None,
    audit_status: str | None,
    execution_status: str | None,
    terminal_outcome: str | None = None,
) -> str:
    if terminal_outcome is not None:
        return "L5"
    if execution_status is not None:
        return "L4"
    if audit_status is not None:
        return "L3"
    if candidate_status is not None:
        return "L2"
    if research_status is not None:
        return "L1"
    # No factual stage evidence exists; degrade to the earliest safe stage.
    return "L1"


def _result_lifecycle_stage_rank(stage: str | None) -> int:
    text = str(stage or "").strip().upper()
    if len(text) == 2 and text.startswith("L") and text[1].isdigit():
        return int(text[1])
    return 0


def _prefer_newer_result_lifecycle_stage(current_stage: str | None, derived_stage: str) -> str:
    if _result_lifecycle_stage_rank(current_stage) >= _result_lifecycle_stage_rank(derived_stage):
        return str(current_stage or "").strip().upper() or derived_stage
    return derived_stage


def _build_data_sync_note(
    *,
    audit_status: str | None,
    execution_status: str | None,
    observation_status: str | None,
    rollback_status: str | None,
    terminal_outcome: str | None,
    source_timestamps: dict[str, str],
) -> str:
    missing: list[str] = []
    if audit_status is None:
        missing.append("审核状态暂缺")
    if execution_status is None:
        missing.append("执行状态暂缺")
    if observation_status is None:
        missing.append("观察状态暂缺")
    if rollback_status is None:
        missing.append("回滚状态暂缺")
    if terminal_outcome is None:
        missing.append("终局结论暂缺")
    newest = max((value for value in source_timestamps.values() if value), default="-")
    if not missing:
        return f"制度字段已对齐，最近来源时间 {newest}。"
    return f"降级显示：{'，'.join(missing)}。最近来源时间 {newest}。"


def _find_candidate_row_by_ts_code(candidate_rows: list[dict[str, str]], ts_code: str) -> dict[str, str]:
    normalized_ts_code = str(ts_code or "").strip()
    if not normalized_ts_code:
        return {}
    for row in candidate_rows:
        if str(row.get("ts_code", "") or "").strip() == normalized_ts_code:
            return row
    return {}


def _artifacts_root_for_exp_dir(exp_dir: Path) -> Path:
    return exp_dir.parent.parent / "artifacts"


def _artifact_registry_for_exp_dir(exp_dir: Path) -> ArtifactRegistry:
    return ArtifactRegistry(_artifacts_root_for_exp_dir(exp_dir) / "artifact_registry.jsonl")


def _artifact_entry_path(entry: dict[str, object]) -> Path | None:
    raw_path = str(entry.get("path") or "").strip()
    if not raw_path:
        return None
    return Path(raw_path)


def _read_result_scoped_json(path: Path, *, result_id: str, ts_code: str) -> dict:
    payload = read_json(path)
    if not payload:
        return {}
    payload_result_id = str(payload.get("result_id") or "").strip()
    payload_ts_code = str(payload.get("ts_code") or "").strip()
    if payload_result_id and payload_result_id != result_id:
        return {}
    if payload_ts_code and payload_ts_code != ts_code:
        return {}
    return payload


def _load_pointer_artifact_entries(
    *,
    exp_dir: Path,
    current_pointer: dict[str, object] | None,
    current_result_record: dict[str, object] | None,
    require_current_pointer: bool,
) -> tuple[list[dict[str, object]], dict[str, dict[str, object]], UnifiedResultRecord | None]:
    if current_pointer is None or current_result_record is None:
        return [], {}, None

    artifact_ids = current_pointer.get("artifact_ids")
    if not isinstance(artifact_ids, list):
        artifact_ids = []
    if not artifact_ids:
        if require_current_pointer:
            return [], {}, _build_fail_closed_record(
                result_id=str(current_pointer.get("result_id") or "primary:unavailable"),
                ts_code=str(current_result_record.get("ts_code") or "对象信息暂缺"),
                stock_name=str(current_result_record.get("stock_name") or "名称信息暂缺"),
                disabled_reason="current_result_pointer 缺少 artifact_ids，当前禁止输出主结果结论。",
                data_sync_note="fail closed：主结果指针缺少工件链标识，需人工修复 pointer/result_registry。",
                result_lifecycle_stage=str(current_result_record.get("lifecycle_stage") or "L1"),
            )
        return [], {}, None

    registry = _artifact_registry_for_exp_dir(exp_dir)
    if not registry.path.exists():
        return [], {}, None

    problems: list[str] = []
    entries: list[dict[str, object]] = []
    pointer_result_id = str(current_pointer.get("result_id") or "").strip()
    pointer_run_id = str(current_pointer.get("run_id") or "").strip()
    for artifact_id in artifact_ids:
        try:
            entry = registry.get_entry(str(artifact_id))
        except FileNotFoundError:
            problems.append(f"missing artifact_registry entry: {artifact_id}")
            continue
        if entry.get("run_id") != pointer_run_id:
            problems.append(f"artifact run_id mismatch: {artifact_id}")
            continue
        entry_result_id = str(entry.get("result_id") or "").strip()
        if entry_result_id and entry_result_id != pointer_result_id:
            problems.append(f"artifact result_id mismatch: {artifact_id}")
            continue
        entries.append(entry)

    if problems and require_current_pointer:
        return [], {}, _build_fail_closed_record(
            result_id=pointer_result_id or "primary:unavailable",
            ts_code=str(current_result_record.get("ts_code") or "对象信息暂缺"),
            stock_name=str(current_result_record.get("stock_name") or "名称信息暂缺"),
            disabled_reason="artifact_registry 与 current_result_pointer 不一致，当前禁止输出主结果结论。",
            data_sync_note=f"fail closed：{'；'.join(problems)}。",
            result_lifecycle_stage=str(current_result_record.get("lifecycle_stage") or "L1"),
        )

    entries_by_type = {
        str(entry.get("artifact_type") or "").strip(): entry
        for entry in entries
        if str(entry.get("artifact_type") or "").strip()
    }
    lifecycle_stage = str(current_result_record.get("lifecycle_stage") or "").strip().upper()
    required_types = _REQUIRED_CHAIN_ARTIFACT_TYPES_BY_STAGE.get(lifecycle_stage, ())
    missing_required_types = [artifact_type for artifact_type in required_types if artifact_type not in entries_by_type]
    if missing_required_types and require_current_pointer:
        return [], {}, _build_fail_closed_record(
            result_id=pointer_result_id or "primary:unavailable",
            ts_code=str(current_result_record.get("ts_code") or "对象信息暂缺"),
            stock_name=str(current_result_record.get("stock_name") or "名称信息暂缺"),
            disabled_reason="current_result_pointer 工件链缺少当前阶段必需的 lifecycle artifact，当前禁止输出主结果结论。",
            data_sync_note=f"fail closed：missing required artifact types {'、'.join(missing_required_types)}。",
            result_lifecycle_stage=lifecycle_stage or "L1",
        )
    return entries, entries_by_type, None


def _resolve_chain_path(
    *,
    entries_by_type: dict[str, dict[str, object]],
    artifact_type: str,
    fallback_path: Path,
) -> Path:
    entry = entries_by_type.get(artifact_type)
    entry_path = _artifact_entry_path(entry) if entry is not None else None
    if entry_path is not None and entry_path.exists():
        return entry_path
    return fallback_path


def _build_fail_closed_record(
    *,
    result_id: str,
    ts_code: str,
    stock_name: str,
    disabled_reason: str,
    data_sync_note: str,
    result_lifecycle_stage: str = "L1",
    result_type: str | None = None,
) -> UnifiedResultRecord:
    return UnifiedResultRecord(
        result_id=result_id or "primary:unavailable",
        ts_code=ts_code or "对象信息暂缺",
        stock_name=stock_name or "名称信息暂缺",
        result_lifecycle_stage=result_lifecycle_stage,
        source_scope="stock",
        result_type=result_type or _RESULT_TYPE_BY_STAGE.get(result_lifecycle_stage),
        disabled_reason=disabled_reason,
        history_generation_mode="blocked",
        data_sync_note=data_sync_note,
        source_timestamps={},
    )


def _resolve_current_primary_identity(
    *,
    exp_dir: Path,
    require_current_pointer: bool,
) -> tuple[dict[str, object] | None, dict[str, object] | None, UnifiedResultRecord | None]:
    artifacts_dir = _artifacts_root_for_exp_dir(exp_dir)
    pointer_store = CurrentResultPointerStore(pointer_dir=artifacts_dir / "current_result_pointer")
    result_registry = ResultRegistry(results_dir=artifacts_dir / "result_registry")
    pointer = pointer_store.get_current_pointer()
    pointer_result_id = str(pointer.get("result_id") or "").strip()
    pointer_run_id = str(pointer.get("run_id") or "").strip()
    pointer_lifecycle_id = str(pointer.get("lifecycle_id") or "").strip()
    pointer_artifact_ids = pointer.get("artifact_ids")
    pointer_snapshot_path = str(pointer.get("snapshot_path") or "").strip()
    if not pointer_result_id or not pointer_run_id or not pointer_lifecycle_id or not isinstance(pointer_artifact_ids, list) or not pointer_artifact_ids:
        if require_current_pointer:
            return None, None, _build_fail_closed_record(
                result_id=pointer_result_id or "primary:unavailable",
                ts_code="对象信息暂缺",
                stock_name="名称信息暂缺",
                disabled_reason="current_result_pointer 缺失或不完整，当前禁止输出主结果结论。",
                data_sync_note="fail closed：主结果指针不完整，需人工复核并修复 pointer 链路。",
            )
        return None, None, None
    if pointer_snapshot_path and is_rejected_temp_source_path(pointer_snapshot_path):
        resolved_pointer_dir = (artifacts_dir / "current_result_pointer").resolve()
        resolved_snapshot_path = Path(pointer_snapshot_path).resolve()
        try:
            resolved_snapshot_path.relative_to(resolved_pointer_dir)
        except ValueError:
            if require_current_pointer:
                return None, None, _build_fail_closed_record(
                    result_id=pointer_result_id,
                    ts_code="对象信息暂缺",
                    stock_name="名称信息暂缺",
                    disabled_reason="current_result_pointer snapshot_path 指向临时或 pytest 派生路径，当前禁止输出主结果结论。",
                    data_sync_note="fail closed：pointer snapshot_path 不属于受管主链目录，需人工复核并修复 pointer 链路。",
                )
            return None, None, None
    record = result_registry.get_latest_record_for_result(pointer_result_id)
    if record is None:
        if require_current_pointer:
            return None, None, _build_fail_closed_record(
                result_id=pointer_result_id,
                ts_code="对象信息暂缺",
                stock_name="名称信息暂缺",
                disabled_reason="result_registry 未找到 current_result_pointer 对应记录，当前禁止输出主结果结论。",
                data_sync_note="fail closed：pointer 已存在，但 result_registry 未承接该 result_id。",
            )
        return pointer, None, None
    return pointer, record, None


def build_primary_result(
    exp_dir: Path,
    candidate_index: int = 0,
    *,
    require_current_pointer: bool = False,
    ignore_current_pointer: bool = False,
) -> UnifiedResultRecord:
    candidates_csv = exp_dir / "candidates_top_latest.csv"
    daily_research_status_path = exp_dir / "daily_research_status_latest.json"
    primary_result_audit_path = exp_dir / "primary_result_audit_latest.json"
    primary_result_execution_path = exp_dir / "primary_result_execution_latest.json"
    primary_result_observation_path = exp_dir / "primary_result_observation_latest.json"
    primary_result_rollback_path = exp_dir / "primary_result_rollback_latest.json"
    primary_result_terminal_path = exp_dir / "primary_result_terminal_latest.json"
    governance_audit_path = exp_dir / "governance_audit_latest.json"
    t1_checklist_path = exp_dir / "t1_execution_checklist_latest.json"
    rollback_drill_path = exp_dir / "t12_rollback_drill_latest.json"
    buylist_path = exp_dir / "buylist_latest.json"

    if require_current_pointer and ignore_current_pointer:
        raise ValueError("require_current_pointer and ignore_current_pointer cannot both be true")

    current_pointer: dict[str, object] | None = None
    current_result_record: dict[str, object] | None = None
    pointer_artifact_entries: list[dict[str, object]] = []
    pointer_entries_by_type: dict[str, dict[str, object]] = {}
    if not ignore_current_pointer:
        current_pointer, current_result_record, fail_closed_record = _resolve_current_primary_identity(
            exp_dir=exp_dir,
            require_current_pointer=require_current_pointer,
        )
        if fail_closed_record is not None:
            return fail_closed_record

        pointer_artifact_entries, pointer_entries_by_type, fail_closed_record = _load_pointer_artifact_entries(
            exp_dir=exp_dir,
            current_pointer=current_pointer,
            current_result_record=current_result_record,
            require_current_pointer=require_current_pointer,
        )
        if fail_closed_record is not None:
            return fail_closed_record

    candidate_rows = load_csv_rows(candidates_csv, limit=max(candidate_index + 20, 20))
    if current_result_record is not None:
        top_candidate = _find_candidate_row_by_ts_code(candidate_rows, str(current_result_record.get("ts_code") or ""))
    else:
        top_candidate = candidate_rows[candidate_index] if candidate_index < len(candidate_rows) else {}

    pointer_run_id = str(current_pointer.get("run_id") or "").strip() if current_pointer is not None else None
    pointer_lifecycle_id = str(current_pointer.get("lifecycle_id") or "").strip() if current_pointer is not None else None
    pointer_as_of_date = str(current_pointer.get("as_of_date") or "").strip() if current_pointer is not None else None

    resolved_audit_path = _resolve_chain_path(
        entries_by_type=pointer_entries_by_type,
        artifact_type="primary_result_audit",
        fallback_path=primary_result_audit_path,
    )
    resolved_execution_path = _resolve_chain_path(
        entries_by_type=pointer_entries_by_type,
        artifact_type="primary_result_execution",
        fallback_path=primary_result_execution_path,
    )
    resolved_observation_path = _resolve_chain_path(
        entries_by_type=pointer_entries_by_type,
        artifact_type="primary_result_observation",
        fallback_path=primary_result_observation_path,
    )
    resolved_rollback_path = _resolve_chain_path(
        entries_by_type=pointer_entries_by_type,
        artifact_type="primary_result_rollback",
        fallback_path=primary_result_rollback_path,
    )
    resolved_terminal_path = _resolve_chain_path(
        entries_by_type=pointer_entries_by_type,
        artifact_type="primary_result_terminal",
        fallback_path=primary_result_terminal_path,
    )

    daily_research_status = read_json(daily_research_status_path)
    governance_audit = read_json(governance_audit_path)
    t1_checklist = read_json(t1_checklist_path)
    rollback_drill = read_json(rollback_drill_path)
    buylist = read_json(buylist_path)

    if current_result_record is not None:
        ts_code = str(current_result_record.get("ts_code", "") or "").strip()
        stock_name = str(current_result_record.get("stock_name", "") or "").strip()
        result_id = str(current_result_record.get("result_id", "") or "").strip() or f"primary:{ts_code or 'unknown'}"
        source_scope = (
            str(current_pointer.get("source_scope") or "").strip()
            if current_pointer is not None
            else str(current_result_record.get("source_scope", "") or "").strip()
        )
    else:
        ts_code = str(top_candidate.get("ts_code", "") or "").strip()
        stock_name = str(top_candidate.get("stock_name", "") or "").strip()
        result_id = f"primary:{ts_code or 'unknown'}"
        source_scope = str(current_pointer.get("source_scope") or "").strip() if current_pointer is not None else "stock"

    primary_result_audit = _read_result_scoped_json(resolved_audit_path, result_id=result_id, ts_code=ts_code)
    primary_result_execution = _read_result_scoped_json(resolved_execution_path, result_id=result_id, ts_code=ts_code)
    primary_result_observation = _read_result_scoped_json(resolved_observation_path, result_id=result_id, ts_code=ts_code)
    primary_result_rollback = _read_result_scoped_json(resolved_rollback_path, result_id=result_id, ts_code=ts_code)
    primary_result_terminal = _read_result_scoped_json(resolved_terminal_path, result_id=result_id, ts_code=ts_code)
    research_status = _derive_research_status(daily_research_status)
    candidate_status = _derive_candidate_status(top_candidate, buylist)
    signal_level = _derive_signal_level(top_candidate)
    risk_level = _derive_risk_level(top_candidate)
    audit_status = _derive_primary_result_audit_status(primary_result_audit, result_id, ts_code)
    if audit_status is None:
        audit_status = _derive_audit_status(governance_audit, ts_code)
    promotion_status = None
    execution_status = _derive_primary_result_execution_status(primary_result_execution, result_id, ts_code)
    if execution_status is None:
        execution_status = _derive_execution_status(t1_checklist, buylist, ts_code)
    observation_status = _derive_primary_result_observation_status(primary_result_observation, result_id, ts_code)
    rollback_status = _derive_primary_result_rollback_status(primary_result_rollback, result_id, ts_code)
    if rollback_status is None:
        rollback_status = _derive_rollback_status(rollback_drill)
    terminal_outcome = _derive_primary_result_terminal_outcome(primary_result_terminal, result_id, ts_code)
    if terminal_outcome is None:
        terminal_outcome = _derive_terminal_outcome(top_candidate)
    history_summary = _build_history_summary(
        research_status=research_status,
        candidate_status=candidate_status,
        audit_status=audit_status,
        execution_status=execution_status,
        observation_status=observation_status,
        rollback_status=rollback_status,
        terminal_outcome=terminal_outcome,
    )
    disabled_reason = _build_disabled_reason(
        audit_status=audit_status,
        promotion_status=promotion_status,
        risk_level=risk_level,
        terminal_outcome=terminal_outcome,
    )
    invalid_reason = _build_invalid_reason(terminal_outcome=terminal_outcome)

    derived_result_lifecycle_stage = resolve_result_lifecycle_stage(
        research_status=research_status,
        candidate_status=candidate_status,
        audit_status=audit_status,
        execution_status=execution_status,
        terminal_outcome=terminal_outcome,
    )
    if current_result_record is not None:
        current_registry_stage = str(current_result_record.get("lifecycle_stage", "") or "").strip().upper()
        result_lifecycle_stage = _prefer_newer_result_lifecycle_stage(current_registry_stage, derived_result_lifecycle_stage)
    else:
        result_lifecycle_stage = derived_result_lifecycle_stage
    result_type = _RESULT_TYPE_BY_STAGE.get(result_lifecycle_stage)

    source_timestamps = {
        "candidates_top_latest.csv": _format_mtime(candidates_csv) or "-",
        "daily_research_status_latest.json": _format_mtime(daily_research_status_path) or "-",
        "primary_result_audit_latest.json": _format_mtime(resolved_audit_path) or "-",
        "primary_result_execution_latest.json": _format_mtime(resolved_execution_path) or "-",
        "primary_result_observation_latest.json": _format_mtime(resolved_observation_path) or "-",
        "primary_result_rollback_latest.json": _format_mtime(resolved_rollback_path) or "-",
        "primary_result_terminal_latest.json": _format_mtime(resolved_terminal_path) or "-",
        "governance_audit_latest.json": _format_mtime(governance_audit_path) or "-",
        "t1_execution_checklist_latest.json": _format_mtime(t1_checklist_path) or "-",
        "t12_rollback_drill_latest.json": _format_mtime(rollback_drill_path) or "-",
        "buylist_latest.json": _format_mtime(buylist_path) or "-",
    }
    if current_pointer is not None:
        pointer_snapshot_path = Path(str(current_pointer.get("snapshot_path") or ""))
        source_timestamps["current_result_pointer.current.json"] = _format_mtime(exp_dir.parent.parent / "artifacts" / "current_result_pointer" / "current.json") or "-"
        source_timestamps["current_result_pointer.snapshot.json"] = _format_mtime(pointer_snapshot_path) or "-"
    if current_result_record is not None:
        result_registry_entry_path = Path(str(current_result_record.get("_entry_path") or ""))
        source_timestamps["result_registry.record.json"] = _format_mtime(result_registry_entry_path) or "-"
    for artifact_entry in pointer_artifact_entries:
        artifact_id = str(artifact_entry.get("artifact_id") or "").strip()
        artifact_path = _artifact_entry_path(artifact_entry)
        if artifact_id and artifact_path is not None:
            source_timestamps[f"artifact_registry:{artifact_id}"] = _format_mtime(artifact_path) or "-"

    chain_mode = "chain_verified" if pointer_artifact_entries else ("pointer_alias_fallback" if current_pointer is not None else None)
    history_source_file, history_source_timestamp, history_generation_mode = _select_history_source(
        source_timestamps=source_timestamps,
        research_status=research_status,
        candidate_status=candidate_status,
        audit_status=audit_status,
        execution_status=execution_status,
        observation_status=observation_status,
        rollback_status=rollback_status,
        terminal_outcome=terminal_outcome,
    )
    if history_generation_mode == "degraded" and chain_mode == "chain_verified":
        history_generation_mode = "chain_verified"
    data_sync_note = _build_data_sync_note(
        audit_status=audit_status,
        execution_status=execution_status,
        observation_status=observation_status,
        rollback_status=rollback_status,
        terminal_outcome=terminal_outcome,
        source_timestamps=source_timestamps,
    )

    return UnifiedResultRecord(
        result_id=result_id,
        ts_code=ts_code or "暂无",
        stock_name=stock_name,
        result_lifecycle_stage=result_lifecycle_stage,
        source_scope=source_scope or "stock",
        run_id=pointer_run_id,
        lifecycle_id=pointer_lifecycle_id,
        artifact_ids=[str(item) for item in (current_pointer.get("artifact_ids") or [])] if current_pointer is not None else [],
        as_of_date=pointer_as_of_date,
        result_type=result_type,
        research_status=research_status,
        candidate_status=candidate_status,
        signal_level=signal_level,
        risk_level=risk_level,
        audit_status=audit_status,
        promotion_status=promotion_status,
        execution_status=execution_status,
        observation_status=observation_status,
        rollback_status=rollback_status,
        terminal_outcome=terminal_outcome,
        history_summary=history_summary,
        history_source_file=history_source_file,
        history_source_timestamp=history_source_timestamp,
        history_generation_mode=history_generation_mode,
        disabled_reason=disabled_reason,
        invalid_reason=invalid_reason,
        source_timestamps=source_timestamps,
        data_sync_note=data_sync_note,
    )


def build_primary_result_api_payload(
    exp_dir: Path,
    candidate_index: int = 0,
    *,
    require_current_pointer: bool = False,
    ignore_current_pointer: bool = False,
) -> dict[str, object]:
    result = build_primary_result(
        exp_dir,
        candidate_index=candidate_index,
        require_current_pointer=require_current_pointer,
        ignore_current_pointer=ignore_current_pointer,
    )
    return {
        "schema_version": "primary_result_v1",
        "result_id": result.result_id,
        "ts_code": result.ts_code,
        "stock_name": result.stock_name,
        "result_lifecycle_stage": result.result_lifecycle_stage,
        "source_scope": result.source_scope,
        "run_id": result.run_id,
        "lifecycle_id": result.lifecycle_id,
        "artifact_ids": result.artifact_ids,
        "as_of_date": result.as_of_date,
        "result_type": result.result_type,
        "research_status": result.research_status,
        "candidate_status": result.candidate_status,
        "signal_level": result.signal_level,
        "risk_level": result.risk_level,
        "audit_status": result.audit_status,
        "promotion_status": result.promotion_status,
        "execution_status": result.execution_status,
        "observation_status": result.observation_status,
        "rollback_status": result.rollback_status,
        "terminal_outcome": result.terminal_outcome,
        "history_summary": result.history_summary,
        "history_source_file": result.history_source_file,
        "history_source_timestamp": result.history_source_timestamp,
        "history_generation_mode": result.history_generation_mode,
        "disabled_reason": result.disabled_reason,
        "invalid_reason": result.invalid_reason,
        "data_sync_note": result.data_sync_note,
        "source_timestamps": result.source_timestamps,
    }

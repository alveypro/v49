from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_PUBLIC_EXPLANATION_VERSION = "candidate_public_explanation.v1"
CANDIDATE_INTERNAL_EXPLANATION_VERSION = "candidate_internal_explanation.v1"
CANDIDATE_REJECTION_EXPLANATION_VERSION = "candidate_rejection_explanation.v1"
PUBLIC_REQUIRED_FIELDS = (
    "why_watch",
    "main_risk",
    "invalid_when",
    "next_observation",
)
NON_INVESTMENT_ADVICE = "本内容只用于研究观察，不构成买入、卖出或持有建议。"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str | Path | None) -> dict[str, Any]:
    if not path or not Path(path).exists():
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected object json: {path}")
    return payload


def _by_observation_id(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("observation_id") or ""): item for item in payload.get("items", []) or payload.get("candidates", []) or []}


def _by_code(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("ts_code") or ""): item for item in payload.get("items", []) or payload.get("candidates", []) or []}


def _first_text(values: list[object], default: str) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return default


def _risk_text(*, risk_item: dict[str, Any], portfolio_quality: dict[str, Any], capacity_item: dict[str, Any]) -> str:
    reasons = [str(reason) for reason in risk_item.get("reasons", []) or []]
    if "portfolio_quality_blocked" in reasons:
        return "组合质量证明未通过，当前不能按正常观察名单展示。"
    if "capacity_review" in reasons:
        return "单票容量接近观察线，放大资金规模时成交承载需要复核。"
    if portfolio_quality.get("status") == "blocked":
        return "组合层证据不足，质量分低于晋级线。"
    if capacity_item.get("state") == "review":
        return "流动性容量需要复核。"
    return "观察窗口尚未完成，结论仍可能被后续价格和风险数据推翻。"


def _invalid_text(*, risk_item: dict[str, Any], observation_item: dict[str, Any]) -> str:
    reasons = [str(reason) for reason in risk_item.get("reasons", []) or []]
    if "max_drawdown_exceeds_invalid_threshold" in reasons:
        return "若最大回撤继续超过失效阈值，观察结论失效。"
    if "return_exceeds_invalid_threshold" in reasons:
        return "若观察期收益跌破失效阈值，观察结论失效。"
    if "data_quality_failed" in reasons:
        return "若数据质量无法恢复，该候选不应继续纳入外部展示。"
    pending = ", ".join(str(reason) for reason in observation_item.get("blocking_reasons", []) or [])
    if pending:
        return f"若后续观察窗口仍无法补齐，当前判断不成立。"
    return "若组合质量、容量或观察收益继续不达标，当前判断失效。"


def _next_observation_text(*, risk_item: dict[str, Any], observation_item: dict[str, Any]) -> str:
    state = str(risk_item.get("risk_state") or "")
    if state == "degrade":
        return "下一步先看组合质量分、行业集中度和容量参与率是否恢复到复核线以内。"
    if state == "invalid":
        return "下一步只检查失效原因是否解除，解除前不恢复为正常观察。"
    if state == "closed":
        return "观察窗口已完成，下一步查看闭环收益、超额收益和失败归因。"
    if observation_item.get("status") == "pending":
        return "下一步等待 5D、20D、60D 观察窗口形成可计算结果。"
    return "下一步继续跟踪收益、回撤、容量和组合质量变化。"


def _factor_contribution(snapshot_item: dict[str, Any]) -> list[dict[str, Any]]:
    reason = str(snapshot_item.get("selection_reason") or "")
    parts = [part.strip() for part in reason.split(",") if part.strip()]
    contributions = []
    for part in parts:
        label = part
        score = None
        if "(" in part and ")" in part:
            label = part.split("(", 1)[0].strip()
            score = part.split("(", 1)[1].split(")", 1)[0].strip()
        contributions.append({"factor": label or part, "evidence": part, "score": score})
    if not contributions:
        contributions.append({"factor": "selection_reason_missing", "evidence": "候选原因缺失", "score": None})
    return contributions


def build_candidate_explanations(
    *,
    snapshot_path: str | Path,
    risk_state_path: str | Path,
    observation_result_path: str | Path | None = None,
    failure_attribution_path: str | Path | None = None,
    portfolio_path: str | Path | None = None,
    portfolio_quality_path: str | Path | None = None,
    portfolio_capacity_path: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    snapshot = _read_json(snapshot_path)
    risk_state = _read_json(risk_state_path)
    observation_result = _read_json(observation_result_path)
    failure_attribution = _read_json(failure_attribution_path)
    portfolio = _read_json(portfolio_path)
    portfolio_quality = _read_json(portfolio_quality_path)
    portfolio_capacity = _read_json(portfolio_capacity_path)
    risk_by_id = _by_observation_id(risk_state)
    result_by_id = _by_observation_id(observation_result)
    attribution_by_id = _by_observation_id(failure_attribution)
    portfolio_by_id = _by_observation_id(portfolio)
    capacity_by_code = _by_code(portfolio_capacity)
    generated_at = _utc_now()
    public_items: list[dict[str, Any]] = []
    internal_items: list[dict[str, Any]] = []
    rejection_items: list[dict[str, Any]] = []
    blocking_reasons: list[str] = []
    internal_review_reasons: list[str] = []

    for snapshot_item in snapshot.get("items", []) or []:
        observation_id = str(snapshot_item.get("observation_id") or "")
        code = str(snapshot_item.get("ts_code") or "")
        risk_item = risk_by_id.get(observation_id, {})
        observation_item = result_by_id.get(observation_id, {})
        attribution_item = attribution_by_id.get(observation_id, {})
        portfolio_item = portfolio_by_id.get(observation_id, {})
        capacity_item = capacity_by_code.get(code, {})
        stock_name = str(snapshot_item.get("stock_name") or code)
        industry = str(snapshot_item.get("industry") or "未知行业")
        state = str(risk_item.get("risk_state") or "review")
        why_watch = _first_text(
            [
                f"{stock_name} 属于{industry}，进入观察名单的主要依据是：{snapshot_item.get('selection_reason')}",
                snapshot_item.get("selection_reason"),
            ],
            f"{stock_name} 进入观察名单，但候选依据需要继续核验。",
        )
        public_item = {
            "observation_id": observation_id,
            "ts_code": code,
            "stock_name": stock_name,
            "risk_state": state,
            "external_display_allowed": state in {"watch", "review"},
            "why_watch": why_watch,
            "main_risk": _risk_text(risk_item=risk_item, portfolio_quality=portfolio_quality, capacity_item=capacity_item),
            "invalid_when": _invalid_text(risk_item=risk_item, observation_item=observation_item),
            "next_observation": _next_observation_text(risk_item=risk_item, observation_item=observation_item),
            "boundary": NON_INVESTMENT_ADVICE,
        }
        missing_public = [field for field in PUBLIC_REQUIRED_FIELDS if not str(public_item.get(field) or "").strip()]
        if missing_public:
            blocking_reasons.append(f"public_explanation_missing:{code}:{','.join(missing_public)}")
        public_items.append(public_item)

        internal_item = {
            "observation_id": observation_id,
            "ts_code": code,
            "stock_name": stock_name,
            "risk_state": state,
            "rank": snapshot_item.get("rank"),
            "final_score": snapshot_item.get("final_score"),
            "factor_contributions": _factor_contribution(snapshot_item),
            "model_confidence": {
                "lineage_run_id": snapshot_item.get("lineage_run_id"),
                "lineage_hash": snapshot_item.get("lineage_hash"),
                "data_quality_level": snapshot_item.get("data_quality_level"),
                "data_quality_score": snapshot_item.get("data_quality_score"),
            },
            "portfolio_evidence": {
                "weight": portfolio_item.get("weight"),
                "portfolio_role": portfolio_item.get("portfolio_role"),
                "quality_status": portfolio_quality.get("status"),
                "quality_score": portfolio_quality.get("quality_score"),
                "quality_blocking_reasons": portfolio_quality.get("blocking_reasons", []),
            },
            "capacity_evidence": capacity_item,
            "observation_evidence": observation_item,
            "failure_attribution": attribution_item,
            "risk_state_reasons": risk_item.get("reasons", []),
            "risk_state_evidence": risk_item.get("evidence", []),
            "historical_similar_samples": [],
            "exclusion_reasons": risk_item.get("reasons", []) if state in {"degrade", "invalid"} else [],
        }
        required_internal = ["factor_contributions", "model_confidence", "observation_evidence", "risk_state_reasons"]
        missing_internal = [field for field in required_internal if not internal_item.get(field)]
        if missing_internal:
            internal_review_reasons.append(f"internal_explanation_missing:{code}:{','.join(missing_internal)}")
        internal_items.append(internal_item)

        if state in {"degrade", "invalid"} or not public_item["external_display_allowed"]:
            rejection_item = {
                "observation_id": observation_id,
                "ts_code": code,
                "stock_name": stock_name,
                "rank": snapshot_item.get("rank"),
                "final_score": snapshot_item.get("final_score"),
                "risk_state": state,
                "excluded_from_normal_public_display": True,
                "rejection_reasons": risk_item.get("reasons", []) or ["risk_state_not_normal"],
                "rejection_evidence": risk_item.get("evidence", []),
                "boundary": "该候选不是正常观察展示对象，需先解除风险状态。",
            }
            if not rejection_item["rejection_reasons"]:
                blocking_reasons.append(f"rejection_reason_missing:{code}")
            rejection_items.append(rejection_item)

    public_status = "blocked" if blocking_reasons else "passed"
    internal_status = "review" if internal_review_reasons else "passed"
    rejection_status = "blocked" if any(not item.get("rejection_reasons") for item in rejection_items) else "passed"
    public_payload = {
        "schema_version": CANDIDATE_PUBLIC_EXPLANATION_VERSION,
        "status": public_status,
        "generated_at": generated_at,
        "snapshot_date": snapshot.get("snapshot_date"),
        "candidate_count": len(public_items),
        "blocking_reasons": sorted(set(blocking_reasons)),
        "required_fields": list(PUBLIC_REQUIRED_FIELDS),
        "non_investment_advice": NON_INVESTMENT_ADVICE,
        "items": public_items,
    }
    internal_payload = {
        "schema_version": CANDIDATE_INTERNAL_EXPLANATION_VERSION,
        "status": internal_status,
        "generated_at": generated_at,
        "snapshot_date": snapshot.get("snapshot_date"),
        "candidate_count": len(internal_items),
        "review_reasons": sorted(set(internal_review_reasons)),
        "items": internal_items,
    }
    rejection_payload = {
        "schema_version": CANDIDATE_REJECTION_EXPLANATION_VERSION,
        "status": rejection_status,
        "generated_at": generated_at,
        "snapshot_date": snapshot.get("snapshot_date"),
        "candidate_count": len(rejection_items),
        "blocking_reasons": [] if rejection_status == "passed" else ["rejection_reason_missing"],
        "items": rejection_items,
    }
    return public_payload, internal_payload, rejection_payload


def write_candidate_explanation_payload(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)

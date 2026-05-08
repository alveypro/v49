from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CANDIDATE_QUALITY_REMEDIATION_PLAN_VERSION = "candidate_quality_remediation_plan.v1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: str | Path) -> dict[str, Any]:
    resolved = Path(path)
    if not resolved.exists():
        return {"status": "missing"}
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {"status": "invalid"}


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _selected_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return list(payload.get("items", []) or payload.get("candidates", []) or [])


def _dominant_replacement_target(portfolio: dict[str, Any], capacity: dict[str, Any]) -> dict[str, Any] | None:
    summary = portfolio.get("summary", {}) or {}
    dominant = str(summary.get("top_industry") or "")
    capacity_by_code = {str(item.get("ts_code") or ""): item for item in _selected_items(capacity)}
    dominant_items = [
        item
        for item in _selected_items(portfolio)
        if str(item.get("industry") or "") == dominant
    ]
    if not dominant_items:
        return None
    dominant_items.sort(
        key=lambda item: (
            capacity_by_code.get(str(item.get("ts_code") or ""), {}).get("state") != "review",
            _to_float(item.get("weight"), 0.0),
            _to_float(item.get("final_score"), 0.0),
        )
    )
    target = dominant_items[0]
    capacity_item = capacity_by_code.get(str(target.get("ts_code") or ""), {})
    return {
        "ts_code": target.get("ts_code"),
        "stock_name": target.get("stock_name"),
        "industry": target.get("industry"),
        "weight": target.get("weight"),
        "capacity_state": capacity_item.get("state"),
        "capacity_reasons": capacity_item.get("reasons", []),
    }


def build_candidate_quality_remediation_plan(*, exp_dir: str | Path) -> dict[str, Any]:
    root = Path(exp_dir)
    proof = _read_json(root / "candidate_quality_proof_report_latest.json")
    sample = _read_json(root / "candidate_quality_20_sample_report.json")
    backtest = _read_json(root / "realistic_backtest_latest.json")
    portfolio = _read_json(root / "candidate_portfolio_latest.json")
    capacity = _read_json(root / "portfolio_capacity_report_latest.json")
    quality = _read_json(root / "candidate_portfolio_quality_latest.json")
    risk_state = _read_json(root / "candidate_risk_state_latest.json")
    actions: list[dict[str, Any]] = []

    sample_count = int(_to_float(sample.get("sample_count"), 0.0))
    required_sample_count = int(_to_float(sample.get("required_sample_count"), 20.0))
    if sample_count < required_sample_count:
        actions.append(
            {
                "id": "R1",
                "priority": "P0",
                "blocker": "insufficient_completed_samples",
                "owner": "observation_pipeline",
                "action": "continue_daily_snapshot_freeze_and_ledger_until_20_completed_samples",
                "current_value": sample_count,
                "target_value": required_sample_count,
                "remaining": required_sample_count - sample_count,
                "verification": "candidate_quality_20_sample_report.sample_count >= 20 and status != blocked",
                "can_be_fixed_now": False,
                "reason": "需要真实观察窗口完成，不能补造样本。",
            }
        )

    rule_blocks = (backtest.get("summary", {}) or {}).get("rule_block_stats", {}) or {}
    if rule_blocks.get("insufficient_future_trade_dates", 0):
        actions.append(
            {
                "id": "R2",
                "priority": "P0",
                "blocker": "insufficient_future_trade_dates",
                "owner": "realistic_backtest_pipeline",
                "action": "rerun_realistic_backtest_after_future_trade_window_is_available",
                "current_value": rule_blocks.get("insufficient_future_trade_dates", 0),
                "target_value": 0,
                "verification": "realistic_backtest_latest.status != failed and transaction_cost_breakdown_latest.status != failed",
                "can_be_fixed_now": False,
                "reason": "当前本地数据没有足够未来交易日，不能用未来函数替代。",
            }
        )

    portfolio_summary = portfolio.get("summary", {}) or {}
    top_industry_weight = _to_float(portfolio_summary.get("top_industry_weight"), 0.0)
    target_industry_weight = _to_float((portfolio.get("policy", {}) or {}).get("target_max_industry_weight"), 0.5)
    if top_industry_weight > target_industry_weight:
        replacement_target = _dominant_replacement_target(portfolio, capacity)
        actions.append(
            {
                "id": "R3",
                "priority": "P1",
                "blocker": "industry_target_limit_exceeded",
                "owner": "candidate_generation",
                "action": "expand_candidate_pool_and_replace_or_downweight_dominant_industry_candidate",
                "current_value": round(top_industry_weight, 6),
                "target_value": target_industry_weight,
                "required_weight_reduction": round(top_industry_weight - target_industry_weight, 6),
                "dominant_industry": portfolio_summary.get("top_industry"),
                "preferred_replacement_target": replacement_target,
                "verification": "candidate_portfolio_latest.summary.top_industry_weight <= target_max_industry_weight",
                "can_be_fixed_now": False,
                "reason": "当前候选池只有正式入选对象，缺少非主导行业备选，不能凭空替换。",
            }
        )

    capacity_summary = capacity.get("summary", {}) or {}
    worst_participation = _to_float(capacity_summary.get("worst_participation_rate"), 0.0)
    if capacity.get("status") == "review":
        review_items = [
            item
            for item in _selected_items(capacity)
            if str(item.get("state") or "") == "review"
        ]
        actions.append(
            {
                "id": "R4",
                "priority": "P1",
                "blocker": "capacity_participation_exceeds_watch_limit",
                "owner": "portfolio_construction",
                "action": "reduce_or_replace_capacity_review_candidate_before_quality_promotion",
                "current_value": round(worst_participation, 8),
                "target_value": _to_float(review_items[0].get("warn_participation_rate"), 0.05) if review_items else 0.05,
                "affected_candidates": review_items,
                "verification": "portfolio_capacity_report_latest.status == passed",
                "can_be_fixed_now": False,
                "reason": "需要下一轮组合构建降低相关单票权重或替换更高容量对象。",
            }
        )

    quality_score = _to_float(quality.get("quality_score"), 0.0)
    min_quality = _to_float(quality.get("minimum_quality_score"), 60.0)
    if quality_score < min_quality:
        actions.append(
            {
                "id": "R5",
                "priority": "P0",
                "blocker": "portfolio_quality_score_below_minimum",
                "owner": "quality_gate",
                "action": "clear_sample_backtest_industry_capacity_blockers_then_rebuild_quality_score",
                "current_value": round(quality_score, 4),
                "target_value": min_quality,
                "score_gap": round(min_quality - quality_score, 4),
                "penalties": quality.get("penalties", []),
                "verification": "candidate_portfolio_quality_latest.quality_score >= minimum_quality_score and status != blocked",
                "can_be_fixed_now": False,
                "reason": "质量分由多个真实证据项扣分，不能直接调分。",
            }
        )

    degraded = int(_to_float((risk_state.get("state_counts", {}) or {}).get("degrade"), 0.0))
    if degraded:
        actions.append(
            {
                "id": "R6",
                "priority": "P2",
                "blocker": "risk_state_degrade",
                "owner": "risk_state_machine",
                "action": "rerun_risk_state_after_quality_and_capacity_reports_pass",
                "current_value": degraded,
                "target_value": 0,
                "verification": "candidate_risk_state_latest.state_counts.degrade == 0 before normal external watchlist",
                "can_be_fixed_now": False,
                "reason": "状态机正确降级，只有上游证据恢复后才能解除。",
            }
        )

    status = "blocked" if actions else "passed"
    next_run_order = [action["id"] for action in actions]
    return {
        "schema_version": CANDIDATE_QUALITY_REMEDIATION_PLAN_VERSION,
        "status": status,
        "generated_at": _utc_now(),
        "proof_status": proof.get("status"),
        "action_count": len(actions),
        "next_run_order": next_run_order,
        "actions": actions,
        "operator_summary": (
            "真实阻断项仍存在：不能声称候选质量已证明；按 R1-R6 顺序补样本、等交易窗口、降集中度、修容量、重算质量分。"
            if actions
            else "当前无质量证明阻断项。"
        ),
    }


def write_candidate_quality_remediation_plan(payload: dict[str, Any], *, output_path: str | Path) -> str:
    resolved = Path(output_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(resolved)

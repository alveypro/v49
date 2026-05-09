from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from typing import Any, Dict, Iterable, List

from openclaw.services.backtest_credibility_service import evaluate_backtest_credibility
from openclaw.services.ensemble_core_contract_service import build_ensemble_core_contract_review
from openclaw.services.lineage_service import apply_professional_migrations
from strategies.registry import all_strategy_names, get_profile


RECOMMENDATION_RUN_TYPES = {"scan", "experiment"}
TOP_STRATEGY_LIMIT = 3
TOP_STOCK_LIMIT = 5
RESEARCH_ONLY_STRATEGIES = {"ai", "ensemble_core"}


def _safe_json_loads(value: Any, fallback: Any) -> Any:
    try:
        parsed = json.loads(str(value or ""))
    except Exception:
        return fallback
    return parsed


def _clip_score(value: Any) -> float:
    try:
        parsed = float(value or 0.0)
    except (TypeError, ValueError):
        parsed = 0.0
    return max(0.0, min(100.0, parsed))


def _profile_payload(strategy: str) -> Dict[str, str]:
    try:
        profile = get_profile(strategy)
        return {
            "strategy_tier": profile.tier,
            "strategy_stage": profile.stage,
            "strategy_role": profile.role,
        }
    except KeyError:
        return {
            "strategy_tier": "unknown",
            "strategy_stage": "unknown",
            "strategy_role": "",
        }


def _research_only_payload(strategy: str) -> Dict[str, Any]:
    if strategy == "ensemble_core":
        contract = build_ensemble_core_contract_review({})
        return {
            "research_only_reason": "top_level_multi_alpha_portfolio_contract_missing",
            "required_to_compete": list(contract.get("blocking_reasons") or []),
            "research_contract": contract,
        }
    return {
        "research_only_reason": "no_real_runtime_backtest_handler_or_explainable_fact_chain",
        "required_to_compete": [
            "real_runtime_backtest_handler",
            "explainable_signal_fact_chain",
            "point_in_time_inputs",
            "cost_slippage_fill_constraints",
        ],
    }


def _backtest_component(summary: Dict[str, Any], *, source_run_id: str = "", source: str = "current_run") -> Dict[str, Any]:
    audit = summary.get("backtest_credibility") if isinstance(summary.get("backtest_credibility"), dict) else {}
    if not audit:
        audit = summary.get("backtest_audit") if isinstance(summary.get("backtest_audit"), dict) else {}
    result = evaluate_backtest_credibility(audit)
    diagnostics = (
        summary.get("strategy_backtest_diagnostics")
        if isinstance(summary.get("strategy_backtest_diagnostics"), dict)
        else {}
    )
    quality_floor_passed = True
    eligible_for_formal_ranking = bool(result.get("passed"))
    if diagnostics:
        quality_floor_passed = bool(diagnostics.get("quality_floor_passed"))
        eligible_for_formal_ranking = bool(diagnostics.get("eligible_for_formal_ranking"))
    return {
        "source": str(source or "current_run"),
        "source_run_id": str(source_run_id or ""),
        "score": 100.0 if result.get("passed") is True else 0.0,
        "passed": bool(result.get("passed")),
        "quality_floor_passed": bool(quality_floor_passed),
        "eligible_for_formal_ranking": bool(eligible_for_formal_ranking),
        "diagnostic_version": str(diagnostics.get("diagnostic_version") or ""),
        "blocking_reasons": list(result.get("blocking_reasons") or []),
    }


def _backtest_evidence_priority(*, summary: Dict[str, Any], artifact_path: str, status: str) -> tuple[int, int, int, int, int, int]:
    diagnostics = summary.get("strategy_backtest_diagnostics")
    credibility = summary.get("backtest_credibility")
    audit = summary.get("backtest_audit")
    has_diagnostics = isinstance(diagnostics, dict) and bool(diagnostics)
    has_credibility = isinstance(credibility, dict) and bool(credibility)
    has_audit = isinstance(audit, dict) and bool(audit)
    has_artifact = bool(str(artifact_path or "").strip())
    successful = str(status or "").lower() == "success"
    component = _backtest_component(summary, source="candidate_backtest")
    return (
        1 if component.get("eligible_for_formal_ranking") is True else 0,
        1 if component.get("passed") is True else 0,
        1 if has_diagnostics or has_credibility or has_audit else 0,
        1 if component.get("quality_floor_passed") is True else 0,
        1 if has_artifact else 0,
        1 if successful else 0,
    )


def _latest_backtest_component_for_strategy(
    conn: sqlite3.Connection,
    *,
    strategy: str,
) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT run_id, status, artifact_path, summary_json, created_at, trade_date
        FROM signal_runs
        WHERE run_type = 'backtest'
          AND strategy = ?
        ORDER BY created_at DESC, trade_date DESC
        """,
        (str(strategy or "").lower(),),
    ).fetchall()
    if not rows:
        missing = _backtest_component({}, source="latest_strategy_backtest")
        missing["source_run_missing"] = True
        return missing
    candidates = []
    for index, row in enumerate(rows):
        summary = _safe_json_loads(row[3], {})
        parsed_summary = summary if isinstance(summary, dict) else {}
        priority = _backtest_evidence_priority(
            summary=parsed_summary,
            artifact_path=str(row[2] or ""),
            status=str(row[1] or ""),
        )
        candidates.append((priority, -index, row, parsed_summary))
    _priority, _recency, row, summary = sorted(candidates, key=lambda item: (item[0], item[1]), reverse=True)[0]
    component = _backtest_component(
        summary,
        source_run_id=str(row[0] or ""),
        source="latest_strategy_backtest",
    )
    component["source_run_status"] = str(row[1] or "")
    component["source_artifact_path"] = str(row[2] or "")
    component["source_created_at"] = str(row[4] or "")
    component["source_trade_date"] = str(row[5] or "")
    component["source_evidence_priority"] = list(
        _backtest_evidence_priority(
            summary=summary,
            artifact_path=str(row[2] or ""),
            status=str(row[1] or ""),
        )
    )
    return component


def _resolve_backtest_component(conn: sqlite3.Connection, run: Dict[str, Any]) -> Dict[str, Any]:
    current = _backtest_component(
        run.get("summary") or {},
        source_run_id=str(run.get("run_id") or ""),
        source="current_run",
    )
    if current.get("passed") is True:
        return current
    if str(run.get("run_type") or "").lower() == "scan":
        latest = _latest_backtest_component_for_strategy(conn, strategy=str(run.get("strategy") or ""))
        if latest.get("source_run_missing") is not True:
            latest["current_run_blocking_reasons"] = list(current.get("blocking_reasons") or [])
            return latest
    return current


def _execution_component(summary: Dict[str, Any]) -> Dict[str, Any]:
    evidence = summary.get("execution_evidence") if isinstance(summary.get("execution_evidence"), dict) else {}
    if not evidence:
        return {"score": 0.0, "sample_count": 0, "mode": "", "passed": False, "blocking_reasons": ["execution_evidence_missing"]}
    sample_count = int(evidence.get("total_orders", evidence.get("sample_count", 0)) or 0)
    mode = str(evidence.get("minimum_source_tier") or evidence.get("mode") or "").strip().lower()
    linked_run_ids = evidence.get("linked_run_ids")
    cases = evidence.get("cases")
    blocking = list(evidence.get("blocking_reasons") or [])
    if sample_count <= 0:
        blocking.append("execution_evidence_empty")
    if not isinstance(linked_run_ids, list) or not [item for item in linked_run_ids if str(item or "").strip()]:
        blocking.append("missing_linked_run_ids")
    if not isinstance(cases, list) or not cases:
        blocking.append("missing_execution_cases")
    elif not _execution_cases_are_traceable(cases):
        blocking.append("incomplete_execution_cases")
    passed = evidence.get("passed") is True and not blocking
    mode_score = {"broker": 100.0, "quasi_live": 80.0, "canary": 70.0, "shadow": 55.0, "simulated": 30.0}.get(mode, 0.0)
    return {
        "score": mode_score if passed else 0.0,
        "sample_count": sample_count,
        "mode": mode,
        "passed": passed,
        "linked_run_ids": list(linked_run_ids or []) if isinstance(linked_run_ids, list) else [],
        "blocking_reasons": blocking,
    }


def _execution_cases_are_traceable(cases: Iterable[Any]) -> bool:
    for case in cases:
        if not isinstance(case, dict):
            return False
        if not str(case.get("decision_id") or "").strip():
            return False
        if not str(case.get("based_on_run_id") or "").strip():
            return False
        if case.get("has_attribution") is not True:
            return False
        if "slippage_bp" not in case:
            return False
    return True


def _latest_decision_for_run(conn: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT decision_id, decision_type, release_gate_state, created_at
        FROM decision_events
        WHERE based_on_run_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (str(run_id or ""),),
    ).fetchone()
    if not row:
        return {
            "decision_id": "",
            "decision_type": "pending",
            "release_gate_state": {"state": "decision_pending"},
            "created_at": "",
        }
    release_gate_state = _safe_json_loads(row[2], {})
    return {
        "decision_id": str(row[0] or ""),
        "decision_type": str(row[1] or ""),
        "release_gate_state": release_gate_state if isinstance(release_gate_state, dict) else {},
        "created_at": str(row[3] or ""),
    }


def _latest_strategy_runs(conn: sqlite3.Connection, *, trade_date: str = "") -> List[Dict[str, Any]]:
    params: List[Any] = []
    trade_filter = ""
    if str(trade_date or "").strip():
        trade_filter = "AND trade_date = ?"
        params.append(str(trade_date or "").strip())
    rows = conn.execute(
        f"""
        SELECT run_id, run_type, strategy, trade_date, data_version, code_version,
               param_version, status, summary_json, created_at
        FROM signal_runs
        WHERE status = 'success'
          AND run_type IN ('scan', 'experiment')
          {trade_filter}
        ORDER BY trade_date DESC, created_at DESC
        """,
        tuple(params),
    ).fetchall()
    latest: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        strategy = str(row[2] or "").lower()
        if not strategy or strategy in latest:
            continue
        summary = _safe_json_loads(row[8], {})
        latest[strategy] = {
            "run_id": str(row[0] or ""),
            "run_type": str(row[1] or ""),
            "strategy": strategy,
            "trade_date": str(row[3] or ""),
            "data_version": str(row[4] or ""),
            "code_version": str(row[5] or ""),
            "param_version": str(row[6] or ""),
            "status": str(row[7] or ""),
            "summary": summary if isinstance(summary, dict) else {},
            "created_at": str(row[9] or ""),
        }
    return list(latest.values())


def _signal_item_stats(conn: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT ts_code, score, rank_idx, reason_codes, raw_payload_json
        FROM signal_items
        WHERE run_id = ?
        ORDER BY COALESCE(rank_idx, 999999), score DESC, ts_code
        """,
        (str(run_id or ""),),
    ).fetchall()
    scores = [_clip_score(row[1]) for row in rows]
    return {
        "item_count": len(rows),
        "top_score": max(scores) if scores else 0.0,
        "avg_top_score": sum(scores[:5]) / min(len(scores), 5) if scores else 0.0,
        "items": [
            {
                "ts_code": str(row[0] or ""),
                "score": _clip_score(row[1]),
                "rank_idx": int(row[2] or 0),
                "reason_codes": _safe_json_loads(row[3], []),
                "raw_payload": _safe_json_loads(row[4], {}),
            }
            for row in rows
        ],
    }


def _strategy_payload(conn: sqlite3.Connection, run: Dict[str, Any]) -> Dict[str, Any]:
    profile = _profile_payload(str(run.get("strategy") or ""))
    item_stats = _signal_item_stats(conn, str(run.get("run_id") or ""))
    backtest = _resolve_backtest_component(conn, run)
    execution = _execution_component(run.get("summary") or {})
    decision = _latest_decision_for_run(conn, str(run.get("run_id") or ""))
    missing_versions = [
        key
        for key in ("data_version", "code_version", "param_version")
        if not str(run.get(key) or "").strip()
    ]
    blocking: List[str] = []
    if not str(run.get("run_id") or "").strip():
        blocking.append("missing_signal_run")
    if item_stats["item_count"] <= 0:
        blocking.append("missing_signal_items")
    if missing_versions:
        blocking.append("missing_versions:" + ",".join(missing_versions))
    if backtest["passed"] is not True:
        blocking.append("backtest_credibility_not_passed")
    if backtest["passed"] is True and backtest.get("eligible_for_formal_ranking") is not True:
        blocking.append("backtest_quality_floor_not_passed")

    signal_score = 0.7 * float(item_stats["top_score"]) + 0.3 * float(item_stats["avg_top_score"])
    final_score = (
        0.60 * signal_score
        + 0.25 * float(backtest["score"])
        + 0.10 * float(execution["score"])
        + 0.05 * (100.0 if decision["decision_id"] else 40.0)
    )
    eligible = not blocking
    return {
        **run,
        **profile,
        "signal_score": round(signal_score, 4),
        "backtest_component": backtest,
        "execution_component": execution,
        "decision_state": decision,
        "item_count": item_stats["item_count"],
        "top_signal_score": round(float(item_stats["top_score"]), 4),
        "avg_top_signal_score": round(float(item_stats["avg_top_score"]), 4),
        "eligible_for_daily_top3": eligible,
        "blocking_reasons": blocking,
        "final_system_score": round(final_score if eligible else min(final_score, 49.0), 4),
        "items": item_stats["items"],
    }


def _placeholder_strategy_run(strategy: str) -> Dict[str, Any]:
    profile = _profile_payload(strategy)
    return {
        "run_id": "",
        "run_type": "",
        "strategy": strategy,
        "trade_date": "",
        "data_version": "",
        "code_version": "",
        "param_version": "",
        "status": "missing",
        "summary": {},
        "created_at": "",
        **profile,
    }


def _build_review_pools(strategies: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    eligible_pool: List[Dict[str, Any]] = []
    observation_pool: List[Dict[str, Any]] = []
    diagnostic_pool: List[Dict[str, Any]] = []
    research_only_pool: List[Dict[str, Any]] = []
    competition_pool: List[Dict[str, Any]] = []
    for item in strategies:
        payload = {key: value for key, value in item.items() if key != "items"}
        backtest = item.get("backtest_component") if isinstance(item.get("backtest_component"), dict) else {}
        if item.get("eligible_for_daily_top3") is True:
            eligible_pool.append(payload)
            competition_status = "formal_eligible"
        elif backtest.get("passed") is True and backtest.get("eligible_for_formal_ranking") is not True:
            observation_pool.append(payload)
            competition_status = "observation"
        elif item.get("strategy") in RESEARCH_ONLY_STRATEGIES and item.get("run_id") == "":
            payload = {
                **payload,
                **_research_only_payload(str(item.get("strategy") or "")),
            }
            research_only_pool.append(payload)
            competition_status = "research_only"
        else:
            diagnostic_pool.append(payload)
            competition_status = "diagnostic"
        competition_pool.append(
            {
                **payload,
                "competition_status": competition_status,
                "competes_for_formal_top": competition_status == "formal_eligible",
            }
        )
    return {
        "competition_pool": competition_pool,
        "eligible_pool": eligible_pool,
        "observation_pool": observation_pool,
        "diagnostic_pool": diagnostic_pool,
        "research_only_pool": research_only_pool,
    }


def _build_top_stocks(strategies: Iterable[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
    contributions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for strategy in strategies:
        if strategy.get("eligible_for_daily_top3") is not True:
            continue
        for item in strategy.get("items") or []:
            ts_code = str(item.get("ts_code") or "")
            if not ts_code:
                continue
            contributions[ts_code].append(
                {
                    "strategy": strategy["strategy"],
                    "run_id": strategy["run_id"],
                    "strategy_stage": strategy["strategy_stage"],
                    "strategy_tier": strategy["strategy_tier"],
                    "signal_score": float(item.get("score") or 0.0),
                    "rank_idx": int(item.get("rank_idx") or 0),
                    "reason_codes": item.get("reason_codes") or [],
                    "strategy_score": float(strategy.get("final_system_score") or 0.0),
                }
            )
    rows = []
    for ts_code, refs in contributions.items():
        avg_signal = sum(ref["signal_score"] for ref in refs) / len(refs)
        avg_strategy = sum(ref["strategy_score"] for ref in refs) / len(refs)
        consensus_bonus = min(20.0, (len(refs) - 1) * 8.0)
        final_score = min(100.0, 0.65 * avg_signal + 0.25 * avg_strategy + consensus_bonus)
        rows.append(
            {
                "ts_code": ts_code,
                "final_stock_score": round(final_score, 4),
                "consensus_count": len(refs),
                "avg_signal_score": round(avg_signal, 4),
                "contributing_strategies": [ref["strategy"] for ref in refs],
                "signal_refs": refs,
            }
        )
    return sorted(
        rows,
        key=lambda item: (-float(item["final_stock_score"]), -int(item["consensus_count"]), str(item["ts_code"])),
    )[: int(limit or TOP_STOCK_LIMIT)]


def build_unified_system_recommendation(
    conn: sqlite3.Connection,
    *,
    trade_date: str = "",
    top_strategy_limit: int = TOP_STRATEGY_LIMIT,
    top_stock_limit: int = TOP_STOCK_LIMIT,
) -> Dict[str, Any]:
    apply_professional_migrations(conn)
    runs = _latest_strategy_runs(conn, trade_date=trade_date)
    actual_strategy_names = {str(run.get("strategy") or "") for run in runs}
    for strategy in all_strategy_names():
        if strategy not in actual_strategy_names:
            runs.append(_placeholder_strategy_run(strategy))
    strategies = [_strategy_payload(conn, run) for run in runs]
    ranked_strategies = sorted(
        strategies,
        key=lambda item: (-float(item["final_system_score"]), str(item["strategy"])),
    )
    pools = _build_review_pools(ranked_strategies)
    top_strategies = [
        item
        for item in ranked_strategies
        if item.get("eligible_for_daily_top3") is True
    ][: int(top_strategy_limit or TOP_STRATEGY_LIMIT)]
    top_stocks = _build_top_stocks(top_strategies, limit=int(top_stock_limit or TOP_STOCK_LIMIT))
    blocking = []
    if not actual_strategy_names:
        blocking.append("no_successful_signal_runs")
    if not top_strategies:
        blocking.append("no_strategy_passed_daily_top3_gate")
    if not top_stocks:
        blocking.append("no_stock_passed_top5_gate")
    return {
        "passed": not blocking,
        "blocking_reasons": blocking,
        "trade_date": str(trade_date or ""),
        "top_strategies": [
            {key: value for key, value in item.items() if key != "items"}
            for item in top_strategies
        ],
        "top_stocks": top_stocks,
        **pools,
        "all_strategy_reviews": [
            {key: value for key, value in item.items() if key != "items"}
            for item in ranked_strategies
        ],
        "policy": {
            "unified_user_output": True,
            "single_competition_dimension": True,
            "preserve_internal_strategy_stage": True,
            "governance_labels_are_informational": True,
            "all_registered_strategies_reviewed": True,
            "top_strategy_limit": int(top_strategy_limit or TOP_STRATEGY_LIMIT),
            "top_stock_limit": int(top_stock_limit or TOP_STOCK_LIMIT),
            "minimum_requirements": [
                "signal_items_present",
                "versions_present",
                "backtest_credibility_passed",
                "backtest_quality_floor_passed",
            ],
        },
    }

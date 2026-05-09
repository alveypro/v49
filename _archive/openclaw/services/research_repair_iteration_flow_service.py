from __future__ import annotations

from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Dict, Iterable, Sequence


JsonDict = Dict[str, Any]


REPAIR_FLOW_STATUSES = {
    "research_blocked",
    "repair_attempt_predeclared",
    "repair_attempt_replayed",
    "repair_review_blocked",
    "observation_watch_discussion_allowed",
    "observation_watch_active",
    "formal_review_candidate",
    "formal_blocked",
    "production_forbidden",
}

PROHIBITED_REPAIR_ACTIONS = (
    "formal",
    "top",
    "production",
    "allocator_throttle_as_alpha",
    "cash_fallback_as_alpha",
    "ui_packaging_as_alpha",
)

ARTIFACT_TYPES = {
    "rule_freeze",
    "unthrottled_benchmark",
    "throttled_benchmark",
    "allocator_attribution",
    "observation_monitor",
    "repair_review",
    "failure_attribution",
    "watch_risk_register",
    "watch_risk_review",
    "oos_monitoring_plan",
    "oos_monitoring_result",
    "failure_attribution_comparison",
    "v7_go_no_go_review",
    "failed_research_candidate_archive",
}

WATCH_RISK_THRESHOLDS = {
    "neutral_signal_sparsity": {"warning": 3.0, "block": 2.0, "direction": "below"},
    "neutral_cash_weight_high": {"warning": 0.75, "block": 0.85, "direction": "above"},
    "over_veto_risk": {"warning": 0.85, "block": 0.92, "direction": "above"},
    "source_strategy_concentration": {"warning": 0.50, "block": 0.60, "direction": "above"},
    "same_window_repair_overfit_risk": {"warning": 1.0, "block": 2.0, "direction": "above"},
}


def apply_research_repair_flow_migration(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS research_repair_iteration_flow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flow_id TEXT NOT NULL UNIQUE,
            strategy TEXT NOT NULL,
            candidate TEXT NOT NULL,
            parent_flow_id TEXT NOT NULL DEFAULT '',
            attempt_no INTEGER NOT NULL,
            rule_version TEXT NOT NULL,
            rule_hash TEXT NOT NULL,
            current_status TEXT NOT NULL,
            current_pool TEXT NOT NULL DEFAULT 'research_blocked',
            repair_objective TEXT NOT NULL,
            forbidden_objectives TEXT NOT NULL DEFAULT '[]',
            predeclared_rules_json TEXT NOT NULL DEFAULT '{}',
            fixed_window_set_json TEXT NOT NULL DEFAULT '[]',
            benchmark_config_json TEXT NOT NULL DEFAULT '{}',
            data_snapshot TEXT NOT NULL DEFAULT '',
            unthrottled_artifact_path TEXT NOT NULL DEFAULT '',
            throttled_artifact_path TEXT NOT NULL DEFAULT '',
            attribution_artifact_path TEXT NOT NULL DEFAULT '',
            monitor_artifact_path TEXT NOT NULL DEFAULT '',
            repair_review_artifact_path TEXT NOT NULL DEFAULT '',
            unthrottled_summary_json TEXT NOT NULL DEFAULT '{}',
            throttled_summary_json TEXT NOT NULL DEFAULT '{}',
            regime_summary_json TEXT NOT NULL DEFAULT '{}',
            blocking_reasons_json TEXT NOT NULL DEFAULT '[]',
            warning_reasons_json TEXT NOT NULL DEFAULT '[]',
            next_allowed_actions_json TEXT NOT NULL DEFAULT '[]',
            prohibited_actions_json TEXT NOT NULL DEFAULT '[]',
            operator_name TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS research_rule_freeze_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_version TEXT NOT NULL UNIQUE,
            candidate TEXT NOT NULL,
            strategy TEXT NOT NULL,
            rule_hash TEXT NOT NULL,
            rule_spec_json TEXT NOT NULL DEFAULT '{}',
            activation_regime TEXT NOT NULL DEFAULT '',
            predeclared INTEGER NOT NULL DEFAULT 1,
            frozen_at TEXT NOT NULL,
            operator_name TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS research_artifact_registry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flow_id TEXT NOT NULL,
            artifact_type TEXT NOT NULL,
            artifact_path TEXT NOT NULL,
            artifact_hash TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            summary_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS observation_watch_risk_register (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            flow_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            candidate TEXT NOT NULL,
            risk_code TEXT NOT NULL,
            risk_level TEXT NOT NULL,
            risk_description TEXT NOT NULL,
            metric_name TEXT NOT NULL DEFAULT '',
            metric_value REAL,
            threshold_value REAL,
            required_monitoring TEXT NOT NULL,
            exit_condition TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_research_repair_flow_candidate
        ON research_repair_iteration_flow(strategy, candidate, current_status);
        CREATE INDEX IF NOT EXISTS idx_research_artifact_flow
        ON research_artifact_registry(flow_id, artifact_type);
        CREATE INDEX IF NOT EXISTS idx_observation_watch_risk_flow
        ON observation_watch_risk_register(flow_id, status);
        """
    )
    conn.commit()


def create_repair_flow(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    strategy: str,
    candidate: str,
    attempt_no: int,
    rule_version: str,
    rule_hash: str,
    repair_objective: str,
    forbidden_objectives: Sequence[str] | None = None,
    predeclared_rules: JsonDict | None = None,
    fixed_window_set: Sequence[str] | None = None,
    benchmark_config: JsonDict | None = None,
    data_snapshot: str = "",
    parent_flow_id: str = "",
    operator_name: str = "",
    governance_reopen_approved: bool = False,
) -> JsonDict:
    apply_research_repair_flow_migration(conn)
    archive = find_failed_research_candidate_archive(conn, strategy=str(strategy), candidate=str(candidate))
    if archive and not bool(governance_reopen_approved):
        raise ValueError(f"failed_research_candidate_archived:{candidate}")
    now = _now_text()
    prohibited = list(dict.fromkeys([*(forbidden_objectives or []), *PROHIBITED_REPAIR_ACTIONS]))
    conn.execute("DELETE FROM research_artifact_registry WHERE flow_id = ?", (str(flow_id),))
    conn.execute("DELETE FROM observation_watch_risk_register WHERE flow_id = ?", (str(flow_id),))
    conn.execute(
        """
        INSERT OR REPLACE INTO research_repair_iteration_flow (
            flow_id, strategy, candidate, parent_flow_id, attempt_no, rule_version, rule_hash,
            current_status, current_pool, repair_objective, forbidden_objectives,
            predeclared_rules_json, fixed_window_set_json, benchmark_config_json, data_snapshot,
            next_allowed_actions_json, prohibited_actions_json, operator_name, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(flow_id),
            str(strategy),
            str(candidate),
            str(parent_flow_id or ""),
            int(attempt_no),
            str(rule_version),
            str(rule_hash),
            "repair_attempt_predeclared",
            "research_blocked",
            str(repair_objective),
            _json(prohibited),
            _json(predeclared_rules or {}),
            _json(list(fixed_window_set or [])),
            _json(benchmark_config or {}),
            str(data_snapshot or ""),
            _json([]),
            _json(prohibited),
            str(operator_name or ""),
            now,
            now,
        ),
    )
    conn.commit()
    return build_repair_flow_snapshot(conn, flow_id=str(flow_id))


def find_failed_research_candidate_archive(
    conn: sqlite3.Connection,
    *,
    strategy: str,
    candidate: str,
) -> JsonDict:
    apply_research_repair_flow_migration(conn)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT f.flow_id, f.strategy, f.candidate, a.artifact_path, a.created_at, a.summary_json
        FROM research_repair_iteration_flow f
        JOIN research_artifact_registry a ON a.flow_id = f.flow_id
        WHERE f.strategy = ? AND f.candidate = ? AND a.artifact_type = 'failed_research_candidate_archive'
        ORDER BY a.id DESC
        LIMIT 1
        """,
        (str(strategy), str(candidate)),
    ).fetchall()
    if not rows:
        return {}
    row = dict(rows[0])
    row["summary_json"] = _loads(row.get("summary_json"))
    return row


def register_rule_freeze(
    conn: sqlite3.Connection,
    *,
    strategy: str,
    candidate: str,
    rule_version: str,
    rule_hash: str,
    rule_spec: JsonDict,
    activation_regime: str = "",
    operator_name: str = "",
    notes: str = "",
) -> JsonDict:
    apply_research_repair_flow_migration(conn)
    conn.execute(
        """
        INSERT OR REPLACE INTO research_rule_freeze_registry (
            rule_version, candidate, strategy, rule_hash, rule_spec_json,
            activation_regime, predeclared, frozen_at, operator_name, notes
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
        """,
        (
            str(rule_version),
            str(candidate),
            str(strategy),
            str(rule_hash),
            _json(rule_spec),
            str(activation_regime or ""),
            _now_text(),
            str(operator_name or ""),
            str(notes or ""),
        ),
    )
    conn.commit()
    return {"rule_version": str(rule_version), "rule_hash": str(rule_hash), "predeclared": True}


def attach_repair_artifact(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    artifact_type: str,
    artifact_path: str,
    summary: JsonDict | None = None,
) -> JsonDict:
    apply_research_repair_flow_migration(conn)
    if artifact_type not in ARTIFACT_TYPES:
        raise ValueError(f"unsupported_artifact_type:{artifact_type}")
    path = Path(artifact_path)
    artifact_hash = _file_hash(path) if path.exists() else ""
    conn.execute(
        "DELETE FROM research_artifact_registry WHERE flow_id = ? AND artifact_type = ? AND artifact_path = ?",
        (str(flow_id), str(artifact_type), str(artifact_path)),
    )
    conn.execute(
        """
        INSERT INTO research_artifact_registry (
            flow_id, artifact_type, artifact_path, artifact_hash, created_at, summary_json
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (str(flow_id), str(artifact_type), str(artifact_path), artifact_hash, _now_text(), _json(summary or {})),
    )
    column = {
        "unthrottled_benchmark": "unthrottled_artifact_path",
        "throttled_benchmark": "throttled_artifact_path",
        "allocator_attribution": "attribution_artifact_path",
        "observation_monitor": "monitor_artifact_path",
        "repair_review": "repair_review_artifact_path",
    }.get(artifact_type)
    if column:
        conn.execute(
            f"UPDATE research_repair_iteration_flow SET {column} = ?, updated_at = ? WHERE flow_id = ?",
            (str(artifact_path), _now_text(), str(flow_id)),
        )
    conn.commit()
    return build_repair_flow_snapshot(conn, flow_id=str(flow_id))


def close_repair_review(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    unthrottled_artifact_path: str,
    throttled_artifact_path: str,
    attribution_artifact_path: str,
    repair_review_artifact_path: str,
    monitor_artifact_path: str = "",
    operator_name: str = "",
) -> JsonDict:
    apply_research_repair_flow_migration(conn)
    flow = build_repair_flow_snapshot(conn, flow_id=flow_id)
    failures = validate_repair_flow_artifacts(
        flow,
        unthrottled_artifact_path=unthrottled_artifact_path,
        throttled_artifact_path=throttled_artifact_path,
        repair_review_artifact_path=repair_review_artifact_path,
    )
    unthrottled = _load_json(unthrottled_artifact_path)
    throttled = _load_json(throttled_artifact_path)
    review = _load_json(repair_review_artifact_path)
    status = "repair_review_blocked"
    next_actions: list[str] = []
    blocking = list(failures)
    warnings = list(review.get("warning_reasons") or [])
    if not failures and review.get("risk_off_alpha_repair_passed") is True:
        status = "observation_watch_discussion_allowed"
        next_actions = ["observation_watch_discussion", "watch_risk_register_review", "out_of_sample_monitoring_plan"]
    else:
        blocking.extend(str(item) for item in review.get("blocking_reasons") or [])
    conn.execute("DELETE FROM research_artifact_registry WHERE flow_id = ?", (str(flow_id),))
    conn.execute("DELETE FROM observation_watch_risk_register WHERE flow_id = ?", (str(flow_id),))
    conn.commit()
    _attach_many(
        conn,
        flow_id=flow_id,
        artifacts=[
            ("unthrottled_benchmark", unthrottled_artifact_path),
            ("throttled_benchmark", throttled_artifact_path),
            ("allocator_attribution", attribution_artifact_path),
            ("repair_review", repair_review_artifact_path),
            *([("observation_monitor", monitor_artifact_path)] if monitor_artifact_path else []),
        ],
    )
    conn.execute(
        """
        UPDATE research_repair_iteration_flow
        SET current_status = ?, current_pool = ?, unthrottled_summary_json = ?,
            throttled_summary_json = ?, regime_summary_json = ?, blocking_reasons_json = ?,
            warning_reasons_json = ?, next_allowed_actions_json = ?, prohibited_actions_json = ?,
            operator_name = COALESCE(NULLIF(?, ''), operator_name), updated_at = ?
        WHERE flow_id = ?
        """,
        (
            status,
            "observation_watch_discussion_only" if status == "observation_watch_discussion_allowed" else "research_blocked",
            _json(_benchmark_summary(unthrottled)),
            _json(_benchmark_summary(throttled)),
            _json((unthrottled.get("benchmark") or {}).get("regime_split") or {}),
            _json(sorted(set(blocking))),
            _json(sorted(set(str(item) for item in warnings))),
            _json(next_actions),
            _json(list(PROHIBITED_REPAIR_ACTIONS)),
            str(operator_name or ""),
            _now_text(),
            str(flow_id),
        ),
    )
    conn.commit()
    risks = record_watch_risks_from_benchmark(conn, flow_id=flow_id, benchmark_payload=unthrottled) if unthrottled else []
    block_risks = [str(item.get("risk_code") or "") for item in risks if item.get("risk_level") == "block"]
    if status == "observation_watch_discussion_allowed" and block_risks:
        warnings.extend(f"watch_risk_block_level:{item}" for item in block_risks)
        conn.execute(
            """
            UPDATE research_repair_iteration_flow
            SET warning_reasons_json = ?, next_allowed_actions_json = ?, updated_at = ?
            WHERE flow_id = ?
            """,
            (
                _json(sorted(set(str(item) for item in warnings))),
                _json(["watch_risk_register_review", "out_of_sample_monitoring_plan"]),
                _now_text(),
                str(flow_id),
            ),
        )
        conn.commit()
    return build_repair_flow_snapshot(conn, flow_id=flow_id)


def validate_repair_flow_artifacts(
    flow: JsonDict,
    *,
    unthrottled_artifact_path: str,
    throttled_artifact_path: str,
    repair_review_artifact_path: str,
) -> list[str]:
    failures: list[str] = []
    if not unthrottled_artifact_path:
        failures.append("missing_unthrottled_artifact")
    if not throttled_artifact_path:
        failures.append("missing_throttled_artifact")
    unthrottled = _load_json(unthrottled_artifact_path)
    throttled = _load_json(throttled_artifact_path)
    review = _load_json(repair_review_artifact_path)
    if not unthrottled:
        failures.append("unthrottled_artifact_unreadable")
    if not throttled:
        failures.append("throttled_artifact_unreadable")
    if review.get("formal_candidate_allowed") is True or review.get("formal_ranking_allowed") is True:
        failures.append("repair_review_attempted_formal_eligibility")
    if review.get("production_candidate_allowed") is True:
        failures.append("repair_review_attempted_production_eligibility")
    if unthrottled and throttled:
        if _window_set(unthrottled) != _window_set(throttled):
            failures.append("paired_benchmark_window_set_mismatch")
        expected_hash = str(flow.get("rule_hash") or "")
        hashes = {str((payload.get("rule_freeze") or {}).get("rule_hash") or "") for payload in (unthrottled, throttled)}
        if expected_hash and hashes != {expected_hash}:
            failures.append("paired_benchmark_rule_hash_mismatch")
    return sorted(set(failures))


def record_watch_risks_from_benchmark(conn: sqlite3.Connection, *, flow_id: str, benchmark_payload: JsonDict) -> list[JsonDict]:
    flow = build_repair_flow_snapshot(conn, flow_id=flow_id)
    windows = [row for row in benchmark_payload.get("windows") or [] if isinstance(row, dict)]
    neutral = [row for row in windows if str(row.get("market_regime_label") or "") == "neutral"]
    position_counts = [len(((row.get("shadow_portfolio") or {}).get("shadow_weights") or [])) for row in neutral]
    cash_weights = [float((row.get("shadow_portfolio") or {}).get("cash_weight", 0.0) or 0.0) for row in neutral]
    veto_rates = []
    for row in neutral:
        port = row.get("shadow_portfolio") if isinstance(row.get("shadow_portfolio"), dict) else {}
        kept = len(port.get("shadow_weights") or [])
        vetoed = len(port.get("neutral_vetoed_signals") or [])
        total = kept + vetoed
        if total > 0:
            veto_rates.append(vetoed / float(total))
    risk = (benchmark_payload.get("benchmark") or {}).get("risk_contribution") or {}
    sources = risk.get("source_strategy_weight_share") if isinstance(risk.get("source_strategy_weight_share"), dict) else {}
    max_source = max((float(v or 0.0) for v in sources.values()), default=0.0)
    risks = [
        _risk_item("neutral_signal_sparsity", _avg(position_counts), "neutral average position count is sparse"),
        _risk_item("neutral_cash_weight_high", _avg(cash_weights), "neutral cash weight remains high after alpha veto"),
        _risk_item("over_veto_risk", _avg(veto_rates), "neutral veto rate may indicate over-filtering"),
        _risk_item("source_strategy_concentration", max_source, "source strategy weight share is concentrated"),
        _risk_item("same_window_repair_overfit_risk", 1.0, "repair passed on the fixed repair window set and requires OOS watch"),
    ]
    for item in risks:
        record_watch_risk(
            conn,
            flow_id=flow_id,
            strategy=str(flow.get("strategy") or "ensemble_core"),
            candidate=str(flow.get("candidate") or "hard_event_alpha_candidate"),
            **item,
        )
    return risks


def record_watch_risk(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    strategy: str,
    candidate: str,
    risk_code: str,
    risk_level: str,
    risk_description: str,
    metric_name: str,
    metric_value: float,
    threshold_value: float,
    required_monitoring: str,
    exit_condition: str,
    status: str = "open",
) -> JsonDict:
    now = _now_text()
    conn.execute(
        """
        INSERT INTO observation_watch_risk_register (
            flow_id, strategy, candidate, risk_code, risk_level, risk_description,
            metric_name, metric_value, threshold_value, required_monitoring,
            exit_condition, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(flow_id),
            str(strategy),
            str(candidate),
            str(risk_code),
            str(risk_level),
            str(risk_description),
            str(metric_name),
            float(metric_value),
            float(threshold_value),
            str(required_monitoring),
            str(exit_condition),
            str(status),
            now,
            now,
        ),
    )
    conn.commit()
    return {"risk_code": str(risk_code), "risk_level": str(risk_level), "metric_value": float(metric_value)}


def build_repair_flow_snapshot(conn: sqlite3.Connection, *, flow_id: str) -> JsonDict:
    apply_research_repair_flow_migration(conn)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM research_repair_iteration_flow WHERE flow_id = ?", (str(flow_id),)).fetchone()
    if row is None:
        return {}
    payload = dict(row)
    for key in (
        "forbidden_objectives",
        "predeclared_rules_json",
        "fixed_window_set_json",
        "benchmark_config_json",
        "unthrottled_summary_json",
        "throttled_summary_json",
        "regime_summary_json",
        "blocking_reasons_json",
        "warning_reasons_json",
        "next_allowed_actions_json",
        "prohibited_actions_json",
    ):
        payload[key] = _loads(payload.get(key))
    payload["artifacts"] = [dict(item) for item in conn.execute("SELECT * FROM research_artifact_registry WHERE flow_id = ? ORDER BY id", (str(flow_id),)).fetchall()]
    payload["watch_risks"] = [dict(item) for item in conn.execute("SELECT * FROM observation_watch_risk_register WHERE flow_id = ? ORDER BY id", (str(flow_id),)).fetchall()]
    payload["formal_candidate_allowed"] = False
    payload["production_candidate_allowed"] = False
    return payload


def export_repair_flow_evidence_manifest(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    output_dir: str,
) -> JsonDict:
    snapshot = build_repair_flow_snapshot(conn, flow_id=flow_id)
    if not snapshot:
        raise ValueError(f"repair_flow_not_found:{flow_id}")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    flow_path = output / f"research_repair_flow_snapshot_{flow_id}_{stamp}.json"
    risks_path = output / f"watch_risk_register_{flow_id}_{stamp}.json"
    manifest_path = output / f"repair_flow_evidence_manifest_{flow_id}_{stamp}.json"
    flow_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    risks_path.write_text(
        json.dumps({"flow_id": flow_id, "watch_risks": snapshot.get("watch_risks") or []}, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    manifest = {
        "flow_id": flow_id,
        "rule_hash": str(snapshot.get("rule_hash") or ""),
        "flow_snapshot_artifact": str(flow_path),
        "unthrottled_benchmark_artifact": str(snapshot.get("unthrottled_artifact_path") or ""),
        "throttled_benchmark_artifact": str(snapshot.get("throttled_artifact_path") or ""),
        "attribution_artifact": str(snapshot.get("attribution_artifact_path") or ""),
        "monitor_artifact": str(snapshot.get("monitor_artifact_path") or ""),
        "repair_review_artifact": str(snapshot.get("repair_review_artifact_path") or ""),
        "watch_risk_register_artifact": str(risks_path),
        "watch_risk_review_artifact": _latest_artifact_path(snapshot, "watch_risk_review"),
        "oos_monitoring_plan_artifact": _latest_artifact_path(snapshot, "oos_monitoring_plan"),
        "oos_monitoring_result_artifact": _latest_artifact_path(snapshot, "oos_monitoring_result"),
        "failure_attribution_comparison_artifact": _latest_artifact_path(snapshot, "failure_attribution_comparison"),
        "v7_go_no_go_review_artifact": _latest_artifact_path(snapshot, "v7_go_no_go_review"),
        "failed_research_candidate_archive_artifact": _latest_artifact_path(snapshot, "failed_research_candidate_archive"),
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return {
        "flow_id": flow_id,
        "flow_snapshot_artifact": str(flow_path),
        "watch_risk_register_artifact": str(risks_path),
        "watch_risk_review_artifact": manifest["watch_risk_review_artifact"],
        "oos_monitoring_plan_artifact": manifest["oos_monitoring_plan_artifact"],
        "oos_monitoring_result_artifact": manifest["oos_monitoring_result_artifact"],
        "failure_attribution_comparison_artifact": manifest["failure_attribution_comparison_artifact"],
        "v7_go_no_go_review_artifact": manifest["v7_go_no_go_review_artifact"],
        "failed_research_candidate_archive_artifact": manifest["failed_research_candidate_archive_artifact"],
        "repair_flow_evidence_manifest": str(manifest_path),
    }


def create_watch_risk_review_artifact(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    evidence_manifest_path: str,
    output_dir: str,
    operator_name: str = "",
) -> JsonDict:
    """Create an independent review of whether v5 improved by refusing trades."""
    snapshot = build_repair_flow_snapshot(conn, flow_id=flow_id)
    if not snapshot:
        raise ValueError(f"repair_flow_not_found:{flow_id}")
    manifest = _load_json(evidence_manifest_path)
    unthrottled = _load_json(str(manifest.get("unthrottled_benchmark_artifact") or snapshot.get("unthrottled_artifact_path") or ""))
    throttled = _load_json(str(manifest.get("throttled_benchmark_artifact") or snapshot.get("throttled_artifact_path") or ""))
    risks = list(snapshot.get("watch_risks") or [])
    neutral_stats = _neutral_trade_stats(unthrottled)
    block_risks = sorted(str(item.get("risk_code") or "") for item in risks if item.get("risk_level") == "block")
    warnings = sorted(str(item.get("risk_code") or "") for item in risks if item.get("risk_level") == "warning")
    blocking_reasons: list[str] = []
    if block_risks:
        blocking_reasons.append("block_level_watch_risk_open")
    if neutral_stats["avg_veto_rate"] > WATCH_RISK_THRESHOLDS["over_veto_risk"]["block"]:
        blocking_reasons.append("over_veto_risk_indicates_possible_refusal_to_trade")
    if neutral_stats["avg_position_count"] < WATCH_RISK_THRESHOLDS["neutral_signal_sparsity"]["warning"]:
        blocking_reasons.append("neutral_signal_sparsity_requires_oos_confirmation")
    if neutral_stats["avg_cash_weight"] > WATCH_RISK_THRESHOLDS["neutral_cash_weight_high"]["warning"]:
        blocking_reasons.append("neutral_cash_weight_high_requires_oos_confirmation")
    status = "watch_risk_review_blocked" if blocking_reasons else "watch_risk_review_cleared_for_oos_only"
    artifact = {
        "artifact_version": "research_repair_watch_risk_review.v1",
        "flow_id": flow_id,
        "candidate": str(snapshot.get("candidate") or ""),
        "rule_version": str(snapshot.get("rule_version") or ""),
        "rule_hash": str(snapshot.get("rule_hash") or ""),
        "evidence_manifest_path": str(evidence_manifest_path),
        "review_status": status,
        "current_repair_status": str(snapshot.get("current_status") or ""),
        "neutral_trade_stats": neutral_stats,
        "risk_register_summary": {
            "block_risks": block_risks,
            "warning_risks": warnings,
            "open_risk_count": len([item for item in risks if str(item.get("status") or "open") == "open"]),
        },
        "benchmark_summary": {
            "unthrottled": _benchmark_summary(unthrottled),
            "throttled": _benchmark_summary(throttled),
            "unthrottled_regime_split": (unthrottled.get("benchmark") or {}).get("regime_split") or {},
            "throttled_regime_split": (throttled.get("benchmark") or {}).get("regime_split") or {},
        },
        "blocking_reasons": sorted(set(blocking_reasons)),
        "required_next_actions": [
            "predeclare_oos_monitoring_plan",
            "run_oos_unthrottled_and_throttled_same_windows",
            "keep_formal_top_production_blocked",
        ],
        "formal_candidate_allowed": False,
        "production_candidate_allowed": False,
        "operator_name": str(operator_name or ""),
        "created_at": _now_text(),
    }
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_path = output / f"watch_risk_review_{flow_id}_{stamp}.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    attach_repair_artifact(
        conn,
        flow_id=flow_id,
        artifact_type="watch_risk_review",
        artifact_path=str(artifact_path),
        summary={"review_status": status, "blocking_reasons": artifact["blocking_reasons"]},
    )
    return {"watch_risk_review_artifact": str(artifact_path), **artifact}


def create_oos_monitoring_plan_artifact(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    evidence_manifest_path: str,
    oos_windows: Sequence[str],
    output_dir: str,
    operator_name: str = "",
) -> JsonDict:
    snapshot = build_repair_flow_snapshot(conn, flow_id=flow_id)
    if not snapshot:
        raise ValueError(f"repair_flow_not_found:{flow_id}")
    windows = [str(item).strip() for item in oos_windows if str(item).strip()]
    if not windows:
        raise ValueError("missing_oos_windows")
    fixed_windows = [str(item) for item in snapshot.get("fixed_window_set_json") or []]
    overlap = sorted(set(windows).intersection(fixed_windows))
    if overlap:
        raise ValueError(f"oos_windows_overlap_repair_windows:{','.join(overlap)}")
    artifact = {
        "artifact_version": "research_repair_oos_monitoring_plan.v1",
        "flow_id": flow_id,
        "candidate": str(snapshot.get("candidate") or ""),
        "rule_version": str(snapshot.get("rule_version") or ""),
        "rule_hash": str(snapshot.get("rule_hash") or ""),
        "evidence_manifest_path": str(evidence_manifest_path),
        "plan_status": "predeclared_oos_monitoring_only",
        "oos_windows": windows,
        "paired_run_requirement": {
            "unthrottled_required": True,
            "throttled_required": True,
            "same_window_set_required": True,
            "same_rule_hash_required": True,
            "regime_split_required": ["risk_off", "neutral", "risk_on"],
        },
        "pass_conditions": {
            "unthrottled_all_regimes_stand": True,
            "neutral_unthrottled_after_cost_excess_return_gt": 0.0,
            "neutral_hit_rate_gte": 0.5,
            "unthrottled_turnover_lte": 0.75,
            "coverage_not_too_low": True,
            "risk_off_repair_not_regressed": True,
            "block_level_watch_risk_allowed": False,
        },
        "failure_actions": [
            "remain_blocked",
            "do_not_enter_observation_watch",
            "return_to_v6_predeclared_alpha_repair",
        ],
        "prohibited_actions": list(PROHIBITED_REPAIR_ACTIONS),
        "formal_candidate_allowed": False,
        "production_candidate_allowed": False,
        "operator_name": str(operator_name or ""),
        "created_at": _now_text(),
    }
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_path = output / f"oos_monitoring_plan_{flow_id}_{stamp}.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    attach_repair_artifact(
        conn,
        flow_id=flow_id,
        artifact_type="oos_monitoring_plan",
        artifact_path=str(artifact_path),
        summary={"plan_status": artifact["plan_status"], "oos_window_count": len(windows)},
    )
    return {"oos_monitoring_plan_artifact": str(artifact_path), **artifact}


def create_oos_monitoring_result_artifact(
    conn: sqlite3.Connection,
    *,
    flow_id: str,
    oos_plan_path: str,
    unthrottled_artifact_path: str,
    throttled_artifact_path: str,
    output_dir: str,
    operator_name: str = "",
) -> JsonDict:
    snapshot = build_repair_flow_snapshot(conn, flow_id=flow_id)
    if not snapshot:
        raise ValueError(f"repair_flow_not_found:{flow_id}")
    plan = _load_json(oos_plan_path)
    unthrottled = _load_json(unthrottled_artifact_path)
    throttled = _load_json(throttled_artifact_path)
    required_windows = [str(item) for item in plan.get("oos_windows") or []]
    required_regimes = [str(item) for item in ((plan.get("paired_run_requirement") or {}).get("regime_split_required") or [])]
    rule_hash = str(snapshot.get("rule_hash") or "")
    failures = _validate_oos_pair(
        rule_hash=rule_hash,
        required_windows=required_windows,
        required_regimes=required_regimes,
        unthrottled=unthrottled,
        throttled=throttled,
    )
    oos_risks = _oos_watch_risk_summary(unthrottled)
    failures.extend(item["failure"] for item in oos_risks if item.get("risk_level") == "block")
    failures.extend(_oos_pass_condition_failures(plan=plan, unthrottled=unthrottled))
    status = "oos_monitoring_passed_discussion_only" if not failures else "oos_monitoring_failed_blocked"
    artifact = {
        "artifact_version": "research_repair_oos_monitoring_result.v1",
        "flow_id": flow_id,
        "candidate": str(snapshot.get("candidate") or ""),
        "rule_version": str(snapshot.get("rule_version") or ""),
        "rule_hash": rule_hash,
        "oos_plan_artifact": str(oos_plan_path),
        "unthrottled_benchmark_artifact": str(unthrottled_artifact_path),
        "throttled_benchmark_artifact": str(throttled_artifact_path),
        "result_status": status,
        "unthrottled_summary": _benchmark_summary(unthrottled),
        "throttled_summary": _benchmark_summary(throttled),
        "unthrottled_regime_split": (unthrottled.get("benchmark") or {}).get("regime_split") or {},
        "throttled_regime_split": (throttled.get("benchmark") or {}).get("regime_split") or {},
        "neutral_trade_stats": _neutral_trade_stats(unthrottled),
        "watch_risk_summary": oos_risks,
        "blocking_reasons": sorted(set(str(item) for item in failures if str(item or ""))),
        "allowed_next_status": "observation_watch_discussion_allowed" if status == "oos_monitoring_passed_discussion_only" else "research_blocked",
        "formal_candidate_allowed": False,
        "production_candidate_allowed": False,
        "operator_name": str(operator_name or ""),
        "created_at": _now_text(),
    }
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    artifact_path = output / f"oos_monitoring_result_{flow_id}_{stamp}.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    attach_repair_artifact(
        conn,
        flow_id=flow_id,
        artifact_type="oos_monitoring_result",
        artifact_path=str(artifact_path),
        summary={"result_status": status, "blocking_reasons": artifact["blocking_reasons"]},
    )
    return {"oos_monitoring_result_artifact": str(artifact_path), **artifact}


def _attach_many(conn: sqlite3.Connection, *, flow_id: str, artifacts: Iterable[tuple[str, str]]) -> None:
    for artifact_type, path in artifacts:
        if path:
            attach_repair_artifact(conn, flow_id=flow_id, artifact_type=artifact_type, artifact_path=path)


def _latest_artifact_path(snapshot: JsonDict, artifact_type: str) -> str:
    matches = [item for item in snapshot.get("artifacts") or [] if str(item.get("artifact_type") or "") == artifact_type]
    if not matches:
        return ""
    return str(matches[-1].get("artifact_path") or "")


def _validate_oos_pair(
    *,
    rule_hash: str,
    required_windows: Sequence[str],
    required_regimes: Sequence[str],
    unthrottled: JsonDict,
    throttled: JsonDict,
) -> list[str]:
    failures: list[str] = []
    if not unthrottled:
        failures.append("missing_oos_unthrottled_benchmark")
    if not throttled:
        failures.append("missing_oos_throttled_benchmark")
    if not unthrottled or not throttled:
        return failures
    u_windows = _window_set(unthrottled)
    t_windows = _window_set(throttled)
    if u_windows != t_windows:
        failures.append("oos_paired_benchmark_window_set_mismatch")
    if required_windows and u_windows != list(required_windows):
        failures.append("oos_windows_do_not_match_predeclared_plan")
    u_hash = str((unthrottled.get("rule_freeze") or {}).get("rule_hash") or "")
    t_hash = str((throttled.get("rule_freeze") or {}).get("rule_hash") or "")
    if not u_hash or u_hash != t_hash or (rule_hash and u_hash != rule_hash):
        failures.append("oos_paired_benchmark_rule_hash_mismatch")
    regimes = (unthrottled.get("benchmark") or {}).get("regime_split") or {}
    missing = [item for item in required_regimes if item not in regimes]
    if missing:
        failures.append(f"oos_missing_required_regime_split:{','.join(missing)}")
    return failures


def _oos_pass_condition_failures(*, plan: JsonDict, unthrottled: JsonDict) -> list[str]:
    failures: list[str] = []
    benchmark = unthrottled.get("benchmark") if isinstance(unthrottled.get("benchmark"), dict) else {}
    regimes = benchmark.get("regime_split") if isinstance(benchmark.get("regime_split"), dict) else {}
    conditions = plan.get("pass_conditions") if isinstance(plan.get("pass_conditions"), dict) else {}
    if benchmark.get("passed") is not True:
        failures.append("oos_unthrottled_benchmark_not_passed")
    turnover_limit = conditions.get("unthrottled_turnover_lte")
    if turnover_limit is not None and float(benchmark.get("turnover", 999.0) or 999.0) > float(turnover_limit):
        failures.append("oos_unthrottled_turnover_above_limit")
    neutral = regimes.get("neutral") if isinstance(regimes.get("neutral"), dict) else {}
    neutral_alpha_floor = conditions.get("neutral_unthrottled_after_cost_excess_return_gt")
    if neutral_alpha_floor is not None and float(neutral.get("avg_after_cost_excess_return", -999.0) or -999.0) <= float(neutral_alpha_floor):
        failures.append("oos_neutral_unthrottled_alpha_not_positive")
    neutral_hit_floor = conditions.get("neutral_hit_rate_gte")
    if neutral_hit_floor is not None and float(neutral.get("hit_rate", 0.0) or 0.0) < float(neutral_hit_floor):
        failures.append("oos_neutral_hit_rate_below_floor")
    if conditions.get("unthrottled_all_regimes_stand") is True:
        for regime, row in regimes.items():
            if float((row or {}).get("avg_after_cost_excess_return", -999.0) or -999.0) <= 0.0:
                failures.append(f"oos_regime_alpha_not_positive:{regime}")
            if float((row or {}).get("hit_rate", 0.0) or 0.0) < 0.5:
                failures.append(f"oos_regime_hit_rate_below_half:{regime}")
    if conditions.get("risk_off_repair_not_regressed") is True:
        risk_off = regimes.get("risk_off") if isinstance(regimes.get("risk_off"), dict) else {}
        if float(risk_off.get("avg_after_cost_excess_return", -999.0) or -999.0) <= 0.0:
            failures.append("oos_risk_off_repair_regressed")
    if conditions.get("coverage_not_too_low") is True:
        stats = _neutral_trade_stats(unthrottled)
        if float(stats.get("avg_position_count", 0.0) or 0.0) < WATCH_RISK_THRESHOLDS["neutral_signal_sparsity"]["warning"]:
            failures.append("oos_neutral_coverage_too_low")
    return failures


def _oos_watch_risk_summary(payload: JsonDict) -> list[JsonDict]:
    stats = _neutral_trade_stats(payload)
    risk = (payload.get("benchmark") or {}).get("risk_contribution") or {}
    sources = risk.get("source_strategy_weight_share") if isinstance(risk.get("source_strategy_weight_share"), dict) else {}
    max_source = max((float(v or 0.0) for v in sources.values()), default=0.0)
    items = [
        _risk_item("neutral_signal_sparsity", float(stats["avg_position_count"]), "OOS neutral average position count is sparse"),
        _risk_item("neutral_cash_weight_high", float(stats["avg_cash_weight"]), "OOS neutral cash weight remains high"),
        _risk_item("over_veto_risk", float(stats["avg_veto_rate"]), "OOS neutral veto rate may indicate refusal to trade"),
        _risk_item("source_strategy_concentration", max_source, "OOS source strategy weight share is concentrated"),
    ]
    for item in items:
        item["failure"] = f"oos_block_level_watch_risk:{item['risk_code']}"
    return items


def _neutral_trade_stats(payload: JsonDict) -> JsonDict:
    windows = [row for row in payload.get("windows") or [] if isinstance(row, dict)]
    neutral = [row for row in windows if str(row.get("market_regime_label") or "") == "neutral"]
    positions: list[int] = []
    cash: list[float] = []
    veto_rates: list[float] = []
    for row in neutral:
        port = row.get("shadow_portfolio") if isinstance(row.get("shadow_portfolio"), dict) else {}
        kept = len(port.get("shadow_weights") or [])
        vetoed = len(port.get("neutral_vetoed_signals") or [])
        positions.append(kept)
        cash.append(float(port.get("cash_weight", 0.0) or 0.0))
        total = kept + vetoed
        if total > 0:
            veto_rates.append(vetoed / float(total))
    return {
        "neutral_window_count": len(neutral),
        "avg_position_count": _avg(positions),
        "min_position_count": min(positions) if positions else 0,
        "avg_cash_weight": _avg(cash),
        "max_cash_weight": max(cash) if cash else 0.0,
        "avg_veto_rate": _avg(veto_rates),
        "max_veto_rate": max(veto_rates) if veto_rates else 0.0,
    }


def _benchmark_summary(payload: JsonDict) -> JsonDict:
    benchmark = payload.get("benchmark") if isinstance(payload.get("benchmark"), dict) else {}
    return {
        key: benchmark.get(key)
        for key in ("passed", "after_cost_excess_return", "hit_rate", "max_drawdown", "turnover", "capacity_utilization", "industry_concentration")
    }


def _risk_item(code: str, value: float, description: str) -> JsonDict:
    spec = WATCH_RISK_THRESHOLDS.get(code, {"warning": 0.0, "block": 0.0, "direction": "above"})
    level = _risk_level(float(value or 0.0), spec)
    return {
        "risk_code": code,
        "risk_level": level,
        "risk_description": description,
        "metric_name": code,
        "metric_value": round(float(value or 0.0), 6),
        "threshold_value": float(spec.get("warning", 0.0) or 0.0),
        "required_monitoring": f"track_{code}_during_observation_watch",
        "exit_condition": f"{code}_breaches_block_threshold_or_oos_regime_fails",
    }


def _risk_level(value: float, spec: JsonDict) -> str:
    direction = str(spec.get("direction") or "above")
    warning = float(spec.get("warning", 0.0) or 0.0)
    block = float(spec.get("block", 0.0) or 0.0)
    if direction == "below":
        if value < block:
            return "block"
        if value < warning:
            return "warning"
        return "info"
    if value > block:
        return "block"
    if value > warning:
        return "warning"
    return "info"


def _window_set(payload: JsonDict) -> list[str]:
    return [str(row.get("as_of_date") or "") for row in payload.get("windows") or [] if isinstance(row, dict)]


def _load_json(path: str) -> JsonDict:
    if not str(path or "").strip():
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    payload = json.loads(p.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _file_hash(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False, sort_keys=True)


def _loads(value: Any) -> Any:
    try:
        return json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return value


def _avg(values: Sequence[float] | Sequence[int]) -> float:
    return round(sum(float(v or 0.0) for v in values) / float(len(values)), 6) if values else 0.0


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from src.primary_result_market_data_readiness import build_primary_result_market_data_readiness
from src.primary_result_observation_closure_preflight import build_primary_result_observation_closure_preflight
from src.primary_result_performance_ledger import PrimaryResultPerformanceLedger
from src.primary_result_price_history_artifact import build_primary_result_price_history_artifact
from src.primary_result_price_history_sqlite_ingest import import_primary_result_price_history_from_sqlite
from src.utils.project_paths import resolve_project_path


PRIMARY_RESULT_DAILY_CLOSURE_ORCHESTRATOR_VERSION = "primary_result_daily_closure_orchestrator.v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _stage(name: str, exit_code: int, payload: dict[str, object]) -> dict[str, object]:
    return {"name": name, "exit_code": exit_code, "status": payload.get("status"), "payload": payload}


def _stage_failed(stage: dict[str, object]) -> bool:
    return int(stage["exit_code"]) != 0


def _load_observation_metrics_runner():
    from scripts.run_primary_result_observation_metrics import run_primary_result_observation_metrics

    return run_primary_result_observation_metrics


def _load_terminal_recorder():
    from scripts.record_primary_result_terminal import record_primary_result_terminal

    return record_primary_result_terminal


def _exp_dir_default_path(exp_dir: Path, value: str | Path, default_value: str, filename: str) -> Path:
    if str(value) == default_value:
        return exp_dir / filename
    return resolve_project_path(value)


def run_primary_result_daily_closure_orchestrator(
    *,
    sqlite_db_path: str | Path,
    sqlite_table: str = "daily_trading_data",
    exp_dir: str | Path = "data/experiments",
    candidate_index: int = 0,
    ts_code: str,
    benchmark_ts_code: str,
    window_start: str,
    window_end: str,
    price_history_csv: str | Path = "data/experiments/primary_result_price_history_latest.csv",
    price_history_manifest_json: str | Path = "data/experiments/primary_result_price_history_manifest_latest.json",
    sqlite_ingest_manifest_json: str | Path = "data/experiments/primary_result_price_history_sqlite_ingest_latest.json",
    market_data_readiness_json: str | Path = "data/experiments/primary_result_market_data_readiness_latest.json",
    closure_preflight_json: str | Path = "data/experiments/primary_result_observation_closure_preflight_latest.json",
    metrics_output_json: str | Path = "data/experiments/primary_result_observation_metrics_latest.json",
    observation_output_json: str | Path = "data/experiments/primary_result_observation_latest.json",
    terminal_output_json: str | Path = "data/experiments/primary_result_terminal_latest.json",
    performance_ledger_jsonl: str | Path = "artifacts/primary_result_performance/ledger.jsonl",
    performance_summary_json: str | Path = "artifacts/primary_result_performance/summary.json",
    min_success_return: float = 0.0,
    max_drawdown_floor: float = -0.08,
    output_path: str | Path | None = None,
) -> tuple[int, dict[str, object]]:
    stages: list[dict[str, object]] = []
    resolved_exp_dir = resolve_project_path(exp_dir)
    resolved_price_history_csv = resolve_project_path(price_history_csv)
    resolved_price_history_manifest = resolve_project_path(price_history_manifest_json)
    resolved_metrics_output = _exp_dir_default_path(
        resolved_exp_dir,
        metrics_output_json,
        "data/experiments/primary_result_observation_metrics_latest.json",
        "primary_result_observation_metrics_latest.json",
    )
    resolved_observation_output = _exp_dir_default_path(
        resolved_exp_dir,
        observation_output_json,
        "data/experiments/primary_result_observation_latest.json",
        "primary_result_observation_latest.json",
    )
    resolved_terminal_output = _exp_dir_default_path(
        resolved_exp_dir,
        terminal_output_json,
        "data/experiments/primary_result_terminal_latest.json",
        "primary_result_terminal_latest.json",
    )

    readiness_code, readiness = build_primary_result_market_data_readiness(
        sqlite_db_path=sqlite_db_path,
        sqlite_table=sqlite_table,
        ts_code=ts_code,
        benchmark_ts_code=benchmark_ts_code,
        window_start=window_start,
        window_end=window_end,
        output_path=market_data_readiness_json,
    )
    stages.append(_stage("market_data_readiness", readiness_code, readiness))
    if readiness_code != 0:
        payload = _build_payload("blocked", stages, terminal_outcome=None)
        if output_path:
            _write_json(resolve_project_path(output_path), payload)
        return 1, payload

    ingest_code, ingest = import_primary_result_price_history_from_sqlite(
        sqlite_db_path=sqlite_db_path,
        sqlite_table=sqlite_table,
        output_csv_path=resolved_price_history_csv,
        manifest_output_path=sqlite_ingest_manifest_json,
        ts_code=ts_code,
        benchmark_ts_code=benchmark_ts_code,
        window_start=window_start,
        window_end=window_end,
    )
    stages.append(_stage("sqlite_price_history_ingest", ingest_code, ingest))
    if ingest_code != 0:
        payload = _build_payload("blocked", stages, terminal_outcome=None)
        if output_path:
            _write_json(resolve_project_path(output_path), payload)
        return 1, payload

    manifest_code, manifest = build_primary_result_price_history_artifact(
        price_history_path=resolved_price_history_csv,
        ts_code=ts_code,
        benchmark_ts_code=benchmark_ts_code,
        window_start=window_start,
        window_end=window_end,
        output_path=resolved_price_history_manifest,
    )
    stages.append(_stage("price_history_manifest", manifest_code, manifest))
    if manifest_code != 0:
        payload = _build_payload("blocked", stages, terminal_outcome=None)
        if output_path:
            _write_json(resolve_project_path(output_path), payload)
        return 1, payload

    closure_code, closure = build_primary_result_observation_closure_preflight(
        exp_dir=exp_dir,
        candidate_index=candidate_index,
        price_history_path=resolved_price_history_csv,
        price_history_manifest_path=resolved_price_history_manifest,
        benchmark_ts_code=benchmark_ts_code,
        window_start=window_start,
        window_end=window_end,
        min_success_return=min_success_return,
        max_drawdown_floor=max_drawdown_floor,
        output_path=closure_preflight_json,
    )
    stages.append(_stage("observation_closure_preflight", closure_code, closure))
    if closure.get("closure_outcome") not in {"completed", "failed"} or closure.get("metrics") is None:
        payload = _build_payload("blocked", stages, terminal_outcome=None)
        if output_path:
            _write_json(resolve_project_path(output_path), payload)
        return 1, payload

    metrics_runner = _load_observation_metrics_runner()
    metrics_code, metrics_payload = metrics_runner(
        exp_dir=exp_dir,
        candidate_index=candidate_index,
        price_history_path=resolved_price_history_csv,
        benchmark_ts_code=benchmark_ts_code,
        window_start=window_start,
        window_end=window_end,
        reason="daily closure orchestrator completed local observation window",
        min_success_return=min_success_return,
        max_drawdown_floor=max_drawdown_floor,
        metrics_output_path=resolved_metrics_output,
        observation_output_path=resolved_observation_output,
    )
    stages.append(_stage("observation_metrics", metrics_code, metrics_payload))

    terminal_outcome = "success" if metrics_payload.get("status") == "passed" else "failed"
    terminal_reason = (
        "daily closure orchestrator recorded successful observation"
        if terminal_outcome == "success"
        else "daily closure orchestrator recorded failed observation"
    )
    terminal_recorder = _load_terminal_recorder()
    terminal_code, terminal = terminal_recorder(
        exp_dir=exp_dir,
        candidate_index=candidate_index,
        terminal_outcome=terminal_outcome,
        reason=terminal_reason,
        output_path=resolved_terminal_output,
    )
    stages.append(_stage("terminal_outcome", terminal_code, {"status": "recorded", **terminal}))
    if terminal_code != 0:
        payload = _build_payload("failed", stages, terminal_outcome=terminal_outcome)
        if output_path:
            _write_json(resolve_project_path(output_path), payload)
        return 1, payload

    ledger = PrimaryResultPerformanceLedger(
        ledger_path=performance_ledger_jsonl,
        summary_path=performance_summary_json,
    )
    try:
        entry = ledger.append_observation(observation_path=resolved_observation_output)
        ledger_payload = {
            "status": "registered",
            "entry_id": entry["entry_id"],
            "result_id": entry["result_id"],
            "ts_code": entry["ts_code"],
            "outcome": entry["outcome"],
            "ledger_path": str(ledger.ledger_path),
            "summary_path": str(ledger.summary_path),
        }
        ledger_code = 0
    except Exception as exc:
        ledger_payload = {"status": "failed", "error": str(exc)}
        ledger_code = 1
    stages.append(_stage("performance_ledger", ledger_code, ledger_payload))

    status = "closed_success" if terminal_outcome == "success" and ledger_code == 0 else "closed_failed"
    if ledger_code != 0:
        status = "failed"
    payload = _build_payload(status, stages, terminal_outcome=terminal_outcome)
    if output_path:
        _write_json(resolve_project_path(output_path), payload)
    return (0 if status == "closed_success" else 1), payload


def _build_payload(status: str, stages: list[dict[str, object]], *, terminal_outcome: str | None) -> dict[str, object]:
    blocking = []
    for stage in stages:
        payload = stage.get("payload")
        if isinstance(payload, dict):
            blocking.extend(str(reason) for reason in payload.get("blocking_reasons", []) or [])
            if _stage_failed(stage) and payload.get("error"):
                blocking.append(str(payload["error"]))
    return {
        "orchestrator_version": PRIMARY_RESULT_DAILY_CLOSURE_ORCHESTRATOR_VERSION,
        "generated_at": _utc_now_iso(),
        "status": status,
        "terminal_outcome": terminal_outcome,
        "stages": stages,
        "blocking_reasons": blocking,
        "production_boundary": (
            "daily closure orchestrator only advances existing local lifecycle evidence when every prior gate allows it; "
            "it does not fetch external market data, trade, promote baselines, deploy, or bypass blocking checks"
        ),
    }

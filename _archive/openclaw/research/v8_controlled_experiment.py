from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from openclaw.paths import db_path as default_db_path
from openclaw.research.backtest_param_sweep import SweepConfig, run_param_sweep
from openclaw.services.rejected_backtest_artifact_ledger_service import append_rejected_backtest_artifact
from tools.strategy_optimization_stage_audit import run_stage_audit


JsonDict = Dict[str, Any]


@dataclass(frozen=True)
class V8ControlledExperimentConfig:
    module_path: Path = Path("v49_app.py")
    output_dir: Path = Path("logs/openclaw/v8_controlled_experiments")
    date_from: str = "2025-11-01"
    date_to: str = "2026-05-03"
    train_window_days: int = 90
    test_window_days: int = 30
    step_days: int = 30
    score_thresholds: Sequence[int] = (50, 55, 60)
    sample_sizes: Sequence[int] = (30,)
    holding_days: Sequence[int] = (6,)
    max_stop_loss_pcts: Sequence[float] = (0.06, 0.08, 0.10)
    max_take_profit_pcts: Sequence[Optional[float]] = (None, 0.10, 0.14)
    db_path: str = ""
    rejected_ledger: str = "logs/openclaw/rejected_backtest_artifacts.jsonl"
    per_run_timeout_sec: Optional[int] = 180
    max_runs: Optional[int] = None
    operator_name: str = "v8_controlled_experiment"


def run_v8_controlled_experiment(cfg: V8ControlledExperimentConfig) -> JsonDict:
    """Run the fixed v8 experiment protocol and immediately audit the resulting evidence chain."""
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    db = str(cfg.db_path or default_db_path())
    sweep = run_param_sweep(
        SweepConfig(
            strategy="v8",
            module_path=cfg.module_path,
            output_dir=cfg.output_dir,
            date_from=cfg.date_from,
            date_to=cfg.date_to,
            mode="rolling",
            train_window_days=cfg.train_window_days,
            test_window_days=cfg.test_window_days,
            step_days=cfg.step_days,
            score_thresholds=cfg.score_thresholds,
            sample_sizes=cfg.sample_sizes,
            holding_days=cfg.holding_days,
            max_stop_loss_pcts=cfg.max_stop_loss_pcts,
            max_take_profit_pcts=cfg.max_take_profit_pcts,
            db_path=db,
            max_runs=cfg.max_runs,
            per_run_timeout_sec=cfg.per_run_timeout_sec,
        )
    )

    artifact_path = str((sweep.get("artifacts") or {}).get("json") or "")
    rejected_entry = _record_rejection_if_needed(
        artifact_path=artifact_path,
        sweep=sweep,
        ledger_path=cfg.rejected_ledger,
        operator_name=cfg.operator_name,
    )
    stage_audit = run_stage_audit(
        db_path=db,
        trade_date="",
        rejected_artifacts_path=cfg.rejected_ledger,
        output_dir=str(cfg.output_dir),
    )
    payload: JsonDict = {
        "experiment_version": "v8_controlled_experiment.v1",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "strategy": "v8",
        "status": _classify_status(sweep=sweep, rejected_entry=rejected_entry),
        "fixed_protocol": {
            "date_from": cfg.date_from,
            "date_to": cfg.date_to,
            "mode": "rolling",
            "train_window_days": cfg.train_window_days,
            "test_window_days": cfg.test_window_days,
            "step_days": cfg.step_days,
            "sample_sizes": list(cfg.sample_sizes),
            "holding_days": list(cfg.holding_days),
            "cost_slippage_policy": "fixed_backtest_engine_defaults",
            "tradeability_and_volume_constraints": "required_by_strategy_backtest_diagnostics",
        },
        "controlled_variables": {
            "score_thresholds": list(cfg.score_thresholds),
            "max_stop_loss_pcts": list(cfg.max_stop_loss_pcts),
            "max_take_profit_pcts": list(cfg.max_take_profit_pcts),
        },
        "sweep_artifacts": sweep.get("artifacts") or {},
        "backtest_credibility": sweep.get("backtest_credibility") or {},
        "strategy_backtest_diagnostics": sweep.get("strategy_backtest_diagnostics") or {},
        "rejected_ledger": cfg.rejected_ledger,
        "rejected_entry": rejected_entry,
        "stage_audit_artifacts": stage_audit.get("artifacts") or {},
        "stage_audit_passed": bool(stage_audit.get("passed") is True),
    }
    artifacts = _write_experiment_artifacts(cfg.output_dir, payload)
    payload["artifacts"] = artifacts
    _write_json(Path(artifacts["json"]), payload)
    return payload


def _record_rejection_if_needed(
    *,
    artifact_path: str,
    sweep: JsonDict,
    ledger_path: str,
    operator_name: str,
) -> Optional[JsonDict]:
    reasons = _rejection_reasons(sweep=sweep, artifact_path=artifact_path)
    if not reasons:
        return None
    return append_rejected_backtest_artifact(
        ledger_path,
        artifact_path=artifact_path,
        strategy="v8",
        reason=",".join(reasons),
        reused_as_runtime_default=False,
        source_run_id=str(sweep.get("run_id") or ""),
        operator_name=operator_name,
        note="auto_recorded_by_v8_controlled_experiment",
    )


def _rejection_reasons(*, sweep: JsonDict, artifact_path: str) -> list[str]:
    diagnostics = sweep.get("strategy_backtest_diagnostics") if isinstance(sweep.get("strategy_backtest_diagnostics"), dict) else {}
    credibility = sweep.get("backtest_credibility") if isinstance(sweep.get("backtest_credibility"), dict) else {}
    reasons: list[str] = []
    if str(sweep.get("status")) != "success":
        reasons.append("sweep_status_failed")
    if diagnostics.get("eligible_for_formal_ranking") is not True:
        reasons.append("eligible_for_formal_ranking_false")
    if diagnostics.get("credible_evidence_present") is not True:
        reasons.append("credible_evidence_false")
    if diagnostics.get("quality_floor_passed") is not True:
        reasons.append("quality_floor_failed")
    if not credibility:
        reasons.append("missing_backtest_credibility")
    if not _artifact_contains(artifact_path, "v8_suppression_diagnostics"):
        reasons.append("missing_v8_suppression_diagnostics")
    return reasons


def _classify_status(*, sweep: JsonDict, rejected_entry: Optional[JsonDict]) -> str:
    if rejected_entry is not None:
        return "rejected"
    diagnostics = sweep.get("strategy_backtest_diagnostics") if isinstance(sweep.get("strategy_backtest_diagnostics"), dict) else {}
    if diagnostics.get("eligible_for_formal_ranking") is True:
        return "candidate_discussion_only"
    return "observation"


def _artifact_contains(path: str, needle: str) -> bool:
    if not path:
        return False
    p = Path(path)
    if not p.exists():
        return False
    return needle in p.read_text(encoding="utf-8")


def _write_experiment_artifacts(output_dir: Path, payload: JsonDict) -> JsonDict:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"v8_controlled_experiment_{ts}.json"
    md_path = output_dir / f"v8_controlled_experiment_{ts}.md"
    _write_json(json_path, payload)
    lines = [
        "# V8 Controlled Experiment",
        "",
        f"- status: `{payload.get('status')}`",
        f"- sweep_json: `{(payload.get('sweep_artifacts') or {}).get('json')}`",
        f"- rejected_ledger: `{payload.get('rejected_ledger')}`",
        f"- stage_audit_json: `{(payload.get('stage_audit_artifacts') or {}).get('json')}`",
        f"- stage_audit_passed: `{payload.get('stage_audit_passed')}`",
        "",
        "## Boundary",
        "",
        "- candidate discussion only; not production maturity evidence",
        "- do not lower thresholds to manufacture signal density",
        "- only factor distribution and risk-control variables are swept",
    ]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json": str(json_path), "markdown": str(md_path)}


def _write_json(path: Path, payload: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

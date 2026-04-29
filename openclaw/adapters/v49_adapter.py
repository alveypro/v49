"""Adapter layer between OpenClaw workflows and local v49 strategy system.

This file provides a stable, testable contract for scan/backtest/report orchestration.
It is intentionally independent from Streamlit UI code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
import csv
import importlib.util
import json
import uuid


JsonDict = Dict[str, Any]
Handler = Callable[..., JsonDict]
DEFAULT_MODULE_PATH = Path(__file__).resolve().parents[2] / "v49_app.py"


@dataclass
class AdapterResult:
    ok: bool
    payload: JsonDict
    error: Optional[str] = None


@dataclass
class V49Adapter:
    """Thin adapter for orchestrating strategy actions.

    Strategy handlers can be registered explicitly, which is the safest option for
    integrating the existing v49 codebase that is currently UI-centric.
    """

    module_path: Path = DEFAULT_MODULE_PATH
    scan_handlers: Dict[str, Handler] = field(default_factory=dict)
    backtest_handlers: Dict[str, Handler] = field(default_factory=dict)

    def register_scan_handler(self, strategy: str, handler: Handler) -> None:
        self.scan_handlers[strategy] = handler

    def register_backtest_handler(self, strategy: str, handler: Handler) -> None:
        self.backtest_handlers[strategy] = handler

    def load_v49_module(self) -> AdapterResult:
        if not self.module_path.exists():
            return AdapterResult(ok=False, payload={}, error=f"module not found: {self.module_path}")

        spec = importlib.util.spec_from_file_location("v49_main", self.module_path)
        if spec is None or spec.loader is None:
            return AdapterResult(ok=False, payload={}, error="failed to create import spec")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return AdapterResult(ok=True, payload={"module": module})

    def run_scan(self, strategy: str, params: Optional[JsonDict] = None) -> JsonDict:
        params = params or {}
        run_id = self._new_run_id("scan", strategy)

        handler = self.scan_handlers.get(strategy)
        if handler is None:
            return {
                "run_id": run_id,
                "status": "failed",
                "error": f"scan handler not registered for strategy={strategy}",
                "hints": [
                    "register handler with register_scan_handler",
                    "or expose scan callable from v49 core module",
                ],
            }

        try:
            output = handler(params)
            return {
                "run_id": run_id,
                "status": "success",
                "strategy": strategy,
                "result": output,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "run_id": run_id,
                "status": "failed",
                "strategy": strategy,
                "error": str(exc),
            }

    def run_backtest(
        self,
        strategy: str,
        date_from: str,
        date_to: str,
        params: Optional[JsonDict] = None,
    ) -> JsonDict:
        params = params or {}
        run_id = self._new_run_id("backtest", strategy)

        handler = self.backtest_handlers.get(strategy)
        if handler is None:
            return {
                "run_id": run_id,
                "status": "failed",
                "error": f"backtest handler not registered for strategy={strategy}",
                "hints": ["register handler with register_backtest_handler"],
            }

        payload = {"date_from": date_from, "date_to": date_to, **params}
        try:
            output = handler(payload)
            return {
                "run_id": run_id,
                "status": "success",
                "strategy": strategy,
                "result": output,
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "run_id": run_id,
                "status": "failed",
                "strategy": strategy,
                "error": str(exc),
            }

    def merge_signals(self, inputs: Iterable[JsonDict], weights: Optional[Dict[str, float]] = None) -> JsonDict:
        weights = weights or {}
        merged: Dict[str, JsonDict] = {}

        for row in inputs:
            ts_code = str(row.get("ts_code", "")).strip()
            strategy = str(row.get("strategy", "")).strip()
            score = float(row.get("score", 0.0))
            if not ts_code or not strategy:
                continue

            weight = float(weights.get(strategy, 1.0))
            weighted_score = score * weight

            item = merged.setdefault(
                ts_code,
                {
                    "ts_code": ts_code,
                    "weighted_score": 0.0,
                    "raw_scores": {},
                    "strategies": [],
                    "reasons": [],
                },
            )
            item["weighted_score"] += weighted_score
            item["raw_scores"][strategy] = score
            if strategy not in item["strategies"]:
                item["strategies"].append(strategy)

        ranked = sorted(merged.values(), key=lambda x: x["weighted_score"], reverse=True)
        for item in ranked:
            if len(item["strategies"]) >= 3:
                item["reasons"].append("consensus_strong")
            elif len(item["strategies"]) == 1:
                item["reasons"].append("high_score_low_consensus")

        return {
            "ranked_list": ranked,
            "conflicts": [x for x in ranked if len(x["strategies"]) == 1],
            "reason_codes": ["consensus_strong", "high_score_low_consensus"],
        }

    def risk_check(self, stats: JsonDict, thresholds: JsonDict) -> JsonDict:
        triggered: List[str] = []

        win_rate = float(stats.get("win_rate", 0.0))
        max_dd = float(stats.get("max_drawdown", 0.0))
        signal_density = float(stats.get("signal_density", 0.0))

        if win_rate < float(thresholds.get("win_rate_min", 0.0)):
            triggered.append("win_rate_breach")
        if max_dd > float(thresholds.get("max_drawdown_max", 1.0)):
            triggered.append("drawdown_breach")
        if signal_density < float(thresholds.get("signal_density_min", 0.0)):
            triggered.append("signal_density_collapse")

        if "drawdown_breach" in triggered:
            level = "red"
        elif triggered:
            level = "orange"
        else:
            level = "green"

        actions = {
            "green": ["keep current mode"],
            "orange": ["reduce aggressiveness", "review latest parameters"],
            "red": ["stop automation", "manual review required"],
        }[level]

        return {
            "risk_level": level,
            "triggered_rules": triggered,
            "recommended_actions": actions,
            "evidence": {
                "win_rate": win_rate,
                "max_drawdown": max_dd,
                "signal_density": signal_density,
            },
        }

    def generate_report(self, report_type: str, context: JsonDict, output_dir: Path = Path("logs/openclaw")) -> JsonDict:
        output_dir.mkdir(parents=True, exist_ok=True)
        run_id = self._new_run_id("report", report_type)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        md_path = output_dir / f"{report_type}_{ts}.md"
        csv_path = output_dir / f"{report_type}_{ts}.csv"

        summary = context.get("summary", {})
        opportunities = context.get("opportunities", [])
        overnight_decision = context.get("overnight_decision", {})
        validation_review = context.get("validation_review", {})
        validation_streak = context.get("validation_streak", {})
        scan_publish_gate = context.get("scan_publish_gate", {})
        publish_preview = context.get("publish_preview", {})

        md_body = [
            f"# {report_type} Report",
            "",
            f"- run_id: {run_id}",
            f"- generated_at: {datetime.now().isoformat()}",
            "",
            "## Executive Summary",
            f"- strategy: {summary.get('strategy', '')}",
            f"- risk_level: {summary.get('risk_level', '')}",
            f"- opportunity_count: {summary.get('count', 0)}",
            f"- execution_mode: {summary.get('execution_mode', '')}",
        ]
        if validation_review:
            md_body.append(f"- validation_gates: {','.join(validation_review.get('gates', []) or []) or 'ok'}")
            md_body.append(f"- consecutive_severe_runs: {validation_streak.get('consecutive_severe_runs', 0)}")
        if scan_publish_gate:
            md_body.append(f"- scan_publish_gate: {scan_publish_gate.get('gate', '') or 'ok'}")
            md_body.append(f"- publish_allowed: {bool(scan_publish_gate.get('ok', True))}")
        if overnight_decision:
            calib = overnight_decision.get("calibration") or {}
            selection_policy = overnight_decision.get("selection_policy") or {}
            md_body.append(f"- overnight_trade_date: {overnight_decision.get('trade_date', '')}")
            md_body.append(f"- calibration_samples: {calib.get('samples', 0)}")
            md_body.append(f"- selected_picks: {selection_policy.get('selected_count', len(overnight_decision.get('recommendations') or []))}")
            md_body.append(f"- candidate_pool_size: {selection_policy.get('candidate_pool_size', '')}")
        md_body.extend(["", "## Top Opportunities"])
        for idx, row in enumerate(opportunities[:10], 1):
            md_body.append(
                f"{idx}. {row.get('ts_code', '')} | score={row.get('score', row.get('weighted_score', ''))} | "
                f"strategy={row.get('strategy', '') or ','.join(row.get('strategies', []) or [])} | "
                f"reason={','.join(row.get('reasons', []) or [str(row.get('reason', ''))]).strip(',')}"
            )
        if overnight_decision:
            recs = overnight_decision.get("recommendations") or []
            holds = overnight_decision.get("holding_comparisons") or []
            decisions = overnight_decision.get("position_decisions") or []
            md_body.extend(["", "## Overnight Two-Pick Plan"])
            if recs:
                for idx, row in enumerate(recs, 1):
                    md_body.append(
                        f"{idx}. {row.get('stock_name', '')}({row.get('ts_code', '')}) | "
                        f"预测涨幅={row.get('expected_return_pct', '')}% | 风险值={row.get('risk_value', '')} | "
                        f"建议={row.get('action', '')} | 时间段={((row.get('trade_window') or {}).get('window', ''))}"
                    )
                    md_body.append(f"   理由: {((row.get('trade_window') or {}).get('reason', ''))}")
            else:
                md_body.append("- 无可执行两票推荐")
            md_body.extend(["", "## Current Holdings Review"])
            if holds:
                for row in holds:
                    md_body.append(
                        f"- {row.get('stock_name', '')}({row.get('ts_code', '')}) | "
                        f"今日预测涨幅={row.get('predicted_return_pct', '')}% | 风险值={row.get('risk_value', '')} | "
                        f"当前盈亏={row.get('profit_loss_pct', '')}%"
                    )
            else:
                md_body.append("- 当前无持仓数据")
            md_body.extend(["", "## Switch Decision"])
            if decisions:
                for row in decisions:
                    md_body.append(
                        f"- 当前={row.get('current_ts_code', '-')}, 候选={row.get('candidate_ts_code', '-')} | "
                        f"决策={row.get('decision', '')} | 换仓分={row.get('switch_score', '')}"
                    )
                    md_body.append(f"  依据: {row.get('rationale', '')}")
            else:
                md_body.append("- 暂无换仓比较")
            md_body.extend(
                [
                    "",
                    "## Validation Review",
                    f"- recommended_count: {validation_review.get('recommended_count', 0)}",
                    f"- executed_count: {validation_review.get('executed_count', 0)}",
                    f"- execution_rate: {validation_review.get('execution_rate', 0)}",
                    f"- avg_realized_return_pct: {validation_review.get('avg_realized_return_pct', None)}",
                    f"- win_rate: {validation_review.get('win_rate', None)}",
                    f"- validation_gates: {','.join(validation_review.get('gates', []) or []) or 'ok'}",
                    f"- consecutive_severe_runs: {validation_streak.get('consecutive_severe_runs', 0)}",
                    "",
                    "## Risk Alerts",
                    f"- policy_note: {overnight_decision.get('policy_note', '')}",
                ]
            )
            if scan_publish_gate or publish_preview:
                md_body.extend(
                    [
                        "",
                        "## Publish Gate",
                        f"- scan_gate: {scan_publish_gate.get('gate', '') or 'ok'}",
                        f"- publish_allowed: {bool(scan_publish_gate.get('ok', True))}",
                        f"- gate_reason: {scan_publish_gate.get('reason', '') or 'none'}",
                        f"- scan_observability_at: {scan_publish_gate.get('scan_observability_at', '') or 'none'}",
                        f"- scan_observability_age_minutes: {scan_publish_gate.get('scan_observability_age_minutes', '')}",
                    ]
                )
                if publish_preview:
                    md_body.extend(
                        [
                            "",
                            "## Notification Preview",
                            f"- title: {publish_preview.get('title', '')}",
                            f"- would_publish: {publish_preview.get('would_publish', False)}",
                            f"- blocked_by_gate: {publish_preview.get('blocked_by_gate', '') or 'none'}",
                            f"- blocked_reason: {publish_preview.get('blocked_reason', '') or 'none'}",
                            "",
                            "```text",
                            str(publish_preview.get('body', '') or ''),
                            "```",
                        ]
                    )
            if selection_policy:
                md_body.extend(
                    [
                        "",
                        "## Pick Policy",
                        f"- min_expected_return: {selection_policy.get('min_expected_return', '')}",
                        f"- max_risk_value: {selection_policy.get('max_risk_value', '')}",
                        f"- second_pick_min_gap: {selection_policy.get('second_pick_min_gap', '')}",
                    ]
                )

        md_path.write_text("\n".join(md_body), encoding="utf-8")

        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "ts_code",
                    "stock_name",
                    "score",
                    "strategy",
                    "reason",
                    "expected_return_pct",
                    "risk_value",
                    "action",
                    "trade_window",
                    "strategy_focus",
                    "strategy_ui_tag",
                    "primary_strategies_hit",
                ],
            )
            writer.writeheader()
            for row in opportunities:
                writer.writerow(
                    {
                        "ts_code": row.get("ts_code", ""),
                        "stock_name": row.get("stock_name", row.get("name", "")),
                        "score": row.get("score", row.get("weighted_score", "")),
                        "strategy": row.get("strategy", ""),
                        "reason": row.get("reason", ""),
                        "expected_return_pct": row.get("expected_return_pct", ""),
                        "risk_value": row.get("risk_value", ""),
                        "action": row.get("action", ""),
                        "trade_window": (row.get("trade_window") or {}).get("window", ""),
                        "strategy_focus": row.get("strategy_focus", ""),
                        "strategy_ui_tag": row.get("strategy_ui_tag", ""),
                        "primary_strategies_hit": ",".join(row.get("primary_strategies_hit", []) or []),
                    }
                )

        return {
            "run_id": run_id,
            "markdown": str(md_path),
            "csv_paths": [str(csv_path)],
            "metadata": {
                "type": report_type,
                "generated_at": datetime.now().isoformat(),
            },
        }

    @staticmethod
    def _new_run_id(stage: str, strategy: str) -> str:
        token = uuid.uuid4().hex[:8]
        return f"{stage}_{strategy}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{token}"

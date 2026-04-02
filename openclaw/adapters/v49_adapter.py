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

        md_body = [
            f"# {report_type} Report",
            "",
            f"- run_id: {run_id}",
            f"- generated_at: {datetime.now().isoformat()}",
            "",
            "## Summary",
            json.dumps(summary, ensure_ascii=False, indent=2),
            "",
            "## Opportunities",
        ]
        for row in opportunities:
            md_body.append(f"- {row}")

        md_path.write_text("\n".join(md_body), encoding="utf-8")

        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "ts_code",
                    "score",
                    "strategy",
                    "reason",
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
                        "score": row.get("score", row.get("weighted_score", "")),
                        "strategy": row.get("strategy", ""),
                        "reason": row.get("reason", ""),
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

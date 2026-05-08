from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
import json

from src.utils.project_paths import resolve_project_path
from src.utils.serialization import save_json


class EvolutionVersionManager:
    def __init__(self, registry_path: str = "data/experiments/evolution_registry_latest.json") -> None:
        self.registry_path = resolve_project_path(registry_path)

    def load_registry(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return self._empty_registry()
        try:
            import json

            registry = json.loads(self.registry_path.read_text(encoding="utf-8"))
            return self._normalize_registry(registry)
        except Exception:
            return self._empty_registry()

    @staticmethod
    def _empty_registry() -> dict[str, Any]:
        return {
            "champion_version": "",
            "champion_summary": {},
            "champion_payload": {},
            "research_champion_version": "",
            "research_champion_summary": {},
            "research_champion_payload": {},
            "staging_champion_version": "",
            "staging_champion_summary": {},
            "staging_champion_payload": {},
            "production_champion_version": "",
            "production_champion_summary": {},
            "production_champion_payload": {},
            "history": [],
        }

    @classmethod
    def _normalize_registry(cls, registry: dict[str, Any]) -> dict[str, Any]:
        normalized = cls._empty_registry()
        normalized.update(registry or {})
        staging_version = str(normalized.get("staging_champion_version", "") or "")
        if not staging_version:
            staging_version = str(normalized.get("champion_version", "") or "")
            normalized["staging_champion_version"] = staging_version
        if not normalized.get("staging_champion_summary"):
            normalized["staging_champion_summary"] = normalized.get("champion_summary", {}) or {}
        if not normalized.get("staging_champion_payload"):
            normalized["staging_champion_payload"] = normalized.get("champion_payload", {}) or {}
        normalized["champion_version"] = staging_version
        normalized["champion_summary"] = normalized.get("staging_champion_summary", {}) or {}
        normalized["champion_payload"] = normalized.get("staging_champion_payload", {}) or {}
        return normalized

    @staticmethod
    def _metric(summary: dict[str, Any], key: str) -> float:
        try:
            return float(summary.get(key, 0.0) or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def _extract_candidate_feedback(cls, candidate_payload: dict[str, Any] | None) -> dict[str, Any]:
        payload = candidate_payload or {}
        feedback = payload.get("candidate_basket_feedback", {}) or {}
        if isinstance(feedback, dict) and feedback:
            return feedback

        feedback_path = payload.get("candidate_basket_feedback_path")
        if feedback_path:
            resolved_path = resolve_project_path(str(feedback_path))
            if resolved_path.exists():
                try:
                    loaded = cls._read_json(resolved_path)
                    if isinstance(loaded, dict):
                        return loaded
                except Exception:
                    return {}

        default_path = resolve_project_path("artifacts/primary_result_candidate_baskets/feedback_latest.json")
        if default_path.exists():
            try:
                loaded = cls._read_json(default_path)
                if isinstance(loaded, dict):
                    return loaded
            except Exception:
                return {}
        return {}

    @classmethod
    def _extract_capacity_pressure(cls, candidate_payload: dict[str, Any] | None) -> dict[str, Any]:
        payload = candidate_payload or {}
        pressure = payload.get("capacity_pressure", {}) or {}
        if isinstance(pressure, dict) and pressure:
            return pressure

        summary = payload.get("candidate_basket_summary", {}) or {}
        embedded = summary.get("capacity_pressure", {}) or {}
        if isinstance(embedded, dict) and embedded:
            return embedded

        summary_path = payload.get("candidate_basket_summary_path")
        if summary_path:
            resolved_path = resolve_project_path(str(summary_path))
            if resolved_path.exists():
                try:
                    loaded = cls._read_json(resolved_path)
                    capacity = (loaded.get("capacity_pressure", {}) or {}) if isinstance(loaded, dict) else {}
                    if isinstance(capacity, dict):
                        return capacity
                except Exception:
                    return {}

        default_path = resolve_project_path("data/experiments/candidates_basket_summary_latest.json")
        if default_path.exists():
            try:
                loaded = cls._read_json(default_path)
                capacity = (loaded.get("capacity_pressure", {}) or {}) if isinstance(loaded, dict) else {}
                if isinstance(capacity, dict):
                    return capacity
            except Exception:
                return {}
        return {}

    @classmethod
    def _evaluate_gates(
        cls,
        candidate_summary: dict[str, Any],
        *,
        candidate_payload: dict[str, Any] | None = None,
        min_walk_forward_score: float,
        min_stability: float,
    ) -> dict[str, dict[str, Any]]:
        statistical = {
            "passed": (
                cls._metric(candidate_summary, "walk_forward_score") >= float(min_walk_forward_score)
                and cls._metric(candidate_summary, "trade_objective_stability") >= float(min_stability)
            ),
            "walk_forward_score": cls._metric(candidate_summary, "walk_forward_score"),
            "trade_objective_stability": cls._metric(candidate_summary, "trade_objective_stability"),
            "fold_count": cls._metric(candidate_summary, "fold_count"),
            "pool_count": cls._metric(candidate_summary, "pool_count"),
        }
        robustness = {
            "passed": (
                cls._metric(candidate_summary, "fold_count") >= 1.0
                and cls._metric(candidate_summary, "pool_count") >= 1.0
            ),
            "fold_count": cls._metric(candidate_summary, "fold_count"),
            "pool_count": cls._metric(candidate_summary, "pool_count"),
        }
        production_ready = bool(candidate_summary.get("production_ready", False))
        production = {
            "passed": production_ready,
            "production_ready": production_ready,
        }
        feedback = cls._extract_candidate_feedback(candidate_payload)
        feedback_level = str(feedback.get("feedback_level", "") or "").strip().lower() or "unavailable"
        change_total = int(feedback.get("change_total", 0) or 0)
        requires_manual_review = bool(feedback.get("requires_manual_review", False))
        execution_feedback = {
            "passed": feedback_level != "tighten",
            "feedback_level": feedback_level,
            "change_total": change_total,
            "requires_manual_review": requires_manual_review,
            "window_label": str(feedback.get("window_label", "") or ""),
            "summary_note": str(feedback.get("summary_note", "") or ""),
            "review_only": feedback_level == "review",
            "available": bool(feedback),
        }
        capacity_pressure = cls._extract_capacity_pressure(candidate_payload)
        capacity_state = str(capacity_pressure.get("capacity_state", "") or "").strip().lower() or "unavailable"
        recommended_scale_profile = str(capacity_pressure.get("recommended_scale_profile", "") or "")
        worst_stress_score = cls._metric(capacity_pressure, "worst_stress_score")
        capacity_gate = {
            "passed": capacity_state not in {"stretched"},
            "capacity_state": capacity_state,
            "recommended_scale_profile": recommended_scale_profile,
            "worst_stress_score": worst_stress_score,
            "available": bool(capacity_pressure),
            "watch_only": capacity_state == "watch",
        }
        overall_passed = all(gate["passed"] for gate in (statistical, robustness, execution_feedback, capacity_gate))
        return {
            "statistical": statistical,
            "robustness": robustness,
            "production": production,
            "execution_feedback": execution_feedback,
            "capacity_pressure": capacity_gate,
            "overall_passed": overall_passed,
        }

    def evaluate_candidate(
        self,
        candidate_version: str,
        candidate_summary: dict[str, Any],
        *,
        candidate_payload: dict[str, Any] | None = None,
        min_improvement: float = 0.02,
        min_walk_forward_score: float = 0.12,
        min_stability: float = 0.55,
    ) -> dict[str, Any]:
        registry = self.load_registry()
        champion_summary = registry.get("staging_champion_summary", {}) or registry.get("champion_summary", {}) or {}
        candidate_score = self._metric(candidate_summary, "walk_forward_score")
        champion_score = self._metric(champion_summary, "walk_forward_score")
        candidate_stability = self._metric(candidate_summary, "trade_objective_stability")
        gates = self._evaluate_gates(
            candidate_summary,
            candidate_payload=candidate_payload,
            min_walk_forward_score=float(min_walk_forward_score),
            min_stability=float(min_stability),
        )
        production_ready = bool(gates["production"]["passed"])
        feedback_gate = gates["execution_feedback"]
        feedback_level = str(feedback_gate.get("feedback_level", "") or "")
        capacity_gate = gates["capacity_pressure"]
        capacity_state = str(capacity_gate.get("capacity_state", "") or "")

        if capacity_state == "stretched":
            action = "reject"
            reason = "最近容量压力评估显示放大后会明显劣化，当前版本不得晋级。"
        elif feedback_level == "tighten":
            action = "reject"
            reason = "最近候选篮子反馈要求收紧，当前版本不得晋级。"
        elif not gates["overall_passed"]:
            action = "reject"
            reason = "候选版本未通过统计或稳健性门禁。"
        elif bool(capacity_gate.get("watch_only")):
            action = "observe"
            reason = "最近容量压力评估提示放大需观察，候选版本先观察，不直接晋级。"
        elif bool(feedback_gate.get("review_only")):
            action = "observe"
            reason = "最近候选篮子反馈要求复核，候选版本先观察，不直接晋级。"
        elif not registry.get("staging_champion_version"):
            action = "promote_to_production" if production_ready else "promote_to_staging"
            reason = "首个通过门禁的候选版本，设为当前冠军。"
        elif candidate_score >= champion_score + float(min_improvement) and candidate_stability >= float(min_stability):
            action = "promote_to_production" if production_ready else "promote_to_staging"
            reason = "候选版本通过门禁，且核心评分优于当前 staging 冠军。"
        elif candidate_score < float(min_walk_forward_score):
            action = "reject"
            reason = "候选版本 walk-forward 分数过低，拒绝晋级。"
        else:
            action = "observe"
            reason = "候选版本通过基础门槛，但未超过当前 staging 冠军，先保留观察。"

        record = {
            "version": candidate_version,
            "created_at": datetime.now().isoformat(),
            "action": action,
            "reason": reason,
            "summary": candidate_summary,
            "payload": candidate_payload or {},
            "gates": gates,
            "feedback_level": feedback_level,
        }
        history = list(registry.get("history", []) or [])
        history.append(record)

        registry["research_champion_version"] = candidate_version
        registry["research_champion_summary"] = candidate_summary
        registry["research_champion_payload"] = candidate_payload or {}
        if action in {"promote_to_staging", "promote_to_production"}:
            registry["staging_champion_version"] = candidate_version
            registry["staging_champion_summary"] = candidate_summary
            registry["staging_champion_payload"] = candidate_payload or {}
            registry["champion_version"] = candidate_version
            registry["champion_summary"] = candidate_summary
            registry["champion_payload"] = candidate_payload or {}
        if action == "promote_to_production":
            registry["production_champion_version"] = candidate_version
            registry["production_champion_summary"] = candidate_summary
            registry["production_champion_payload"] = candidate_payload or {}
        registry["history"] = history[-50:]
        save_json(registry, str(self.registry_path))
        return {
            "action": action,
            "reason": reason,
            "candidate_version": candidate_version,
            "champion_version": registry.get("champion_version", ""),
            "champion_summary": registry.get("champion_summary", {}),
            "champion_payload": registry.get("champion_payload", {}),
            "research_champion_version": registry.get("research_champion_version", ""),
            "staging_champion_version": registry.get("staging_champion_version", ""),
            "production_champion_version": registry.get("production_champion_version", ""),
            "gates": gates,
            "registry_path": str(self.registry_path),
        }

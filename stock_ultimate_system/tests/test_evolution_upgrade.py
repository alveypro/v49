from pathlib import Path

import pandas as pd

from src.evolution.trade_objective import summarize_trade_metrics, trade_objective_from_predictions
from src.evolution.version_manager import EvolutionVersionManager
from src.evolution.walk_forward_evaluator import WalkForwardEvaluator, WalkForwardPool


def test_trade_objective_rewards_high_precision_and_returns():
    metrics = trade_objective_from_predictions(
        [1, 1, 0, 0, 1],
        [0.9, 0.8, 0.3, 0.2, 0.7],
        [0.05, 0.04, -0.01, -0.02, 0.03],
        top_quantile=0.4,
    )
    assert metrics["precision_at_top"] == 1.0
    assert metrics["avg_return_at_top"] > 0.0
    assert metrics["trade_objective"] > 0.3


def test_summarize_trade_metrics_reports_stability():
    summary = summarize_trade_metrics(
        [
            {"trade_objective": 0.32, "precision_at_top": 0.8, "avg_return_at_top": 0.02, "return_sharpe_proxy": 0.4},
            {"trade_objective": 0.28, "precision_at_top": 0.7, "avg_return_at_top": 0.015, "return_sharpe_proxy": 0.35},
        ]
    )
    assert summary["trade_objective_mean"] > 0.25
    assert summary["trade_objective_stability"] > 0.5


def test_walk_forward_evaluator_scores_pools():
    rows = []
    for idx in range(120):
        rows.append(
            {
                "f1": float(idx % 10),
                "f2": float((idx % 7) / 10.0),
                "label_direction_5": 1 if idx % 3 != 0 else 0,
                "label_return_5": 0.03 if idx % 3 != 0 else -0.02,
            }
        )
    frame = pd.DataFrame(rows)
    evaluator = WalkForwardEvaluator(
        model_params={"enabled_models": ["logistic"]},
        enable_deep=False,
    )
    result = evaluator.evaluate([WalkForwardPool("core", frame, ["f1", "f2"], "label_direction_5")], folds=2)
    assert result["summary"]["pool_count"] == 1.0
    assert result["summary"]["fold_count"] >= 1.0
    assert "logistic" in dict(result["dominant_models"])


def test_version_manager_promotes_then_holds(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    promoted = manager.evaluate_candidate(
        "v1",
        {"walk_forward_score": 0.20, "trade_objective_stability": 0.7, "fold_count": 3, "pool_count": 2},
        candidate_payload={"model_evolution": {"selected_models": ["logistic"]}},
    )
    assert promoted["action"] == "promote_to_staging"
    assert promoted["champion_payload"]["model_evolution"]["selected_models"] == ["logistic"]
    assert promoted["staging_champion_version"] == "v1"
    assert promoted["production_champion_version"] == ""

    observed = manager.evaluate_candidate(
        "v2",
        {"walk_forward_score": 0.21, "trade_objective_stability": 0.52, "fold_count": 3, "pool_count": 2},
        min_improvement=0.02,
        min_stability=0.55,
    )
    assert observed["action"] == "reject"
    assert Path(observed["registry_path"]).exists()


def test_version_manager_promotes_to_production_when_ready(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_prod",
        {
            "walk_forward_score": 0.28,
            "trade_objective_stability": 0.76,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={"model_evolution": {"selected_models": ["logistic", "random_forest"]}},
    )
    assert result["action"] == "promote_to_production"
    assert result["production_champion_version"] == "v_prod"
    assert result["champion_version"] == "v_prod"
    assert result["gates"]["overall_passed"] is True


def test_version_manager_rejects_candidate_when_feedback_requires_tighten(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_tighten",
        {
            "walk_forward_score": 0.31,
            "trade_objective_stability": 0.79,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={
            "candidate_basket_feedback": {
                "feedback_level": "tighten",
                "change_total": 3,
                "requires_manual_review": True,
                "window_label": "5D",
                "summary_note": "recent basket observation requires tighter selection and basket risk controls",
            }
        },
    )
    assert result["action"] == "reject"
    assert result["gates"]["execution_feedback"]["passed"] is False
    assert result["gates"]["execution_feedback"]["feedback_level"] == "tighten"


def test_version_manager_observes_candidate_when_feedback_requires_review(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_review",
        {
            "walk_forward_score": 0.29,
            "trade_objective_stability": 0.73,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={
            "candidate_basket_feedback": {
                "feedback_level": "review",
                "change_total": 1,
                "requires_manual_review": True,
                "window_label": "5D",
                "summary_note": "recent basket observation requires review before trusting the current candidate profile",
            }
        },
    )
    assert result["action"] == "observe"
    assert result["gates"]["execution_feedback"]["review_only"] is True
    assert result["production_champion_version"] == ""


def test_version_manager_promotes_when_feedback_reinforces_candidate(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_reinforce",
        {
            "walk_forward_score": 0.30,
            "trade_objective_stability": 0.78,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={
            "candidate_basket_feedback": {
                "feedback_level": "reinforce",
                "change_total": 0,
                "requires_manual_review": False,
                "window_label": "10D",
                "summary_note": "recent basket observation is strong enough to reinforce the current selection profile",
            }
        },
    )
    assert result["action"] == "promote_to_production"
    assert result["gates"]["execution_feedback"]["passed"] is True
    assert result["gates"]["execution_feedback"]["feedback_level"] == "reinforce"


def test_version_manager_rejects_candidate_when_capacity_pressure_is_stretched(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_capacity_blocked",
        {
            "walk_forward_score": 0.32,
            "trade_objective_stability": 0.80,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={
            "capacity_pressure": {
                "capacity_state": "stretched",
                "recommended_scale_profile": "top1_only",
                "worst_stress_score": 58.0,
            }
        },
    )
    assert result["action"] == "reject"
    assert result["gates"]["capacity_pressure"]["passed"] is False
    assert result["gates"]["capacity_pressure"]["capacity_state"] == "stretched"


def test_version_manager_observes_candidate_when_capacity_pressure_is_watch(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_capacity_watch",
        {
            "walk_forward_score": 0.30,
            "trade_objective_stability": 0.77,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={
            "capacity_pressure": {
                "capacity_state": "watch",
                "recommended_scale_profile": "defensive",
                "worst_stress_score": 28.0,
            }
        },
    )
    assert result["action"] == "observe"
    assert result["gates"]["capacity_pressure"]["watch_only"] is True


def test_version_manager_can_promote_when_capacity_pressure_is_scalable(tmp_path):
    manager = EvolutionVersionManager(registry_path=str(tmp_path / "evolution_registry.json"))
    result = manager.evaluate_candidate(
        "v_capacity_ok",
        {
            "walk_forward_score": 0.29,
            "trade_objective_stability": 0.76,
            "fold_count": 4,
            "pool_count": 3,
            "production_ready": True,
        },
        candidate_payload={
            "capacity_pressure": {
                "capacity_state": "scalable",
                "recommended_scale_profile": "normal",
                "worst_stress_score": 10.0,
            }
        },
    )
    assert result["action"] == "promote_to_production"
    assert result["gates"]["capacity_pressure"]["passed"] is True

from __future__ import annotations

from openclaw.services.ensemble_alpha_rebuild_lab_service import (
    _candidate_score,
    build_ensemble_alpha_rebuild_lab,
    build_ensemble_alpha_rebuild_multi_window_lab,
)


def test_ensemble_alpha_rebuild_lab_blocks_single_window_candidates_even_with_positive_ic():
    fact_chain = {
        "as_of_date": "20260101",
        "sample_facts": [
            _item("000001.SZ", 90.0, 80.0, 10.0, 1.0, 2.0, 3.0, 4.0, 5.0),
            _item("000002.SZ", 70.0, 75.0, 10.0, 0.5, 1.0, 1.5, 2.0, 3.0),
            _item("000003.SZ", 50.0, 70.0, 10.0, -0.5, -1.0, -1.5, -2.0, -3.0),
        ],
    }

    review = build_ensemble_alpha_rebuild_lab(fact_chain, min_samples=3, min_research_windows=5)

    assert review["research_only"] is True
    assert review["candidate_alpha_sleeves"] == []
    assert "no_rebuilt_alpha_candidate_passed_policy" in review["blocking_reasons"]
    momentum = review["candidate_reviews"]["quality_adjusted_momentum"]
    assert momentum["ic"] is not None
    assert momentum["recommended_use"] == "research_blocked_insufficient_research_windows"
    assert "do_not_promote_rebuilt_sleeves_from_single_window_ic" in review["hard_boundaries"]


def test_ensemble_alpha_rebuild_lab_can_mark_candidate_only_when_window_gate_is_met():
    fact_chain = {
        "as_of_date": "20260101",
        "sample_facts": [
            _item("000001.SZ", 90.0, 80.0, 10.0, 1.0, 2.0, 3.0, 4.0, 5.0),
            _item("000002.SZ", 70.0, 75.0, 10.0, 0.5, 1.0, 1.5, 2.0, 3.0),
            _item("000003.SZ", 50.0, 70.0, 10.0, -0.5, -1.0, -1.5, -2.0, -3.0),
        ],
    }

    review = build_ensemble_alpha_rebuild_lab(fact_chain, min_samples=3, min_research_windows=1)

    assert "quality_adjusted_momentum" in review["candidate_alpha_sleeves"]
    assert review["candidate_reviews"]["quality_adjusted_momentum"]["recommended_use"] == "positive_alpha_candidate"


def test_ensemble_alpha_rebuild_multi_window_lab_requires_minimum_windows():
    one_window = {
        "as_of_date": "20260101",
        "sample_facts": [
            _item("000001.SZ", 90.0, 80.0, 10.0, 1.0, 2.0, 3.0, 4.0, 5.0),
            _item("000002.SZ", 70.0, 75.0, 10.0, 0.5, 1.0, 1.5, 2.0, 3.0),
            _item("000003.SZ", 50.0, 70.0, 10.0, -0.5, -1.0, -1.5, -2.0, -3.0),
        ],
    }

    review = build_ensemble_alpha_rebuild_multi_window_lab([one_window], min_samples=3, min_research_windows=5)

    assert review["research_window_count"] == 1
    assert "insufficient_research_windows:1/5" in review["blocking_reasons"]
    assert review["candidate_alpha_sleeves"] == []


def test_ensemble_alpha_rebuild_multi_window_lab_can_accept_stable_candidate_after_window_gate():
    windows = []
    for idx in range(5):
        windows.append(
            {
                "as_of_date": f"2026010{idx + 1}",
                "sample_facts": [
                    _item(f"00000{idx}1.SZ", 90.0, 80.0, 10.0, 1.0, 2.0, 3.0, 4.0, 5.0),
                    _item(f"00000{idx}2.SZ", 70.0, 75.0, 10.0, 0.5, 1.0, 1.5, 2.0, 3.0),
                    _item(f"00000{idx}3.SZ", 50.0, 70.0, 10.0, -0.5, -1.0, -1.5, -2.0, -3.0),
                ],
            }
        )

    review = build_ensemble_alpha_rebuild_multi_window_lab(windows, min_samples=3, min_research_windows=5)

    assert review["research_window_count"] == 5
    assert "quality_adjusted_momentum" in review["candidate_alpha_sleeves"]
    assert review["candidate_reviews"]["quality_adjusted_momentum"]["window_positive_count"] == 5
    assert review["blocking_reasons"] == []


def test_ensemble_alpha_rebuild_lab_exposes_rebased_candidate_recipes():
    fact_chain = {
        "as_of_date": "20260101",
        "sample_facts": [
            _item("000001.SZ", 90.0, 80.0, 10.0, 1.0, 2.0, 3.0, 4.0, 5.0, money_flow=75.0),
            _item("000002.SZ", 70.0, 75.0, 10.0, 0.5, 1.0, 1.5, 2.0, 3.0, money_flow=60.0),
            _item("000003.SZ", 50.0, 70.0, 10.0, -0.5, -1.0, -1.5, -2.0, -3.0, money_flow=55.0),
        ],
    }

    review = build_ensemble_alpha_rebuild_lab(fact_chain, min_samples=3, min_research_windows=1)

    assert "industry_neutral_quality_momentum" in review["candidate_recipes"]
    assert "flow_trend_event_guard" in review["candidate_recipes"]
    assert "reversal_exhaustion_quality_guard" in review["candidate_recipes"]
    assert "hard_event_alpha_candidate" in review["candidate_recipes"]
    assert review["candidate_reviews"]["flow_trend_event_guard"]["active_signal_count"] == 3


def test_hard_event_alpha_candidate_penalizes_crowded_exhaustion_structure():
    exhausted = _hard_event_item(
        hard_event=60.0,
        capacity=76.0,
        flow=88.0,
        seat=92.0,
        limit_structure=72.0,
        margin=62.0,
    )
    confirmed = _hard_event_item(
        hard_event=60.0,
        capacity=54.0,
        flow=56.0,
        seat=46.0,
        limit_structure=48.0,
        margin=28.0,
    )
    low_capacity = _hard_event_item(
        hard_event=60.0,
        capacity=30.0,
        flow=56.0,
        seat=46.0,
        limit_structure=48.0,
        margin=28.0,
    )

    exhausted_score = _candidate_score("hard_event_alpha_candidate", exhausted)
    confirmed_score = _candidate_score("hard_event_alpha_candidate", confirmed)

    assert exhausted_score < confirmed_score
    assert _candidate_score("hard_event_alpha_candidate", low_capacity) == 0.0


def _item(
    ts_code: str,
    momentum: float,
    quality: float,
    sector: float,
    r1: float,
    r3: float,
    r5: float,
    r10: float,
    r20: float,
    money_flow: float = 0.0,
    reversal: float = 0.0,
    event_risk: float = 0.0,
) -> dict:
    return {
        "ts_code": ts_code,
        "sleeve_scores": {
            "momentum": {"score": momentum},
            "quality_low_vol": {"score": quality},
            "sector_rotation": {"score": sector},
            "money_flow": {"score": money_flow},
            "reversal": {"score": reversal},
            "event_risk": {"score": event_risk},
        },
        "forward_returns": {
            "1": {"available": True, "return_pct": r1},
            "3": {"available": True, "return_pct": r3},
            "5": {"available": True, "return_pct": r5},
            "10": {"available": True, "return_pct": r10},
            "20": {"available": True, "return_pct": r20},
        },
    }


def _hard_event_item(
    *,
    hard_event: float,
    capacity: float,
    flow: float,
    seat: float,
    limit_structure: float,
    margin: float,
    crowding: float = 0.0,
) -> dict:
    return {
        "sleeve_scores": {
            "hard_event_alpha": {"score": hard_event},
        },
        "tushare_pro_alpha_features": {
            "evidence": {
                "hard_alpha": {
                    "capacity_liquidity": {"score": capacity},
                    "money_flow_persistence": {"score": flow},
                    "dragon_tiger_seat_quality": {"score": seat},
                    "limit_break_structure": {"score": limit_structure},
                    "margin_pressure": {"score": margin},
                    "industry_crowding": {"score": crowding},
                }
            }
        },
    }

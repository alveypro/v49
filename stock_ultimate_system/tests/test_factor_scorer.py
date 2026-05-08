import pandas as pd

from src.signal_engine.factor_scorer import FactorScorer


def test_score_research_factors_rewards_transition_setup_without_full_trend_stack():
    scorer = FactorScorer()
    df = pd.DataFrame([
        {
            "close": 59.14,
            "ma_20": 50.931,
            "ma_60": 51.6147,
            "macd": 1.8547,
            "volume_ratio": 0.8875,
        }
    ])

    scores = scorer.score_research_factors(df)

    assert scores["trend_transition"] == 8
    assert scores["quiet_breakout"] == 4


def test_score_research_factors_does_not_reward_blocked_or_weak_structure():
    scorer = FactorScorer()
    df = pd.DataFrame([
        {
            "close": 20.0,
            "ma_20": 21.0,
            "ma_60": 23.0,
            "macd": -0.2,
            "volume_ratio": 0.7,
        }
    ])

    scores = scorer.score_research_factors(df)

    assert scores["trend_transition"] == 0
    assert scores["quiet_breakout"] == 0


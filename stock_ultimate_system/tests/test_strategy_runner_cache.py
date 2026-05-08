from types import SimpleNamespace

import pandas as pd

from src.backtest_engine.strategy_runner import StrategyRunner


def _sample_df(rows: int = 40) -> pd.DataFrame:
    dates = pd.date_range("2025-01-01", periods=rows, freq="B")
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "close": [10 + i * 0.1 for i in range(rows)],
            "open": [10 + i * 0.1 for i in range(rows)],
            "high": [10 + i * 0.1 for i in range(rows)],
            "low": [10 + i * 0.1 for i in range(rows)],
            "volume": [1000 + i for i in range(rows)],
            "amount": [2_000_000 + i * 1000 for i in range(rows)],
        }
    )


def test_strategy_runner_reuses_shared_feature_and_forecast_cache():
    calls = {"build_features": 0, "regime": 0, "forecast": 0}

    class StubFeatureAgent:
        @staticmethod
        def build_features(df):
            calls["build_features"] += 1
            return df

    class StubForecastAgent:
        feature_cols = ["close"]

        @staticmethod
        def predict(df, feature_cols, regime_info=None):
            calls["forecast"] += 1
            return {"prob_up": 0.6}

    class StubRegimeAgent:
        @staticmethod
        def detect_market_regime(df):
            calls["regime"] += 1
            return {"regime": "neutral"}

    class StubRiskAgent:
        @staticmethod
        def evaluate_trade_risk(df, forecast_result, regime_info):
            return {"allow_trade": False}

    class StubSignalAgent:
        @staticmethod
        def generate_signal(df, forecast_result, regime_info, risk_info):
            return {"signal": "watch"}

    class StubPositionAgent:
        @staticmethod
        def calculate_position_size(signal_result, risk_info, account_info):
            return {"position_pct": 0.1}

    shared = {
        "feature_cache": {},
        "date_index_cache": {},
        "regime_cache": {},
        "forecast_cache": {},
    }
    data_dict = {"000001.SZ": _sample_df()}
    market = {"000001.SZ": pd.Series({"close": 13.0, "amount": 2_500_000})}
    account = SimpleNamespace(cash=1_000_000, positions={})
    date = str(data_dict["000001.SZ"]["date"].iloc[-1])

    runner1 = StrategyRunner(
        config={},
        feature_agent=StubFeatureAgent(),
        forecast_agent=StubForecastAgent(),
        regime_agent=StubRegimeAgent(),
        signal_agent=StubSignalAgent(),
        risk_agent=StubRiskAgent(),
        position_agent=StubPositionAgent(),
        data_dict=data_dict,
        **shared,
    )
    runner2 = StrategyRunner(
        config={},
        feature_agent=StubFeatureAgent(),
        forecast_agent=StubForecastAgent(),
        regime_agent=StubRegimeAgent(),
        signal_agent=StubSignalAgent(),
        risk_agent=StubRiskAgent(),
        position_agent=StubPositionAgent(),
        data_dict=data_dict,
        **shared,
    )

    runner1.generate_signals(date, market, account)
    runner2.generate_signals(date, market, account)

    assert calls["build_features"] == 1
    assert calls["regime"] == 1
    assert calls["forecast"] == 1

from __future__ import annotations

import logging
from typing import Any, Callable

import pandas as pd

from src.agents.feature_agent import FeatureAgent
from src.agents.forecast_agent import ForecastAgent
from src.agents.regime_agent import RegimeAgent
from src.agents.risk_agent import RiskAgent
from src.agents.signal_agent import SignalAgent
from src.agents.position_agent import PositionAgent
from src.backtest_engine.event_engine import Account

logger = logging.getLogger(__name__)


class StrategyRunner:
    """Generate trading signals for the backtester using the full agent chain."""

    def __init__(
        self,
        config: dict,
        feature_agent: FeatureAgent,
        forecast_agent: ForecastAgent,
        regime_agent: RegimeAgent,
        signal_agent: SignalAgent,
        risk_agent: RiskAgent,
        position_agent: PositionAgent,
        data_dict: dict[str, pd.DataFrame] | None = None,
        feature_cache: dict[str, pd.DataFrame] | None = None,
        date_index_cache: dict[str, dict[str, int]] | None = None,
        regime_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
        forecast_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
    ) -> None:
        self.config = config
        self.feature_agent = feature_agent
        self.forecast_agent = forecast_agent
        self.regime_agent = regime_agent
        self.signal_agent = signal_agent
        self.risk_agent = risk_agent
        self.position_agent = position_agent
        self.data_dict = data_dict or {}
        self._feature_cache = feature_cache if feature_cache is not None else {}
        self._date_index_cache = date_index_cache if date_index_cache is not None else {}
        self._regime_cache = regime_cache if regime_cache is not None else {}
        self._forecast_cache = forecast_cache if forecast_cache is not None else {}
        market_rules = config.get('market_rules', {})
        risk_rules = config.get('risk_rules', {})
        signal_rules = config.get('signal_rules', {})
        self._min_turnover = float(
            market_rules.get('liquidity_min_turnover', risk_rules.get('liquidity_min_turnover', 1_000_000))
        )
        self._low_confidence_skip_threshold = float(signal_rules.get('low_confidence_skip_threshold', 0.55))

    def _get_features_up_to(self, ts_code: str, date: str) -> pd.DataFrame | None:
        if ts_code not in self._feature_cache:
            raw = self.data_dict.get(ts_code)
            if raw is None or raw.empty:
                return None
            full = self.feature_agent.build_features(raw.copy()).reset_index(drop=True)
            self._feature_cache[ts_code] = full
            self._date_index_cache[ts_code] = {d: i for i, d in enumerate(full['date'].astype(str).tolist())}
        full = self._feature_cache[ts_code]
        idx = self._date_index_cache.get(ts_code, {}).get(date)
        if idx is None:
            return None
        if idx < 29:
            return None
        return full.iloc[:idx + 1]

    def _get_context(self, ts_code: str, date: str) -> tuple[pd.DataFrame | None, dict[str, Any] | None, dict[str, Any] | None]:
        df = self._get_features_up_to(ts_code, date)
        if df is None:
            return None, None, None
        cache_key = (ts_code, str(date))
        regime_info = self._regime_cache.get(cache_key)
        forecast_result = self._forecast_cache.get(cache_key)
        if regime_info is None:
            regime_info = self.regime_agent.detect_market_regime(df)
            self._regime_cache[cache_key] = regime_info
        if forecast_result is None:
            forecast_result = self.forecast_agent.predict(
                df,
                self.forecast_agent.feature_cols or None,
                regime_info=regime_info,
            )
            self._forecast_cache[cache_key] = forecast_result
        return df, regime_info, forecast_result

    def generate_signals(self, date: str, market: dict[str, pd.Series], account: Account) -> list[dict]:
        signals: list[dict] = []
        equity = account.cash + sum(
            float(market.get(c, pd.Series({'close': p.avg_cost})).get('close', p.avg_cost)) * p.qty
            for c, p in account.positions.items()
        )
        if hasattr(self.risk_agent, 'update_equity'):
            self.risk_agent.update_equity(equity)

        for code in list(account.positions.keys()):
            pos = account.positions[code]
            row = market.get(code)
            if row is None:
                continue
            current_price = float(row.get('close', pos.avg_cost))
            pnl_pct = (current_price - pos.avg_cost) / pos.avg_cost if pos.avg_cost > 0 else 0.0
            risk_cfg = self.config.get('settings', {}).get('risk', {})
            if pnl_pct <= -risk_cfg.get('stop_loss_pct', 0.05):
                signals.append({'ts_code': code, 'side': 'sell', 'reason': 'stop_loss'})
                continue
            if pnl_pct >= risk_cfg.get('take_profit_pct', 0.10):
                signals.append({'ts_code': code, 'side': 'sell', 'reason': 'take_profit'})
                continue

        for code in self.data_dict:
            if code in account.positions:
                continue
            day_row = market.get(code)
            if day_row is None:
                continue
            if float(day_row.get('amount', 0.0)) < self._min_turnover:
                continue
            df, regime_info, forecast_result = self._get_context(code, date)
            if df is None or regime_info is None or forecast_result is None:
                continue
            try:
                risk_info = self.risk_agent.evaluate_trade_risk(df, forecast_result, regime_info)
                if not risk_info.get('allow_trade', False):
                    continue
                signal_result = self.signal_agent.generate_signal(df, forecast_result, regime_info, risk_info)
                if signal_result.get('signal') in ('strong_buy', 'buy'):
                    forecast_confidence = float(signal_result.get('forecast_confidence', forecast_result.get('confidence', 0.0)))
                    if forecast_confidence < self._low_confidence_skip_threshold:
                        continue
                    pos_result = self.position_agent.calculate_position_size(
                        signal_result, risk_info, {'cash': account.cash, 'equity': equity}
                    )
                    target_pct = pos_result.get('position_pct', 0.1)
                    signals.append({
                        'ts_code': code, 'side': 'buy', 'target_pct': target_pct,
                        'reason': signal_result.get('signal'),
                        'regime': regime_info.get('regime', ''),
                        'environment_score': regime_info.get('environment_score', 0.5),
                    })
            except Exception as e:
                logger.debug('Signal generation failed for %s on %s: %s', code, date, e)

        return signals

    def as_signal_func(self) -> Callable:
        return self.generate_signals

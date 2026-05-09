from src.features.trend_features import TrendFeatureBuilder
from src.features.momentum_features import MomentumFeatureBuilder
from src.features.volatility_features import VolatilityFeatureBuilder
from src.features.volume_price_features import VolumePriceFeatureBuilder
from src.features.market_context_features import MarketContextFeatureBuilder
from src.features.risk_features import RiskFeatureBuilder
from src.labels.label_builder import LabelBuilder


class FeatureAgent:
    def __init__(self, config):
        self.config = config
        self.trend = TrendFeatureBuilder()
        self.momentum = MomentumFeatureBuilder()
        self.vol = VolatilityFeatureBuilder()
        self.vp = VolumePriceFeatureBuilder()
        self.ctx = MarketContextFeatureBuilder()
        self.risk = RiskFeatureBuilder()
        self.labels = LabelBuilder()

    def build_features(self, df):
        params = self.config.get('feature_params') or {}
        ma_windows = params.get('ma_windows', [5, 10, 20])
        ema_windows = params.get('ema_windows', [5, 10, 20])
        df = self.trend.add_ma_features(df, ma_windows)
        df = self.trend.add_ema_features(df, ema_windows)
        df = self.trend.add_slope_features(df, [5, 10, 20])
        df = self.trend.add_ma_gap_features(df, fast_window=5, slow_window=20)
        df = self.trend.add_price_vs_average_features(df, ma_windows)
        df = self.momentum.add_rsi(df)
        df = self.momentum.add_macd(df)
        df = self.momentum.add_return_features(df)
        df = self.vol.add_atr(df)
        df = self.vol.add_historical_volatility(df)
        df = self.vol.add_intraday_range(df)
        df = self.vp.add_volume_ma(df, [5, 10, 20])
        df = self.vp.add_volume_ratio(df)
        df = self.vp.add_liquidity_features(df)
        df = self.ctx.add_index_relative_strength(df)
        df = self.ctx.add_market_trend_context(df)
        df = self.risk.add_drawdown_features(df)
        df = self.risk.add_downside_risk_features(df)
        df = self.labels.build_direction_label(df)
        df = self.labels.build_return_label(df)
        df = self.labels.build_excess_return_label(df)
        df = self.labels.build_excess_direction_label(df)
        return df

    def prepare_training_frame(self, df):
        df = df.dropna().reset_index(drop=True)
        training_cfg = self.config.get('settings', {}).get('training', {})
        target_priority = [
            str(training_cfg.get('target_col', 'label_excess_direction_5') or 'label_excess_direction_5'),
            'label_cross_sectional_excess_direction_5',
            'label_excess_direction_5',
            'label_direction_5',
        ]
        target_col = next((candidate for candidate in target_priority if candidate in df.columns), 'label_direction_5')
        feature_cols = [
            c for c in df.columns
            if c not in {'date', 'ts_code'}
            and not str(c).startswith('label_')
            and df[c].dtype != 'O'
        ]
        return df, feature_cols, target_col

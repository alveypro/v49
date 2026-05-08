class RiskFeatureBuilder:
    def add_drawdown_features(self, df, window=60):
        roll_max = df['close'].rolling(window).max()
        df[f'drawdown_{window}'] = df['close'] / roll_max - 1
        return df

    def add_downside_risk_features(self, df, window=20):
        returns = df['close'].pct_change()
        downside = returns.where(returns < 0, 0.0)
        df[f'downside_vol_{window}'] = downside.rolling(window).std()
        return df

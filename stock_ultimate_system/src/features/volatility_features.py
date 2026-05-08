class VolatilityFeatureBuilder:
    def add_atr(self, df, window=14):
        tr = (df[['high', 'close']].max(axis=1) - df[['low', 'close']].min(axis=1)).abs()
        df['atr'] = tr.rolling(window).mean()
        df['atr_pct'] = df['atr'] / df['close'].replace(0, 1e-9)
        return df

    def add_historical_volatility(self, df, window=20):
        df[f'hist_vol_{window}'] = df['close'].pct_change().rolling(window).std()
        return df

    def add_intraday_range(self, df, window=10):
        daily_range = (df['high'] - df['low']) / df['close'].replace(0, 1e-9)
        df['intraday_range'] = daily_range
        df[f'intraday_range_ma_{window}'] = daily_range.rolling(window).mean()
        return df

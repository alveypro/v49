class TrendFeatureBuilder:
    def add_ma_features(self, df, windows):
        for w in windows:
            df[f'ma_{w}'] = df['close'].rolling(w).mean()
        return df

    def add_ema_features(self, df, windows):
        for w in windows:
            df[f'ema_{w}'] = df['close'].ewm(span=w, adjust=False).mean()
        return df

    def add_slope_features(self, df, windows):
        for w in windows:
            df[f'slope_{w}'] = df['close'].diff(w) / w
        return df

    def add_ma_gap_features(self, df, fast_window=5, slow_window=20):
        fast_col = f'ma_{fast_window}'
        slow_col = f'ma_{slow_window}'
        if fast_col in df.columns and slow_col in df.columns:
            base = df[slow_col].replace(0, 1e-9)
            df[f'ma_gap_{fast_window}_{slow_window}'] = (df[fast_col] - df[slow_col]) / base
        return df

    def add_price_vs_average_features(self, df, windows):
        for w in windows:
            avg_col = f'ma_{w}'
            if avg_col in df.columns:
                base = df[avg_col].replace(0, 1e-9)
                df[f'close_vs_ma_{w}'] = (df['close'] - df[avg_col]) / base
        return df

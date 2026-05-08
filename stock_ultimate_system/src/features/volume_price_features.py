class VolumePriceFeatureBuilder:
    def add_volume_ma(self, df, windows):
        for w in windows:
            df[f'vol_ma_{w}'] = df['volume'].rolling(w).mean()
        return df

    def add_volume_ratio(self, df, window=5):
        df['volume_ratio'] = df['volume'] / df['volume'].rolling(window).mean().replace(0, 1e-9)
        return df

    def add_liquidity_features(self, df, window=10):
        df['dollar_volume'] = df['close'] * df['volume']
        df[f'dollar_volume_ma_{window}'] = df['dollar_volume'].rolling(window).mean()
        if 'turnover_rate' in df.columns:
            mean = df['turnover_rate'].rolling(window).mean()
            std = df['turnover_rate'].rolling(window).std().replace(0, 1e-9)
            df[f'turnover_zscore_{window}'] = (df['turnover_rate'] - mean) / std
        return df

class MomentumFeatureBuilder:
    def add_rsi(self, df, window=14):
        delta = df['close'].diff()
        gain = delta.clip(lower=0).rolling(window).mean()
        loss = -delta.clip(upper=0).rolling(window).mean()
        rs = gain / loss.replace(0, 1e-9)
        df['rsi'] = 100 - (100 / (1 + rs))
        return df

    def add_macd(self, df):
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        return df

    def add_return_features(self, df, windows=(1, 5, 10, 20)):
        for w in windows:
            df[f'return_{w}'] = df['close'].pct_change(w)
        return df

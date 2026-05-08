class MarketContextFeatureBuilder:
    def add_index_relative_strength(self, df):
        if 'index_close' in df.columns:
            df['rel_strength_index'] = df['close'].pct_change(20) - df['index_close'].pct_change(20)
            df['rel_strength_index_5'] = df['close'].pct_change(5) - df['index_close'].pct_change(5)
        return df

    def add_market_trend_context(self, df):
        if 'index_close' in df.columns:
            df['market_trend'] = (df['index_close'] > df['index_close'].rolling(20).mean()).astype(int)
            df['market_return_5'] = df['index_close'].pct_change(5)
            df['market_return_20'] = df['index_close'].pct_change(20)
        return df

from __future__ import annotations

import numpy as np
import pandas as pd


class LabelBuilder:
    """Build all label types specified in the development guide."""

    def build_direction_label(self, df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
        df[f'label_direction_{horizon}'] = (df['close'].shift(-horizon) > df['close']).astype(int)
        return df

    def build_return_label(self, df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
        df[f'label_return_{horizon}'] = df['close'].shift(-horizon) / df['close'] - 1
        return df

    def build_excess_return_label(self, df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
        if 'index_close' not in df.columns:
            return df
        stock_return = df['close'].shift(-horizon) / df['close'] - 1
        index_return = df['index_close'].shift(-horizon) / df['index_close'] - 1
        df[f'label_excess_return_{horizon}'] = stock_return - index_return
        return df

    def build_excess_direction_label(self, df: pd.DataFrame, horizon: int = 5) -> pd.DataFrame:
        if f'label_excess_return_{horizon}' not in df.columns:
            df = self.build_excess_return_label(df, horizon=horizon)
        if f'label_excess_return_{horizon}' in df.columns:
            df[f'label_excess_direction_{horizon}'] = (df[f'label_excess_return_{horizon}'] > 0).astype(int)
        return df

    def build_range_label(self, df: pd.DataFrame, horizon: int = 5, bins: int = 5) -> pd.DataFrame:
        """Discretise future returns into range buckets (0..bins-1)."""
        ret = df['close'].shift(-horizon) / df['close'] - 1
        df[f'label_range_{horizon}'] = pd.qcut(ret, q=bins, labels=False, duplicates='drop')
        return df

    def build_trade_success_label(self, df: pd.DataFrame, horizon: int = 5,
                                   tp_pct: float = 0.05, sl_pct: float = 0.03) -> pd.DataFrame:
        """1 if future high reaches take-profit before low hits stop-loss."""
        results = np.zeros(len(df), dtype=int)
        close = df['close'].values
        high = df['high'].values
        low = df['low'].values

        for i in range(len(df) - horizon):
            entry = close[i]
            tp = entry * (1 + tp_pct)
            sl = entry * (1 - sl_pct)
            success = 0
            for j in range(i + 1, min(i + horizon + 1, len(df))):
                if high[j] >= tp:
                    success = 1
                    break
                if low[j] <= sl:
                    success = 0
                    break
            results[i] = success

        df[f'label_trade_success_{horizon}'] = results
        return df

import pandas as pd


class DataCleaner:
    REQUIRED_COLUMNS = {
        'date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close',
        'volume', 'amount', 'turnover_rate',
    }

    def normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.strip().lower() for c in df.columns]
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates().reset_index(drop=True)

    def sort_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.sort_values('date').reset_index(drop=True)

    def fill_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        num_cols = df.select_dtypes(include='number').columns
        if len(num_cols) > 0:
            df[num_cols] = df[num_cols].replace([float('inf'), float('-inf')], pd.NA)
        return df.ffill().bfill()

    def validate_required_columns(self, df: pd.DataFrame) -> None:
        missing = self.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f'Missing required columns: {sorted(missing)}')

    def validate_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        # Keep only rows with valid OHLCV relationships and non-negative volumes.
        valid = (
            (df['high'] >= df['low'])
            & (df['high'] >= df['open'])
            & (df['high'] >= df['close'])
            & (df['low'] <= df['open'])
            & (df['low'] <= df['close'])
            & (df['volume'] >= 0)
            & (df['amount'] >= 0)
        )
        return df[valid].reset_index(drop=True)

    def mark_suspension(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'is_suspend' not in df.columns:
            df['is_suspend'] = 0
        suspend_mask = (df['volume'] <= 0) | (df['open'] <= 0) | (df['close'] <= 0)
        df.loc[suspend_mask, 'is_suspend'] = 1
        return df

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.normalize_column_names(df)
        self.validate_required_columns(df)
        df = self.remove_duplicates(df)
        df = self.sort_by_date(df)
        df = self.fill_missing_values(df)
        df = self.validate_ohlcv(df)
        df = self.mark_suspension(df)
        return df

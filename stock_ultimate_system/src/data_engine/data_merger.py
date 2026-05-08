import pandas as pd


class DataMerger:
    def merge_stock_index(self, stock_df: pd.DataFrame, index_df: pd.DataFrame) -> pd.DataFrame:
        x = index_df[['date', 'close']].rename(columns={'close': 'index_close'})
        return stock_df.merge(x, on='date', how='left')

    def merge_stock_sector(self, stock_df: pd.DataFrame, sector_df: pd.DataFrame) -> pd.DataFrame:
        x = sector_df[['date', 'close']].rename(columns={'close': 'sector_close'})
        return stock_df.merge(x, on='date', how='left')

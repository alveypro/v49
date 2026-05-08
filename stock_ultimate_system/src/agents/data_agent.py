from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from src.data_engine.data_fetcher import DataFetcher
from src.data_engine.data_cleaner import DataCleaner
from src.data_engine.data_merger import DataMerger
from src.data_engine.data_storage import DataStorage
from src.data_engine.trade_calendar_manager import TradeCalendarManager

logger = logging.getLogger(__name__)


class DataAgent:
    """Fetch, clean, merge, and cache datasets."""

    def __init__(self, config: dict) -> None:
        self.config = config
        self.fetcher = DataFetcher(config)
        self.cleaner = DataCleaner()
        self.merger = DataMerger()
        self.storage = DataStorage()
        self.calendar = TradeCalendarManager()
        self._cache: dict[str, pd.DataFrame] = {}

    def prepare_dataset(self, ts_code: str) -> pd.DataFrame:
        if ts_code in self._cache:
            return self._cache[ts_code].copy()

        settings = self.config.get('settings', self.config)
        data_cfg = settings.get('data', {})
        start = data_cfg.get('start_date', '2020-01-01')
        end = data_cfg.get('end_date', '2026-12-31')

        stock = self.fetcher.fetch_stock_daily(ts_code, start, end)
        index_df = self.fetcher.fetch_index_daily('000001.SH', start, end)
        sector_df = self.fetcher.fetch_sector_daily('BK0001', start, end)

        stock = self.cleaner.clean(stock)
        stock = self.merger.merge_stock_index(stock, index_df)
        stock = self.merger.merge_stock_sector(stock, sector_df)
        stock = stock[stock['date'].apply(self.calendar.is_trade_date)].reset_index(drop=True)

        self._cache[ts_code] = stock
        logger.info('Prepared dataset for %s: %d rows', ts_code, len(stock))
        return stock.copy()

    def fetch_pool(self, ts_codes: list[str], start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
        result = {}
        for code in ts_codes:
            try:
                result[code] = self.prepare_dataset(code)
            except Exception as e:
                logger.warning('Failed to prepare %s: %s', code, e)
        return result

    def load_market_dataset(self, ts_code: str) -> pd.DataFrame:
        return self.prepare_dataset(ts_code)

    def clear_cache(self) -> None:
        self._cache.clear()

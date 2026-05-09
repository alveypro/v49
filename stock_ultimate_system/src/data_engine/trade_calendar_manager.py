from __future__ import annotations

import pandas as pd


class TradeCalendarManager:
    """A-share trade calendar management."""

    def __init__(self) -> None:
        self._calendar: list[str] | None = None
        self._try_load_qlib_calendar()

    def _try_load_qlib_calendar(self) -> None:
        try:
            from qlib.data import D
            cal = D.calendar(start_time='2000-01-01', end_time='2030-12-31')
            self._calendar = [pd.Timestamp(d).strftime('%Y-%m-%d') for d in cal]
        except Exception:
            self._calendar = None

    def is_trade_date(self, date: str) -> bool:
        if self._calendar:
            return date in self._calendar
        return pd.Timestamp(date).weekday() < 5

    def get_trade_dates_between(self, start_date: str, end_date: str) -> list[str]:
        if self._calendar:
            return [d for d in self._calendar if start_date <= d <= end_date]
        return pd.bdate_range(start_date, end_date).strftime('%Y-%m-%d').tolist()

    def next_trade_date(self, date: str) -> str | None:
        if self._calendar:
            for d in self._calendar:
                if d > date:
                    return d
            return None
        ts = pd.Timestamp(date)
        for _ in range(10):
            ts += pd.Timedelta(days=1)
            if ts.weekday() < 5:
                return ts.strftime('%Y-%m-%d')
        return None

    def prev_trade_date(self, date: str) -> str | None:
        if self._calendar:
            prev = None
            for d in self._calendar:
                if d >= date:
                    return prev
                prev = d
            return prev
        ts = pd.Timestamp(date)
        for _ in range(10):
            ts -= pd.Timedelta(days=1)
            if ts.weekday() < 5:
                return ts.strftime('%Y-%m-%d')
        return None

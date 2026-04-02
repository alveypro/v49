from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


@dataclass(frozen=True)
class StockScanRow:
    row: pd.Series
    stock_data: pd.DataFrame
    score_result: Dict[str, Any]


def run_stock_scan_pipeline(
    *,
    stocks_df: pd.DataFrame,
    conn: Any,
    tag: str,
    min_history: int,
    load_history: Callable[[Any, str], pd.DataFrame],
    evaluate: Callable[[pd.Series, pd.DataFrame], Optional[Dict[str, Any]]],
    build_result: Callable[[StockScanRow], Optional[Dict[str, Any]]],
    on_progress: Optional[Callable[[str, int, int], None]] = None,
    on_no_result: Optional[Callable[[pd.Series, pd.DataFrame], None]] = None,
    on_exception: Optional[Callable[[pd.Series, Exception], None]] = None,
) -> List[Dict[str, Any]]:
    """Run shared stock scan loop and return built rows."""
    results: List[Dict[str, Any]] = []
    total = len(stocks_df)
    for idx, row in stocks_df.iterrows():
        if on_progress:
            on_progress(tag, int(idx), int(total))
        ts_code = str(row.get("ts_code", ""))
        stock_name = str(row.get("name", ""))
        try:
            stock_data = load_history(conn, ts_code)
            if stock_data is None or len(stock_data) < int(min_history):
                continue
            stock_data = stock_data.copy()
            stock_data["name"] = stock_name
            score_result = evaluate(row, stock_data)
            if not score_result:
                if on_no_result:
                    on_no_result(row, stock_data)
                continue
            out = build_result(StockScanRow(row=row, stock_data=stock_data, score_result=score_result))
            if out:
                results.append(out)
        except Exception as e:
            if on_exception:
                on_exception(row, e)
            continue
    return results

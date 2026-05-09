"""Tests for strategies.scan_pipeline — shared stock scan loop."""
from __future__ import annotations

from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pandas as pd
import pytest

from strategies.scan_pipeline import StockScanRow, run_stock_scan_pipeline


def _make_stocks_df() -> pd.DataFrame:
    return pd.DataFrame({
        "ts_code": ["000001.SZ", "600000.SH", "000002.SZ"],
        "name": ["平安银行", "浦发银行", "万科A"],
    })


def _make_stock_data(length: int = 100) -> pd.DataFrame:
    return pd.DataFrame({
        "close_price": [10.0 + i * 0.1 for i in range(length)],
        "vol": [100000] * length,
    })


class TestRunStockScanPipeline:
    def test_basic_scan(self):
        stocks_df = _make_stocks_df()
        results = run_stock_scan_pipeline(
            stocks_df=stocks_df,
            conn=None,
            tag="test",
            min_history=10,
            load_history=lambda conn, code: _make_stock_data(100),
            evaluate=lambda row, data: {"score": 85.0},
            build_result=lambda scan_row: {
                "ts_code": str(scan_row.row["ts_code"]),
                "score": scan_row.score_result["score"],
            },
        )
        assert len(results) == 3
        assert all(r["score"] == 85.0 for r in results)

    def test_skips_insufficient_history(self):
        stocks_df = _make_stocks_df()
        results = run_stock_scan_pipeline(
            stocks_df=stocks_df,
            conn=None,
            tag="test",
            min_history=200,
            load_history=lambda conn, code: _make_stock_data(50),
            evaluate=lambda row, data: {"score": 85.0},
            build_result=lambda scan_row: {"ts_code": "x"},
        )
        assert len(results) == 0

    def test_skips_when_evaluate_returns_none(self):
        stocks_df = _make_stocks_df()
        results = run_stock_scan_pipeline(
            stocks_df=stocks_df,
            conn=None,
            tag="test",
            min_history=10,
            load_history=lambda conn, code: _make_stock_data(100),
            evaluate=lambda row, data: None,
            build_result=lambda scan_row: {"ts_code": "x"},
        )
        assert len(results) == 0

    def test_exception_in_evaluate_is_caught(self):
        stocks_df = _make_stocks_df()

        def bad_evaluate(row, data):
            raise ValueError("boom")

        on_exception = MagicMock()
        results = run_stock_scan_pipeline(
            stocks_df=stocks_df,
            conn=None,
            tag="test",
            min_history=10,
            load_history=lambda conn, code: _make_stock_data(100),
            evaluate=bad_evaluate,
            build_result=lambda scan_row: {"ts_code": "x"},
            on_exception=on_exception,
        )
        assert len(results) == 0
        assert on_exception.call_count == 3

    def test_progress_callback(self):
        stocks_df = _make_stocks_df()
        on_progress = MagicMock()
        run_stock_scan_pipeline(
            stocks_df=stocks_df,
            conn=None,
            tag="myscan",
            min_history=10,
            load_history=lambda conn, code: _make_stock_data(100),
            evaluate=lambda row, data: {"score": 50},
            build_result=lambda scan_row: {"ts_code": "x"},
            on_progress=on_progress,
        )
        assert on_progress.call_count == 3
        on_progress.assert_any_call("myscan", 0, 3)

    def test_build_result_returning_none_is_skipped(self):
        stocks_df = _make_stocks_df()
        results = run_stock_scan_pipeline(
            stocks_df=stocks_df,
            conn=None,
            tag="test",
            min_history=10,
            load_history=lambda conn, code: _make_stock_data(100),
            evaluate=lambda row, data: {"score": 85.0},
            build_result=lambda scan_row: None,
        )
        assert len(results) == 0

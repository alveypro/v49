from __future__ import annotations

import pandas as pd

from openclaw.runtime.v8_evaluation_handler import build_v8_empty_result, evaluate_v8_signal


class FakeV7Evaluator:
    def __init__(self):
        self.calls = []

    def evaluate_stock_v7(self, stock_data, ts_code, industry):
        self.calls.append((stock_data["trade_date"].iloc[0], ts_code, industry))
        return {"success": True, "final_score": 70}


class FailingV7Evaluator:
    def evaluate_stock_v7(self, stock_data, ts_code, industry):
        return {"success": False, "final_score": 0, "reason": "base failed"}


def _stock_frame(rows: int = 70) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trade_date": list(reversed(range(rows))),
            "close_price": [10.0 + i * 0.1 for i in range(rows)],
            "high_price": [10.2 + i * 0.1 for i in range(rows)],
            "low_price": [9.8 + i * 0.1 for i in range(rows)],
            "vol": [1000.0 + i for i in range(rows)],
            "float_share": [100000.0 for _ in range(rows)],
        }
    )


def test_build_v8_empty_result_freezes_compat_payload():
    assert build_v8_empty_result("8.0") == {
        "success": False,
        "final_score": 0,
        "grade": "D",
        "star_rating": 0,
        "description": "数据不足或不符合标准",
        "version": "8.0",
    }


def test_evaluate_v8_signal_freezes_runtime_orchestration_happy_path():
    evaluator = FakeV7Evaluator()
    index_data = pd.DataFrame(
        {
            "trade_date": list(reversed(range(70))),
            "close": list(reversed([3000.0 + i for i in range(70)])),
            "volume": list(reversed([100000.0] * 65 + [120000.0] * 5)),
        }
    )

    result = evaluate_v8_signal(
        stock_data=_stock_frame(),
        version="8.0",
        base_evaluator=evaluator,
        ts_code="000001.SZ",
        index_data=index_data,
        industry_resolver=lambda ts_code: "银行",
        timestamp="2026-05-01 09:30:00",
    )

    assert result["success"] is True
    assert result["version"] == "8.0"
    assert result["timestamp"] == "2026-05-01 09:30:00"
    assert result["v7_score"] == 70.0
    assert result["advanced_factors"]["max_score"] == 140
    assert result["market_status"]["position_multiplier"] in {0.5, 0.7, 1.0}
    assert result["atr_stops"]["atr_value"] > 0
    assert evaluator.calls == [(0, "000001.SZ", "银行")]


def test_evaluate_v8_signal_returns_empty_for_short_data():
    result = evaluate_v8_signal(
        stock_data=_stock_frame(rows=20),
        version="8.0",
        base_evaluator=FakeV7Evaluator(),
    )

    assert result == build_v8_empty_result("8.0")


def test_evaluate_v8_signal_returns_base_failure_without_freezing_v8_payload():
    result = evaluate_v8_signal(
        stock_data=_stock_frame(),
        version="8.0",
        base_evaluator=FailingV7Evaluator(),
        ts_code="000001.SZ",
        industry="银行",
    )

    assert result == {"success": False, "final_score": 0, "reason": "base failed"}

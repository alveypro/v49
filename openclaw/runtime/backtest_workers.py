from __future__ import annotations

import traceback
from typing import Any, Callable, Dict, List

import pandas as pd


def run_single_backtest_worker(
    payload: Dict[str, Any],
    *,
    analyzer_factory: Callable[[], Any],
    load_history_df: Callable[..., pd.DataFrame],
) -> Dict[str, Any]:
    try:
        analyzer = analyzer_factory()
        df = load_history_df(days=int(payload.get("history_days", 240)))
        if df.empty:
            return {"success": False, "error": "无法获取历史数据"}
        strategy = str(payload.get("strategy", "v9.0 中线均衡版（生产）"))
        full_market_mode = bool(payload.get("full_market_mode", False))
        if full_market_mode:
            sample_size = max(1, int(df["ts_code"].nunique())) if "ts_code" in df.columns else len(df)
        else:
            sample_size = int(payload.get("sample_size", 800))
        holding_days = int(payload.get("holding_days", 10))
        score_threshold = float(payload.get("score_threshold", 65))
        if "v5.0" in strategy:
            result = analyzer.backtest_bottom_breakthrough(df, sample_size=sample_size, holding_days=holding_days)
        elif "v8.0" in strategy:
            result = analyzer.backtest_v8_ultimate(df, sample_size=sample_size, holding_days=holding_days, score_threshold=score_threshold)
        elif "组合策略" in strategy:
            result = analyzer.backtest_combo_production(
                df,
                sample_size=sample_size,
                holding_days=holding_days,
                combo_threshold=score_threshold,
                min_agree=2,
            )
        else:
            result = analyzer.backtest_v9_midterm(df, sample_size=sample_size, holding_days=holding_days, score_threshold=score_threshold)
        return {"success": bool(result.get("success")), "result": result}
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }


def run_comparison_backtest_worker(
    payload: Dict[str, Any],
    *,
    analyzer_factory: Callable[[], Any],
    load_history_df: Callable[..., pd.DataFrame],
) -> Dict[str, Any]:
    analyzer = analyzer_factory()
    df = load_history_df(days=int(payload.get("history_days", 240)))
    if df.empty:
        return {"success": False, "error": "无法获取历史数据"}

    sample_size = int(payload.get("sample_size", 300))
    full_market_mode = bool(payload.get("full_market_mode", False))
    strategy_params = payload.get("strategy_params") or {}
    v5_hold = int((strategy_params.get("v5") or {}).get("holding_days", 8))
    v8_hold = int((strategy_params.get("v8") or {}).get("holding_days", 10))
    v8_thr = float((strategy_params.get("v8") or {}).get("score_threshold", 65))
    v9_hold = int((strategy_params.get("v9") or {}).get("holding_days", 20))
    v9_thr = float((strategy_params.get("v9") or {}).get("score_threshold", 65))
    combo_hold = int((strategy_params.get("combo") or {}).get("holding_days", 10))
    combo_thr = float((strategy_params.get("combo") or {}).get("score_threshold", 68))
    validation_mode = str(payload.get("validation_mode", "快速全样本"))
    wf_folds = int(payload.get("wf_folds", 4))
    wf_window_days = int(payload.get("wf_window_days", 180))
    wf_step_days = int(payload.get("wf_step_days", 40))

    def _run_once(df_slice: pd.DataFrame, sample_for_run: int) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        resolved_sample_size = (
            max(1, int(df_slice["ts_code"].nunique())) if full_market_mode and "ts_code" in df_slice.columns else int(sample_for_run)
        )
        v5 = analyzer.backtest_bottom_breakthrough(df_slice, sample_size=resolved_sample_size, holding_days=v5_hold)
        if v5.get("success"):
            out["v5.0 趋势趋势版"] = v5.get("stats", {})
        v8 = analyzer.backtest_v8_ultimate(df_slice, sample_size=resolved_sample_size, holding_days=v8_hold, score_threshold=v8_thr)
        if v8.get("success"):
            out["v8.0 进阶版"] = v8.get("stats", {})
        v9 = analyzer.backtest_v9_midterm(df_slice, sample_size=resolved_sample_size, holding_days=v9_hold, score_threshold=v9_thr)
        if v9.get("success"):
            out["v9.0 中线均衡版"] = v9.get("stats", {})
        combo = analyzer.backtest_combo_production(
            df_slice,
            sample_size=resolved_sample_size,
            holding_days=combo_hold,
            combo_threshold=combo_thr,
            min_agree=2,
        )
        if combo.get("success"):
            out["组合策略（生产共识）"] = combo.get("stats", {})
        return out

    def _aggregate(fold_results: List[Dict[str, Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
        by_strategy: Dict[str, List[Dict[str, Any]]] = {}
        for fold_result in fold_results:
            for strategy_name, stats in fold_result.items():
                by_strategy.setdefault(strategy_name, []).append(stats)
        out: Dict[str, Dict[str, Any]] = {}
        for strategy_name, rows in by_strategy.items():
            total_signals = float(sum(float(row.get("total_signals", 0) or 0) for row in rows))
            weight = total_signals if total_signals > 0 else float(max(1, len(rows)))

            def _wavg(field: str) -> float:
                if total_signals > 0:
                    return float(sum(float(row.get(field, 0) or 0) * float(row.get("total_signals", 0) or 0) for row in rows) / weight)
                return float(sum(float(row.get(field, 0) or 0) for row in rows) / max(1, len(rows)))

            out[strategy_name] = {
                "total_signals": int(total_signals),
                "analyzed_stocks": int(sum(int(row.get("analyzed_stocks", 0) or 0) for row in rows)),
                "win_rate": _wavg("win_rate"),
                "avg_return": _wavg("avg_return"),
                "median_return": _wavg("median_return"),
                "max_return": max(float(row.get("max_return", -999) or -999) for row in rows),
                "min_return": min(float(row.get("min_return", 999) or 999) for row in rows),
                "sharpe_ratio": _wavg("sharpe_ratio"),
                "sortino_ratio": _wavg("sortino_ratio"),
                "max_drawdown": _wavg("max_drawdown"),
                "profit_loss_ratio": _wavg("profit_loss_ratio"),
                "avg_holding_days": _wavg("avg_holding_days"),
                "annualized_return": _wavg("annualized_return"),
                "volatility": _wavg("volatility"),
                "fold_count": len(rows),
            }
        return out

    meta: Dict[str, Any] = {"validation_mode": validation_mode}
    if validation_mode.startswith("Walk-forward"):
        unique_dates = sorted([str(x) for x in df["trade_date"].dropna().unique()])
        fold_results: List[Dict[str, Dict[str, Any]]] = []
        fold_ranges: List[str] = []
        fold_details: List[Dict[str, Any]] = []
        pos = max(0, len(unique_dates) - int(wf_window_days))
        sample_for_run = max(60, min(int(sample_size), int(300 / max(1, int(wf_folds)))))
        for i in range(int(wf_folds)):
            start_pos = pos - i * int(wf_step_days)
            end_pos = start_pos + int(wf_window_days)
            if start_pos < 0 or end_pos > len(unique_dates):
                continue
            d0 = unique_dates[start_pos]
            d1 = unique_dates[end_pos - 1]
            df_fold = df[(df["trade_date"] >= d0) & (df["trade_date"] <= d1)].copy()
            if df_fold.empty:
                continue
            fold_ranges.append(f"{d0}-{d1}")
            one = _run_once(df_fold, sample_for_run)
            if one:
                fold_results.append(one)
                for strategy_name, stats in one.items():
                    fold_details.append(
                        {
                            "fold": int(i + 1),
                            "range": f"{d0}-{d1}",
                            "strategy": strategy_name,
                            "win_rate": float(stats.get("win_rate", 0) or 0),
                            "avg_return": float(stats.get("avg_return", 0) or 0),
                            "sharpe_ratio": float(stats.get("sharpe_ratio", 0) or 0),
                            "max_drawdown": float(stats.get("max_drawdown", 0) or 0),
                            "total_signals": int(stats.get("total_signals", 0) or 0),
                        }
                    )
        fold_results = list(reversed(fold_results))
        results = _aggregate(fold_results)
        meta.update(
            {
                "fold_count": len(fold_results),
                "fold_ranges": fold_ranges,
                "fold_details": fold_details,
                "wf_window_days": int(wf_window_days),
                "wf_step_days": int(wf_step_days),
            }
        )
    else:
        results = _run_once(df, int(sample_size))
    return {"success": bool(results), "results": results, "meta": meta}

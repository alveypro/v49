from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd


def calculate_relative_strength_momentum(
    stock_returns: pd.Series,
    index_returns: pd.Series,
    window: int = 60,
) -> Dict:
    stock_cum_return = (1 + stock_returns.iloc[-window:]).prod() - 1
    index_cum_return = (1 + index_returns.iloc[-window:]).prod() - 1

    if index_cum_return != 0:
        rsm = stock_cum_return / index_cum_return
    else:
        rsm = 1.0

    if rsm > 1.8:
        score = 15
        grade = "超级强势"
    elif rsm > 1.4:
        score = 12
        grade = "极强"
    elif rsm > 1.15:
        score = 9
        grade = "强势"
    elif rsm > 0.95:
        score = 6
        grade = "略强"
    elif rsm > 0.8:
        score = 3
        grade = "稍弱"
    else:
        score = 1
        grade = "弱势"

    return {
        "rsm": round(rsm, 2),
        "score": score,
        "grade": grade,
    }


def calculate_momentum_acceleration(returns: pd.Series) -> Dict:
    recent_return = returns.iloc[-10:].sum()
    previous_return = returns.iloc[-20:-10].sum()

    if previous_return != 0:
        acceleration = (recent_return - previous_return) / abs(previous_return)
    else:
        acceleration = 0

    if acceleration > 0.5:
        score = 12
        grade = "极速加速"
    elif acceleration > 0.2:
        score = 9
        grade = "强加速"
    elif acceleration > 0:
        score = 6
        grade = "温和加速"
    elif acceleration > -0.2:
        score = 3
        grade = "稳定"
    else:
        score = 1
        grade = "减速"

    return {
        "acceleration": round(acceleration, 2),
        "score": score,
        "grade": grade,
    }


def calculate_momentum_persistence(close: pd.Series, window: int = 60) -> Dict:
    rolling_max = close.rolling(window=window).max()
    new_highs = (close == rolling_max).astype(int).iloc[-window:].sum()

    if new_highs >= 8:
        score = 12
        grade = "强势突破"
    elif new_highs >= 5:
        score = 9
        grade = "持续强势"
    elif new_highs >= 3:
        score = 7
        grade = "间歇强势"
    elif new_highs >= 1:
        score = 5
        grade = "有突破"
    else:
        score = 3
        grade = "震荡"

    return {
        "new_highs_count": int(new_highs),
        "score": score,
        "grade": grade,
    }


def calculate_obv_energy(close: pd.Series, volume: pd.Series) -> Dict:
    direction = np.sign(close.diff())
    obv = (direction * volume).cumsum()

    if len(obv) >= 20:
        recent_obv = obv.iloc[-20:]
        x = np.arange(len(recent_obv))
        slope = np.polyfit(x, recent_obv, 1)[0]

        price_trend = close.iloc[-1] - close.iloc[-20]
        obv_trend = obv.iloc[-1] - obv.iloc[-20]

        divergence = False
        if price_trend > 0 and obv_trend < 0:
            divergence = True
            score = 3
            grade = "顶背离"
        elif price_trend < 0 and obv_trend > 0:
            divergence = True
            score = 13
            grade = "底背离买入"
        elif slope > 0 and price_trend > 0:
            score = 12
            grade = "量价共振"
        elif slope > 0 or price_trend > 0:
            score = 7
            grade = "量价配合"
        else:
            score = 4
            grade = "量价正常"
    else:
        slope = 0
        divergence = False
        score = 5
        grade = "数据不足"

    return {
        "obv_slope": round(float(slope), 2),
        "divergence": divergence,
        "score": score,
        "grade": grade,
    }


def calculate_chip_concentration(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    window: int = 20,
) -> Dict:
    price_change_pct = close.pct_change().abs()
    avg_volume = volume.rolling(window=window).mean()
    big_orders = ((price_change_pct > 0.015) & (volume > avg_volume * 0.8)).astype(int)
    big_order_ratio = big_orders.iloc[-window:].sum() / window

    if big_order_ratio > 0.4:
        score = 15
        grade = "强力控盘"
    elif big_order_ratio > 0.25:
        score = 12
        grade = "高度控盘"
    elif big_order_ratio > 0.15:
        score = 9
        grade = "中度控盘"
    elif big_order_ratio > 0.05:
        score = 6
        grade = "有主力参与"
    else:
        score = 4
        grade = "散户为主"

    return {
        "concentration_ratio": round(big_order_ratio, 2),
        "score": score,
        "grade": grade,
    }


def calculate_valuation_repair(close: pd.Series, volume: pd.Series) -> Dict:
    ma60 = close.rolling(window=60).mean()
    current_price = close.iloc[-1]
    avg_price_60d = ma60.iloc[-1] if len(ma60) > 0 else current_price

    if avg_price_60d > 0:
        price_ratio = current_price / avg_price_60d
        if price_ratio < 0.85:
            score = 12
            grade = "深度折价"
        elif price_ratio < 0.92:
            score = 9
            grade = "明显折价"
        elif price_ratio < 0.98:
            score = 7
            grade = "轻微折价"
        elif price_ratio <= 1.05:
            score = 5
            grade = "合理估值"
        else:
            score = 2
            grade = "偏高估"
    else:
        score = 5
        grade = "无法判断"

    return {
        "score": score,
        "grade": grade,
        "price_ratio": round(price_ratio, 2) if avg_price_60d > 0 else 1.0,
    }


def calculate_turnover_momentum(
    volume: pd.Series,
    turnover_rate: pd.Series,
    window: int = 20,
) -> Dict:
    vol_ma = volume.rolling(window).mean()
    vol_rel = volume / vol_ma
    turnover_ma = turnover_rate.rolling(window).mean()
    turnover_rel = turnover_rate / turnover_ma

    vol_rel_recent = vol_rel.iloc[-5:].mean()
    turnover_rel_recent = turnover_rel.iloc[-5:].mean()

    if vol_rel_recent > 1.5 and turnover_rel_recent > 1.3:
        score = 12
        grade = "放量强换手"
    elif vol_rel_recent > 1.2 and turnover_rel_recent > 1.1:
        score = 9
        grade = "稳步放量"
    elif vol_rel_recent > 1.0 and turnover_rel_recent > 1.0:
        score = 6
        grade = "轻微放量"
    else:
        score = 4
        grade = "正常"

    return {
        "score": score,
        "grade": grade,
        "vol_rel": round(vol_rel_recent, 2) if pd.notna(vol_rel_recent) else 1.0,
        "turnover_rel": round(turnover_rel_recent, 2) if pd.notna(turnover_rel_recent) else 1.0,
    }


def calculate_profit_quality(close: pd.Series, volume: pd.Series, pct_chg: pd.Series) -> Dict:
    recent_returns = pct_chg.iloc[-20:]
    positive_days = (recent_returns > 0).sum()
    avg_return = recent_returns.mean()
    return_std = recent_returns.std()
    stability = positive_days / 20

    if stability > 0.6 and return_std < 2.0 and avg_return > 0:
        score = 10
        grade = "优质上涨"
    elif stability > 0.5 and avg_return > 0:
        score = 8
        grade = "稳健上涨"
    elif stability > 0.4:
        score = 6
        grade = "震荡向上"
    else:
        score = 3
        grade = "波动较大"

    return {
        "score": score,
        "grade": grade,
        "stability": round(stability, 2),
    }


def calculate_capital_flow(close: pd.Series, volume: pd.Series, pct_chg: pd.Series) -> Dict:
    recent_vol = volume.iloc[-10:]
    recent_chg = pct_chg.iloc[-10:]
    base_vol = recent_vol.mean()

    inflow_score = 0
    for i in range(len(recent_chg)):
        price_up = recent_chg.iloc[i] > 0
        vol_up = i > 0 and recent_vol.iloc[i] > recent_vol.iloc[i - 1]
        vol_rel = recent_vol.iloc[i] / base_vol if base_vol > 0 else 1.0

        vol_weight = 1.0
        if vol_rel > 1.5:
            vol_weight = 1.4
        elif vol_rel > 1.2:
            vol_weight = 1.2

        if price_up:
            inflow_score += 1.0 * vol_weight
        elif vol_up:
            inflow_score -= 1.2
        else:
            inflow_score -= 0.3

    if inflow_score > 10:
        score = 12
        grade = "强势流入"
    elif inflow_score > 6:
        score = 9
        grade = "持续流入"
    elif inflow_score > 3:
        score = 6
        grade = "缓慢流入"
    elif inflow_score > 0:
        score = 4
        grade = "弱流入"
    else:
        score = 1
        grade = "资金流出"

    return {
        "score": score,
        "grade": grade,
        "inflow_score": round(inflow_score, 2),
        "avg_vol_rel": round(vol_rel, 2) if "vol_rel" in locals() else 1.0,
    }


def calculate_sector_resonance(
    stock_returns: pd.Series,
    index_data: Optional[pd.DataFrame],
    index_returns: pd.Series,
) -> Dict:
    if index_data is None or len(index_data) < 20:
        return {"score": 6, "grade": "无大盘对比"}

    stock_return_20d = (1 + stock_returns.iloc[-20:]).prod() - 1
    index_return_20d = (1 + index_returns.iloc[-20:]).prod() - 1
    excess_return = stock_return_20d - index_return_20d

    if excess_return > 0.15:
        score = 10
        grade = "强势领涨"
    elif excess_return > 0.08:
        score = 8
        grade = "明显领先"
    elif excess_return > 0.03:
        score = 6
        grade = "略微领先"
    elif excess_return > -0.03:
        score = 5
        grade = "跟随大盘"
    else:
        score = 3
        grade = "弱于大盘"

    extra = 0
    if hasattr(index_data, "columns"):
        if "up_count" in index_data.columns:
            up_count = index_data["up_count"].iloc[-1]
            if up_count >= 50:
                extra += 2
            elif up_count >= 30:
                extra += 1
        if "strong_count" in index_data.columns:
            strong_count = index_data["strong_count"].iloc[-1]
            if strong_count >= 20:
                extra += 1

    score = min(12, score + extra)
    return {
        "score": score,
        "grade": grade,
        "excess_return": round(excess_return * 100, 2),
    }


def calculate_smart_money(close: pd.Series, volume: pd.Series, pct_chg: pd.Series) -> Dict:
    recent_30d = slice(-30, None)
    recent_close = close.iloc[recent_30d]
    recent_vol = volume.iloc[recent_30d]
    recent_chg = pct_chg.iloc[recent_30d]

    price_trend = (recent_close.iloc[-1] - recent_close.iloc[0]) / recent_close.iloc[0]
    is_gradual_rise = 0.03 < price_trend < 0.25

    vol_first_half = recent_vol.iloc[:15].mean()
    vol_second_half = recent_vol.iloc[15:].mean()
    vol_increasing = vol_second_half > vol_first_half * 1.1

    volatility_first = recent_chg.iloc[:15].std()
    volatility_second = recent_chg.iloc[15:].std()
    vol_decreasing = volatility_second < volatility_first

    smart_features = sum([is_gradual_rise, vol_increasing, vol_decreasing])

    if smart_features == 3 and price_trend > 0.08:
        score = 15
        grade = "机构重点"
    elif smart_features >= 2 and price_trend > 0:
        score = 11
        grade = "机构关注"
    elif smart_features >= 1:
        score = 7
        grade = "有建仓迹象"
    else:
        score = 4
        grade = "普通"

    return {
        "score": score,
        "grade": grade,
        "smart_features": smart_features,
        "price_trend": round(price_trend * 100, 2),
    }

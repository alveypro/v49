"""Point-in-time context adapters for v6 runtime backtests."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

import pandas as pd


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _trade_date_key(df: pd.DataFrame) -> pd.Series:
    if "trade_date" not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df["trade_date"].astype(str)


def _last_nonempty_string(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df.columns:
        return ""
    values = df[column].dropna()
    if values.empty:
        return ""
    return str(values.iloc[-1] or "")


def build_v6_runtime_diagnostics(score_diagnostics: Dict[str, Any], *, replay_step: int) -> Dict[str, Any]:
    threshold = float(score_diagnostics.get("threshold", 0.0) or 0.0)
    evaluated = int(score_diagnostics.get("evaluated", 0) or 0)
    near = score_diagnostics.get("near_threshold") if isinstance(score_diagnostics.get("near_threshold"), dict) else {}
    breakdown = score_diagnostics.get("score_breakdown") if isinstance(score_diagnostics.get("score_breakdown"), dict) else {}
    risk_penalty = breakdown.get("risk_penalty") if isinstance(breakdown.get("risk_penalty"), dict) else {}
    return {
        "type": "v6_runtime_diagnostics",
        "point_in_time_context": True,
        "production_candidate_allowed": False,
        "candidate_filter_mode": str(score_diagnostics.get("candidate_filter_mode", "strict") or "strict"),
        "candidate_filter_relaxed_count": int(
            (score_diagnostics.get("reason_counts") or {}).get("candidate_filter_relaxed", 0) or 0
        ),
        "replay_step": int(max(1, replay_step)),
        "short_cycle_noise_review": {
            "enabled": True,
            "replay_step": int(max(1, replay_step)),
            "coarse_step": int(replay_step) > 5,
            "reason": "short_cycle_strategy_requires_dense_replay_review",
        },
        "threshold_near_samples": {
            "threshold": threshold,
            "evaluated": evaluated,
            "within_2": int(near.get("within_2", 0) or 0),
            "within_5": int(near.get("within_5", 0) or 0),
            "within_10": int(near.get("within_10", 0) or 0),
        },
        "factor_distribution": {
            str(key): value
            for key, value in breakdown.items()
            if str(key).startswith("dim:") or str(key) in {"base_score", "synergy_bonus", "risk_penalty"}
        },
        "risk_penalty_avg": float(risk_penalty.get("avg", 0.0) or 0.0),
    }


@dataclass
class PointInTimeV6DataProvider:
    """V6 data provider derived only from rows visible at the replay timestamp."""

    market_frame: pd.DataFrame
    stock_hist: pd.DataFrame
    as_of_date: str
    cache: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.cache is None:
            self.cache = {}
        frame_key = f"market_frame:{self.as_of_date}"
        cached_frame = self.cache.get(frame_key)
        if isinstance(cached_frame, pd.DataFrame):
            frame = cached_frame
        else:
            base_key = "market_frame:prepared"
            base = self.cache.get(base_key)
            if isinstance(base, pd.DataFrame):
                frame = base
            else:
                frame = self.market_frame.copy() if self.market_frame is not None else pd.DataFrame()
                if not frame.empty and "trade_date" in frame.columns:
                    frame["_trade_date_key"] = _trade_date_key(frame)
                    frame = frame.sort_values("trade_date").reset_index(drop=True)
                self.cache[base_key] = frame
            if not frame.empty and "trade_date" in frame.columns:
                if "_trade_date_key" not in frame.columns:
                    frame = frame.copy()
                    frame["_trade_date_key"] = _trade_date_key(frame)
                frame = frame[frame["_trade_date_key"] <= str(self.as_of_date)].copy()
                dates = frame["_trade_date_key"].drop_duplicates().sort_values()
                if len(dates) > 80:
                    frame = frame[frame["_trade_date_key"] >= str(dates.iloc[-80])].copy()
            self.cache[frame_key] = frame
        self.market_frame = frame
        hist = self.stock_hist.copy() if self.stock_hist is not None else pd.DataFrame()
        if not hist.empty and "trade_date" in hist.columns:
            hist["_trade_date_key"] = _trade_date_key(hist)
            hist = hist[hist["_trade_date_key"] <= str(self.as_of_date)].copy()
        self.stock_hist = hist
        self._stock_industry = _last_nonempty_string(hist, "industry")
        self._stock_name = _last_nonempty_string(hist, "name")

    def get_stock_sector(self, ts_code: str) -> Dict[str, Any]:
        industry = str(getattr(self, "_stock_industry", "") or "")
        name = str(getattr(self, "_stock_name", "") or "")
        if not industry:
            meta = self._stock_sector_meta().get(str(ts_code), {})
            industry = str(meta.get("industry", "") or "")
            name = str(meta.get("name", "") or name or "")
        concepts = []
        hot_keywords = {
            "新能源": ["新能源", "锂电", "光伏", "储能"],
            "人工智能": ["AI", "人工智能", "大模型", "算力"],
            "芯片": ["芯片", "半导体", "集成电路"],
            "生物医药": ["医药", "生物", "疫苗", "医疗"],
        }
        for concept, keywords in hot_keywords.items():
            if any(keyword in name or keyword in industry for keyword in keywords):
                concepts.append(concept)
        return {"industry": industry or "其他", "concept": concepts[:3], "area": "未知"}

    def _stock_sector_meta(self) -> Dict[str, Dict[str, str]]:
        cache_key = f"stock_sector_meta:{self.as_of_date}"
        cached = self.cache.get(cache_key) if self.cache is not None else None
        if isinstance(cached, dict):
            return cached
        latest = self._latest_stock_meta_frame()
        if latest.empty:
            return {}
        out: Dict[str, Dict[str, str]] = {}
        for row in latest.itertuples(index=False):
            payload = row._asdict()
            code = str(payload.get("ts_code", "") or "")
            if not code:
                continue
            out[code] = {
                "industry": str(payload.get("industry", "") or ""),
                "name": str(payload.get("name", "") or ""),
            }
        if self.cache is not None:
            self.cache[cache_key] = out
        return out

    def _latest_stock_meta_frame(self) -> pd.DataFrame:
        cache_key = f"latest_stock_meta:{self.as_of_date}"
        cached = self.cache.get(cache_key) if self.cache is not None else None
        if isinstance(cached, pd.DataFrame):
            return cached
        frame = self._market_frame_by_code()
        if frame.empty or "ts_code" not in frame.columns:
            return pd.DataFrame(columns=["ts_code", "industry", "name"])
        columns = [c for c in ("ts_code", "industry", "name") if c in frame.columns]
        latest = frame[columns].drop_duplicates(subset=["ts_code"], keep="last").copy()
        if self.cache is not None:
            self.cache[cache_key] = latest
        return latest

    def _market_frame_by_code(self) -> pd.DataFrame:
        cache_key = f"market_frame_by_code:{self.as_of_date}"
        cached = self.cache.get(cache_key) if self.cache is not None else None
        if isinstance(cached, pd.DataFrame):
            return cached
        if self.market_frame.empty or "ts_code" not in self.market_frame.columns:
            return pd.DataFrame()
        columns = [
            c
            for c in ("ts_code", "trade_date", "_trade_date_key", "industry", "name", "pct_chg")
            if c in self.market_frame.columns
        ]
        frame = self.market_frame[columns].copy()
        if "_trade_date_key" not in frame.columns and "trade_date" in frame.columns:
            frame["_trade_date_key"] = _trade_date_key(frame)
        if "pct_chg" in frame.columns:
            frame["pct_chg"] = pd.to_numeric(frame["pct_chg"], errors="coerce").fillna(0.0)
        sort_cols = ["ts_code"]
        if "_trade_date_key" in frame.columns:
            sort_cols.append("_trade_date_key")
        elif "trade_date" in frame.columns:
            sort_cols.append("trade_date")
        frame = frame.sort_values(sort_cols).reset_index(drop=True)
        if self.cache is not None:
            self.cache[cache_key] = frame
        return frame

    def get_sector_performance(self, industry: str, days: int = 3) -> Dict[str, Any]:
        cache_key = f"sector_performance:{self.as_of_date}:{int(days)}"
        cached_perf = self.cache.get(cache_key) if self.cache is not None else None
        if isinstance(cached_perf, pd.DataFrame):
            perf = cached_perf
            total = int(len(perf)) or 1
            row = perf[perf["industry"].astype(str) == str(industry)]
            if row.empty:
                return {"change_3d": 0, "avg_change": 0, "rank": total, "total_industries": total, "money_flow": 0, "data_available": False, "proxy_source": "industry_not_observable_as_of_date"}
            r = row.iloc[0]
            change = round(float(r["mean"]), 2)
            return {"change_3d": change, "avg_change": round(change / max(int(days), 1), 2), "rank": int(r["rank"]), "total_industries": total, "money_flow": 0, "stock_count": int(r["count"]), "data_available": True, "proxy_source": "pit_market_frame_industry_pct_chg"}
        if self.market_frame.empty or "industry" not in self.market_frame.columns or "pct_chg" not in self.market_frame.columns:
            return {"change_3d": 0, "avg_change": 0, "rank": 50, "total_industries": 100, "money_flow": 0, "data_available": False, "proxy_source": "missing_market_frame_industry_pct_chg"}
        stock_recent = self._stock_recent_change_frame(days=int(days))
        if stock_recent.empty:
            return {"change_3d": 0, "avg_change": 0, "rank": 50, "total_industries": 100, "money_flow": 0, "data_available": False, "proxy_source": "insufficient_recent_industry_window"}
        perf = stock_recent.groupby("industry")["change"].agg(["mean", "count"]).reset_index()
        perf = perf[perf["count"] >= 1].sort_values("mean", ascending=False).reset_index(drop=True)
        perf["rank"] = range(1, len(perf) + 1)
        if self.cache is not None:
            self.cache[cache_key] = perf
        total = int(len(perf)) or 1
        row = perf[perf["industry"].astype(str) == str(industry)]
        if row.empty:
            return {"change_3d": 0, "avg_change": 0, "rank": total, "total_industries": total, "money_flow": 0, "data_available": False, "proxy_source": "industry_not_observable_as_of_date"}
        r = row.iloc[0]
        change = round(float(r["mean"]), 2)
        return {"change_3d": change, "avg_change": round(change / max(int(days), 1), 2), "rank": int(r["rank"]), "total_industries": total, "money_flow": 0, "stock_count": int(r["count"]), "data_available": True, "proxy_source": "pit_market_frame_industry_pct_chg"}

    def _stock_recent_change_frame(self, days: int) -> pd.DataFrame:
        cache_key = f"stock_recent_change:{self.as_of_date}:{int(days)}"
        cached = self.cache.get(cache_key) if self.cache is not None else None
        if isinstance(cached, pd.DataFrame):
            return cached
        frame = self._market_frame_by_code()
        required = {"ts_code", "industry", "pct_chg"}
        if frame.empty or not required.issubset(set(frame.columns)):
            return pd.DataFrame(columns=["ts_code", "industry", "change", "count"])
        columns = [c for c in ("ts_code", "trade_date", "_trade_date_key", "industry", "pct_chg") if c in frame.columns]
        frame = frame[columns].dropna(subset=["ts_code", "industry"]).copy()
        if frame.empty:
            return pd.DataFrame(columns=["ts_code", "industry", "change", "count"])
        if "_trade_date_key" not in frame.columns and "trade_date" in frame.columns:
            frame["_trade_date_key"] = _trade_date_key(frame)
        if "_trade_date_key" in frame.columns:
            recent_dates = frame["_trade_date_key"].drop_duplicates().sort_values().tail(int(days)).tolist()
            frame = frame[frame["_trade_date_key"].isin(recent_dates)].copy()
        grouped = frame.groupby("ts_code", sort=False).agg(
            industry=("industry", "last"),
            change=("pct_chg", "sum"),
            count=("pct_chg", "count"),
        ).reset_index()
        grouped = grouped[grouped["count"] >= int(days)].copy()
        if self.cache is not None:
            self.cache[cache_key] = grouped
        return grouped

    def get_money_flow(self, ts_code: str, days: int = 3) -> Dict[str, Any]:
        cache_key = f"money_flow:{self.as_of_date}:{ts_code}:{int(days)}"
        cached = self.cache.get(cache_key) if self.cache is not None else None
        if isinstance(cached, dict):
            return dict(cached)
        hist = self.stock_hist.sort_values("trade_date").copy()
        if hist.empty or "vol" not in hist.columns or "pct_chg" not in hist.columns:
            return self._default_money_flow(data_available=False, proxy_source="missing_stock_hist_vol_pct_chg")
        recent = hist.tail(int(days))
        baseline = hist.iloc[: max(len(hist) - int(days), 0)]
        if len(recent) < int(days) or baseline.empty:
            return self._default_money_flow(data_available=False, proxy_source="insufficient_stock_money_flow_window")
        avg_vol = _as_float(pd.to_numeric(baseline["vol"], errors="coerce").mean())
        if avg_vol <= 0:
            return self._default_money_flow(data_available=False, proxy_source="invalid_baseline_volume")
        net_flow = 0.0
        consecutive = 0
        latest_day_flow = 0.0
        for _, row in recent.iterrows():
            vol_ratio = _as_float(row.get("vol")) / avg_vol
            price_chg = _as_float(row.get("pct_chg"))
            day_flow = 0.0
            if price_chg > 0 and vol_ratio > 1.0:
                day_flow = price_chg * (vol_ratio - 1.0) * 8000.0
                net_flow += day_flow
                consecutive += 1
            elif price_chg < 0 and vol_ratio > 1.0:
                day_flow = price_chg * (vol_ratio - 1.0) * 8000.0
                net_flow += day_flow
                consecutive = 0
            else:
                consecutive = 0
            latest_day_flow = day_flow
        out = {
            "net_mf_amount": round(net_flow, 2),
            "consecutive_inflow_days": int(consecutive),
            "buy_lg_amount": max(0.0, round(net_flow, 2)),
            "sell_lg_amount": max(0.0, round(-net_flow, 2)),
            "buy_elg_amount": 0.0,
            "sell_elg_amount": 0.0,
            "today_net": round(latest_day_flow, 2),
            "data_available": True,
            "proxy_source": "pit_price_volume_money_flow_proxy",
        }
        if self.cache is not None:
            self.cache[cache_key] = dict(out)
        return out

    def get_north_money_flow(self, ts_code: str, days: int = 3) -> Dict[str, Any]:
        return {"buy_amount": 0, "sell_amount": 0, "net_amount": 0, "north_net_3d": 0, "consecutive_buy_days": 0, "is_connect_stock": False}

    def get_market_change(self, days: int = 3) -> float:
        cache_key = f"market_change:{self.as_of_date}:{int(days)}"
        cached = self.cache.get(cache_key) if self.cache is not None else None
        if cached is not None:
            return _as_float(cached)
        frame = self.market_frame
        if frame.empty or "pct_chg" not in frame.columns:
            return 0.0
        if "ts_code" in frame.columns:
            index_rows = frame[frame["ts_code"].astype(str) == "000001.SH"].sort_values("trade_date")
            if len(index_rows) >= int(days):
                out = float(pd.to_numeric(index_rows.tail(int(days))["pct_chg"], errors="coerce").fillna(0).sum())
                if self.cache is not None:
                    self.cache[cache_key] = out
                return out
        daily = frame.groupby("trade_date")["pct_chg"].mean().sort_index().tail(int(days))
        out = float(pd.to_numeric(daily, errors="coerce").fillna(0).sum()) if len(daily) else 0.0
        if self.cache is not None:
            self.cache[cache_key] = out
        return out

    def _default_money_flow(self, *, data_available: bool = False, proxy_source: str = "unavailable") -> Dict[str, Any]:
        return {"buy_lg_amount": 0, "sell_lg_amount": 0, "net_mf_amount": 0, "buy_elg_amount": 0, "sell_elg_amount": 0, "consecutive_inflow_days": 0, "today_net": 0, "data_available": bool(data_available), "proxy_source": str(proxy_source)}


@dataclass
class PointInTimeV6LeaderAnalyzer:
    provider: PointInTimeV6DataProvider

    def calculate_leader_score(self, ts_code: str, industry: str, recent_change_3d: float) -> Dict[str, Any]:
        score_key = f"leader_score:{self.provider.as_of_date}:{industry}:{ts_code}"
        cached_score = self.provider.cache.get(score_key) if self.provider.cache is not None else None
        if isinstance(cached_score, dict):
            return dict(cached_score)
        frame = self.provider.market_frame
        if frame.empty or "ts_code" not in frame.columns or "pct_chg" not in frame.columns:
            return self._default_leader_score()
        cache_key = f"leader_ranking:{self.provider.as_of_date}:{industry}"
        ranking = self.provider.cache.get(cache_key) if self.provider.cache is not None else None
        if not isinstance(ranking, pd.DataFrame):
            recent = self.provider._stock_recent_change_frame(days=3)
            if recent.empty:
                return self._default_leader_score()
            ranking = recent[recent["industry"].astype(str) == str(industry)][["ts_code", "change"]].copy()
            if ranking.empty:
                return self._default_leader_score()
            ranking = ranking.rename(columns={"change": "change_3d"}).sort_values("change_3d", ascending=False).reset_index(drop=True)
            ranking["rank"] = range(1, len(ranking) + 1)
            if self.provider.cache is not None:
                self.provider.cache[cache_key] = ranking
        stock_row = ranking[ranking["ts_code"] == str(ts_code)]
        if stock_row.empty:
            return self._default_leader_score(total=len(ranking))
        rank = int(stock_row["rank"].iloc[0])
        total = int(len(ranking))
        pct = pd.to_numeric(self.provider.stock_hist.get("pct_chg", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        count_20d = int((pct.tail(20) >= 9.5).sum())
        count_60d = int((pct.tail(60) >= 9.5).sum())
        sector_rank_score = 6.0 if rank == 1 else 5.0 if rank <= 3 else 3.0 if rank <= 10 else 1.0 if rank / max(total, 1) <= 0.3 else 0.0
        limit_up_score = 4.0 if count_20d >= 3 else 3.0 if count_20d >= 2 else 2.0 if count_20d >= 1 else 1.0 if count_60d >= 2 else 0.0
        out = {
            "sector_rank": rank,
            "total_stocks": total,
            "sector_rank_score": sector_rank_score,
            "limit_up_count_20d": count_20d,
            "limit_up_count_60d": count_60d,
            "limit_up_score": limit_up_score,
            "total_score": round(sector_rank_score + limit_up_score, 1),
            "is_sector_leader": rank <= 3,
        }
        if self.provider.cache is not None:
            self.provider.cache[score_key] = dict(out)
        return out

    def _default_leader_score(self, total: int = 1) -> Dict[str, Any]:
        return {"sector_rank": 999, "total_stocks": int(total), "sector_rank_score": 0.0, "limit_up_count_20d": 0, "limit_up_count_60d": 0, "limit_up_score": 0.0, "total_score": 0.0, "is_sector_leader": False}


@contextmanager
def v6_point_in_time_context(
    evaluator: Any,
    *,
    market_frame: pd.DataFrame,
    stock_hist: pd.DataFrame,
    as_of_date: Optional[str] = None,
    cache: Optional[Dict[str, Any]] = None,
) -> Iterator[None]:
    """Temporarily attach point-in-time v6 dependencies for one replay evaluation."""

    if evaluator is None:
        yield
        return
    date = str(as_of_date or (stock_hist["trade_date"].iloc[-1] if stock_hist is not None and not stock_hist.empty and "trade_date" in stock_hist.columns else ""))
    provider = PointInTimeV6DataProvider(market_frame=market_frame, stock_hist=stock_hist, as_of_date=date, cache=cache)
    leader = PointInTimeV6LeaderAnalyzer(provider)
    old_provider = getattr(evaluator, "data_provider", None)
    old_leader = getattr(evaluator, "leader_analyzer", None)
    evaluator.data_provider = provider
    evaluator.leader_analyzer = leader
    try:
        yield
    finally:
        evaluator.data_provider = old_provider
        evaluator.leader_analyzer = old_leader

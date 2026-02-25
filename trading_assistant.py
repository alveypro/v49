#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能交易助手 v1.0
作者：AI量化专家
创建时间：2026-01-06

功能：
1. 每日自动选股
2. 持仓监控管理
3. 止盈止损提醒
4. 交易记录管理
5. 每日报告生成
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any
import json
import logging
import os
from pathlib import Path

# 配置日志（优先写入 /opt/airivo/logs，可通过环境变量覆盖）
_log_path = Path(os.getenv("TRADING_ASSISTANT_LOG_PATH", "/opt/airivo/logs/trading_assistant.log"))
if not _log_path.parent.exists() or not os.access(str(_log_path.parent), os.W_OK):
    _log_path = Path(__file__).with_name("trading_assistant.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(str(_log_path)),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 导入通知服务
try:
    from notification_service import NotificationService
    NOTIFICATION_AVAILABLE = True
except ImportError:
    logger.warning("⚠️ 通知服务模块未找到，通知功能将被禁用")
    NOTIFICATION_AVAILABLE = False


class TradingAssistant:
    """智能交易助手"""
    
    def __init__(self, db_path: str = "permanent_stock_database.db"):
        """
        初始化交易助手
        
        Args:
            db_path: 主数据库路径
        """
        self.db_path = self._resolve_db_path(db_path)
        self.assistant_db = self._resolve_assistant_db()
        self._init_database()
        self.last_scan_debug = {}
        self._learning_assistant = None
        
        # 初始化通知服务
        self.notifier = None
        if NOTIFICATION_AVAILABLE:
            try:
                self.notifier = NotificationService()
                logger.info("📧 通知服务已启用")
            except Exception as e:
                logger.warning(f"⚠️ 通知服务初始化失败: {e}")
        
        logger.info("🚀 智能交易助手初始化完成")

    def _get_learning_assistant(self):
        if self._learning_assistant is not None:
            return self._learning_assistant
        try:
            from openclaw.assistant import OpenClawStockAssistant

            self._learning_assistant = OpenClawStockAssistant(
                log_dir="logs/openclaw",
                db_path=self.db_path,
            )
            return self._learning_assistant
        except Exception as e:
            logger.warning(f"⚠️ 自学习助手初始化失败: {e}")
            self._learning_assistant = False
            return None

    def _record_learning_event(
        self,
        module: str,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        ts_code: Optional[str] = None,
    ) -> None:
        learner = self._get_learning_assistant()
        if not learner:
            return
        try:
            learner.record_module_outcome(
                module=module,
                event_type=event_type,
                payload=payload or {},
                ts_code=ts_code,
                route="stock_core",
            )
        except Exception as e:
            logger.warning(f"⚠️ 记录自学习事件失败: {module}/{event_type} -> {e}")

    def record_learning_event(
        self,
        module: str,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        ts_code: Optional[str] = None,
    ) -> None:
        """对外暴露的自学习事件记录接口。"""
        self._record_learning_event(
            module=module,
            event_type=event_type,
            payload=payload,
            ts_code=ts_code,
        )

    def _get_config_float(self, key: str, default: float) -> float:
        try:
            val = self.get_config(key)
            return float(val) if val is not None else float(default)
        except Exception:
            return float(default)

    def get_auto_tuning_recommendation(self, lookback_days: int = 30, min_samples: int = 8) -> Dict[str, Any]:
        """基于学习卡片结果与交易表现生成自动调参建议（不直接落库）。"""
        learner = self._get_learning_assistant()
        if not learner:
            return {"ok": False, "reason": "learning assistant unavailable"}

        learning_db = Path("logs/openclaw/assistant_learning.db")
        if not learning_db.exists():
            return {"ok": False, "reason": "learning db not found"}

        since = (datetime.now() - timedelta(days=max(1, int(lookback_days)))).strftime("%Y-%m-%d %H:%M:%S")
        d5_returns: List[float] = []
        d20_returns: List[float] = []
        try:
            conn = sqlite3.connect(str(learning_db))
            rows = conn.execute(
                """
                SELECT outcome_json
                FROM learning_cards
                WHERE created_at >= ? AND route = 'stock_core' AND status IN ('closed', 'pending')
                ORDER BY created_at DESC
                """,
                (since,),
            ).fetchall()
            conn.close()
            for (raw,) in rows:
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                horizons = (obj or {}).get("horizons") or {}
                d5 = (horizons.get("d5") or {}).get("ret_pct")
                d20 = (horizons.get("d20") or {}).get("ret_pct")
                if d5 is not None:
                    d5_returns.append(float(d5))
                if d20 is not None:
                    d20_returns.append(float(d20))
        except Exception as e:
            return {"ok": False, "reason": f"read learning db failed: {e}"}

        sample_count = len(d5_returns)
        fallback_source = "learning_cards"
        if sample_count < max(1, int(min_samples)):
            # Fallback to realized trade outcomes when learning horizon samples are still sparse.
            try:
                conn = sqlite3.connect(self.assistant_db)
                sold_df = pd.read_sql_query(
                    """
                    SELECT profit_loss_pct
                    FROM trade_history
                    WHERE action = 'sell' AND trade_date >= ?
                    ORDER BY trade_date DESC
                    """,
                    conn,
                    params=((datetime.now() - timedelta(days=max(1, int(lookback_days)))).strftime("%Y-%m-%d"),),
                )
                conn.close()
                if not sold_df.empty:
                    pnl = pd.to_numeric(sold_df["profit_loss_pct"], errors="coerce").dropna()
                    if not pnl.empty:
                        d5_returns = (pnl * 100.0).tolist()
                        sample_count = len(d5_returns)
                        fallback_source = "trade_history"
            except Exception:
                pass
        if sample_count < max(1, int(min_samples)):
            return {
                "ok": True,
                "regime": "neutral",
                "insufficient_samples": True,
                "reason": "insufficient samples",
                "metrics": {"sample_count": sample_count, "required": int(min_samples), "sample_source": "none"},
                "current": {
                    "min_score": self._get_config_float("min_score", 55.0),
                    "take_profit_pct": self._get_config_float("take_profit_pct", 0.06),
                    "stop_loss_pct": self._get_config_float("stop_loss_pct", 0.04),
                    "single_position_pct": self._get_config_float("single_position_pct", 0.20),
                    "max_position_pct": self._get_config_float("max_position_pct", 0.80),
                },
                "target": {},
                "changes": {},
                "rationale": ["样本不足，暂不调整参数。建议继续积累交易/回填结果。"],
            }

        d5_series = pd.Series(d5_returns, dtype=float)
        win_rate = float((d5_series > 0).mean())
        avg_ret = float(d5_series.mean())
        vol = float(d5_series.std()) if len(d5_series) > 1 else 0.0
        d20_avg = float(pd.Series(d20_returns, dtype=float).mean()) if d20_returns else None

        current = {
            "min_score": self._get_config_float("min_score", 55.0),
            "take_profit_pct": self._get_config_float("take_profit_pct", 0.06),
            "stop_loss_pct": self._get_config_float("stop_loss_pct", 0.04),
            "single_position_pct": self._get_config_float("single_position_pct", 0.20),
            "max_position_pct": self._get_config_float("max_position_pct", 0.80),
        }
        target = dict(current)
        regime = "neutral"
        rationale: List[str] = []

        if win_rate < 0.45 or avg_ret < 0:
            regime = "defensive"
            target["min_score"] = min(80.0, current["min_score"] + 2.0)
            target["stop_loss_pct"] = max(0.02, current["stop_loss_pct"] - 0.005)
            target["take_profit_pct"] = max(0.04, current["take_profit_pct"] - 0.005)
            target["single_position_pct"] = max(0.10, current["single_position_pct"] - 0.02)
            target["max_position_pct"] = max(0.50, current["max_position_pct"] - 0.05)
            rationale.append("近30天样本胜率/收益偏弱，进入防守调参。")
        elif win_rate >= 0.60 and avg_ret >= 1.2 and vol <= 3.5:
            regime = "offensive"
            target["min_score"] = max(50.0, current["min_score"] - 1.0)
            target["stop_loss_pct"] = min(0.07, current["stop_loss_pct"] + 0.003)
            target["take_profit_pct"] = min(0.15, current["take_profit_pct"] + 0.01)
            target["single_position_pct"] = min(0.25, current["single_position_pct"] + 0.01)
            target["max_position_pct"] = min(0.95, current["max_position_pct"] + 0.03)
            rationale.append("近30天胜率和收益较好且波动可控，进入进攻调参。")
        else:
            rationale.append("近30天表现中性，建议维持参数或小幅微调。")

        changes: Dict[str, Dict[str, float]] = {}
        for k, old in current.items():
            new = round(float(target[k]), 4)
            if round(float(old), 4) != new:
                changes[k] = {"from": round(float(old), 4), "to": new}

        return {
            "ok": True,
            "regime": regime,
            "metrics": {
                "sample_count": sample_count,
                "sample_source": fallback_source,
                "d5_win_rate": round(win_rate, 4),
                "d5_avg_ret_pct": round(avg_ret, 4),
                "d5_vol_pct": round(vol, 4),
                "d20_avg_ret_pct": round(float(d20_avg), 4) if d20_avg is not None else None,
            },
            "current": current,
            "target": target,
            "changes": changes,
            "rationale": rationale,
        }

    def apply_auto_tuning(self, recommendation: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """应用自动调参建议并落库。"""
        rec = recommendation or self.get_auto_tuning_recommendation()
        if not rec.get("ok"):
            return {"ok": False, "reason": rec.get("reason", "invalid recommendation"), "detail": rec}
        changes = rec.get("changes") or {}
        if not changes:
            return {"ok": True, "applied": False, "message": "no parameter changes needed", "detail": rec}

        mapping = {
            "min_score": lambda x: str(int(round(float(x)))),
            "take_profit_pct": lambda x: str(float(x)),
            "stop_loss_pct": lambda x: str(float(x)),
            "single_position_pct": lambda x: str(float(x)),
            "max_position_pct": lambda x: str(float(x)),
        }
        for key, diff in changes.items():
            if key not in mapping:
                continue
            self.update_config(key, mapping[key](diff["to"]))

        self._record_learning_event(
            module="auto_tuning",
            event_type="config_updated",
            payload={
                "changes": changes,
                "metrics": rec.get("metrics", {}),
                "regime": rec.get("regime", "neutral"),
                "rationale": rec.get("rationale", []),
            },
        )
        return {"ok": True, "applied": True, "changes": changes, "detail": rec}
    
    def _init_database(self):
        """初始化助手数据库"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        # 创建持仓表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                stock_name TEXT,
                buy_date TEXT,
                buy_price REAL,
                quantity INTEGER,
                cost_total REAL,
                current_price REAL,
                current_value REAL,
                profit_loss REAL,
                profit_loss_pct REAL,
                status TEXT DEFAULT 'holding',
                strategy TEXT DEFAULT 'v4.0',
                score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建交易记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                stock_name TEXT,
                action TEXT,  -- 'buy' or 'sell'
                trade_date TEXT,
                price REAL,
                quantity INTEGER,
                amount REAL,
                reason TEXT,
                profit_loss REAL,
                profit_loss_pct REAL,
                strategy TEXT DEFAULT 'v4.0',
                score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建每日选股表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommend_date TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                stock_name TEXT,
                score REAL,
                price REAL,
                reason TEXT,
                strategy TEXT DEFAULT 'v4.0',
                market_cap REAL,
                industry TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(recommend_date, ts_code)
            )
        """)
        
        # 创建配置表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        
        # 初始化默认配置
        self._init_default_config()
        
        logger.info("✅ 数据库初始化完成")

    def _resolve_db_path(self, db_path: str) -> str:
        """解析主数据库路径"""
        cand = Path(db_path)
        if cand.exists():
            return str(cand)
        # 尝试读取配置文件
        cfg_path = Path(__file__).with_name("config.json")
        if cfg_path.exists():
            try:
                cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
                cfg_db = cfg.get("PERMANENT_DB_PATH")
                if cfg_db and Path(cfg_db).exists():
                    return cfg_db
            except Exception:
                pass
        # 服务器默认路径
        fallback = Path("/opt/airivo/data/permanent_stock_database.db")
        if fallback.exists():
            return str(fallback)
        return str(cand)

    def _resolve_assistant_db(self) -> str:
        env_db = os.getenv("TRADING_ASSISTANT_DB_PATH")
        if env_db:
            return env_db
        server_dir = Path("/opt/airivo/data")
        if server_dir.exists() and os.access(str(server_dir), os.W_OK):
            return str(server_dir / "trading_assistant.db")
        return str(Path(__file__).with_name("trading_assistant.db"))
    
    def _init_default_config(self):
        """初始化默认配置"""
        default_config = {
            'strategy': 'v4.0',
            'min_score': '55',  # 日常输出更稳定
            'max_score': '90',  # 🔧 新增：最高分数，避免过度筛选
            'market_cap_min': '5000000000',  # 50亿
            'market_cap_max': '100000000000',  # 1000亿
            'recommend_count': '5',
            'single_position_pct': '0.2',  # 单只20%
            'max_position_pct': '0.8',  # 最多80%仓位
            'take_profit_pct': '0.06',  # 6%止盈
            'stop_loss_pct': '0.04',  # 4%止损
            'holding_days': '5',  # 建议持仓天数
            'notification_enabled': 'true',
            'notification_email': '',
            'auto_scan_time': '09:15',  # 每天9:15扫描
            'score_analysis': 'v4_65-90分最优'  # 分析结论备注
        }
        
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        for key, value in default_config.items():
            cursor.execute("""
                INSERT OR IGNORE INTO config (key, value, description)
                VALUES (?, ?, ?)
            """, (key, value, f'默认{key}'))
        
        conn.commit()
        conn.close()
    
    def get_config(self, key: str) -> str:
        """获取配置"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def update_config(self, key: str, value: str):
        """更新配置"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE config 
            SET value = ?, updated_at = CURRENT_TIMESTAMP
            WHERE key = ?
        """, (value, key))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 配置更新: {key} = {value}")
    
    def daily_stock_scan(self, top_n: int = 5) -> List[Dict]:
        """
        每日自动选股
        
        Args:
            top_n: 推荐股票数量
            
        Returns:
            推荐股票列表
        """
        logger.info("🔍 开始每日选股扫描...")

        try:
            # 获取配置
            min_score = float(self.get_config('min_score'))
            market_cap_min = float(self.get_config('market_cap_min'))
            market_cap_max = float(self.get_config('market_cap_max'))
            base_threshold = min_score
            thr_v4 = base_threshold
            thr_v5 = base_threshold
            thr_v7 = base_threshold + 2
            thr_v8 = base_threshold + 2
            thr_v9 = base_threshold

            weights = {
                "v4": 0.15,
                "v5": 0.15,
                "v7": 0.30,
                "v8": 0.25,
                "v9": 0.15,
            }

            # 共识策略评分器
            from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
            try:
                from comprehensive_stock_evaluator_v5 import ComprehensiveStockEvaluatorV5
                v5_ok = True
            except Exception:
                ComprehensiveStockEvaluatorV5 = None
                v5_ok = False
                logger.warning("⚠️ v5评分器未找到，回退使用v4评分器")
            from comprehensive_stock_evaluator_v7_ultimate import ComprehensiveStockEvaluatorV7Ultimate
            from comprehensive_stock_evaluator_v8_ultimate import ComprehensiveStockEvaluatorV8Ultimate

            evaluator_v4 = ComprehensiveStockEvaluatorV4()
            evaluator_v5 = ComprehensiveStockEvaluatorV5() if v5_ok else evaluator_v4
            evaluator_v7 = ComprehensiveStockEvaluatorV7Ultimate(self.db_path)
            evaluator_v8 = ComprehensiveStockEvaluatorV8Ultimate(self.db_path)

            # 获取候选股票
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT DISTINCT sb.ts_code, sb.name, sb.industry, sb.circ_mv
                FROM stock_basic sb
                WHERE sb.circ_mv >= ? AND sb.circ_mv <= ?
                ORDER BY RANDOM()
                LIMIT 1500
            """
            
            candidates = pd.read_sql_query(
                query, 
                conn, 
                params=(market_cap_min/10000, market_cap_max/10000)
            )
            conn.close()
            
            logger.info(f"📊 候选股票: {len(candidates)}只")

            # 预加载指数数据（上证指数）供v8使用
            index_data = pd.DataFrame()
            try:
                conn = sqlite3.connect(self.db_path)
                index_data = pd.read_sql_query("""
                    SELECT trade_date, close_price as close, vol as volume
                    FROM daily_trading_data
                    WHERE ts_code = '000001.SH'
                    ORDER BY trade_date DESC
                    LIMIT 120
                """, conn)
                conn.close()
            except Exception:
                index_data = pd.DataFrame()
            if len(index_data) >= 60 and 'trade_date' in index_data.columns:
                index_data = index_data.sort_values('trade_date').reset_index(drop=True)
            else:
                index_data = None

            # 资金类加分（全局/个股/行业）
            bonus_global, bonus_stock_map, top_list_set, top_inst_set, bonus_industry_map = self._load_external_bonus_maps()

            # 预读取历史数据 + 行业强度
            history_cache = {}
            industry_vals = {}
            for _, row in candidates.iterrows():
                ts_code = row['ts_code']
                conn = sqlite3.connect(self.db_path)
                stock_data = pd.read_sql_query(f"""
                    SELECT trade_date, close_price, high_price, low_price, vol, amount, pct_chg, turnover_rate
                    FROM daily_trading_data
                    WHERE ts_code = '{ts_code}'
                    ORDER BY trade_date DESC
                    LIMIT 160
                """, conn)
                conn.close()
                if stock_data is None or len(stock_data) < 80:
                    continue
                stock_data = stock_data.sort_values('trade_date').reset_index(drop=True)
                history_cache[ts_code] = stock_data
                close = pd.to_numeric(stock_data['close_price'], errors='coerce').ffill()
                if len(close) > 21:
                    r20 = (close.iloc[-1] / close.iloc[-21] - 1.0) * 100
                    industry_vals.setdefault(row['industry'], []).append(r20)
            industry_scores = {k: float(np.mean(v)) for k, v in industry_vals.items() if v}

            # 评分筛选（共识）
            recommendations = []
            debug_counts = {
                "cand": 0,
                "scored": 0,
                "agree3": 0,
                "agree2": 0,
                "pass_base": 0,
            }

            for _, row in candidates.iterrows():
                debug_counts["cand"] += 1
                ts_code = row['ts_code']
                stock_name = row['name']
                industry = row['industry']
                stock_data = history_cache.get(ts_code)
                if stock_data is None or len(stock_data) < 80:
                    continue

                stock_data['name'] = stock_name

                v4_res = evaluator_v4.evaluate_stock_v4(stock_data)
                v4_score = float(v4_res.get('final_score', 0)) if v4_res else None

                v5_res = evaluator_v5.evaluate_stock_v4(stock_data)
                v5_score = float(v5_res.get('final_score', 0)) if v5_res else None

                v7_res = evaluator_v7.evaluate_stock_v7(stock_data, ts_code, industry)
                v7_score = float(v7_res.get('final_score', 0)) if v7_res and v7_res.get('success') else None

                v8_res = evaluator_v8.evaluate_stock_v8(stock_data, ts_code=ts_code, index_data=index_data)
                v8_score = float(v8_res.get('final_score', 0)) if v8_res and v8_res.get('success') else None

                ind_strength = industry_scores.get(industry, 0.0)
                v9_info = self._calc_v9_score_from_hist(stock_data, industry_strength=ind_strength)
                v9_score = float(v9_info.get('score', 0)) if v9_info else None

                agree = 0
                if v4_score is not None and v4_score >= thr_v4:
                    agree += 1
                if v5_score is not None and v5_score >= thr_v5:
                    agree += 1
                if v7_score is not None and v7_score >= thr_v7:
                    agree += 1
                if v8_score is not None and v8_score >= thr_v8:
                    agree += 1
                if v9_score is not None and v9_score >= thr_v9:
                    agree += 1
                debug_counts["scored"] += 1

                scores = {
                    "v4": v4_score,
                    "v5": v5_score,
                    "v7": v7_score,
                    "v8": v8_score,
                    "v9": v9_score,
                }
                weight_sum = sum(weights[k] for k, v in scores.items() if v is not None)
                if weight_sum <= 0:
                    continue
                weighted_score = sum(
                    (scores[k] * weights[k]) for k in scores if scores[k] is not None
                ) / weight_sum

                extra = self._calc_external_bonus(
                    ts_code,
                    industry,
                    bonus_global,
                    bonus_stock_map,
                    top_list_set,
                    top_inst_set,
                    bonus_industry_map,
                )
                final_score = weighted_score + extra

                latest_price = stock_data.iloc[-1]['close_price']
                reason = f"共识评分{final_score:.1f} | 一致数{agree} | 资金加分{extra:.1f}"

                recommendations.append({
                    'ts_code': ts_code,
                    'stock_name': stock_name,
                    'score': final_score,
                    'price': latest_price,
                    'reason': reason,
                    'market_cap': row['circ_mv'] * 10000,
                    'industry': industry,
                    'grade': '',
                    'dimension_scores': {
                        'v4': v4_score,
                        'v5': v5_score,
                        'v7': v7_score,
                        'v8': v8_score,
                        'v9': v9_score,
                        'agree': agree
                    }
                })

            # 按条件分层筛选
            recs_agree3 = [r for r in recommendations if r["dimension_scores"]["agree"] >= 3]
            recs_agree2 = [r for r in recommendations if r["dimension_scores"]["agree"] >= 2]
            recs_pass_base = [r for r in recommendations if r["score"] >= base_threshold]

            debug_counts["agree3"] = len(recs_agree3)
            debug_counts["agree2"] = len(recs_agree2)
            debug_counts["pass_base"] = len(recs_pass_base)

            if recs_agree3 and recs_pass_base:
                candidates_final = [r for r in recs_agree3 if r["score"] >= base_threshold]
            elif recs_agree2:
                # 降低门槛，保证有结果
                lower_threshold = max(50.0, base_threshold - 5)
                candidates_final = [r for r in recs_agree2 if r["score"] >= lower_threshold]
                if not candidates_final:
                    # 兜底：至少输出一致性>=2的Top
                    candidates_final = recs_agree2
            else:
                candidates_final = recommendations

            candidates_final.sort(key=lambda x: x['score'], reverse=True)
            top_recommendations = candidates_final[:top_n]
            
            # 保存到数据库
            today = datetime.now().strftime('%Y-%m-%d')
            self._save_daily_recommendations(today, top_recommendations)
            
            self.last_scan_debug = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "min_score": min_score,
                "market_cap_min": market_cap_min,
                "market_cap_max": market_cap_max,
                "base_threshold": base_threshold,
                "thresholds": {
                    "v4": thr_v4,
                    "v5": thr_v5,
                    "v7": thr_v7,
                    "v8": thr_v8,
                    "v9": thr_v9,
                },
                "counts": debug_counts,
                "selected": len(top_recommendations),
            }

            logger.info(
                "✅ 选股完成，推荐%s只 | cand=%s scored=%s agree3=%s agree2=%s pass_base=%s",
                len(top_recommendations),
                debug_counts["cand"],
                debug_counts["scored"],
                debug_counts["agree3"],
                debug_counts["agree2"],
                debug_counts["pass_base"],
            )
            self._record_learning_event(
                module="daily_stock_scan",
                event_type="scan_completed",
                payload={
                    "recommend_count": len(top_recommendations),
                    "debug_counts": debug_counts,
                    "thresholds": {
                        "v4": thr_v4,
                        "v5": thr_v5,
                        "v7": thr_v7,
                        "v8": thr_v8,
                        "v9": thr_v9,
                    },
                    "base_threshold": base_threshold,
                },
            )
            
            # 🆕 发送选股通知
            self._send_stock_selection_notification(top_recommendations)
            
            return top_recommendations
            
        except Exception as e:
            logger.error(f"❌ 选股失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.last_scan_debug = {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error": str(e),
            }
            self._record_learning_event(
                module="daily_stock_scan",
                event_type="scan_failed",
                payload={"error": str(e)},
            )
            return []

    def _calc_v9_score_from_hist(self, hist: pd.DataFrame, industry_strength: float = 0.0) -> Dict:
        if hist is None or hist.empty or len(hist) < 80:
            return {"score": 0.0, "details": {}}
        h = hist.sort_values("trade_date")
        close = pd.to_numeric(h["close_price"], errors="coerce").ffill()
        vol = pd.to_numeric(h.get("vol", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        amount = pd.to_numeric(h.get("amount", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        pct = pd.to_numeric(h.get("pct_chg", pd.Series(dtype=float)), errors="coerce")
        if pct.isna().all():
            pct = close.pct_change() * 100

        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        ma120 = close.rolling(120).mean()

        trend_strong = bool(ma20.iloc[-1] > ma60.iloc[-1] > ma120.iloc[-1])
        trend_ok = bool((ma20.iloc[-1] > ma60.iloc[-1]) and (ma20.iloc[-1] > ma20.iloc[-5]) and (ma60.iloc[-1] >= ma60.iloc[-5]))

        momentum_20 = (close.iloc[-1] / close.iloc[-21] - 1.0) if len(close) > 21 else 0.0
        momentum_60 = (close.iloc[-1] / close.iloc[-61] - 1.0) if len(close) > 61 else 0.0

        vol_ratio = (vol.iloc[-1] / vol.tail(20).mean()) if vol.tail(20).mean() > 0 else 0.0

        flow_sign = pct.fillna(0).apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        flow_val = (amount * flow_sign).tail(20).sum()
        flow_base = amount.tail(20).sum() if amount.tail(20).sum() > 0 else 1.0
        flow_ratio = flow_val / flow_base

        vol_20 = pct.tail(20).std() / 100.0 if pct.tail(20).std() is not None else 0.0

        fund_score = max(0.0, min(20.0, (flow_ratio + 0.03) / 0.12 * 20.0))
        volume_score = max(0.0, min(15.0, (vol_ratio - 0.5) / 1.0 * 15.0))
        momentum_score = max(0.0, min(8.0, momentum_20 * 100 / 8.0 * 8.0)) + \
                         max(0.0, min(7.0, momentum_60 * 100 / 16.0 * 7.0))
        sector_score = max(0.0, min(15.0, (industry_strength + 2.0) / 6.0 * 15.0))

        if vol_20 <= 0.03:
            vola_score = 12.0
        elif vol_20 <= 0.06:
            vola_score = 15.0
        elif vol_20 <= 0.10:
            vola_score = 8.0
        else:
            vola_score = 0.0

        trend_score = 15.0 if trend_strong else (10.0 if trend_ok else 0.0)

        rolling_peak = close.cummax()
        drawdown = (rolling_peak - close) / rolling_peak
        max_dd = float(drawdown.tail(60).max())
        dd_penalty = 0.0
        if max_dd > 0.15:
            dd_penalty = min(10.0, (max_dd - 0.15) / 0.15 * 10.0)

        total_score = fund_score + volume_score + momentum_score + sector_score + vola_score + trend_score - dd_penalty
        return {"score": round(total_score, 2), "details": {}}

    def _load_external_bonus_maps(self) -> Tuple[float, Dict[str, float], set, set, Dict[str, float]]:
        bonus_global = 0.0
        bonus_stock = {}
        bonus_industry = {}
        top_list_set = set()
        top_inst_set = set()
        last_trade = None
        try:
            conn = sqlite3.connect(self.db_path)
            df_last = pd.read_sql_query("SELECT MAX(trade_date) AS max_date FROM daily_trading_data", conn)
            last_trade = str(df_last["max_date"].iloc[0]) if not df_last.empty else None
            conn.close()
        except Exception:
            last_trade = None

        try:
            conn = sqlite3.connect(self.db_path)
            nb = pd.read_sql_query("SELECT north_money FROM northbound_flow ORDER BY trade_date DESC LIMIT 5", conn)
            if not nb.empty:
                avg_nb = float(nb["north_money"].mean())
                if avg_nb > 0:
                    bonus_global += 1.0
            conn.close()
        except Exception:
            pass

        try:
            conn = sqlite3.connect(self.db_path)
            m = pd.read_sql_query("SELECT rzye, rqye FROM margin_summary ORDER BY trade_date DESC LIMIT 5", conn)
            if not m.empty:
                rzye = float(m["rzye"].mean())
                if rzye > 0:
                    bonus_global += 0.5
            conn.close()
        except Exception:
            pass

        try:
            conn = sqlite3.connect(self.db_path)
            mf = pd.read_sql_query(
                "SELECT ts_code, net_mf_amount FROM moneyflow_daily WHERE trade_date = (SELECT MAX(trade_date) FROM moneyflow_daily)",
                conn,
            )
            if not mf.empty:
                bonus_stock = {r["ts_code"]: float(r["net_mf_amount"]) for _, r in mf.iterrows()}
            conn.close()
        except Exception:
            pass

        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT ts_code FROM top_list WHERE trade_date = (SELECT MAX(trade_date) FROM top_list)",
                conn,
            )
            if not df.empty:
                top_list_set = set(df["ts_code"].astype(str).tolist())
            conn.close()
        except Exception:
            pass

        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT ts_code FROM top_inst WHERE trade_date = (SELECT MAX(trade_date) FROM top_inst)",
                conn,
            )
            if not df.empty:
                top_inst_set = set(df["ts_code"].astype(str).tolist())
            conn.close()
        except Exception:
            pass

        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(
                "SELECT industry, net_mf_amount FROM moneyflow_ind_ths WHERE trade_date = (SELECT MAX(trade_date) FROM moneyflow_ind_ths)",
                conn,
            )
            if not df.empty:
                bonus_industry = {r["industry"]: float(r["net_mf_amount"]) for _, r in df.iterrows() if r.get("industry")}
            conn.close()
        except Exception:
            pass

        return bonus_global, bonus_stock, top_list_set, top_inst_set, bonus_industry

    def _calc_external_bonus(
        self,
        ts_code: str,
        industry: str | None,
        bonus_global: float,
        bonus_stock_map: Dict[str, float],
        top_list_set: set,
        top_inst_set: set,
        bonus_industry_map: Dict[str, float],
    ) -> float:
        extra = 0.0
        extra += bonus_global
        mf_net = bonus_stock_map.get(ts_code, 0.0)
        if mf_net > 1e8:
            extra += 2.0
        elif mf_net > 0:
            extra += 1.0
        elif mf_net < 0:
            extra -= 1.0
        if ts_code in top_list_set:
            extra += 1.5
        if ts_code in top_inst_set:
            extra += 1.0
        if industry:
            ind_flow = bonus_industry_map.get(industry, 0.0)
            if ind_flow > 0:
                extra += 1.0
            elif ind_flow < 0:
                extra -= 1.0
        return extra
    
    def _save_daily_recommendations(self, date: str, recommendations: List[Dict]):
        """保存每日推荐"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        for rec in recommendations:
            cursor.execute("""
                INSERT OR REPLACE INTO daily_recommendations
                (recommend_date, ts_code, stock_name, score, price, reason, 
                 strategy, market_cap, industry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date, rec['ts_code'], rec['stock_name'], rec['score'],
                rec['price'], rec['reason'], 'v4.0',
                rec['market_cap'], rec['industry']
            ))
        
        conn.commit()
        conn.close()
    
    def add_holding(self, ts_code: str, buy_price: float, quantity: int, 
                   score: float = 0, strategy: str = 'v4.0'):
        """
        添加持仓
        
        Args:
            ts_code: 股票代码
            buy_price: 买入价格
            quantity: 买入数量
            score: 评分
            strategy: 策略
        """
        # 获取股票名称
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM stock_basic WHERE ts_code = ?", (ts_code,))
        result = cursor.fetchone()
        conn.close()
        
        stock_name = result[0] if result else ''
        
        # 计算成本
        cost_total = buy_price * quantity
        
        # 保存到持仓表
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO holdings 
            (ts_code, stock_name, buy_date, buy_price, quantity, cost_total, 
             current_price, current_value, profit_loss, profit_loss_pct, 
             status, strategy, score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'holding', ?, ?)
        """, (
            ts_code, stock_name, datetime.now().strftime('%Y-%m-%d'),
            buy_price, quantity, cost_total, buy_price, cost_total, 
            0.0, 0.0,  # 初始化profit_loss和profit_loss_pct为0
            strategy, score
        ))
        
        # 保存到交易记录
        cursor.execute("""
            INSERT INTO trade_history
            (ts_code, stock_name, action, trade_date, price, quantity, amount, 
             reason, strategy, score)
            VALUES (?, ?, 'buy', ?, ?, ?, ?, '手动买入', ?, ?)
        """, (
            ts_code, stock_name, datetime.now().strftime('%Y-%m-%d'),
            buy_price, quantity, cost_total, strategy, score
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 添加持仓: {stock_name}({ts_code}), {quantity}股 @ ¥{buy_price}")
        self._record_learning_event(
            module="portfolio_management",
            event_type="buy_recorded",
            payload={
                "action": "buy",
                "stock_name": stock_name,
                "price": float(buy_price),
                "quantity": int(quantity),
                "cost_total": float(cost_total),
                "strategy": strategy,
                "score": float(score or 0),
            },
            ts_code=ts_code,
        )
    
    def update_holdings(self):
        """更新持仓信息"""
        logger.info("🔄 更新持仓信息...")
        
        conn_assistant = sqlite3.connect(self.assistant_db)
        holdings = pd.read_sql_query(
            "SELECT * FROM holdings WHERE status = 'holding'",
            conn_assistant
        )
        
        if holdings.empty:
            logger.info("📊 当前无持仓")
            conn_assistant.close()
            self._record_learning_event(
                module="portfolio_management",
                event_type="holdings_empty",
                payload={"holding_count": 0},
            )
            return
        
        # 获取最新价格
        conn_main = sqlite3.connect(self.db_path)
        
        for idx, holding in holdings.iterrows():
            ts_code = holding['ts_code']
            
            # 获取最新价格
            latest_data = pd.read_sql_query(f"""
                SELECT close_price FROM daily_trading_data
                WHERE ts_code = '{ts_code}'
                ORDER BY trade_date DESC
                LIMIT 1
            """, conn_main)
            
            if not latest_data.empty:
                current_price = latest_data.iloc[0]['close_price']
                current_value = current_price * holding['quantity']
                profit_loss = current_value - holding['cost_total']
                profit_loss_pct = profit_loss / holding['cost_total'] if holding['cost_total'] > 0 else 0
                
                # 更新数据库
                cursor = conn_assistant.cursor()
                cursor.execute("""
                    UPDATE holdings
                    SET current_price = ?,
                        current_value = ?,
                        profit_loss = ?,
                        profit_loss_pct = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (current_price, current_value, profit_loss, 
                     profit_loss_pct, holding['id']))
                
                conn_assistant.commit()
                
                logger.info(f"📊 {holding['stock_name']}: ¥{current_price:.2f}, "
                          f"盈亏{profit_loss_pct*100:.2f}%")
            else:
                # 没有找到数据，确保字段不为None
                logger.warning(f"⚠️ 未找到{holding['stock_name']}的最新数据")
                cursor = conn_assistant.cursor()
                cursor.execute("""
                    UPDATE holdings
                    SET current_price = COALESCE(current_price, buy_price),
                        current_value = COALESCE(current_value, cost_total),
                        profit_loss = COALESCE(profit_loss, 0),
                        profit_loss_pct = COALESCE(profit_loss_pct, 0),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (holding['id'],))
                conn_assistant.commit()
        
        conn_main.close()
        conn_assistant.close()
        
        logger.info("✅ 持仓更新完成")
        holdings_after = holdings.copy()
        avg_profit_pct = (
            float(pd.to_numeric(holdings_after.get('profit_loss_pct', pd.Series(dtype=float)), errors='coerce').fillna(0.0).mean())
            if not holdings_after.empty else 0.0
        )
        self._record_learning_event(
            module="portfolio_management",
            event_type="holdings_updated",
            payload={
                "holding_count": int(len(holdings_after)),
                "avg_profit_pct": avg_profit_pct,
            },
        )
    
    def check_stop_conditions(self) -> List[Dict]:
        """
        检查止盈止损条件
        
        Returns:
            需要提醒的持仓列表
        """
        logger.info("🔍 检查止盈止损条件...")
        
        take_profit_pct = float(self.get_config('take_profit_pct'))
        stop_loss_pct = float(self.get_config('stop_loss_pct'))
        
        conn = sqlite3.connect(self.assistant_db)
        holdings = pd.read_sql_query(
            "SELECT * FROM holdings WHERE status = 'holding'",
            conn
        )
        conn.close()
        
        alerts = []
        
        for idx, holding in holdings.iterrows():
            profit_pct = holding['profit_loss_pct']
            
            # 跳过无效数据
            if profit_pct is None or pd.isna(profit_pct):
                continue
            
            if profit_pct >= take_profit_pct:
                alerts.append({
                    'type': 'take_profit',
                    'ts_code': holding['ts_code'],
                    'stock_name': holding['stock_name'],
                    'buy_price': holding['buy_price'],
                    'current_price': holding['current_price'],
                    'profit_pct': profit_pct,
                    'message': f"🎉 {holding['stock_name']} 已达止盈条件！"
                               f"盈利{profit_pct*100:.2f}%，建议卖出！"
                })
                logger.warning(f"🎉 止盈提醒: {holding['stock_name']} +{profit_pct*100:.2f}%")
                
            elif profit_pct <= -stop_loss_pct:
                alerts.append({
                    'type': 'stop_loss',
                    'ts_code': holding['ts_code'],
                    'stock_name': holding['stock_name'],
                    'buy_price': holding['buy_price'],
                    'current_price': holding['current_price'],
                    'profit_pct': profit_pct,
                    'message': f"⚠️ {holding['stock_name']} 触发止损！"
                               f"亏损{abs(profit_pct)*100:.2f}%，建议止损！"
                })
                logger.warning(f"⚠️ 止损预警: {holding['stock_name']} {profit_pct*100:.2f}%")
        
        # 🆕 发送止盈止损通知
        if alerts:
            self._send_stop_condition_notification(alerts)
        self._record_learning_event(
            module="portfolio_management",
            event_type="stop_conditions_checked",
            payload={
                "alert_count": int(len(alerts)),
                "take_profit_count": int(sum(1 for a in alerts if a.get("type") == "take_profit")),
                "stop_loss_count": int(sum(1 for a in alerts if a.get("type") == "stop_loss")),
            },
        )
        
        return alerts
    
    def sell_holding(self, ts_code: str, sell_price: float, reason: str = '手动卖出'):
        """
        卖出持仓
        
        Args:
            ts_code: 股票代码
            sell_price: 卖出价格
            reason: 卖出原因
        """
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        # 获取持仓信息
        cursor.execute("""
            SELECT * FROM holdings 
            WHERE ts_code = ? AND status = 'holding'
        """, (ts_code,))
        
        holding = cursor.fetchone()
        
        if not holding:
            logger.warning(f"⚠️ 未找到持仓: {ts_code}")
            conn.close()
            return
        
        # 计算盈亏
        quantity = holding[5]  # quantity字段
        cost_total = holding[6]  # cost_total字段
        sell_amount = sell_price * quantity
        profit_loss = sell_amount - cost_total
        profit_loss_pct = profit_loss / cost_total
        
        # 更新持仓状态
        cursor.execute("""
            UPDATE holdings
            SET status = 'sold',
                current_price = ?,
                current_value = ?,
                profit_loss = ?,
                profit_loss_pct = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE ts_code = ? AND status = 'holding'
        """, (sell_price, sell_amount, profit_loss, profit_loss_pct, ts_code))
        
        # 记录交易
        cursor.execute("""
            INSERT INTO trade_history
            (ts_code, stock_name, action, trade_date, price, quantity, amount,
             reason, profit_loss, profit_loss_pct, strategy)
            VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?, 'v4.0')
        """, (
            ts_code, holding[2], datetime.now().strftime('%Y-%m-%d'),
            sell_price, quantity, sell_amount, reason, profit_loss, profit_loss_pct
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 卖出成功: {holding[2]}({ts_code}), "
                   f"盈亏{profit_loss_pct*100:.2f}%")
        self._record_learning_event(
            module="trade_history",
            event_type="sell_recorded",
            payload={
                "action": "sell",
                "stock_name": holding[2],
                "price": float(sell_price),
                "quantity": int(quantity),
                "sell_amount": float(sell_amount),
                "profit_loss": float(profit_loss),
                "profit_loss_pct": float(profit_loss_pct),
                "reason": reason,
            },
            ts_code=ts_code,
        )
    
    def generate_daily_report(self) -> str:
        """
        生成每日报告
        
        Returns:
            报告文本
        """
        logger.info("📝 生成每日报告...")
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 获取今日推荐
        conn = sqlite3.connect(self.assistant_db)
        recommendations = pd.read_sql_query(f"""
            SELECT * FROM daily_recommendations
            WHERE recommend_date = '{today}'
            ORDER BY score DESC
        """, conn)
        
        # 获取当前持仓
        holdings = pd.read_sql_query("""
            SELECT * FROM holdings WHERE status = 'holding'
        """, conn)
        
        # 获取今日交易
        trades_today = pd.read_sql_query(f"""
            SELECT * FROM trade_history
            WHERE trade_date = '{today}'
            ORDER BY created_at DESC
        """, conn)
        
        conn.close()
        
        # 生成报告
        report = f"""
{'='*80}
📊 智能交易助手 - 每日报告
{'='*80}

📅 日期: {today}
🕐 生成时间: {datetime.now().strftime('%H:%M:%S')}

{'='*80}
🎯 【今日选股推荐】
{'='*80}

"""
        
        if not recommendations.empty:
            for idx, rec in recommendations.iterrows():
                report += f"""
{idx+1}. {rec['stock_name']} ({rec['ts_code']})
   ⭐ 评分: {rec['score']:.1f}分 ({rec['grade'] if 'grade' in rec else ''})
   💰 价格: ¥{rec['price']:.2f}
   🏭 行业: {rec['industry']}
   💎 市值: {rec['market_cap']/100000000:.1f}亿
   📝 理由: {rec['reason'][:100]}...
   
"""
        else:
            report += "\n   暂无推荐股票\n\n"
        
        report += f"""
{'='*80}
📊 【当前持仓】
{'='*80}

"""
        
        if not holdings.empty:
            total_cost = 0
            total_value = 0
            total_profit = 0
            
            for idx, holding in holdings.iterrows():
                total_cost += holding['cost_total']
                total_value += holding['current_value']
                total_profit += holding['profit_loss']
                
                report += f"""
{idx+1}. {holding['stock_name']} ({holding['ts_code']})
   📅 买入日期: {holding['buy_date']}
   💰 买入价格: ¥{holding['buy_price']:.2f}
   📊 当前价格: ¥{holding['current_price']:.2f}
   📈 数量: {holding['quantity']}股
   💵 成本: ¥{holding['cost_total']:.2f}
   💰 市值: ¥{holding['current_value']:.2f}
   {"📈" if holding['profit_loss'] > 0 else "📉"} 盈亏: ¥{holding['profit_loss']:.2f} ({holding['profit_loss_pct']*100:.2f}%)
   
"""
            
            total_profit_pct = total_profit / total_cost if total_cost > 0 else 0
            
            report += f"""
【持仓汇总】
   总成本: ¥{total_cost:.2f}
   总市值: ¥{total_value:.2f}
   总盈亏: ¥{total_profit:.2f} ({total_profit_pct*100:.2f}%)
   
"""
        else:
            report += "\n   当前无持仓\n\n"
        
        report += f"""
{'='*80}
📝 【今日交易】
{'='*80}

"""
        
        if not trades_today.empty:
            for idx, trade in trades_today.iterrows():
                action_emoji = "🟢" if trade['action'] == 'buy' else "🔴"
                action_text = "买入" if trade['action'] == 'buy' else "卖出"
                
                report += f"""
{action_emoji} {action_text}: {trade['stock_name']} ({trade['ts_code']})
   💰 价格: ¥{trade['price']:.2f}
   📊 数量: {trade['quantity']}股
   💵 金额: ¥{trade['amount']:.2f}
"""
                
                if trade['action'] == 'sell' and trade['profit_loss']:
                    report += f"   {'📈' if trade['profit_loss'] > 0 else '📉'} 盈亏: ¥{trade['profit_loss']:.2f} ({trade['profit_loss_pct']*100:.2f}%)\n"
                
                report += f"   📝 原因: {trade['reason']}\n\n"
        else:
            report += "\n   今日无交易\n\n"
        
        report += f"""
{'='*80}
💡 【风险提示】
{'='*80}

⚠️ 本系统仅供参考，不构成投资建议
⚠️ 股市有风险，投资需谨慎
⚠️ 请严格执行止盈止损纪律
⚠️ 建议单只股票仓位不超过20%

{'='*80}
📊 报告结束
{'='*80}
"""
        
        # 🆕 发送每日报告通知
        self._send_daily_report_notification(report)
        self._record_learning_event(
            module="daily_report",
            event_type="report_generated",
            payload={
                "date": today,
                "recommendations_count": int(len(recommendations)),
                "holdings_count": int(len(holdings)),
                "trades_today_count": int(len(trades_today)),
            },
        )
        
        return report
    
    def _send_stock_selection_notification(self, recommendations: List[Dict]):
        """
        发送选股通知
        
        Args:
            recommendations: 推荐股票列表
        """
        if not self.notifier:
            return
        
        if not recommendations:
            return
        
        try:
            # 构建通知内容
            title = f"📊 每日选股推荐 ({datetime.now().strftime('%Y-%m-%d')})"
            
            content = f"✅ 今日选出 {len(recommendations)} 只优质股票：\n\n"
            
            for i, rec in enumerate(recommendations, 1):
                content += f"{i}. {rec['stock_name']} ({rec['ts_code']})\n"
                content += f"   ⭐ 评分: {rec['score']:.1f}分\n"
                content += f"   💰 价格: ¥{rec['price']:.2f}\n"
                content += f"   🏭 行业: {rec.get('industry', 'N/A')}\n"
                content += f"   📝 理由: {rec.get('reason', 'N/A')[:50]}...\n\n"
            
            content += "\n⚠️ 请人工审核后决策，不构成投资建议！"
            
            # 发送通知
            self.notifier.send_notification(title, content, urgent=False)
            logger.info("📧 选股通知已发送")
            
        except Exception as e:
            logger.error(f"❌ 发送选股通知失败: {e}")
    
    def _send_stop_condition_notification(self, alerts: List[Dict]):
        """
        发送止盈止损通知
        
        Args:
            alerts: 提醒列表
        """
        if not self.notifier:
            return
        
        if not alerts:
            return
        
        try:
            title = "⚠️ 止盈止损提醒"
            
            content = f"检测到 {len(alerts)} 只股票触发条件：\n\n"
            
            for alert in alerts:
                if alert['type'] == 'take_profit':
                    content += f"🎉 止盈: {alert['stock_name']} ({alert['ts_code']})\n"
                    content += f"   买入: ¥{alert['buy_price']:.2f}\n"
                    content += f"   当前: ¥{alert['current_price']:.2f}\n"
                    content += f"   盈利: {alert['profit_pct']*100:.2f}%\n"
                    content += f"   建议: 考虑止盈卖出\n\n"
                else:
                    content += f"⚠️ 止损: {alert['stock_name']} ({alert['ts_code']})\n"
                    content += f"   买入: ¥{alert['buy_price']:.2f}\n"
                    content += f"   当前: ¥{alert['current_price']:.2f}\n"
                    content += f"   亏损: {abs(alert['profit_pct'])*100:.2f}%\n"
                    content += f"   建议: 及时止损！\n\n"
            
            content += "\n⚠️ 请及时处理，严格执行交易纪律！"
            
            # 发送通知（紧急）
            self.notifier.send_notification(title, content, urgent=True)
            logger.info("📧 止盈止损通知已发送")
            
        except Exception as e:
            logger.error(f"❌ 发送止盈止损通知失败: {e}")
    
    def _send_daily_report_notification(self, report: str):
        """
        发送每日报告通知
        
        Args:
            report: 报告内容
        """
        if not self.notifier:
            return
        
        try:
            title = f"📊 每日交易报告 ({datetime.now().strftime('%Y-%m-%d')})"
            
            # 报告内容可能很长，只发送摘要
            lines = report.split('\n')
            summary_lines = []
            in_summary = False
            
            for line in lines:
                if '【今日选股推荐】' in line or '【当前持仓】' in line or '【持仓汇总】' in line:
                    in_summary = True
                    summary_lines.append(line)
                elif in_summary and len(summary_lines) < 30:
                    summary_lines.append(line)
                elif '【今日交易】' in line:
                    break
            
            content = '\n'.join(summary_lines[:30])
            content += "\n\n📱 完整报告请登录系统查看"
            
            # 发送通知
            self.notifier.send_notification(title, content, urgent=False)
            logger.info("📧 每日报告通知已发送")
            
        except Exception as e:
            logger.error(f"❌ 发送每日报告通知失败: {e}")
    
    def setup_email_notification(self, receiver_emails: List[str], 
                                 sender_email: str = None, 
                                 sender_password: str = None):
        """
        快速配置邮件通知
        
        Args:
            receiver_emails: 接收邮箱列表（必填）
            sender_email: 发件邮箱（可选，使用默认）
            sender_password: 发件邮箱授权码（可选，使用默认）
        """
        # 加载或创建配置
        config_file = "notification_config.json"
        
        if Path(config_file).exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
        else:
            config = {
                "enabled": True,
                "email": {
                    "enabled": True,
                    "smtp_server": "smtp.qq.com",
                    "smtp_port": 587,
                    "sender_email": "",
                    "sender_password": "",
                    "receiver_emails": []
                },
                "wechat_work": {"enabled": False, "webhook_url": ""},
                "dingtalk": {"enabled": False, "webhook_url": "", "secret": ""},
                "serverchan": {"enabled": False, "sendkey": ""},
                "bark": {"enabled": False, "device_key": ""}
            }
        
        # 更新接收邮箱
        config['enabled'] = True
        config['email']['enabled'] = True
        config['email']['receiver_emails'] = receiver_emails
        
        # 如果提供了发件邮箱信息，更新它
        if sender_email:
            config['email']['sender_email'] = sender_email
        if sender_password:
            config['email']['sender_password'] = sender_password
        
        # 保存配置
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        # 重新初始化通知服务
        if NOTIFICATION_AVAILABLE:
            try:
                self.notifier = NotificationService(config_file)
                logger.info(f"✅ 邮件通知配置成功！接收邮箱: {len(receiver_emails)}个")
                for email in receiver_emails:
                    logger.info(f"   📧 {email}")
                return True
            except Exception as e:
                logger.error(f"❌ 通知服务初始化失败: {e}")
                return False
        return False
    
    def add_receiver_email(self, email: str):
        """
        添加接收邮箱
        
        Args:
            email: 邮箱地址
        """
        config_file = "notification_config.json"
        
        if not Path(config_file).exists():
            logger.error("❌ 请先配置邮件通知")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        receivers = config['email']['receiver_emails']
        if email not in receivers:
            receivers.append(email)
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 已添加接收邮箱: {email}")
            return True
        else:
            logger.info(f"ℹ️ 邮箱已存在: {email}")
            return False
    
    def remove_receiver_email(self, email: str):
        """
        删除接收邮箱
        
        Args:
            email: 邮箱地址
        """
        config_file = "notification_config.json"
        
        if not Path(config_file).exists():
            logger.error("❌ 配置文件不存在")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        receivers = config['email']['receiver_emails']
        if email in receivers:
            receivers.remove(email)
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 已删除接收邮箱: {email}")
            return True
        else:
            logger.info(f"ℹ️ 邮箱不存在: {email}")
            return False
    
    def list_receiver_emails(self) -> List[str]:
        """
        列出所有接收邮箱
        
        Returns:
            接收邮箱列表
        """
        config_file = "notification_config.json"
        
        if not Path(config_file).exists():
            logger.warning("⚠️ 配置文件不存在")
            return []
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        return config.get('email', {}).get('receiver_emails', [])
    
    def get_statistics(self) -> Dict:
        """获取统计数据"""
        conn = sqlite3.connect(self.assistant_db)
        
        # 总交易次数
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trade_history")
        total_trades = cursor.fetchone()[0]
        
        # 买入次数
        cursor.execute("SELECT COUNT(*) FROM trade_history WHERE action = 'buy'")
        buy_count = cursor.fetchone()[0]
        
        # 卖出次数
        cursor.execute("SELECT COUNT(*) FROM trade_history WHERE action = 'sell'")
        sell_count = cursor.fetchone()[0]
        
        # 盈利次数
        cursor.execute("SELECT COUNT(*) FROM trade_history WHERE action = 'sell' AND profit_loss > 0")
        profit_count = cursor.fetchone()[0]
        
        # 总盈亏
        cursor.execute("SELECT SUM(profit_loss) FROM trade_history WHERE action = 'sell'")
        total_profit = cursor.fetchone()[0] or 0
        
        # 胜率
        win_rate = profit_count / sell_count if sell_count > 0 else 0
        
        # 当前持仓数
        cursor.execute("SELECT COUNT(*) FROM holdings WHERE status = 'holding'")
        holding_count = cursor.fetchone()[0]
        
        # 持仓总市值
        cursor.execute("SELECT SUM(current_value) FROM holdings WHERE status = 'holding'")
        holding_value = cursor.fetchone()[0] or 0
        
        # 持仓总盈亏
        cursor.execute("SELECT SUM(profit_loss) FROM holdings WHERE status = 'holding'")
        holding_profit = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            'total_trades': total_trades,
            'buy_count': buy_count,
            'sell_count': sell_count,
            'profit_count': profit_count,
            'win_rate': win_rate,
            'total_profit': total_profit,
            'holding_count': holding_count,
            'holding_value': holding_value,
            'holding_profit': holding_profit
        }


# 便捷函数
def create_assistant() -> TradingAssistant:
    """创建交易助手实例"""
    return TradingAssistant()


if __name__ == "__main__":
    # 测试代码
    assistant = TradingAssistant()
    
    print("🚀 智能交易助手测试")
    print("="*80)
    
    # 测试每日选股
    print("\n📊 测试每日选股...")
    recommendations = assistant.daily_stock_scan(top_n=5)
    
    if recommendations:
        print(f"\n✅ 找到{len(recommendations)}个推荐:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec['stock_name']}({rec['ts_code']}): {rec['score']:.1f}分")
    
    # 生成报告
    print("\n📝 生成每日报告...")
    report = assistant.generate_daily_report()
    print(report)
    
    # 统计
    stats = assistant.get_statistics()
    print("\n📊 统计数据:")
    print(f"   总交易: {stats['total_trades']}次")
    print(f"   当前持仓: {stats['holding_count']}只")
    print(f"   胜率: {stats['win_rate']*100:.1f}%")
    
    print("\n✅ 测试完成！")

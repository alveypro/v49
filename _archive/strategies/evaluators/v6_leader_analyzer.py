#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 v6.0龙头属性分析器
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
识别板块龙头和涨停基因，提升超短线选股精准度
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import os
import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = str(ROOT / "config.json")
PERMANENT_DB_PATH = str(ROOT / "permanent_stock_database.db")


def _load_config() -> Dict:
    if not os.path.exists(CONFIG_PATH):
        return {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


class V6LeaderAnalyzer:
    """v6.0龙头属性分析器"""
    
    def __init__(self):
        cfg = _load_config()
        self.db_path = cfg.get("PERMANENT_DB_PATH", PERMANENT_DB_PATH)
        self._sector_ranking_cache = {}  # 板块排名缓存
        self._limit_up_cache = {}  # 涨停缓存
        
    def calculate_leader_score(self, ts_code: str, industry: str, 
                               recent_change_3d: float) -> Dict:
        """
        计算龙头属性得分（10分）
        
        参数：
            ts_code: 股票代码
            industry: 所属行业
            recent_change_3d: 近3日涨幅
            
        返回：
        {
            'sector_rank': 1,  # 板块内排名
            'sector_rank_score': 6.0,  # 板块排名得分
            'limit_up_count_20d': 3,  # 近20天涨停次数
            'limit_up_score': 4.0,  # 涨停基因得分
            'total_score': 10.0,  # 总分
            'is_sector_leader': True  # 是否板块龙头
        }
        """
        try:
            # 1. 板块内排名（6分）
            rank_info = self._get_sector_ranking(ts_code, industry, recent_change_3d)
            sector_rank_score = self._score_sector_ranking(rank_info)
            
            # 2. 涨停基因（4分）
            limit_up_info = self._get_limit_up_stats(ts_code)
            limit_up_score = self._score_limit_up_gene(limit_up_info)
            
            # 3. 综合得分
            total_score = sector_rank_score + limit_up_score
            
            return {
                'sector_rank': rank_info['rank'],
                'total_stocks': rank_info['total'],
                'sector_rank_score': sector_rank_score,
                'limit_up_count_20d': limit_up_info['count_20d'],
                'limit_up_count_60d': limit_up_info['count_60d'],
                'limit_up_score': limit_up_score,
                'total_score': round(total_score, 1),
                'is_sector_leader': rank_info['rank'] <= 3  # 前3名是龙头
            }
            
        except Exception as e:
            logger.warning(f"龙头属性计算失败 {ts_code}: {e}")
            return self._default_leader_score()
    
    def _get_sector_ranking(self, ts_code: str, industry: str, 
                           recent_change_3d: float) -> Dict:
        """
        获取股票在板块内的排名
        
        返回：
        {
            'rank': 1,  # 排名（1-N）
            'total': 50,  # 板块总股票数
            'rank_pct': 0.02  # 排名百分比
        }
        """
        try:
            cache_key = f"{industry}_ranking"
            
            # 检查缓存（5分钟有效）
            if cache_key in self._sector_ranking_cache:
                cached = self._sector_ranking_cache[cache_key]
                if (datetime.now() - cached['time']).seconds < 300:
                    # 从缓存中查找该股票排名
                    ranking_df = cached['data']
                    stock_row = ranking_df[ranking_df['ts_code'] == ts_code]
                    if len(stock_row) > 0:
                        return {
                            'rank': int(stock_row['rank'].iloc[0]),
                            'total': len(ranking_df),
                            'rank_pct': stock_row['rank'].iloc[0] / len(ranking_df)
                        }
            
            # 查询板块内所有股票的3日涨幅
            conn = sqlite3.connect(self.db_path)
            
            # 获取最近3个交易日
            dates_query = """
                SELECT DISTINCT trade_date
                FROM daily_trading_data
                ORDER BY trade_date DESC
                LIMIT 3
            """
            dates_df = pd.read_sql_query(dates_query, conn)
            
            if len(dates_df) < 3:
                conn.close()
                return {'rank': 999, 'total': 1, 'rank_pct': 1.0}
            
            start_date = dates_df['trade_date'].iloc[-1]
            end_date = dates_df['trade_date'].iloc[0]
            
            # 查询板块内所有股票的涨跌幅
            query = """
                SELECT 
                    sb.ts_code,
                    SUM(dt.pct_chg) as change_3d
                FROM stock_basic sb
                INNER JOIN daily_trading_data dt ON sb.ts_code = dt.ts_code
                WHERE sb.industry = ?
                AND dt.trade_date BETWEEN ? AND ?
                GROUP BY sb.ts_code
                HAVING COUNT(*) = 3
                ORDER BY change_3d DESC
            """
            
            ranking_df = pd.read_sql_query(query, conn, params=(industry, start_date, end_date))
            conn.close()
            
            if len(ranking_df) == 0:
                return {'rank': 999, 'total': 1, 'rank_pct': 1.0}
            
            # 添加排名
            ranking_df['rank'] = range(1, len(ranking_df) + 1)
            
            # 缓存结果
            self._sector_ranking_cache[cache_key] = {
                'data': ranking_df,
                'time': datetime.now()
            }
            
            # 查找当前股票排名
            stock_row = ranking_df[ranking_df['ts_code'] == ts_code]
            if len(stock_row) > 0:
                rank = int(stock_row['rank'].iloc[0])
                total = len(ranking_df)
                return {
                    'rank': rank,
                    'total': total,
                    'rank_pct': rank / total
                }
            else:
                # 股票不在排名中，可能是数据不足
                return {'rank': 999, 'total': len(ranking_df), 'rank_pct': 1.0}
                
        except Exception as e:
            logger.warning(f"板块排名获取失败 {ts_code}: {e}")
            return {'rank': 999, 'total': 1, 'rank_pct': 1.0}
    
    def _get_limit_up_stats(self, ts_code: str) -> Dict:
        """
        获取涨停统计
        
        返回：
        {
            'count_20d': 2,  # 近20天涨停次数
            'count_60d': 5,  # 近60天涨停次数
            'last_limit_up': '20231220'  # 最后一次涨停日期
        }
        """
        try:
            cache_key = ts_code
            
            # 检查缓存（当日有效）
            if cache_key in self._limit_up_cache:
                cached = self._limit_up_cache[cache_key]
                if cached['date'] == datetime.now().strftime('%Y%m%d'):
                    return cached['data']
            
            # 查询涨停数据
            conn = sqlite3.connect(self.db_path)
            
            # 近20天涨停
            query_20d = """
                SELECT COUNT(*) as count
                FROM daily_trading_data
                WHERE ts_code = ?
                AND pct_chg >= 9.5
                ORDER BY trade_date DESC
                LIMIT 20
            """
            count_20d = pd.read_sql_query(query_20d, conn, params=(ts_code,))['count'].iloc[0]
            
            # 近60天涨停
            query_60d = """
                SELECT COUNT(*) as count
                FROM daily_trading_data
                WHERE ts_code = ?
                AND pct_chg >= 9.5
                ORDER BY trade_date DESC
                LIMIT 60
            """
            count_60d = pd.read_sql_query(query_60d, conn, params=(ts_code,))['count'].iloc[0]
            
            # 最后一次涨停日期
            query_last = """
                SELECT trade_date
                FROM daily_trading_data
                WHERE ts_code = ?
                AND pct_chg >= 9.5
                ORDER BY trade_date DESC
                LIMIT 1
            """
            last_df = pd.read_sql_query(query_last, conn, params=(ts_code,))
            last_limit_up = last_df['trade_date'].iloc[0] if len(last_df) > 0 else None
            
            conn.close()
            
            result = {
                'count_20d': int(count_20d),
                'count_60d': int(count_60d),
                'last_limit_up': last_limit_up
            }
            
            # 缓存结果
            self._limit_up_cache[cache_key] = {
                'data': result,
                'date': datetime.now().strftime('%Y%m%d')
            }
            
            return result
            
        except Exception as e:
            logger.warning(f"涨停统计获取失败 {ts_code}: {e}")
            return {'count_20d': 0, 'count_60d': 0, 'last_limit_up': None}
    
    def _score_sector_ranking(self, rank_info: Dict) -> float:
        """
        板块排名评分（6分）
        
        规则：
        - 第1名：6分（绝对龙头）
        - 前3名：5分（龙头）
        - 前10名：3分（强势）
        - 前30%：1分（跟随）
        """
        rank = rank_info['rank']
        rank_pct = rank_info['rank_pct']
        
        if rank == 1:
            return 6.0  # 绝对龙头
        elif rank <= 3:
            return 5.0  # 龙头
        elif rank <= 10:
            return 3.0  # 强势
        elif rank_pct <= 0.3:
            return 1.0  # 跟随
        else:
            return 0.0
    
    def _score_limit_up_gene(self, limit_up_info: Dict) -> float:
        """
        涨停基因评分（4分）
        
        规则：
        - 近20天≥3次涨停：4分（强妖股）
        - 近20天≥2次涨停：3分（妖股）
        - 近20天≥1次涨停：2分（有爆发力）
        - 近60天≥2次涨停：1分（潜力股）
        """
        count_20d = limit_up_info['count_20d']
        count_60d = limit_up_info['count_60d']
        
        if count_20d >= 3:
            return 4.0  # 强妖股
        elif count_20d >= 2:
            return 3.0  # 妖股
        elif count_20d >= 1:
            return 2.0  # 有爆发力
        elif count_60d >= 2:
            return 1.0  # 潜力股
        else:
            return 0.0
    
    def _default_leader_score(self) -> Dict:
        """默认龙头属性得分（全0）"""
        return {
            'sector_rank': 999,
            'total_stocks': 1,
            'sector_rank_score': 0.0,
            'limit_up_count_20d': 0,
            'limit_up_count_60d': 0,
            'limit_up_score': 0.0,
            'total_score': 0.0,
            'is_sector_leader': False
        }


# ========== 便捷调用接口 ==========

_leader_analyzer = None

def get_leader_analyzer() -> V6LeaderAnalyzer:
    """获取龙头属性分析器（单例）"""
    global _leader_analyzer
    if _leader_analyzer is None:
        _leader_analyzer = V6LeaderAnalyzer()
    return _leader_analyzer


def calculate_leader_score(ts_code: str, industry: str, recent_change_3d: float) -> Dict:
    """便捷调用：计算龙头属性得分"""
    analyzer = get_leader_analyzer()
    return analyzer.calculate_leader_score(ts_code, industry, recent_change_3d)

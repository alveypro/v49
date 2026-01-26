#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 v6.0数据提供模块 - 优化版（使用本地数据库）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
避免Tushare API限流，使用本地数据库计算板块热度
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import sqlite3
import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

TUSHARE_TOKEN = "9ad24a6745c2625e7e2064d03855f5a419efa06c97e5e7df70c64856"
PERMANENT_DB_PATH = "/Users/mac/QLIB/permanent_stock_database.db"


class V6DataProviderOptimized:
    """v6.0数据提供者 - 优化版（使用本地数据库）"""
    
    def __init__(self):
        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api(TUSHARE_TOKEN)
        self._sector_cache = {}
        self._money_flow_cache = {}
        self._north_money_cache = {}
        self._industry_performance_cache = None  # 全局行业表现缓存
        self._hs_const_stocks = None  # 陆股通标的缓存
        
    def get_stock_sector(self, ts_code: str) -> Dict:
        """
        获取股票所属板块/行业（从本地数据库）
        
        返回：
        {
            'industry': '电子',
            'concept': [],  # 概念暂时简化
            'area': '深圳'
        }
        """
        try:
            # 使用缓存
            cache_key = ts_code
            if cache_key in self._sector_cache:
                return self._sector_cache[cache_key]
            
            # 从本地数据库获取
            conn = sqlite3.connect(PERMANENT_DB_PATH)
            query = """
                SELECT industry, name
                FROM stock_basic
                WHERE ts_code = ?
            """
            df = pd.read_sql_query(query, conn, params=(ts_code,))
            conn.close()
            
            if len(df) == 0:
                return {'industry': '其他', 'concept': [], 'area': '未知'}
            
            industry = df['industry'].iloc[0] if not pd.isna(df['industry'].iloc[0]) else '其他'
            
            # 从股票名称推断概念（简化版）
            name = df['name'].iloc[0] if 'name' in df.columns else ''
            concepts = []
            hot_keywords = {
                '新能源': ['新能源', '锂电', '光伏', '储能'],
                '人工智能': ['AI', '人工智能', '大模型', '算力'],
                '芯片': ['芯片', '半导体', '集成电路'],
                '生物医药': ['医药', '生物', '疫苗', '医疗']
            }
            for concept_name, keywords in hot_keywords.items():
                if any(keyword in name or keyword in industry for keyword in keywords):
                    concepts.append(concept_name)
            
            result = {
                'industry': industry,
                'concept': concepts[:3],  # 最多3个概念
                'area': '未知'
            }
            
            # 缓存
            self._sector_cache[cache_key] = result
            return result
            
        except Exception as e:
            logger.warning(f"获取板块信息失败 {ts_code}: {e}")
            return {'industry': '其他', 'concept': [], 'area': '未知'}
    
    def get_sector_performance(self, industry: str, days: int = 3) -> Dict:
        """
        获取板块表现（从本地数据库计算，避免API限流）
        
        返回：
        {
            'change_3d': 5.2,  # 3天涨跌幅%
            'avg_change': 1.7,  # 日均涨跌幅%
            'rank': 5,  # 行业排名
            'total_industries': 30  # 总行业数
        }
        """
        try:
            # 使用缓存
            cache_key = f"{industry}_{days}"
            if cache_key in self._sector_cache:
                return self._sector_cache[cache_key]
            
            # 如果全局行业表现缓存不存在，先计算所有行业
            if self._industry_performance_cache is None:
                self._calculate_all_industries_performance(days)
            
            # 从缓存中获取该行业的表现
            if industry in self._industry_performance_cache:
                result = self._industry_performance_cache[industry]
                self._sector_cache[cache_key] = result
                return result
            else:
                # 行业不存在，返回默认值
                return {
                    'change_3d': 0,
                    'avg_change': 0,
                    'rank': 50,
                    'total_industries': len(self._industry_performance_cache) if self._industry_performance_cache else 100,
                    'money_flow': 0
                }
                
        except Exception as e:
            logger.warning(f"获取板块表现失败: {e}")
            return {
                'change_3d': 0,
                'avg_change': 0,
                'rank': 50,
                'total_industries': 100,
                'money_flow': 0
            }
    
    def _calculate_all_industries_performance(self, days: int = 3):
        """
        一次性计算所有行业的表现（使用本地数据库）
        避免对每只股票都调用API
        """
        try:
            conn = sqlite3.connect(PERMANENT_DB_PATH)
            
            # 获取最近N天的日期
            latest_date_query = """
                SELECT MAX(trade_date) as latest_date
                FROM daily_trading_data
            """
            latest_df = pd.read_sql_query(latest_date_query, conn)
            latest_date = latest_df['latest_date'].iloc[0]
            
            # 计算起始日期（最近N个交易日）
            date_query = """
                SELECT DISTINCT trade_date
                FROM daily_trading_data
                WHERE trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            """
            dates_df = pd.read_sql_query(date_query, conn, params=(latest_date, days+1))
            
            if len(dates_df) < days:
                logger.warning(f"数据不足，只有{len(dates_df)}个交易日")
                conn.close()
                self._industry_performance_cache = {}
                return
            
            start_date = dates_df['trade_date'].iloc[-1]
            
            # 查询所有股票在这段时间的涨跌幅
            query = """
                SELECT 
                    sb.industry,
                    sb.ts_code,
                    SUM(dt.pct_chg) as total_change
                FROM stock_basic sb
                INNER JOIN daily_trading_data dt ON sb.ts_code = dt.ts_code
                WHERE dt.trade_date >= ?
                AND dt.trade_date <= ?
                AND sb.industry IS NOT NULL
                AND sb.industry != ''
                GROUP BY sb.industry, sb.ts_code
            """
            
            df = pd.read_sql_query(query, conn, params=(start_date, latest_date))
            conn.close()
            
            if len(df) == 0:
                logger.warning("没有查询到行业数据")
                self._industry_performance_cache = {}
                return
            
            # 按行业分组，计算平均涨跌幅
            industry_performance = df.groupby('industry')['total_change'].agg(['mean', 'count']).reset_index()
            industry_performance.columns = ['industry', 'change', 'stock_count']
            
            # 过滤掉股票数量太少的行业（<5只）
            industry_performance = industry_performance[industry_performance['stock_count'] >= 5]
            
            # 按涨跌幅排序，计算排名
            industry_performance = industry_performance.sort_values('change', ascending=False).reset_index(drop=True)
            industry_performance['rank'] = range(1, len(industry_performance) + 1)
            
            # 转换为字典缓存
            self._industry_performance_cache = {}
            total_industries = len(industry_performance)
            
            for _, row in industry_performance.iterrows():
                self._industry_performance_cache[row['industry']] = {
                    'change_3d': round(row['change'], 2),
                    'avg_change': round(row['change'] / days, 2),
                    'rank': int(row['rank']),
                    'total_industries': total_industries,
                    'money_flow': 0,
                    'stock_count': int(row['stock_count'])
                }
            
            logger.info(f"✅ 成功计算{total_industries}个行业的表现")
            
        except Exception as e:
            logger.error(f"计算行业表现失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self._industry_performance_cache = {}
    
    def get_money_flow(self, ts_code: str, days: int = 3) -> Dict:
        """
        获取资金流向数据（优化版：API失败时使用本地估算）
        
        返回：
        {
            'buy_lg_amount': 5000.0,  # 大单买入金额（万元）
            'sell_lg_amount': 3000.0,  # 大单卖出金额（万元）
            'net_mf_amount': 2000.0,  # 净流入（万元）
            'buy_elg_amount': 8000.0,  # 超大单买入
            'consecutive_inflow_days': 3  # 连续流入天数
        }
        """
        try:
            # 使用缓存
            cache_key = f"{ts_code}_{days}"
            if cache_key in self._money_flow_cache:
                return self._money_flow_cache[cache_key]
            
            # 获取资金流向数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days+10)).strftime('%Y%m%d')
            
            try:
                # Tushare Pro高级接口：个股资金流向
                money_flow = self.pro.moneyflow(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='trade_date,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount'
                )
                
                if len(money_flow) == 0:
                    # API返回空数据，使用本地降级
                    logger.debug(f"{ts_code} API返回空，使用本地估算")
                    return self._get_money_flow_from_local(ts_code, days)
                
                # 按日期排序（最新的在前）
                money_flow = money_flow.sort_values('trade_date', ascending=False)
                
                # 计算最近N天的数据
                recent = money_flow.head(days)
                
                # 大单净流入
                buy_lg = recent['buy_lg_amount'].sum() if 'buy_lg_amount' in recent.columns else 0
                sell_lg = recent['sell_lg_amount'].sum() if 'sell_lg_amount' in recent.columns else 0
                
                # 超大单
                buy_elg = recent['buy_elg_amount'].sum() if 'buy_elg_amount' in recent.columns else 0
                sell_elg = recent['sell_elg_amount'].sum() if 'sell_elg_amount' in recent.columns else 0
                
                # 净流入
                net_mf = recent['net_mf_amount'].sum() if 'net_mf_amount' in recent.columns else (buy_lg - sell_lg)
                
                # 连续流入天数
                consecutive_days = 0
                for _, row in recent.iterrows():
                    if row.get('net_mf_amount', 0) > 0:
                        consecutive_days += 1
                    else:
                        break
                
                result = {
                    'buy_lg_amount': buy_lg,
                    'sell_lg_amount': sell_lg,
                    'net_mf_amount': net_mf,
                    'buy_elg_amount': buy_elg,
                    'sell_elg_amount': sell_elg,
                    'consecutive_inflow_days': consecutive_days,
                    'today_net': money_flow['net_mf_amount'].iloc[0] if len(money_flow) > 0 else 0
                }
                
                # 缓存
                self._money_flow_cache[cache_key] = result
                return result
                
            except Exception as e:
                # API调用失败，使用本地降级方案
                logger.warning(f"{ts_code} API失败，使用本地估算: {e}")
                return self._get_money_flow_from_local(ts_code, days)
                
        except Exception as e:
            logger.warning(f"获取资金流失败: {e}")
            return self._get_money_flow_from_local(ts_code, days)
    
    def _get_money_flow_from_local(self, ts_code: str, days: int = 3) -> Dict:
        """
        使用本地数据估算资金流向（降级方案）
        
        原理：涨幅 × 放量比例 = 资金流向估算
        """
        try:
            conn = sqlite3.connect(PERMANENT_DB_PATH)
            
            # 查询最近N天的数据
            query = """
                SELECT 
                    vol,
                    pct_chg,
                    close_price,
                    trade_date
                FROM daily_trading_data
                WHERE ts_code = ?
                ORDER BY trade_date DESC
                LIMIT ?
            """
            df = pd.read_sql_query(query, conn, params=(ts_code, days + 20))
            conn.close()
            
            if len(df) < days:
                return self._default_money_flow()
            
            # 计算平均成交量（用于判断放量）
            avg_vol = df['vol'].iloc[days:].mean() if len(df) > days else df['vol'].mean()
            
            if avg_vol == 0:
                return self._default_money_flow()
            
            # 计算资金流向
            net_flow = 0
            consecutive_days = 0
            recent_data = df.head(days)
            
            for i, row in recent_data.iterrows():
                vol_ratio = row['vol'] / avg_vol
                price_chg = row['pct_chg']
                
                # 估算资金流：涨幅 × (放量比例-1) × 基数
                # 🔥 大幅提高估算系数：从500提高到8000（更接近真实资金流规模）
                if price_chg > 0 and vol_ratio > 1.0:
                    day_flow = price_chg * (vol_ratio - 1) * 8000  # 估算万元
                    net_flow += day_flow
                    consecutive_days += 1
                elif price_chg < 0 and vol_ratio > 1.0:
                    day_flow = price_chg * (vol_ratio - 1) * 8000
                    net_flow += day_flow
                    break  # 跌停止计算连续天数
                else:
                    break
            
            return {
                'net_mf_amount': round(net_flow, 2),
                'consecutive_inflow_days': consecutive_days,
                'buy_lg_amount': max(0, net_flow),
                'sell_lg_amount': max(0, -net_flow),
                'buy_elg_amount': 0,
                'sell_elg_amount': 0,
                'today_net': 0
            }
            
        except Exception as e:
            logger.warning(f"本地资金流计算失败 {ts_code}: {e}")
            return self._default_money_flow()
    
    def _load_hs_const_stocks(self):
        """一次性加载所有陆股通标的（避免重复调用API）"""
        if self._hs_const_stocks is not None:
            return  # 已加载
        
        try:
            logger.info("正在加载陆股通标的...")
            # 从Tushare获取陆股通成分股（只调用2次）
            sh_const = self.pro.hs_const(hs_type='SH')  # 沪股通
            sz_const = self.pro.hs_const(hs_type='SZ')  # 深股通
            
            all_const = pd.concat([sh_const, sz_const])
            self._hs_const_stocks = set(all_const['ts_code'].tolist())
            
            logger.info(f"✅ 成功加载{len(self._hs_const_stocks)}只陆股通标的")
            
        except Exception as e:
            logger.warning(f"加载陆股通标的失败: {e}")
            self._hs_const_stocks = set()
    
    def get_north_money_flow(self, ts_code: str, days: int = 3) -> Dict:
        """
        获取北向资金（陆股通）流向（优化版）
        
        返回：
        {
            'buy_amount': 0,  # 买入金额（简化版不提供）
            'sell_amount': 0,  # 卖出金额（简化版不提供）
            'net_amount': 0,  # 净买入（简化版不提供）
            'consecutive_buy_days': 0,  # 连续买入天数（简化版不提供）
            'is_connect_stock': True  # 是否陆股通标的（准确判断）
        }
        """
        try:
            # 使用缓存
            cache_key = f"{ts_code}_north_{days}"
            if cache_key in self._north_money_cache:
                return self._north_money_cache[cache_key]
            
            # 确保已加载陆股通标的
            if self._hs_const_stocks is None:
                self._load_hs_const_stocks()
            
            # 判断是否陆股通标的（从缓存）
            is_connect = ts_code in self._hs_const_stocks if self._hs_const_stocks else False
            
            result = {
                'buy_amount': 0,
                'sell_amount': 0,
                'net_amount': 0,
                'north_net_3d': 0,  # 别名，兼容v6评分器
                'consecutive_buy_days': 0,
                'is_connect_stock': is_connect
            }
            
            # 缓存
            self._north_money_cache[cache_key] = result
            return result
                
        except Exception as e:
            logger.warning(f"获取北向资金失败: {e}")
            return {
                'buy_amount': 0,
                'sell_amount': 0,
                'net_amount': 0,
                'north_net_3d': 0,  # 别名，兼容v6评分器
                'consecutive_buy_days': 0,
                'is_connect_stock': False
            }
    
    def get_market_change(self, days: int = 3) -> float:
        """
        获取大盘涨跌幅（从本地数据库）
        
        返回：大盘N天涨跌幅（%）
        """
        try:
            conn = sqlite3.connect(PERMANENT_DB_PATH)
            
            # 获取上证指数最近N天的涨跌幅
            query = """
                SELECT SUM(pct_chg) as total_change
                FROM (
                    SELECT pct_chg
                    FROM daily_trading_data
                    WHERE ts_code = '000001.SH'
                    ORDER BY trade_date DESC
                    LIMIT ?
                )
            """
            
            df = pd.read_sql_query(query, conn, params=(days,))
            conn.close()
            
            if len(df) > 0 and not pd.isna(df['total_change'].iloc[0]):
                return float(df['total_change'].iloc[0])
            else:
                return 0.0
                
        except Exception as e:
            logger.warning(f"获取大盘涨跌幅失败: {e}")
            return 0.0
    
    def _default_money_flow(self) -> Dict:
        """默认资金流数据"""
        return {
            'buy_lg_amount': 0,
            'sell_lg_amount': 0,
            'net_mf_amount': 0,
            'buy_elg_amount': 0,
            'sell_elg_amount': 0,
            'consecutive_inflow_days': 0,
            'today_net': 0
        }
    
    def clear_cache(self):
        """清空缓存"""
        self._sector_cache.clear()
        self._money_flow_cache.clear()
        self._north_money_cache.clear()
        self._industry_performance_cache = None


# 全局单例
_data_provider_optimized = None

def get_data_provider() -> V6DataProviderOptimized:
    """获取优化版数据提供者单例"""
    global _data_provider_optimized
    if _data_provider_optimized is None:
        _data_provider_optimized = V6DataProviderOptimized()
    return _data_provider_optimized


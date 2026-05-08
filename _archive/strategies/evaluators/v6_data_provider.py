#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔥 v6.0数据提供模块 - Tushare Pro高级接口
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
提供板块、资金流、北向资金等高级数据
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from functools import lru_cache
import time

logger = logging.getLogger(__name__)

TUSHARE_TOKEN = "9ad24a6745c2625e7e2064d03855f5a419efa06c97e5e7df70c64856"


class V6DataProvider:
    """v6.0数据提供者 - 使用Tushare Pro高级接口"""
    
    def __init__(self):
        ts.set_token(TUSHARE_TOKEN)
        self.pro = ts.pro_api(TUSHARE_TOKEN)
        self._sector_cache = {}
        self._money_flow_cache = {}
        self._north_money_cache = {}
        
    def get_stock_sector(self, ts_code: str) -> Dict:
        """
        获取股票所属板块/行业
        
        返回：
        {
            'industry': '电子',
            'concept': ['新能源汽车', '锂电池'],
            'area': '深圳'
        }
        """
        try:
            # 使用缓存
            cache_key = ts_code
            if cache_key in self._sector_cache:
                return self._sector_cache[cache_key]
            
            # 获取股票基本信息（含行业）
            basic_info = self.pro.stock_basic(
                ts_code=ts_code,
                fields='ts_code,name,industry,area'
            )
            
            if len(basic_info) == 0:
                return {'industry': '其他', 'concept': [], 'area': '未知'}
            
            industry = basic_info['industry'].iloc[0] if not pd.isna(basic_info['industry'].iloc[0]) else '其他'
            area = basic_info['area'].iloc[0] if not pd.isna(basic_info['area'].iloc[0]) else '未知'
            
            # 获取概念板块（如果可用）
            try:
                concept = self.pro.concept_detail(ts_code=ts_code)
                concept_list = concept['name'].tolist() if len(concept) > 0 else []
            except:
                concept_list = []
            
            result = {
                'industry': industry,
                'concept': concept_list[:5],  # 最多5个概念
                'area': area
            }
            
            # 缓存
            self._sector_cache[cache_key] = result
            return result
            
        except Exception as e:
            logger.warning(f"获取板块信息失败 {ts_code}: {e}")
            return {'industry': '其他', 'concept': [], 'area': '未知'}
    
    def get_sector_performance(self, industry: str, days: int = 3) -> Dict:
        """
        获取板块表现（近N天）
        
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
            
            # 获取行业日线数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days+5)).strftime('%Y%m%d')
            
            # 获取所有行业的数据
            try:
                # 使用行业指数（如果可用）
                industry_daily = self.pro.index_dailybasic(
                    trade_date=end_date,
                    fields='ts_code,trade_date,turnover_rate,pe,pb'
                )
                
                # 计算行业涨跌幅（使用股票统计）
                # 获取该行业的所有股票
                stocks = self.pro.stock_basic(
                    fields='ts_code,name,industry',
                    list_status='L'
                )
                
                industry_stocks = stocks[stocks['industry'] == industry]['ts_code'].tolist()
                
                if len(industry_stocks) == 0:
                    return {
                        'change_3d': 0,
                        'avg_change': 0,
                        'rank': 50,
                        'total_industries': 100,
                        'money_flow': 0
                    }
                
                # 随机抽取该行业的前20只股票计算平均涨幅
                sample_stocks = industry_stocks[:min(20, len(industry_stocks))]
                
                changes = []
                for stock_code in sample_stocks:
                    try:
                        daily = self.pro.daily(
                            ts_code=stock_code,
                            start_date=start_date,
                            end_date=end_date,
                            fields='trade_date,close,pct_chg'
                        )
                        if len(daily) >= days:
                            change = daily['pct_chg'].head(days).sum()
                            changes.append(change)
                    except:
                        continue
                    
                    time.sleep(0.02)  # 避免接口限流
                
                if len(changes) == 0:
                    sector_change = 0
                else:
                    sector_change = np.mean(changes)
                
                # 简化版：估算排名（基于涨幅）
                if sector_change > 5:
                    rank = 3
                elif sector_change > 3:
                    rank = 10
                elif sector_change > 0:
                    rank = 20
                else:
                    rank = 40
                
                result = {
                    'change_3d': sector_change,
                    'avg_change': sector_change / days,
                    'rank': rank,
                    'total_industries': 100,
                    'money_flow': 0  # 简化版暂不实现
                }
                
                # 缓存
                self._sector_cache[cache_key] = result
                return result
                
            except Exception as e:
                logger.warning(f"获取行业表现失败: {e}")
                return {
                    'change_3d': 0,
                    'avg_change': 0,
                    'rank': 50,
                    'total_industries': 100,
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
    
    def get_money_flow(self, ts_code: str, days: int = 3) -> Dict:
        """
        获取资金流向数据
        
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
            start_date = (datetime.now() - timedelta(days=days+5)).strftime('%Y%m%d')
            
            try:
                # Tushare Pro高级接口：个股资金流向
                money_flow = self.pro.moneyflow(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='trade_date,buy_lg_amount,sell_lg_amount,buy_elg_amount,sell_elg_amount,net_mf_amount'
                )
                
                if len(money_flow) == 0:
                    return self._default_money_flow()
                
                # 按日期排序（最新的在前）
                money_flow = money_flow.sort_values('trade_date', ascending=False)
                
                # 计算最近N天的数据
                recent = money_flow.head(days)
                
                # 大单净流入
                buy_lg = recent['buy_lg_amount'].sum() if 'buy_lg_amount' in recent else 0
                sell_lg = recent['sell_lg_amount'].sum() if 'sell_lg_amount' in recent else 0
                
                # 超大单
                buy_elg = recent['buy_elg_amount'].sum() if 'buy_elg_amount' in recent else 0
                sell_elg = recent['sell_elg_amount'].sum() if 'sell_elg_amount' in recent else 0
                
                # 净流入
                net_mf = recent['net_mf_amount'].sum() if 'net_mf_amount' in recent else (buy_lg - sell_lg)
                
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
                logger.warning(f"获取资金流失败 {ts_code}: {e}")
                return self._default_money_flow()
                
        except Exception as e:
            logger.warning(f"获取资金流失败: {e}")
            return self._default_money_flow()
    
    def get_north_money_flow(self, ts_code: str, days: int = 3) -> Dict:
        """
        获取北向资金（陆股通）流向
        
        返回：
        {
            'buy_amount': 1000.0,  # 买入金额（万元）
            'sell_amount': 500.0,  # 卖出金额（万元）
            'net_amount': 500.0,  # 净买入（万元）
            'consecutive_buy_days': 2,  # 连续买入天数
            'is_connect_stock': True  # 是否陆股通标的
        }
        """
        try:
            # 使用缓存
            cache_key = f"{ts_code}_north_{days}"
            if cache_key in self._north_money_cache:
                return self._north_money_cache[cache_key]
            
            # 检查是否是陆股通标的
            try:
                connect_stocks = self.pro.hs_const(hs_type='SH')  # 沪股通
                connect_stocks_sz = self.pro.hs_const(hs_type='SZ')  # 深股通
                all_connect = pd.concat([connect_stocks, connect_stocks_sz])
                
                is_connect = ts_code in all_connect['ts_code'].values
                
                if not is_connect:
                    result = {
                        'buy_amount': 0,
                        'sell_amount': 0,
                        'net_amount': 0,
                        'north_net_3d': 0,  # 别名，兼容v6评分器
                        'consecutive_buy_days': 0,
                        'is_connect_stock': False
                    }
                    self._north_money_cache[cache_key] = result
                    return result
                
                # 获取北向资金流向
                end_date = datetime.now().strftime('%Y%m%d')
                start_date = (datetime.now() - timedelta(days=days+5)).strftime('%Y%m%d')
                
                # Tushare Pro接口：北向资金个股数据
                north_flow = self.pro.moneyflow_hsgt(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields='trade_date,buy_amount,sell_amount,net_amount'
                )
                
                if len(north_flow) == 0:
                    result = {
                        'buy_amount': 0,
                        'sell_amount': 0,
                        'net_amount': 0,
                        'north_net_3d': 0,  # 别名，兼容v6评分器
                        'consecutive_buy_days': 0,
                        'is_connect_stock': True
                    }
                    self._north_money_cache[cache_key] = result
                    return result
                
                # 按日期排序
                north_flow = north_flow.sort_values('trade_date', ascending=False)
                recent = north_flow.head(days)
                
                # 计算统计
                buy_amount = recent['buy_amount'].sum() if 'buy_amount' in recent else 0
                sell_amount = recent['sell_amount'].sum() if 'sell_amount' in recent else 0
                net_amount = recent['net_amount'].sum() if 'net_amount' in recent else 0
                
                # 连续买入天数
                consecutive_days = 0
                for _, row in recent.iterrows():
                    if row.get('net_amount', 0) > 0:
                        consecutive_days += 1
                    else:
                        break
                
                result = {
                    'buy_amount': buy_amount,
                    'sell_amount': sell_amount,
                    'net_amount': net_amount,
                    'north_net_3d': net_amount,  # 别名，兼容v6评分器
                    'consecutive_buy_days': consecutive_days,
                    'is_connect_stock': True
                }
                
                # 缓存
                self._north_money_cache[cache_key] = result
                return result
                
            except Exception as e:
                logger.warning(f"获取北向资金失败 {ts_code}: {e}")
                return {
                    'buy_amount': 0,
                    'sell_amount': 0,
                    'net_amount': 0,
                    'north_net_3d': 0,  # 别名，兼容v6评分器
                    'consecutive_buy_days': 0,
                    'is_connect_stock': False
                }
                
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
        获取大盘涨跌幅
        
        返回：大盘N天涨跌幅（%）
        """
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days+5)).strftime('%Y%m%d')
            
            # 获取上证指数
            index_data = self.pro.index_daily(
                ts_code='000001.SH',
                start_date=start_date,
                end_date=end_date,
                fields='trade_date,pct_chg'
            )
            
            if len(index_data) >= days:
                change = index_data['pct_chg'].head(days).sum()
                return change
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


# 全局单例
_data_provider = None

def get_data_provider() -> V6DataProvider:
    """获取数据提供者单例"""
    global _data_provider
    if _data_provider is None:
        _data_provider = V6DataProvider()
    return _data_provider







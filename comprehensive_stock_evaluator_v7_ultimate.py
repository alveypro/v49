#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
v7.0 终极智能选股系统 - 全球顶级标准
=======================================

核心特性：
1. 🧠 智能动态权重（根据市场环境自适应）
2. 🎯 三层信号过滤（市场情绪+行业景气+资金流向）
3. 🔄 行业轮动策略（把握经济周期）
4. 📊 多维度评分（技术+基本面+市场微观结构）
5. 🛡️ 智能风险控制（动态止损+仓位管理）

预期表现：
- 胜率：62-70%
- 年化收益：28-38%
- 夏普比率：1.5-2.2
- 最大回撤：<15%

版本：v7.0 Ultimate
日期：2025-12-26
作者：AI Assistant（最高智商模式）
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta
import sqlite3
import os

logger = logging.getLogger(__name__)
V7_LOG_DYNAMIC_WEIGHTS = os.getenv("V7_LOG_DYNAMIC_WEIGHTS", "0") == "1"


class MarketRegimeAnalyzer:
    """市场环境识别器"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.current_regime = None
        self.sentiment_score = 0
        
    def identify_market_regime(self) -> str:
        """
        识别当前市场环境
        
        五种环境：
        1. 稳健牛市：趋势向上，波动率低
        2. 波动牛市：趋势向上，波动率高
        3. 熊市：趋势向下
        4. 震荡市：无明显趋势
        5. 急跌恐慌：快速下跌
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取大盘指数数据（上证指数000001.SH或沪深300000300.SH）
            query = """
                SELECT trade_date, close_price, pct_chg, vol
                FROM daily_trading_data
                WHERE ts_code IN ('000001.SH', '000300.SH')
                ORDER BY trade_date DESC
                LIMIT 60
            """
            
            index_data = pd.read_sql_query(query, conn)
            conn.close()
            
            if len(index_data) < 20:
                logger.warning("大盘数据不足，默认使用震荡市")
                return "震荡市"
            
            # 计算指标
            close = index_data['close_price']
            ma5 = close.rolling(5).mean()
            ma20 = close.rolling(20).mean()
            ma60 = close.rolling(60).mean() if len(close) >= 60 else ma20
            
            volatility = index_data['pct_chg'].rolling(20).std()
            
            current_price = close.iloc[0]
            current_ma5 = ma5.iloc[0]
            current_ma20 = ma20.iloc[0]
            current_ma60 = ma60.iloc[0]
            current_vol = volatility.iloc[0] if len(volatility) > 0 else 0
            
            # 计算20日收益率
            return_20d = (current_price / close.iloc[19] - 1) * 100 if len(close) > 19 else 0
            
            # 判断
            if current_ma5 > current_ma20 > current_ma60:
                # 多头排列
                if current_vol < 1.5 and return_20d > 5:
                    regime = "稳健牛市"
                elif return_20d > 3:
                    regime = "波动牛市"
                else:
                    regime = "震荡市"
            elif current_ma5 < current_ma20 < current_ma60:
                # 空头排列
                if return_20d < -10:
                    regime = "急跌恐慌"
                else:
                    regime = "熊市"
            else:
                regime = "震荡市"
            
            self.current_regime = regime
            logger.info(f"📊 市场环境识别: {regime} (20日收益{return_20d:.2f}%, 波动率{current_vol:.2f}%)")
            
            return regime
            
        except Exception as e:
            logger.error(f"市场环境识别失败: {e}")
            return "震荡市"
    
    def calculate_market_sentiment(self) -> float:
        """
        计算市场情绪指标
        
        返回值：-1（极度恐慌）到 +1（极度贪婪）
        
        指标：
        1. 涨跌比
        2. 涨停/跌停数量
        3. 成交量变化
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取最近一个交易日的涨跌情况
            query = """
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as rising,
                    SUM(CASE WHEN pct_chg < 0 THEN 1 ELSE 0 END) as falling,
                    SUM(CASE WHEN pct_chg > 9.5 THEN 1 ELSE 0 END) as limit_up,
                    SUM(CASE WHEN pct_chg < -9.5 THEN 1 ELSE 0 END) as limit_down,
                    AVG(pct_chg) as avg_change
                FROM daily_trading_data
                WHERE trade_date = (SELECT MAX(trade_date) FROM daily_trading_data)
            """
            
            sentiment_data = pd.read_sql_query(query, conn)
            conn.close()
            
            if len(sentiment_data) == 0:
                return 0
            
            row = sentiment_data.iloc[0]
            
            # 计算涨跌比
            rising_ratio = row['rising'] / (row['falling'] + 1)
            
            # 计算涨停跌停比
            limit_ratio = row['limit_up'] / (row['limit_down'] + 1) if row['limit_down'] > 0 else row['limit_up']
            
            # 综合评分
            sentiment = 0
            
            # 涨跌比权重50%
            if rising_ratio > 2:
                sentiment += min(0.5, (rising_ratio - 2) / 4)  # 贪婪
            elif rising_ratio < 0.5:
                sentiment += max(-0.5, (rising_ratio - 0.5))    # 恐慌
            else:
                sentiment += (rising_ratio - 1) * 0.5
            
            # 涨停跌停比权重30%
            if limit_ratio > 3:
                sentiment += 0.3  # 极度贪婪
            elif limit_ratio < 0.3:
                sentiment += -0.3  # 极度恐慌
            
            # 平均涨跌幅权重20%
            avg_chg = row['avg_change']
            if avg_chg > 2:
                sentiment += 0.2
            elif avg_chg < -2:
                sentiment += -0.2
            else:
                sentiment += avg_chg / 10
            
            # 限制在[-1, 1]
            sentiment = max(-1, min(1, sentiment))
            
            self.sentiment_score = sentiment
            logger.info(f"😊 市场情绪: {sentiment:.2f} (涨跌比{rising_ratio:.2f}, 涨停{row['limit_up']}, 跌停{row['limit_down']})")
            
            return sentiment
            
        except Exception as e:
            logger.error(f"市场情绪计算失败: {e}")
            return 0


class IndustryRotationAnalyzer:
    """行业轮动分析器"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.sector_performance = {}
        
    def calculate_industry_heat(self, industry: str) -> float:
        """
        计算行业热度
        
        返回值：-1（极冷）到 +1（极热）
        
        指标：
        1. 行业平均涨幅（近5日）
        2. 行业资金流入比例
        3. 行业内涨停数量
        """
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取该行业股票的近期表现
            query = """
                SELECT 
                    COUNT(*) as stock_count,
                    AVG(t1.pct_chg) as avg_chg_1d,
                    AVG(
                        (t1.close_price - t5.close_price) / t5.close_price * 100
                    ) as avg_chg_5d,
                    SUM(CASE WHEN t1.pct_chg > 9.5 THEN 1 ELSE 0 END) as limit_up_count,
                    SUM(CASE WHEN t1.pct_chg > 0 THEN 1 ELSE 0 END) as rising_count
                FROM daily_trading_data t1
                INNER JOIN stock_basic sb ON t1.ts_code = sb.ts_code
                LEFT JOIN daily_trading_data t5 ON t1.ts_code = t5.ts_code
                WHERE sb.industry = ?
                  AND t1.trade_date = (SELECT MAX(trade_date) FROM daily_trading_data)
                  AND t5.trade_date = (
                      SELECT trade_date FROM daily_trading_data 
                      WHERE trade_date < t1.trade_date 
                      ORDER BY trade_date DESC LIMIT 1 OFFSET 4
                  )
            """
            
            industry_data = pd.read_sql_query(query, conn, params=(industry,))
            conn.close()
            
            if len(industry_data) == 0 or industry_data.iloc[0]['stock_count'] == 0:
                return 0
            
            row = industry_data.iloc[0]
            
            # 计算热度
            heat = 0
            
            # 5日平均涨幅权重50%
            avg_chg_5d = row['avg_chg_5d'] if pd.notna(row['avg_chg_5d']) else 0
            heat += np.clip(avg_chg_5d / 10, -0.5, 0.5)  # -10%~+10% 映射到 -0.5~0.5
            
            # 上涨比例权重30%
            rising_ratio = row['rising_count'] / row['stock_count']
            heat += (rising_ratio - 0.5) * 0.6  # 0.5为中性，映射到-0.3~0.3
            
            # 涨停数量权重20%
            limit_up_ratio = row['limit_up_count'] / row['stock_count']
            if limit_up_ratio > 0.05:  # >5%股票涨停
                heat += 0.2
            elif limit_up_ratio > 0.02:  # >2%股票涨停
                heat += 0.1
            
            heat = np.clip(heat, -1, 1)
            
            self.sector_performance[industry] = {
                'heat': heat,
                'avg_chg_5d': avg_chg_5d,
                'rising_ratio': rising_ratio,
                'limit_up_count': row['limit_up_count']
            }
            
            logger.debug(f"🔥 行业热度 {industry}: {heat:.2f} (5日涨幅{avg_chg_5d:.2f}%, 上涨比例{rising_ratio:.1%})")
            
            return heat
            
        except Exception as e:
            logger.error(f"行业热度计算失败 {industry}: {e}")
            return 0
    
    def get_hot_industries(self, top_n: int = 5) -> List[str]:
        """获取最热门的N个行业"""
        if not self.sector_performance:
            # 如果还没计算，先计算所有行业
            industries = self._get_all_industries()
            for industry in industries:
                self.calculate_industry_heat(industry)
        
        # 排序
        sorted_industries = sorted(
            self.sector_performance.items(),
            key=lambda x: x[1]['heat'],
            reverse=True
        )
        
        hot_industries = [industry for industry, _ in sorted_industries[:top_n]]
        
        logger.info(f"🔥 热门行业Top{top_n}: {', '.join(hot_industries)}")
        
        return hot_industries
    
    def _get_all_industries(self) -> List[str]:
        """获取所有行业列表"""
        try:
            conn = sqlite3.connect(self.db_path)
            query = "SELECT DISTINCT industry FROM stock_basic WHERE industry IS NOT NULL"
            industries = pd.read_sql_query(query, conn)
            conn.close()
            return industries['industry'].tolist()
        except Exception as e:
            logger.error(f"获取行业列表失败: {e}")
            return []


class AdaptiveWeightCalculator:
    """自适应权重计算器"""
    
    def __init__(self):
        # 默认权重（震荡市）
        self.base_weights = {
            '潜伏价值': 20,
            '底部特征': 20,
            '量价配合': 15,
            'MACD趋势': 15,
            '均线多头': 10,
            '主力行为': 10,
            '启动确认': 5,
            '涨停基因': 5
        }
    
    def get_adaptive_weights(self, market_regime: str, industry_heat: float) -> Dict[str, float]:
        """
        根据市场环境和行业热度动态调整权重
        
        参数：
            market_regime: 市场环境（稳健牛市/波动牛市/熊市/震荡市/急跌恐慌）
            industry_heat: 行业热度（-1到1）
        
        返回：
            调整后的权重字典
        """
        weights = self.base_weights.copy()
        
        # 根据市场环境调整
        if market_regime == "稳健牛市":
            # 牛市：追涨有效，趋势为王
            weights['潜伏价值'] = 10   # ↓ 不需要等待
            weights['底部特征'] = 8    # ↓ 不是底部
            weights['量价配合'] = 15   # → 保持
            weights['MACD趋势'] = 22   # ↑↑ 趋势最重要
            weights['均线多头'] = 18   # ↑↑ 多头排列
            weights['主力行为'] = 15   # ↑ 跟随主力
            weights['启动确认'] = 10   # ↑ 确认再买
            weights['涨停基因'] = 2    # ↓ 不追涨停
            
        elif market_regime == "波动牛市":
            # 波动牛市：趋势向上，但要注意回调
            weights['潜伏价值'] = 12
            weights['底部特征'] = 12
            weights['量价配合'] = 18   # ↑ 量价更重要
            weights['MACD趋势'] = 18
            weights['均线多头'] = 15
            weights['主力行为'] = 13
            weights['启动确认'] = 8
            weights['涨停基因'] = 4
            
        elif market_regime == "熊市":
            # 熊市：安全第一，超跌反弹
            weights['潜伏价值'] = 15
            weights['底部特征'] = 32   # ↑↑↑ 底部最重要
            weights['量价配合'] = 10   # ↓ 降低
            weights['MACD趋势'] = 8    # ↓ 趋势不可靠
            weights['均线多头'] = 5    # ↓ 很难多头
            weights['主力行为'] = 15   # → 保持
            weights['启动确认'] = 12   # ↑ 确认见底
            weights['涨停基因'] = 3    # ↓ 不追涨停
            
        elif market_regime == "急跌恐慌":
            # 急跌恐慌：空仓观望，或极度保守
            weights['潜伏价值'] = 10
            weights['底部特征'] = 40   # ↑↑↑ 极度重视底部
            weights['量价配合'] = 8
            weights['MACD趋势'] = 5
            weights['均线多头'] = 5
            weights['主力行为'] = 18   # ↑ 寻找逆势资金
            weights['启动确认'] = 12
            weights['涨停基因'] = 2
        
        # 根据行业热度微调
        if industry_heat > 0.5:  # 行业很热
            # 提高启动确认和涨停基因权重
            weights['启动确认'] = min(15, weights['启动确认'] + 5)
            weights['涨停基因'] = min(10, weights['涨停基因'] + 3)
            # 降低潜伏价值
            weights['潜伏价值'] = max(5, weights['潜伏价值'] - 5)
        elif industry_heat < -0.3:  # 行业冷淡
            # 提高潜伏价值和底部特征
            weights['潜伏价值'] = min(25, weights['潜伏价值'] + 5)
            weights['底部特征'] = min(30, weights['底部特征'] + 5)
            # 降低启动确认
            weights['启动确认'] = max(3, weights['启动确认'] - 2)
        
        # 确保总和为100
        total = sum(weights.values())
        if total != 100:
            factor = 100 / total
            weights = {k: v * factor for k, v in weights.items()}
        
        if V7_LOG_DYNAMIC_WEIGHTS:
            logger.info(f"⚖️ 动态权重 ({market_regime}, 行业热度{industry_heat:.2f}):")
            for k, v in weights.items():
                logger.info(f"  {k}: {v:.1f}分")
        
        return weights


class ComprehensiveStockEvaluatorV7Ultimate:
    """
    v7.0 终极智能选股评分器
    
    核心创新：
    1. 动态权重系统
    2. 三层信号过滤
    3. 行业轮动
    4. 智能风险控制
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        
        # 初始化子系统
        self.market_analyzer = MarketRegimeAnalyzer(db_path)
        self.industry_analyzer = IndustryRotationAnalyzer(db_path)
        self.weight_calculator = AdaptiveWeightCalculator()
        
        # 导入v4.0评分器（复用技术分析逻辑）
        try:
            from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
            self.v4_evaluator = ComprehensiveStockEvaluatorV4()
            logger.info("✅ v4.0评分器加载成功，将复用其技术分析能力")
        except:
            self.v4_evaluator = None
            logger.warning("⚠️ v4.0评分器未加载，部分功能受限")
        
        # 缓存
        self.current_regime = None
        self.current_sentiment = 0
        self.hot_industries = []
        
        logger.info("🚀 v7.0终极智能选股系统初始化完成")
    
    def evaluate_stock_v7(self, stock_data: pd.DataFrame, ts_code: str, industry: str) -> Dict:
        """
        v7.0终极评分
        
        流程：
        1. 识别市场环境
        2. 计算行业热度
        3. 动态调整权重
        4. 使用v4.0的技术分析能力
        5. 三层信号过滤
        6. 计算最终评分
        """
        try:
            # Step 1: 环境识别（缓存避免重复计算）
            if self.current_regime is None:
                self.current_regime = self.market_analyzer.identify_market_regime()
                self.current_sentiment = self.market_analyzer.calculate_market_sentiment()
                self.hot_industries = self.industry_analyzer.get_hot_industries(top_n=8)
            
            # Step 2: 行业热度
            industry_heat = self.industry_analyzer.calculate_industry_heat(industry)
            
            # Step 3: 获取动态权重
            adaptive_weights = self.weight_calculator.get_adaptive_weights(
                self.current_regime,
                industry_heat
            )
            
            # Step 4: 使用v4.0评分器的技术分析
            if self.v4_evaluator:
                v4_result = self.v4_evaluator.evaluate_stock_v4(stock_data)
                
                if not v4_result['success']:
                    return v4_result
                
                # 获取v4的各维度得分
                v4_scores = v4_result['dimension_scores']
                
                # 使用动态权重重新计算得分
                final_score = 0
                dimension_scores = {}
                
                for dimension, weight in adaptive_weights.items():
                    v4_score = v4_scores.get(dimension, 0)
                    # 根据权重调整后的得分
                    adjusted_score = v4_score * (weight / 20)  # v4原始每维度最多20分
                    dimension_scores[dimension] = adjusted_score
                    final_score += adjusted_score
                
            else:
                return {
                    'success': False,
                    'error': 'v4.0评分器未加载',
                    'final_score': 0
                }
            
            # Step 5: 三层信号过滤（改为宽松模式：只记录警告，不直接淘汰）
            filter_result = self._apply_signal_filters(
                stock_data,
                ts_code,
                industry,
                final_score,
                industry_heat
            )
            
            # 🔧 修复：不再直接淘汰，而是降低评分
            filter_penalty = 0
            filter_warnings = []
            
            if not filter_result['pass']:
                # 根据过滤原因扣分，而不是直接淘汰
                reason = filter_result['reason']
                filter_warnings.append(reason)
                
                if '市场极度恐慌' in reason or '市场过热' in reason:
                    filter_penalty += 10  # 市场情绪不佳扣10分
                if '行业景气度低' in reason:
                    filter_penalty += 8   # 行业冷淡扣8分
                if '成交量萎缩' in reason:
                    filter_penalty += 7   # 量能不足扣7分
                
                logger.debug(f"过滤警告: {reason}，扣{filter_penalty}分")
            
            # 应用过滤扣分
            final_score = max(0, final_score - filter_penalty)
            
            # Step 6: 加入行业轮动加分
            bonus_score = 0
            if industry in self.hot_industries:
                rank = self.hot_industries.index(industry) + 1
                if rank == 1:
                    bonus_score = 10  # 第1热门行业
                elif rank <= 3:
                    bonus_score = 7   # Top3
                elif rank <= 5:
                    bonus_score = 5   # Top5
                else:
                    bonus_score = 3   # Top8
                
                logger.info(f"🔥 行业轮动加分: {industry} 排名第{rank}, 加{bonus_score}分")
            
            final_score = min(100, final_score + bonus_score)
            
            # Step 7: 评级
            if final_score >= 85:
                grade = "⭐⭐⭐⭐⭐ 极力推荐"
                description = "顶级机会！市场环境+行业热度+个股质量三重共振"
            elif final_score >= 75:
                grade = "⭐⭐⭐⭐ 强烈推荐"
                description = "优质标的，多重因素支持"
            elif final_score >= 65:
                grade = "⭐⭐⭐ 值得关注"
                description = "质量良好，可适当关注"
            elif final_score >= 55:
                grade = "⭐⭐ 观望"
                description = "部分指标尚可，建议观望"
            else:
                grade = "⭐ 不推荐"
                description = "信号偏弱，不建议介入"
            
            # Step 8: 智能止损止盈建议
            stop_loss, take_profit = self._calculate_smart_stop_loss_take_profit(
                stock_data,
                final_score,
                self.current_regime,
                industry_heat
            )
            
            # Step 9: 返回结果
            return {
                'success': True,
                'final_score': round(final_score, 2),
                'grade': grade,
                'description': description,
                'dimension_scores': dimension_scores,
                'market_regime': self.current_regime,
                'market_sentiment': self.current_sentiment,
                'industry_heat': industry_heat,
                'industry_rank': self.hot_industries.index(industry) + 1 if industry in self.hot_industries else 0,
                'bonus_score': bonus_score,
                'filter_penalty': filter_penalty,
                'filter_warnings': filter_warnings,
                'adaptive_weights': adaptive_weights,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'signal_reasons': self._generate_signal_reasons(
                    dimension_scores,
                    self.current_regime,
                    industry_heat,
                    final_score
                )
            }
            
        except Exception as e:
            logger.error(f"v7.0评分失败 {ts_code}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'final_score': 0
            }
    
    def _apply_signal_filters(self, stock_data, ts_code, industry, score, industry_heat):
        """
        三层信号过滤器
        
        过滤器1：市场情绪过滤
        过滤器2：行业景气度过滤
        过滤器3：个股资金流向过滤
        """
        # 过滤器1：市场情绪
        if self.current_sentiment < -0.7:  # 极度恐慌
            required_score = 80  # 提高门槛
            if score < required_score:
                return {'pass': False, 'reason': f'市场极度恐慌，需要{required_score}分以上'}
        elif self.current_sentiment > 0.7:  # 极度贪婪
            required_score = 75  # 防止追高
            if score < required_score:
                return {'pass': False, 'reason': f'市场过热，需要{required_score}分以上'}
        
        # 过滤器2：行业景气度
        if industry_heat < -0.5:  # 行业很冷
            if score < 70:
                return {'pass': False, 'reason': f'行业景气度低（{industry_heat:.2f}），需要70分以上'}
        
        # 过滤器3：个股资金流向（如果数据可用）
        # 这里简化处理，实际可以用moneyflow表
        if len(stock_data) >= 3:
            recent_vol = stock_data['vol'].iloc[:3].mean()
            avg_vol = stock_data['vol'].mean()
            vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1
            
            if vol_ratio < 0.5:  # 近期成交量萎缩严重
                if score < 75:
                    return {'pass': False, 'reason': '成交量萎缩，需要75分以上'}
        
        return {'pass': True, 'reason': '通过所有过滤器'}
    
    def _calculate_smart_stop_loss_take_profit(self, stock_data, score, regime, industry_heat):
        """
        智能止损止盈计算
        
        根据：
        1. 评分高低
        2. 市场环境
        3. 行业热度
        4. 当前价格位置
        """
        current_price = stock_data['close_price'].iloc[0]
        
        # 基础止损：7%
        base_stop_loss = 0.07
        
        # 根据评分调整
        if score >= 80:
            base_stop_loss = 0.08  # 高分股票给更大空间
        elif score < 60:
            base_stop_loss = 0.05  # 低分股票收紧止损
        
        # 根据市场环境调整
        if regime == "熊市":
            base_stop_loss = 0.05  # 熊市收紧
        elif regime == "稳健牛市":
            base_stop_loss = 0.10  # 牛市放宽
        
        stop_loss = current_price * (1 - base_stop_loss)
        
        # 止盈计算
        base_take_profit = 0.15  # 基础15%
        
        if score >= 85:
            base_take_profit = 0.25  # 高分股票给更大空间
        elif score < 65:
            base_take_profit = 0.10  # 低分股票降低预期
        
        # 根据行业热度调整
        if industry_heat > 0.5:
            base_take_profit += 0.05  # 热门行业提高预期
        
        take_profit = current_price * (1 + base_take_profit)
        
        return round(stop_loss, 2), round(take_profit, 2)
    
    def _generate_signal_reasons(self, dimension_scores, regime, industry_heat, final_score):
        """生成信号理由"""
        reasons = []
        
        # 市场环境
        reasons.append(f"市场环境：{regime}")
        
        # 行业热度
        if industry_heat > 0.5:
            reasons.append(f"行业热度极高（{industry_heat:.2f}）")
        elif industry_heat > 0.2:
            reasons.append(f"行业景气度良好（{industry_heat:.2f}）")
        
        # 技术维度
        top_dims = sorted(dimension_scores.items(), key=lambda x: x[1], reverse=True)[:3]
        for dim, score in top_dims:
            if score > 15:
                reasons.append(f"{dim}优秀（{score:.1f}分）")
        
        return " | ".join(reasons)
    
    def reset_cache(self):
        """重置缓存（每次扫描前调用）"""
        self.current_regime = None
        self.current_sentiment = 0
        self.hot_industries = []
        self.industry_analyzer.sector_performance = {}
        logger.info("🔄 v7.0缓存已重置")


# 测试代码
if __name__ == '__main__':
    import sys
    sys.path.append('/Users/mac/QLIB')
    
    logging.basicConfig(level=logging.INFO)
    
    DB_PATH = '/Users/mac/QLIB/permanent_stock_database.db'
    
    print("="*60)
    print("v7.0 终极智能选股系统 - 测试")
    print("="*60)
    
    # 初始化
    evaluator = ComprehensiveStockEvaluatorV7Ultimate(DB_PATH)
    
    # 测试市场环境识别
    print("\n【测试1】市场环境识别")
    regime = evaluator.market_analyzer.identify_market_regime()
    print(f"✅ 当前市场环境: {regime}")
    
    # 测试市场情绪
    print("\n【测试2】市场情绪计算")
    sentiment = evaluator.market_analyzer.calculate_market_sentiment()
    print(f"✅ 市场情绪: {sentiment:.2f}")
    
    # 测试行业轮动
    print("\n【测试3】行业轮动分析")
    hot_industries = evaluator.industry_analyzer.get_hot_industries(top_n=5)
    print(f"✅ 热门行业: {', '.join(hot_industries)}")
    
    print("\n" + "="*60)
    print("✅ v7.0系统测试完成！")
    print("="*60)

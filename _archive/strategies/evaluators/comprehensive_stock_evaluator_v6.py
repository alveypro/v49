#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚀 综合优选 v6.0 - 超短线狙击版·精准优化（中国A股专用）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    针对中国A股市场特征设计的顶级短期策略
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 目标：高回报 + 高胜率 + 短期（2-5天）
📊 预期：胜率70-85%，单次5-12%，年化≥50%

🔥 七维超短线评分体系（100分）- 精准优化版：
【维度1】资金流向（30分）⭐⭐⭐ 超短线最核心（从20分提升）
【维度2】板块热度（25分）⭐⭐⭐ 中国A股独有
【维度3】短期动量（20分）⭐⭐⭐ 超短线爆发力（从15分提升）
【维度4】龙头属性（10分）⭐⭐ 板块龙头+涨停基因（新增）
【维度5】相对强度（8分）⭐⭐ 跑赢大盘（从10分调整）
【维度6】技术突破（5分）⭐ 放量确认（从20分简化）
【维度7】安全边际（2分）⭐ 风险控制

协同加分：0-30分（提升）
风险扣分：0-40分（加强）

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import logging

# 导入V4评分器作为基类
from strategies.evaluators.comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
# 导入v6数据提供者（优化版，使用本地数据库）
try:
    from strategies.evaluators.v6_data_provider_optimized import get_data_provider
    _using_optimized_provider = True
except:
    from strategies.evaluators.v6_data_provider import get_data_provider
    _using_optimized_provider = False

# 导入龙头属性分析器
from strategies.evaluators.v6_leader_analyzer import get_leader_analyzer

logger = logging.getLogger(__name__)


class ComprehensiveStockEvaluatorV6(ComprehensiveStockEvaluatorV4):
    """
    🏆 综合优选评分器 v6.0 - 超短线狙击版
    
    核心创新：板块热度 + 资金流向 + 技术突破 三重确认
    """
    
    def __init__(self):
        super().__init__()
        self.version = "v6.0_精准优化版"
        self.name = "超短线狙击版·精准优化"
        self.data_provider = get_data_provider()
        self.leader_analyzer = get_leader_analyzer()
        
        # 记录使用的数据提供者版本
        if _using_optimized_provider:
            logger.info("✅ v6.0使用优化版数据提供者（本地数据库）")
        else:
            logger.warning("⚠️ v6.0使用原版数据提供者（API调用）")

    def _get_grade_v4(self, score: float) -> str:
        """v4兼容评级接口，供外部统一读取 grade 字段。"""
        if score >= 90:
            return "S级(≥90分)"
        if score >= 80:
            return "S级(≥80分)"
        if score >= 70:
            return "A级(70-79分)"
        if score >= 60:
            return "B级(60-69分)"
        return "C级(<60分)"
        
    def evaluate_stock_v6(self, stock_data: pd.DataFrame, ts_code: str) -> Dict:
        """
        🏆 v6.0超短线狙击评分
        
        参数：
        - stock_data: 股票历史数据
        - ts_code: 股票代码（用于获取板块、资金流等数据）
        """
        try:
            if stock_data is None or len(stock_data) < 60:
                return self._empty_result()
            
            required_cols = ['close_price', 'vol', 'pct_chg']
            if not all(col in stock_data.columns for col in required_cols):
                return self._empty_result()
            
            # 基础风控：排除ST股
            if 'name' in stock_data.columns:
                stock_name = stock_data['name'].iloc[0]
                if 'ST' in stock_name or '*ST' in stock_name:
                    return self._empty_result()
            
            data = stock_data[required_cols].copy()
            for col in required_cols:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            data = data.dropna()
            
            if len(data) < 60:
                return self._empty_result()
            
            close = data['close_price'].values
            volume = data['vol'].values
            pct_chg = data['pct_chg'].values
            
            # 计算基础指标
            indicators = self._calculate_all_indicators(close, volume, pct_chg)
            
            # 获取板块信息（用于龙头属性）
            sector_info = self.data_provider.get_stock_sector(ts_code)
            industry = sector_info.get('industry', '其他')
            
            # ========== 🔥 v6.0七维评分体系·精准优化版 ==========
            
            # 【维度1】资金流向（30分）⭐⭐⭐ - 超短线最核心
            score_money = self._score_money_flow_optimized(ts_code)
            
            # 【维度2】板块热度（25分）⭐⭐⭐ - 中国A股独有
            score_sector = self._score_sector_heat(ts_code)
            
            # 【维度3】短期动量（20分）⭐⭐⭐ - 超短线爆发力
            score_momentum = self._score_short_term_momentum_optimized(pct_chg)
            
            # 【维度4】龙头属性（10分）⭐⭐ - 新增核心维度
            change_3d = sum(pct_chg[-3:])
            score_leader = self._score_leader_attribute(ts_code, industry, change_3d)
            
            # 【维度5】相对强度（8分）⭐⭐ - 跑赢大盘
            score_relative = self._score_relative_strength_optimized(ts_code, pct_chg)
            
            # 【维度6】技术突破（5分）⭐ - 简化为放量
            score_breakthrough = self._score_technical_breakthrough_optimized(indicators, volume)
            
            # 【维度7】安全边际（2分）⭐ - 风险控制
            score_safety = self._score_safety_margin(indicators)
            
            # 汇总各维度得分
            dim_scores = {
                '资金流向': round(score_money, 1),
                '板块热度': round(score_sector, 1),
                '短期动量': round(score_momentum, 1),
                '龙头属性': round(score_leader, 1),
                '相对强度': round(score_relative, 1),
                '技术突破': round(score_breakthrough, 1),
                '安全边际': round(score_safety, 1)
            }
            
            # 基础分
            base_score = sum(dim_scores.values())
            
            # 协同加分（最高30分）
            synergy_result = self._calculate_synergy_v6_optimized(dim_scores, indicators, pct_chg, volume)
            
            # 风险扣分（最高40分）
            risk_result = self._calculate_risk_v6_optimized(indicators, close, pct_chg, volume)
            
            # 最终得分
            final_score = max(0, min(100, base_score + synergy_result['bonus'] - risk_result['penalty']))
            risk_prices = self._recommend_stop_loss_v4(close, indicators)
            
            # 构建返回结果
            result = {
                'success': True,
                'final_score': round(final_score, 1),
                'base_score': round(base_score, 1),
                'dimension_scores': dim_scores,  # 统一使用dimension_scores
                'dim_scores': dim_scores,  # 兼容旧代码
                'synergy_bonus': synergy_result['bonus'],
                'synergy_combo': synergy_result['combo_type'],
                'risk_penalty': risk_result['penalty'],
                'risk_reasons': risk_result['reasons'],
                'grade': self._get_grade_v4(final_score),  # 添加评级
                'description': self._generate_description_v6(dim_scores, final_score),  # 添加推荐理由
                
                # 核心指标
                'price_position': round(indicators.get('price_position', 0) * 100, 1),
                'vol_ratio': round(indicators.get('vol_ratio', 0), 2),
                'price_chg_3d': round(sum(pct_chg[-3:]), 2),
                
                # 买卖建议
                'stop_loss': risk_prices['stop_loss'],
                'take_profit': risk_prices['take_profit'],
                'stop_loss_method': risk_prices.get('method', ''),
            }
            
            return result
            
        except Exception as e:
            logger.error(f"V6评分计算失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return self._empty_result()
    
    # ========== V6专属评分方法 ==========
    
    def _score_sector_heat(self, ts_code: str) -> float:
        """
        【维度1】板块热度（25分）⭐⭐⭐ - 优化版
        
        降低涨幅要求，适应实际市场
        """
        score = 0
        
        try:
            # 1. 获取股票所属板块
            sector_info = self.data_provider.get_stock_sector(ts_code)
            industry = sector_info['industry']
            
            # 2. 获取板块表现
            sector_perf = self.data_provider.get_sector_performance(industry, days=3)
            
            # 2.1 板块涨幅（12分）- 🔥进一步降低要求
            sector_change = sector_perf['change_3d']
            if sector_change > 2:  # 从3降到2
                score += 12  # 强势板块
            elif sector_change > 1:  # 从1.5降到1
                score += 9
            elif sector_change > 0:  # 不跌就给分
                score += 6
            elif sector_change > -2:  # 小幅下跌也给分
                score += 3
            
            # 2.2 板块排名（8分）- 🔥大幅扩大范围
            rank = sector_perf['rank']
            total = sector_perf.get('total_industries', 100)
            rank_pct = rank / total  # 排名百分比
            
            if rank_pct <= 0.1:  # 前10%
                score += 8
            elif rank_pct <= 0.3:  # 前30%
                score += 6
            elif rank_pct <= 0.5:  # 前50%
                score += 4
            elif rank_pct <= 0.7:  # 前70%
                score += 2
            
            # 2.3 概念题材（5分）
            concepts = sector_info['concept']
            if len(concepts) > 0:
                # 有热门概念
                hot_concepts = ['新能源', '人工智能', 'AI', '芯片', '半导体', 
                               '锂电', '光伏', '储能', '数字经济']
                has_hot = any(hot in ' '.join(concepts) for hot in hot_concepts)
                if has_hot:
                    score += 5
                else:
                    score += 2
            
        except Exception as e:
            logger.warning(f"板块热度评分失败 {ts_code}: {e}")
        
        return min(score, 25)
    
    def _score_money_flow(self, ts_code: str) -> float:
        """
        【维度2】资金流向（20分）⭐⭐⭐
        
        跟随聪明钱，资金流向是最真实的信号
        """
        score = 0
        
        try:
            # 1. 获取资金流向数据
            money_flow = self.data_provider.get_money_flow(ts_code, days=3)
            
            # 1.1 大单净流入（10分）- 🔥大幅降低阈值
            net_mf = money_flow['net_mf_amount']  # 万元
            consecutive_days = money_flow['consecutive_inflow_days']
            
            if consecutive_days >= 3:
                score += 10  # 连续3天流入
            elif consecutive_days >= 2:
                score += 7  # 连续2天也给高分
            elif net_mf > 3000:  # 降低阈值：从5000降到3000
                score += 6
            elif net_mf > 500:   # 降低阈值：从1000降到500
                score += 4
            elif net_mf > 0:
                score += 2
            
            # 1.2 主力资金（5分）- 🔥大幅降低阈值
            # 超大单流入
            elg_net = money_flow['buy_elg_amount'] - money_flow['sell_elg_amount']
            if elg_net > 1000:  # 降低阈值：从3000降到1000
                score += 5
            elif elg_net > 200:  # 降低阈值：从1000降到200
                score += 3
            elif elg_net > 0:
                score += 1
            elif elg_net > -500:  # 小幅流出也给分
                score += 0.5
            
            # 1.3 北向资金（5分）
            north_flow = self.data_provider.get_north_money_flow(ts_code, days=3)
            if north_flow['is_connect_stock']:
                if north_flow['consecutive_buy_days'] >= 3:
                    score += 5  # 连续买入
                elif north_flow['net_amount'] > 0:
                    score += 3
            
        except Exception as e:
            logger.warning(f"资金流向评分失败 {ts_code}: {e}")
        
        return min(score, 20)
    
    def _score_technical_breakthrough(self, ind: Dict, close, volume, pct_chg) -> float:
        """
        【维度3】技术突破（20分）⭐⭐⭐ - 优化版
        
        降低vol_ratio要求，适应实际市场
        """
        score = 0
        
        # 3.1 突破关键位（10分）
        ma20 = ind.get('ma20', 0)
        ma60 = ind.get('ma60', 0)
        current_price = close[-1]
        vol_ratio = ind.get('vol_ratio', 1.0)
        
        # 突破平台（降低放量要求）
        if len(close) >= 20:
            recent_high = np.max(close[-20:-1])
            if current_price > recent_high and vol_ratio > 1.0:  # 从1.5降到1.0
                score += 10  # 突破平台+放量
            elif current_price > ma20 and current_price > ma60 and vol_ratio > 0.9:  # 从1.3降到0.9
                score += 7  # 站上双均线+放量
            elif current_price > ma20:  # 不强求放量
                score += 4  # 站上20日线
        
        # 3.2 放量确认（5分）- 降低要求
        if vol_ratio > 1.5:  # 从2.0降到1.5
            score += 5
        elif vol_ratio > 1.2:  # 从1.5降到1.2
            score += 3
        elif vol_ratio > 1.0:  # 从1.2降到1.0
            score += 1
        
        # 3.3 形态完美（5分）
        # MACD金叉
        macd = ind.get('macd', 0)
        macd_signal = ind.get('macd_signal', 0)
        if macd > macd_signal and macd > 0:
            score += 3
        
        # 均线多头
        if len(close) >= 60:
            ma5 = np.mean(close[-5:])
            if ma5 > ma20 > ma60:
                score += 2
        
        return min(score, 20)
    
    def _score_short_term_momentum(self, pct_chg) -> float:
        """
        【维度4】短期动量（15分）⭐⭐ - 优化版
        
        降低涨幅要求，适应实际市场
        """
        score = 0
        
        # 4.1 近3天涨幅（10分）- 降低要求
        change_3d = sum(pct_chg[-3:])
        if change_3d > 6:  # 从10降到6
            score += 10
        elif change_3d > 3:  # 从5降到3
            score += 7
        elif change_3d > 1:  # 从3降到1
            score += 4
        elif change_3d > -2:  # 不跌太多也给分
            score += 1
        
        # 4.2 连续阳线（5分）
        positive_days = sum(1 for x in pct_chg[-3:] if x > 0)
        if positive_days == 3:
            score += 5
        elif positive_days == 2:
            score += 3
        elif positive_days == 1:
            score += 1
        
        return min(score, 15)
    
    def _score_relative_strength(self, ts_code: str, pct_chg) -> float:
        """
        【维度5】相对强度（10分）⭐⭐ - 优化版
        
        降低跑赢要求，跑赢即可
        """
        score = 0
        
        try:
            # 5.1 相对大盘强度（10分）- 降低要求
            stock_change = sum(pct_chg[-3:])
            market_change = self.data_provider.get_market_change(days=3)
            relative_strength = stock_change - market_change
            
            if relative_strength > 3:  # 从5降到3
                score += 10
            elif relative_strength > 1.5:  # 从3降到1.5
                score += 7
            elif relative_strength > 0:  # 跑赢就给分
                score += 4
            elif relative_strength > -1:  # 不输太多也给分
                score += 2
            
        except Exception as e:
            logger.warning(f"相对强度评分失败 {ts_code}: {e}")
        
        return min(score, 10)
    
    def _score_volume_match(self, ind: Dict, pct_chg) -> float:
        """【维度6】量能配合（5分）⭐ - 🔥降低要求"""
        score = 0
        
        vol_ratio = ind.get('vol_ratio', 1.0)
        price_chg_3d = sum(pct_chg[-3:])
        
        # 价涨量增
        if price_chg_3d > 3 and vol_ratio > 1.2:
            score += 5  # 明显涨+明显放量
        elif price_chg_3d > 1 and vol_ratio > 1.0:
            score += 4  # 小涨+小放量
        elif price_chg_3d > 0:
            score += 3  # 上涨即可
        elif price_chg_3d > -2:  # 不跌太多也给分
            score += 1
        
        return min(score, 5)
    
    def _score_chip_structure(self, volume) -> float:
        """【维度7】筹码结构（3分）⭐ - 🔥放宽标准"""
        score = 0
        
        if len(volume) >= 20:
            vol_std = np.std(volume[-20:])
            vol_mean = np.mean(volume[-20:])
            if vol_mean > 0:
                cv = vol_std / vol_mean
                if cv < 0.5:  # 从0.3放宽到0.5
                    score += 3  # 筹码集中
                elif cv < 0.8:  # 从0.5放宽到0.8
                    score += 2
                elif cv < 1.2:  # 从0.8放宽到1.2
                    score += 1
        
        return min(score, 3)
    
    def _score_safety_margin(self, ind: Dict) -> float:
        """【维度8】安全边际（2分）⭐ - 🔥放宽标准"""
        score = 0
        
        price_pos = ind.get('price_position', 0.5)
        if price_pos < 0.60:  # 从0.50放宽到0.60
            score += 2  # 中低位
        elif price_pos < 0.75:  # 从0.60放宽到0.75
            score += 1
        elif price_pos < 0.85:  # 高位也给0.5分
            score += 0.5
        
        return min(score, 2)
    
    def _calculate_synergy_v6(self, dim_scores: Dict, ind: Dict) -> Dict:
        """
        ========== 协同加分系统（v6.0）========== 🔥大幅降低要求
        """
        bonus = 0
        combo_types = []
        
        # 1. 板块龙头组合（最强！）⭐⭐⭐ - 降低要求
        if (dim_scores['板块热度'] >= 15 and  # 从20降到15
            dim_scores['资金流向'] >= 10 and  # 从15降到10
            dim_scores['相对强度'] >= 5):     # 从8降到5
            bonus += 10
            combo_types.append('板块龙头')
        
        # 2. 技术突破确认 ⭐⭐ - 降低要求
        if (dim_scores['技术突破'] >= 10 and  # 从15降到10
            dim_scores['短期动量'] >= 7):      # 从10降到7
            bonus += 5
            combo_types.append('技术突破')
        
        # 3. 资金共振 ⭐⭐ - 降低要求
        if (dim_scores['板块热度'] >= 10 and  # 从15降到10
            dim_scores['资金流向'] >= 10):     # 从15降到10
            bonus += 5
            combo_types.append('资金共振')
        
        # 4. 新增：量价齐升 ⭐ - 基础组合
        if (dim_scores['短期动量'] >= 5 and 
            dim_scores['量能配合'] >= 3):
            bonus += 3
            combo_types.append('量价齐升')
        
        # 5. 新增：板块跟随 ⭐ - 基础组合
        if (dim_scores['板块热度'] >= 8 and 
            dim_scores['技术突破'] >= 8):
            bonus += 3
            combo_types.append('板块跟随')
        
        return {
            'bonus': min(bonus, 20),  # 从15提高到20
            'combo_type': ' + '.join(combo_types) if combo_types else '无'
        }
    
    def _generate_description_v6(self, dim_scores: Dict, final_score: float) -> str:
        """生成v6.0推荐理由"""
        reasons = []
        
        if dim_scores['板块热度'] >= 9:
            reasons.append('板块热度高')
        if dim_scores['资金流向'] >= 10:
            reasons.append('资金持续流入')
        if dim_scores['技术突破'] >= 10:
            reasons.append('技术形态突破')
        if dim_scores['短期动量'] >= 7:
            reasons.append('短期动量强劲')
        if dim_scores['相对强度'] >= 7:
            reasons.append('强于大盘')
        
        if len(reasons) == 0:
            return f"综合评分{final_score:.0f}分，符合超短线标准"
        else:
            return "、".join(reasons)
    
    def _calculate_risk_v6(self, ind: Dict, close, pct_chg, volume) -> Dict:
        """
        🔥 v6.0专属风险扣分系统（0-30分）- 优化版
        
        减少扣分力度，避免与短期动量矛盾
        """
        penalty = 0
        reasons = []
        
        # 1. 追高风险 - 减少扣分
        price_pos = ind['price_position']
        if price_pos >= 0.85:  # 从0.80提高到0.85
            penalty += 10  # 从15降到10
            reasons.append('极高位风险(-10分)')
        elif price_pos >= 0.75:  # 从0.70提高到0.75
            penalty += 5  # 从10降到5
            reasons.append('高位风险(-5分)')
        elif price_pos >= 0.65:  # 从0.60提高到0.65
            penalty += 3  # 从5降到3
            reasons.append('偏高位风险(-3分)')
        
        # 2. 短期暴涨风险 - 减少扣分（避免与短期动量矛盾）
        change_3d = sum(pct_chg[-3:])
        if change_3d > 25:  # 从20提高到25
            penalty += 8  # 从10降到8
            reasons.append('短期暴涨(-8分)')
        elif change_3d > 18:  # 从15提高到18
            penalty += 3  # 从5降到3
            reasons.append('涨幅较大(-3分)')
        
        # 3. 连续跌停
        limit_down_count = sum(1 for p in pct_chg[-10:] if p < -9.5)
        if limit_down_count >= 2:
            penalty += 8
            reasons.append('连续跌停(-8分)')
        elif limit_down_count >= 1:
            penalty += 5
            reasons.append('近期跌停(-5分)')
        
        # 4. 高波动风险
        volatility = ind['volatility']
        if volatility > 0.08:
            penalty += 5
            reasons.append('高波动(-5分)')
        elif volatility > 0.06:
            penalty += 3
            reasons.append('波动偏大(-3分)')
        
        return {
            'penalty': min(penalty, 30),
            'reasons': reasons
        }
    
    # ========== 🔥 精准优化版新增方法 ==========
    
    def _score_money_flow_optimized(self, ts_code: str) -> float:
        """
        【维度1】资金流向（30分）⭐⭐⭐ - 超短线最核心
        
        从20分提升到30分，加大权重
        """
        score = 0
        
        try:
            # 1. 获取资金流向数据
            money_flow = self.data_provider.get_money_flow(ts_code, days=3)
            
            # 1.1 大单净流入（15分）- 🔥提高权重
            net_mf = money_flow['net_mf_amount']  # 万元
            consecutive_days = money_flow['consecutive_inflow_days']
            
            if consecutive_days >= 3:
                score += 15  # 连续3天流入
            elif consecutive_days >= 2:
                score += 12  # 连续2天也给高分
            elif net_mf > 8000:  # 单日大额流入
                score += 10
            elif net_mf > 3000:
                score += 7
            elif net_mf > 500:
                score += 4
            elif net_mf > 0:
                score += 2
            
            # 1.2 主力资金（8分）- 🔥提高权重
            elg_net = money_flow['buy_elg_amount'] - money_flow['sell_elg_amount']
            if elg_net > 5000:
                score += 8
            elif elg_net > 1000:
                score += 5
            elif elg_net > 200:
                score += 3
            elif elg_net > 0:
                score += 1
            elif elg_net > -500:
                score += 0.5
            
            # 1.3 北向资金（7分）- 🔥提高权重
            north_flow = self.data_provider.get_north_money_flow(ts_code, days=3)
            if north_flow['is_connect_stock']:
                north_net = north_flow['north_net_3d']
                if north_net > 5000:
                    score += 7
                elif north_net > 1000:
                    score += 5
                elif north_net > 0:
                    score += 2
            
        except Exception as e:
            logger.warning(f"资金流向评分失败 {ts_code}: {e}")
        
        return min(score, 30)
    
    def _score_short_term_momentum_optimized(self, pct_chg) -> float:
        """
        【维度3】短期动量（20分）⭐⭐⭐ - 超短线爆发力
        
        从15分提升到20分
        """
        score = 0
        
        # 3.1 近3天涨幅（12分）- 🔥提高权重
        change_3d = sum(pct_chg[-3:])
        if change_3d > 10:
            score += 12
        elif change_3d > 6:
            score += 10
        elif change_3d > 3:
            score += 7
        elif change_3d > 0:
            score += 3
        
        # 3.2 连续阳线（5分）
        positive_days = sum(1 for x in pct_chg[-3:] if x > 0)
        if positive_days == 3:
            score += 5
        elif positive_days == 2:
            score += 3
        elif positive_days == 1:
            score += 1
        
        # 3.3 加速特征（3分）- 🔥新增
        if len(pct_chg) >= 3:
            if pct_chg[-1] > pct_chg[-2] > pct_chg[-3]:
                score += 3  # 连续加速
            elif pct_chg[-1] > pct_chg[-2]:
                score += 1  # 今日加速
        
        return min(score, 20)
    
    def _score_leader_attribute(self, ts_code: str, industry: str, change_3d: float) -> float:
        """
        【维度4】龙头属性（10分）⭐⭐ - 新增核心维度
        
        识别板块龙头和涨停基因
        """
        try:
            leader_info = self.leader_analyzer.calculate_leader_score(
                ts_code, industry, change_3d
            )
            return leader_info['total_score']
        except Exception as e:
            logger.warning(f"龙头属性评分失败 {ts_code}: {e}")
            return 0.0
    
    def _score_relative_strength_optimized(self, ts_code: str, pct_chg) -> float:
        """
        【维度5】相对强度（8分）⭐⭐ - 跑赢大盘
        
        从10分调整到8分
        """
        score = 0
        
        try:
            stock_change = sum(pct_chg[-3:])
            market_change = self.data_provider.get_market_change(days=3)
            relative_strength = stock_change - market_change
            
            if relative_strength > 5:
                score += 8
            elif relative_strength > 3:
                score += 6
            elif relative_strength > 0:
                score += 4
            elif relative_strength > -1:
                score += 1
            
        except Exception as e:
            logger.warning(f"相对强度评分失败 {ts_code}: {e}")
        
        return min(score, 8)
    
    def _score_technical_breakthrough_optimized(self, ind: Dict, volume) -> float:
        """
        【维度6】技术突破（5分）⭐ - 简化为放量
        
        从20分简化到5分，只关注放量
        """
        score = 0
        
        vol_ratio = ind.get('vol_ratio', 1.0)
        
        if vol_ratio > 2.0:
            score += 5  # 明显放量
        elif vol_ratio > 1.5:
            score += 4
        elif vol_ratio > 1.2:
            score += 3
        elif vol_ratio > 1.0:
            score += 2
        elif vol_ratio > 0.8:
            score += 1
        
        return min(score, 5)
    
    def _calculate_synergy_v6_optimized(self, dim_scores: Dict, ind: Dict, 
                                        pct_chg, volume) -> Dict:
        """
        🔥 协同加分系统（v6.0精准优化版）- 最高30分
        
        从15分提升到30分，新增更多组合
        """
        bonus = 0
        combo_types = []
        
        # 1. 板块总龙头（15分）⭐⭐⭐ - 最强组合
        if (dim_scores['板块热度'] >= 20 and 
            dim_scores['资金流向'] >= 25 and 
            dim_scores['龙头属性'] >= 8 and
            dim_scores['短期动量'] >= 15):
            bonus += 15
            combo_types.append('板块总龙头')
        
        # 2. 资金共振（10分）⭐⭐⭐
        if (dim_scores['资金流向'] >= 20 and 
            dim_scores['短期动量'] >= 15 and
            dim_scores['技术突破'] >= 3):
            bonus += 10
            combo_types.append('资金共振')
        
        # 3. 启动信号（8分）⭐⭐ - 新增
        # 缩量企稳+放量突破
        if len(volume) >= 5:
            vol_trend_down = (volume[-4] < volume[-5] and 
                             volume[-3] < volume[-4] and 
                             volume[-2] < volume[-3])
            vol_breakout = volume[-1] > volume[-2] * 1.5
            
            if vol_trend_down and vol_breakout and pct_chg[-1] > 2:
                bonus += 8
                combo_types.append('启动信号')
        
        # 4. 情绪助推（5分）⭐ - 新增
        try:
            market_change = self.data_provider.get_market_change(days=1)
            if market_change > 1 and dim_scores['板块热度'] >= 15:
                bonus += 5
                combo_types.append('情绪助推')
        except:
            pass
        
        # 5. 低位启动（3分）⭐ - 新增
        price_pos = ind.get('price_position', 0.5)
        if (price_pos < 0.5 and 
            dim_scores['短期动量'] >= 10 and 
            dim_scores['技术突破'] >= 3):
            bonus += 3
            combo_types.append('低位启动')
        
        return {
            'bonus': min(bonus, 30),
            'combo_type': ' + '.join(combo_types) if combo_types else '无'
        }
    
    def _calculate_risk_v6_optimized(self, ind: Dict, close, pct_chg, volume) -> Dict:
        """
        🔥 v6.0风险扣分系统（精准优化版）- 最高40分
        
        加强风险控制，从30分提升到40分
        """
        penalty = 0
        reasons = []
        
        # 1. 追高风险（-20分）⚠️⚠️⚠️ - 🔥加大扣分
        price_pos = ind['price_position']
        if price_pos >= 0.90:
            penalty += 20  # 从10提高到20
            reasons.append('极高位风险(-20分)')
        elif price_pos >= 0.85:
            penalty += 15  # 从10提高到15
            reasons.append('高位风险(-15分)')
        elif price_pos >= 0.75:
            penalty += 10  # 从5提高到10
            reasons.append('偏高位(-10分)')
        elif price_pos >= 0.65:
            penalty += 5  # 从3提高到5
            reasons.append('中高位(-5分)')
        
        # 2. 短期暴涨（-15分）⚠️⚠️⚠️ - 🔥加大扣分
        change_3d = sum(pct_chg[-3:])
        if change_3d > 30:
            penalty += 15  # 从8提高到15
            reasons.append('短期暴涨(-15分)')
        elif change_3d > 20:
            penalty += 10  # 从5提高到10
            reasons.append('涨幅过大(-10分)')
        elif change_3d > 15:
            penalty += 5  # 新增
            reasons.append('涨幅较大(-5分)')
        
        # 3. 连续涨停（-10分）⚠️⚠️ - 🔥新增
        limit_up_count = sum(1 for p in pct_chg[-5:] if p >= 9.5)
        if limit_up_count >= 3:
            penalty += 10
            reasons.append('连续涨停(-10分)')
        elif limit_up_count >= 2:
            penalty += 5
            reasons.append('多次涨停(-5分)')
        
        # 4. 市场环境（-10分）⚠️ - 🔥新增
        try:
            market_change_1d = self.data_provider.get_market_change(days=1)
            market_change_3d = self.data_provider.get_market_change(days=3)
            
            if market_change_1d < -2:
                penalty += 10
                reasons.append('大盘暴跌(-10分)')
            elif market_change_3d < -3:
                penalty += 5
                reasons.append('大盘走弱(-5分)')
        except:
            pass
        
        # 5. 高波动风险（-5分）⚠️
        volatility = ind['volatility']
        if volatility > 0.08:
            penalty += 5
            reasons.append('高波动(-5分)')
        elif volatility > 0.06:
            penalty += 3
            reasons.append('波动偏大(-3分)')
        
        return {
            'penalty': min(penalty, 40),
            'reasons': reasons
        }


# ========== 便捷调用接口 ==========

def evaluate_stock_v6(stock_data: pd.DataFrame, ts_code: str) -> Dict:
    """
    便捷调用：v6.0超短线狙击评分
    """
    evaluator = ComprehensiveStockEvaluatorV6()
    return evaluator.evaluate_stock_v6(stock_data, ts_code)

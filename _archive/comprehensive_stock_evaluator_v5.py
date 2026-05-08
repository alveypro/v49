#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚀 综合优选 v5.0 - 启动确认版（趋势爆发型）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    从"潜伏为王"到"启动确认" - 确认趋势后果断买入！
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 核心差异（对比v4.0）：
1. 【启动确认】提升权重到20分 ⬆️ 翻倍！确认趋势后买入
2. 【涨停基因】提升到8分 ⬆️ 重视爆发力
3. 【主力行为】提升到18分 ⬆️ 重视资金流入
4. 【潜伏价值】降低到10分 ⬇️ 不追求极致底部
5. 【底部特征】降低到10分 ⬇️ 允许中位买入

📊 新的8维100分评分体系：
【维度1】潜伏价值（10分）⬇️ 降权！不强求底部
【维度2】底部特征（10分）⬇️ 降权！中位也可以
【维度3】量价配合（20分）放量突破最重要
【维度4】MACD趋势（7分）趋势确认
【维度5】均线多头（7分）多头排列
【维度6】主力行为（18分）⬆️ 提权！资金流入
【维度7】启动确认（20分）⬆️ 翻倍！最重要！
【维度8】涨停基因（8分）⬆️ 提权！爆发力

🎯 适用场景：
- ✅ 想要确认趋势后买入
- ✅ 追求短期爆发力
- ✅ 不想等待潜伏期
- ❌ 不适合长期价值投资

💡 核心理念：
v4.0在股票启动前潜伏 ⇒ 成本低但等待长
v5.0在股票启动后确认 ⇒ 确定性高见效快

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import logging

# 导入V4评分器作为基类
from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4

logger = logging.getLogger(__name__)


class ComprehensiveStockEvaluatorV5(ComprehensiveStockEvaluatorV4):
    """
    🏆 综合优选评分器 v5.0 - 启动确认版
    
    核心差异：重视启动确认、主力行为、涨停基因，降低对底部的要求
    """
    
    def __init__(self):
        super().__init__()
        self.version = "v5.0"
        self.name = "启动确认版"
        
    def evaluate_stock_v4(self, stock_data: pd.DataFrame) -> Dict:
        """
        🏆 综合优选启动确认版v5.0：调整权重的8维100分评分体系
        
        核心逻辑：启动确认 + 主力行为 + 爆发力 = 高分
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
            
            # ========== 计算所有基础指标 ==========
            indicators = self._calculate_all_indicators(close, volume, pct_chg)
            
            # ========== 🔥 8维评分体系（v5.0启动确认版）==========
            
            # 【维度1】潜伏价值（10分）⬇️ 降权！
            score_lurking = self._score_lurking_value_v5(indicators, close, volume, pct_chg)
            
            # 【维度2】底部特征（10分）⬇️ 降权！
            score_bottom = self._score_bottom_feature_v5(indicators)
            
            # 【维度3】量价配合（20分）重要！
            score_volume = self._score_volume_price_v5(indicators, close, volume)
            
            # 【维度4】MACD趋势（7分）
            score_macd = self._score_macd_trend_v5(indicators, close)
            
            # 【维度5】均线多头（7分）
            score_ma = self._score_ma_trend_v5(indicators)
            
            # 【维度6】主力行为（18分）⬆️ 提权！
            score_main_force = self._score_main_force_v5(indicators, close, volume)
            
            # 【维度7】启动确认（20分）⬆️ 翻倍！最重要！
            score_launch = self._score_launch_confirm_v5(indicators, close, pct_chg)
            
            # 【维度8】涨停基因（8分）⬆️ 提权！
            score_limit_up = self._score_limit_up_gene_v5(pct_chg)
            
            # 汇总各维度得分
            dim_scores = {
                '潜伏价值': round(score_lurking, 1),
                '底部特征': round(score_bottom, 1),
                '量价配合': round(score_volume, 1),
                'MACD趋势': round(score_macd, 1),
                '均线多头': round(score_ma, 1),
                '主力行为': round(score_main_force, 1),
                '启动确认': round(score_launch, 1),
                '涨停基因': round(score_limit_up, 1)
            }
            
            # 基础分
            base_score = sum(dim_scores.values())
            
            # ========== 协同加分（最高10分）==========
            synergy_result = self._calculate_synergy_v5(dim_scores, indicators)
            
            # ========== 风险扣分（最高30分）==========
            # 🔥 v5.0使用专属风险扣分系统（不惩罚启动状态）
            risk_result = self._calculate_risk_v5(indicators, close, pct_chg, volume)
            
            # 最终得分
            final_score = max(0, min(100, base_score + synergy_result['bonus'] - risk_result['penalty']))
            risk_prices = self._recommend_stop_loss_v4(close, indicators)
            
            # 构建返回结果
            result = {
                'success': True,
                'final_score': round(final_score, 1),
                'base_score': round(base_score, 1),
                'dim_scores': dim_scores,
                'synergy_bonus': synergy_result['bonus'],
                'synergy_combo': synergy_result['combo_type'],
                'risk_penalty': risk_result['penalty'],
                'risk_reasons': risk_result['reasons'],
                
                # 核心指标（0-100格式）
                'price_position': round(indicators.get('price_position', 0) * 100, 1),
                'vol_ratio': round(indicators.get('vol_ratio', 0), 2),
                'price_chg_5d': round(indicators.get('price_chg_5d', 0), 2),
                
                # 买卖建议
                'stop_loss': risk_prices['stop_loss'],
                'take_profit': risk_prices['take_profit'],
                'stop_loss_method': risk_prices.get('method', ''),
            }
            
            return result
            
        except Exception as e:
            logger.error(f"V5评分计算失败: {str(e)}")
            return self._empty_result()
    
    # ========== V5专属评分方法（调整权重）==========
    
    def _score_lurking_value_v5(self, ind: Dict, close, volume, pct_chg) -> float:
        """
        【维度1】潜伏价值（10分）⬇️ 降权到10分
        
        v4.0: 20分 -> v5.0: 10分
        """
        score = 0
        
        # 1.1 缩量整理（5分）
        vol_ratio = ind.get('vol_ratio', 1.0)
        if 0.3 <= vol_ratio <= 0.7:
            score += 5  # 温和缩量
        elif 0.7 < vol_ratio <= 0.9:
            score += 3  # 轻微缩量
        
        # 1.2 横盘蓄势（5分）
        price_chg_5d = ind.get('price_chg_5d', 0)
        if abs(price_chg_5d) <= 3:
            score += 5  # 5天内波动小于3%
        elif abs(price_chg_5d) <= 5:
            score += 3
        
        return min(score, 10)  # 最高10分
    
    def _score_bottom_feature_v5(self, ind: Dict) -> float:
        """
        【维度2】底部特征（10分）⬇️ 降权到10分
        
        v4.0: 20分 -> v5.0: 10分
        允许中位买入，不强求极致底部
        """
        score = 0
        
        # 2.1 价格位置（5分）- 放宽要求
        price_pos = ind.get('price_position', 0.5)
        if price_pos < 0.30:
            score += 5  # 低位
        elif price_pos < 0.50:
            score += 3  # 中低位也可以
        elif price_pos < 0.60:
            score += 1  # 中位勉强接受
        
        # 2.2 超跌反弹（5分）
        price_chg_20d = ind.get('price_chg_20d', 0)
        if -20 <= price_chg_20d <= -10:
            score += 5  # 适度超跌
        elif -30 <= price_chg_20d < -20:
            score += 3  # 深度超跌
        
        return min(score, 10)  # 最高10分
    
    def _score_volume_price_v5(self, ind: Dict, close, volume) -> float:
        """
        【维度3】量价配合（20分）重要！
        
        v4.0: 25分 -> v5.0: 20分
        重视放量突破
        """
        score = 0
        
        # 3.1 温和放量（8分）
        vol_ratio = ind.get('vol_ratio', 1.0)
        if 1.5 <= vol_ratio <= 3.0:
            score += 8  # 理想放量
        elif 1.2 <= vol_ratio < 1.5:
            score += 6  # 温和放量
        elif 3.0 < vol_ratio <= 5.0:
            score += 4  # 较大放量
        
        # 3.2 价涨量增（8分）
        price_chg_5d = ind.get('price_chg_5d', 0)
        if price_chg_5d > 3 and vol_ratio > 1.2:
            score += 8  # 价涨量增
        elif price_chg_5d > 0 and vol_ratio > 1.0:
            score += 5
        
        # 3.3 近期量能（4分）
        recent_vol = np.mean(volume[-5:]) if len(volume) >= 5 else 0
        hist_vol = np.mean(volume[-20:-5]) if len(volume) >= 20 else 1
        if hist_vol > 0:
            vol_growth = (recent_vol - hist_vol) / hist_vol
            if vol_growth > 0.3:
                score += 4
            elif vol_growth > 0.15:
                score += 2
        
        return min(score, 20)  # 最高20分
    
    def _score_main_force_v5(self, ind: Dict, close, volume) -> float:
        """
        【维度6】主力行为（18分）⬆️ 提权到18分
        
        v4.0: 15分 -> v5.0: 18分
        更重视主力资金流入
        """
        score = 0
        
        # 6.1 大单吸筹（9分）⬆️ 提高
        vol_ratio = ind.get('vol_ratio', 1.0)
        if vol_ratio > 2.0:
            score += 9  # 明显放量
        elif vol_ratio > 1.5:
            score += 6
        elif vol_ratio > 1.2:
            score += 3
        
        # 6.2 筹码集中（9分）⬆️ 提高
        price_vol_20d = np.std(volume[-20:]) / np.mean(volume[-20:]) if len(volume) >= 20 else 0
        if price_vol_20d < 0.3:
            score += 9  # 筹码高度集中
        elif price_vol_20d < 0.5:
            score += 6
        elif price_vol_20d < 0.8:
            score += 3
        
        return min(score, 18)  # 最高18分
    
    def _score_launch_confirm_v5(self, ind: Dict, close, pct_chg) -> float:
        """
        【维度7】启动确认（20分）⬆️ 翻倍！最重要！
        
        v4.0: 10分 -> v5.0: 20分
        重点确认趋势已经启动
        """
        score = 0
        
        # 7.1 突破关键位（8分）⬆️ 提高
        ma20 = ind.get('ma20', 0)
        ma60 = ind.get('ma60', 0)
        current_price = close[-1]
        
        if current_price > ma20 and current_price > ma60:
            score += 8  # 站上双均线
        elif current_price > ma20:
            score += 5  # 站上20日线
        elif current_price > ma60:
            score += 3  # 站上60日线
        
        # 7.2 连续阳线（6分）⬆️ 提高
        recent_positive = sum(1 for chg in pct_chg[-5:] if chg > 0)
        if recent_positive >= 4:
            score += 6  # 5天4阳
        elif recent_positive >= 3:
            score += 4  # 5天3阳
        elif recent_positive >= 2:
            score += 2
        
        # 7.3 放量上涨（6分）⬆️ 提高
        vol_ratio = ind.get('vol_ratio', 1.0)
        price_chg_5d = ind.get('price_chg_5d', 0)
        if price_chg_5d > 5 and vol_ratio > 1.5:
            score += 6  # 强势放量上涨
        elif price_chg_5d > 3 and vol_ratio > 1.2:
            score += 4
        elif price_chg_5d > 0 and vol_ratio > 1.0:
            score += 2
        
        return min(score, 20)  # 最高20分
    
    def _score_limit_up_gene_v5(self, pct_chg) -> float:
        """
        【维度8】涨停基因（8分）⬆️ 提权到8分
        
        v4.0: 5分 -> v5.0: 8分
        更重视历史爆发力
        """
        score = 0
        
        # 8.1 近期涨停（8分）⬆️ 提高
        recent_limit_ups = sum(1 for chg in pct_chg[-20:] if chg >= 9.5)
        if recent_limit_ups >= 3:
            score += 8  # 3次以上涨停
        elif recent_limit_ups >= 2:
            score += 6  # 2次涨停
        elif recent_limit_ups >= 1:
            score += 4  # 1次涨停
        
        # 8.2 大阳线
        big_positive = sum(1 for chg in pct_chg[-20:] if chg >= 5)
        if big_positive >= 3:
            score += 2  # 额外加分
        
        return min(score, 8)  # 最高8分
    
    def _score_macd_trend_v5(self, ind: Dict, close) -> float:
        """
        【维度4】MACD趋势（7分）
        调用父类v4方法，但限制在7分以内
        """
        # 调用父类v4方法（原本是15分）
        score_v4 = self._score_macd_trend_v4(ind, close)
        # 按比例转换为7分制：7/15 = 0.467
        score_v5 = score_v4 * (7.0 / 15.0)
        return min(score_v5, 7.0)
    
    def _score_ma_trend_v5(self, ind: Dict) -> float:
        """
        【维度5】均线多头（7分）
        调用父类v4方法，但限制在7分以内
        """
        # 调用父类v4方法（原本是10分）
        score_v4 = self._score_ma_trend_v4(ind)
        # 按比例转换为7分制：7/10 = 0.7
        score_v5 = score_v4 * (7.0 / 10.0)
        return min(score_v5, 7.0)
    
    def _calculate_synergy_v5(self, dim_scores: Dict, ind: Dict) -> Dict:
        """
        ========== 协同加分系统（v5.0启动确认版）==========
        
        v5.0调整：更重视"启动+主力"的组合
        """
        bonus = 0
        combo_types = []
        
        # 1. 强势启动（最重要！）⭐⭐⭐
        if dim_scores['启动确认'] >= 15 and dim_scores['主力行为'] >= 12:
            bonus += 4  # v4: 不存在 -> v5: 4分
            combo_types.append('强势启动')
        
        # 2. 爆发组合 ⭐⭐
        if dim_scores['启动确认'] >= 12 and dim_scores['涨停基因'] >= 6:
            bonus += 3  # v4: 不存在 -> v5: 3分
            combo_types.append('爆发组合')
        
        # 3. 资金推动 ⭐⭐
        if dim_scores['主力行为'] >= 12 and dim_scores['量价配合'] >= 15:
            bonus += 3  # v4: 4 -> v5: 3分
            combo_types.append('资金推动')
        
        # 4. 趋势确立 ⭐
        if dim_scores['MACD趋势'] >= 5 and dim_scores['均线多头'] >= 5:
            bonus += 2  # v4: 2.5 -> v5: 2分
            combo_types.append('趋势确立')
        
        # 5. 突破确认 ⭐
        if dim_scores['启动确认'] >= 15 and dim_scores['量价配合'] >= 15:
            bonus += 2  # v4: 不存在 -> v5: 2分
            combo_types.append('突破确认')
        
        return {
            'bonus': min(bonus, 10),  # 最高10分（v4也是10）
            'combo_type': ' + '.join(combo_types) if combo_types else '无'
        }
    
    def _calculate_risk_v5(self, ind: Dict, close, pct_chg, volume) -> Dict:
        """
        🔥 v5.0专属风险扣分系统（0-30分）
        
        核心调整：不惩罚"启动确认"状态，允许中高位买入
        
        与v4.0的差异：
        - price_position阈值提高：0.50 -> 0.70（允许更高位）
        - 涨幅阈值提高：10% -> 20%（允许更大涨幅）
        - 目的：适配"启动确认"策略，不对启动状态惩罚
        """
        penalty = 0
        reasons = []
        
        # 1. 高位风险 - 🔥 阈值提高，允许中高位买入
        price_pos = ind['price_position']
        if price_pos >= 0.85:
            penalty += 12
            reasons.append('极高位风险(-12分)')
        elif price_pos >= 0.75:
            penalty += 8
            reasons.append('偏高位风险(-8分)')
        # ✅ 0.70以下不扣分（vs v4.0的0.50）
        
        # 2. 暴涨风险 - 🔥 阈值提高，允许更大涨幅
        price_chg_5d = ind['price_chg_5d']
        if price_chg_5d > 0.30:  # 5天涨超30%（vs v4.0的15%）
            penalty += 6
            reasons.append('极端追高风险(-6分)')
        elif price_chg_5d > 0.20:  # 5天涨超20%（vs v4.0的10%）
            penalty += 4
            reasons.append('涨幅较大(-4分)')
        # ✅ 20%以下不扣分（vs v4.0的10%）
        
        # 3. 连续跌停 - 保持不变
        limit_down_count = sum(1 for p in pct_chg[-10:] if p < -9.5)
        if limit_down_count >= 2:
            penalty += 8
            reasons.append('连续跌停(-8分)')
        elif limit_down_count >= 1:
            penalty += 5
            reasons.append('近期跌停(-5分)')
        
        # 4. 高波动风险 - 保持不变
        volatility = ind['volatility']
        if volatility > 0.08:
            penalty += 5
            reasons.append('高波动风险(-5分)')
        elif volatility > 0.06:
            penalty += 3
            reasons.append('波动偏大(-3分)')
        
        # 5. 破位风险 - 🔥 调整：允许短期回调
        ma60 = ind.get('ma60', 0)
        current_price = close[-1]
        if current_price < ma60 * 0.90:  # 跌破MA60且低于10%（vs v4.0的直接跌破）
            penalty += 5
            reasons.append('深度破位(-5分)')
        # ✅ 不严格要求站上MA60
        
        # 6. 缩量下跌 - 保持不变
        vol_ratio = ind.get('vol_ratio', 1.0)
        price_chg_5d_val = ind.get('price_chg_5d', 0)
        if price_chg_5d_val < -0.05 and vol_ratio < 0.8:
            penalty += 4
            reasons.append('缩量下跌(-4分)')
        
        return {
            'penalty': min(penalty, 30),  # 最高30分
            'reasons': reasons
        }


# ========== 便捷调用接口 ==========

def evaluate_stock_launch_confirm(stock_data: pd.DataFrame) -> Dict:
    """
    便捷调用：启动确认型评分
    """
    evaluator = ComprehensiveStockEvaluatorV5()
    return evaluator.evaluate_stock_v4(stock_data)

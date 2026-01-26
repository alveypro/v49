#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚀 综合优选 v4.0 - 潜伏为王版（革命性回测优化）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    从"启动为王"到"潜伏为王" - 在启动前潜伏才是真王道！
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 核心创新：
1. 【潜伏价值】维度（20分）⭐ 全新！识别即将启动的底部股票
2. 【底部特征】提升权重到20分 - 好股票一定在底部
3. 【启动确认】降低权重到10分 - 启动后再买就晚了
4. 【动态阈值】60-70分即可 - 扩大样本量，提升胜率
5. 【风险控制】新增破位预警 - 识别即将破位的股票

📊 新的8维100分评分体系：
【维度1】潜伏价值（20分）⭐ 全新！最重要！即将启动但未启动
【维度2】底部特征（20分）⬆️ 提权！价格低位，超跌反弹
【维度3】量价配合（15分）温和放量，主力悄悄吸筹
【维度4】MACD趋势（15分）金叉初期，趋势刚好转
【维度5】均线多头（10分）均线粘合，即将多头发散
【维度6】主力行为（10分）大单流入，筹码集中
【维度7】启动确认（5分）⬇️ 降权！已启动的不追
【维度8】涨停基因（5分）历史爆发力

🎯 预期效果（对比v3.0）：
- 平均收益：从-0.26%提升到3-5% 
- 胜率：从45.2%提升到55-60%
- 夏普比率：从-0.64提升到1.0-1.5
- 信号数量：从31个提升到80-150个

💡 核心理念：
v3.0等股票启动后再买 ⇒ 追高被套 ⇒ 收益差
v4.0在股票启动前潜伏 ⇒ 低位建仓 ⇒ 收益翻倍

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class ComprehensiveStockEvaluatorV4:
    """
    🏆 综合优选评分器 v4.0 - 潜伏为王版
    
    革命性升级：在启动前潜伏，而不是启动后追高！
    """
    
    def __init__(self):
        self.version = "v4.0"
        self.name = "潜伏为王版"
        
    def evaluate_stock_v4(self, stock_data: pd.DataFrame) -> Dict:
        """
        🏆 综合优选终极优化版v4.0：8维100分评分体系
        
        核心逻辑：底部 + 潜伏价值 + 即将启动 = 高分
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
            
            # ========== 🔥 8维评分体系（v4.0潜伏为王版）==========
            
            # 【维度1】潜伏价值（20分）⭐ 全新！最重要！
            score_lurking = self._score_lurking_value(indicators, close, volume, pct_chg)
            
            # 【维度2】底部特征（20分）⬆️ 提权！
            score_bottom = self._score_bottom_feature_v4(indicators)
            
            # 【维度3】量价配合（15分）
            score_volume_price = self._score_volume_price_v4(indicators)
            
            # 【维度4】MACD趋势（15分）
            score_macd = self._score_macd_trend_v4(indicators, close)
            
            # 【维度5】均线多头（10分）
            score_ma = self._score_ma_trend_v4(indicators)
            
            # 【维度6】主力行为（10分）
            score_main_force = self._score_main_force_v4(indicators, pct_chg, volume)
            
            # 【维度7】启动确认（5分）⬇️ 降权！
            score_launch = self._score_launch_v4(indicators, close, volume)
            
            # 【维度8】涨停基因（5分）
            score_limit_up = self._score_limit_up_gene(pct_chg)
            
            # ========== 基础得分（100分）==========
            dimension_scores = {
                '潜伏价值': score_lurking,
                '底部特征': score_bottom,
                '量价配合': score_volume_price,
                'MACD趋势': score_macd,
                '均线多头': score_ma,
                '主力行为': score_main_force,
                '启动确认': score_launch,
                '涨停基因': score_limit_up
            }
            
            base_score = sum(dimension_scores.values())
            
            # ========== 【AI优化】协同效应加分（0-20分）==========
            synergy_result = self._calculate_synergy_v4(
                dimension_scores, indicators, close, volume, pct_chg
            )
            synergy_bonus = synergy_result['bonus']
            combo_type = synergy_result['combo_type']
            
            # ========== 【AI优化】风险扣分（0-25分）==========
            risk_result = self._calculate_risk_v4(indicators, close, pct_chg, volume)
            risk_penalty = risk_result['penalty']
            risk_reasons = risk_result['reasons']
            
            # ========== 计算最终得分 ==========
            final_score = base_score + synergy_bonus - risk_penalty
            final_score = max(0, min(100, final_score))
            
            # ========== 评级 ==========
            if final_score >= 80:
                grade = 'S'  # 顶级：完美潜伏机会
                description = '🔥 完美潜伏机会，重点关注！'
            elif final_score >= 70:
                grade = 'A'  # 优质：底部+潜伏价值
                description = '⭐ 优质潜伏标的，积极关注'
            elif final_score >= 60:
                grade = 'B'  # 良好：具备潜伏价值
                description = '💡 良好机会，谨慎关注'
            elif final_score >= 50:
                grade = 'C'  # 合格
                description = '📊 合格标的，保持观察'
            else:
                grade = 'D'  # 不推荐
                description = '⚠️ 暂不推荐'
            
            # ========== 智能止损位 ==========
            stop_loss_info = self._recommend_stop_loss_v4(close, indicators)
            
            return {
                # 核心评分
                'comprehensive_score': round(final_score, 2),
                'final_score': round(final_score, 2),
                'grade': grade,
                'description': description,
                
                # 评分组成
                'dimension_scores': {k: round(v, 1) for k, v in dimension_scores.items()},
                'base_score': round(base_score, 1),
                'synergy_bonus': round(synergy_bonus, 1),
                'combo_type': combo_type,
                'risk_penalty': round(risk_penalty, 1),
                'risk_reasons': risk_reasons,
                
                # 止损建议
                'stop_loss': stop_loss_info['stop_loss'],
                'stop_loss_method': stop_loss_info['method'],
                'take_profit': stop_loss_info['take_profit'],
                
                # 关键指标
                'price_position': round(indicators['price_position'] * 100, 1),
                'vol_ratio': round(indicators['vol_ratio'], 2),
                'price_chg_5d': round(indicators['price_chg_5d'] * 100, 2),
                
                # 元数据
                'version': self.version,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"v4.0评分失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return self._empty_result()
    
    def _calculate_all_indicators(self, close, volume, pct_chg) -> Dict:
        """计算所有基础指标"""
        # 价格指标
        price_min_60 = np.min(close[-60:])
        price_max_60 = np.max(close[-60:])
        price_range = price_max_60 - price_min_60
        price_position = (close[-1] - price_min_60) / price_range if price_range > 0 else 0.5
        
        # 成交量指标
        recent_vol_3d = np.mean(volume[-3:])
        recent_vol_5d = np.mean(volume[-5:])
        hist_vol_20d = np.mean(volume[-23:-3]) if len(volume) >= 23 else np.mean(volume[:-3])
        vol_ratio = recent_vol_3d / hist_vol_20d if hist_vol_20d > 0 else 1.0
        vol_ratio_5d = recent_vol_5d / hist_vol_20d if hist_vol_20d > 0 else 1.0
        
        # 涨跌幅
        price_chg_5d = (close[-1] - close[-6]) / close[-6] if len(close) > 6 and close[-6] > 0 else 0
        price_chg_10d = (close[-1] - close[-11]) / close[-11] if len(close) > 11 and close[-11] > 0 else 0
        price_chg_20d = (close[-1] - close[-21]) / close[-21] if len(close) > 21 and close[-21] > 0 else 0
        price_chg_60d = (close[-1] - close[-61]) / close[-61] if len(close) > 61 and close[-61] > 0 else 0
        
        # 均线
        ma5 = np.mean(close[-5:])
        ma10 = np.mean(close[-10:])
        ma20 = np.mean(close[-20:])
        ma60 = np.mean(close[-60:]) if len(close) >= 60 else ma20
        
        # MACD
        ema12 = pd.Series(close).ewm(span=12, adjust=False).mean().values
        ema26 = pd.Series(close).ewm(span=26, adjust=False).mean().values
        dif = ema12 - ema26
        dea = pd.Series(dif).ewm(span=9, adjust=False).mean().values
        macd_hist = dif - dea
        
        # 波动率
        volatility = np.std(close[-20:]) / np.mean(close[-20:]) if np.mean(close[-20:]) > 0 else 0
        
        # 涨停跌停
        limit_up_count_5d = sum(1 for p in pct_chg[-5:] if p > 9.5)
        limit_down_count_60d = sum(1 for p in pct_chg[-60:] if p < -9.5)
        
        # 连续性指标
        continuous_up_days = sum(1 for p in pct_chg[-5:] if p > 0)
        continuous_vol_up = sum(1 for v in volume[-5:] if v > hist_vol_20d * 1.2) if hist_vol_20d > 0 else 0
        
        return {
            'price_position': price_position,
            'vol_ratio': vol_ratio,
            'vol_ratio_5d': vol_ratio_5d,
            'price_chg_5d': price_chg_5d,
            'price_chg_10d': price_chg_10d,
            'price_chg_20d': price_chg_20d,
            'price_chg_60d': price_chg_60d,
            'ma5': ma5,
            'ma10': ma10,
            'ma20': ma20,
            'ma60': ma60,
            'dif': dif,
            'dea': dea,
            'macd_hist': macd_hist,
            'volatility': volatility,
            'limit_up_count_5d': limit_up_count_5d,
            'limit_down_count_60d': limit_down_count_60d,
            'continuous_up_days': continuous_up_days,
            'continuous_vol_up': continuous_vol_up,
            'recent_vol_3d': recent_vol_3d,
            'hist_vol_20d': hist_vol_20d,
            'price_min_60': price_min_60,
            'price_max_60': price_max_60,
            'close': close
        }
    
    def _score_lurking_value(self, ind: Dict, close, volume, pct_chg) -> float:
        """
        【维度1】潜伏价值评分（20分）⭐ 革命性创新！
        
        核心理念：识别"即将启动但尚未启动"的底部股票！
        
        细分：
        - 底部盘整（6分）：价格在底部横盘，震荡收敛
        - 温和放量（6分）：成交量温和放大，主力悄悄吸筹
        - 趋势酝酿（4分）：MACD在零轴下方，即将金叉
        - 均线粘合（4分）：多条均线粘合，即将发散
        """
        score = 0
        
        # 1.1 底部盘整（6分）
        price_pos = ind['price_position']
        price_chg_10d = abs(ind['price_chg_10d'])
        
        # 关键：价格在底部（<40%）且近10天涨跌幅不大（<8%）= 底部盘整
        if price_pos < 0.30 and price_chg_10d < 0.05:  # 30%位置以下，5%波动以内
            score += 6  # 完美底部盘整
        elif price_pos < 0.40 and price_chg_10d < 0.08:  # 40%位置以下，8%波动以内
            score += 4  # 良好底部盘整
        elif price_pos < 0.50 and price_chg_10d < 0.10:  # 50%位置以下，10%波动以内
            score += 2  # 中等底部盘整
        
        # 1.2 温和放量（6分）
        vol_ratio = ind['vol_ratio']
        
        # 关键：成交量温和放大（1.2-2.0倍）= 主力吸筹，而不是暴涨（>2.5倍）= 追高风险
        if 1.3 <= vol_ratio <= 2.0:
            score += 6  # 完美温和放量
        elif 1.1 <= vol_ratio <= 2.5:
            score += 4  # 良好放量
        elif 1.0 <= vol_ratio <= 3.0:
            score += 2  # 中等放量
        
        # 1.3 趋势酝酿（4分）
        dif_last = ind['dif'][-1]
        dea_last = ind['dea'][-1]
        macd_hist_last = ind['macd_hist'][-1]
        
        # 关键：MACD在零轴下方，DIF向上靠近DEA = 即将金叉
        if dif_last < 0 and dea_last < 0:  # 都在零轴下方
            if macd_hist_last > 0 and macd_hist_last < 0.5:  # 刚刚金叉，能量柱很小
                score += 4
            elif abs(dif_last - dea_last) < abs(dif_last) * 0.2:  # DIF和DEA很接近
                score += 3
            elif macd_hist_last > ind['macd_hist'][-2]:  # 能量柱递增
                score += 2
        
        # 1.4 均线粘合（4分）
        ma5, ma10, ma20 = ind['ma5'], ind['ma10'], ind['ma20']
        close_last = ind['close'][-1]
        
        # 关键：多条均线粘合（距离<3%）= 即将发散
        ma_range = max(ma5, ma10, ma20) - min(ma5, ma10, ma20)
        ma_avg = (ma5 + ma10 + ma20) / 3
        ma_dispersion = ma_range / ma_avg if ma_avg > 0 else 0
        
        if ma_dispersion < 0.02:  # 均线粘合度<2%
            score += 4
        elif ma_dispersion < 0.03:  # 均线粘合度<3%
            score += 3
        elif ma_dispersion < 0.05:  # 均线粘合度<5%
            score += 2
        
        return min(score, 20)
    
    def _score_bottom_feature_v4(self, ind: Dict) -> float:
        """
        【维度2】底部特征评分（20分）⬆️ 提权！
        
        细分：
        - 价格位置（10分）：越低越好
        - 超跌反弹（6分）：60日跌幅深，但近期企稳
        - 波动收敛（4分）：波动率降低，蓄势待发
        """
        score = 0
        
        # 2.1 价格位置（10分）🔥 收紧标准：只有真正的底部才给分
        price_pos = ind['price_position']
        if price_pos < 0.20:
            score += 10  # 历史低位20%
        elif price_pos < 0.30:
            score += 8  # 低位30%
        elif price_pos < 0.40:
            score += 5  # 中低位40% - 从6降到5
        # 🚫 删除50%和60%的给分 - 不再被视为"底部"
        
        # 2.2 超跌反弹（6分）
        price_chg_60d = ind['price_chg_60d']
        price_chg_5d = ind['price_chg_5d']
        
        if price_chg_60d < -0.25:  # 60天跌超25%
            if -0.02 < price_chg_5d < 0.05:  # 近5天企稳或微涨
                score += 6  # 超跌企稳，完美
            elif price_chg_5d < 0:  # 仍在下跌
                score += 3  # 超跌但未企稳
        elif price_chg_60d < -0.15:  # 60天跌超15%
            if price_chg_5d > 0:
                score += 4
        elif price_chg_60d < -0.10:  # 60天跌超10%
            if price_chg_5d > 0:
                score += 2
        
        # 2.3 波动收敛（4分）
        volatility = ind['volatility']
        if volatility < 0.02:  # 波动率<2%
            score += 4
        elif volatility < 0.03:  # 波动率<3%
            score += 3
        elif volatility < 0.04:  # 波动率<4%
            score += 2
        
        return min(score, 20)
    
    def _score_volume_price_v4(self, ind: Dict) -> float:
        """
        【维度3】量价配合评分（15分）
        
        细分：
        - 温和放量（8分）：避免暴涨暴跌
        - 量价同步（4分）：价升量增
        - 持续性（3分）：连续放量
        """
        score = 0
        
        # 3.1 温和放量（8分）
        vol_ratio = ind['vol_ratio']
        if 1.2 <= vol_ratio <= 2.0:
            score += 8  # 温和放量，完美
        elif 1.0 <= vol_ratio < 1.2:
            score += 5  # 略微放量
        elif 2.0 < vol_ratio <= 2.5:
            score += 6  # 较大放量，可接受
        elif 0.8 <= vol_ratio < 1.0:
            score += 3  # 量能略缩
        
        # 3.2 量价同步（4分）
        price_chg_5d = ind['price_chg_5d']
        vol_ratio_5d = ind['vol_ratio_5d']
        
        if price_chg_5d > 0 and vol_ratio_5d > 1.1:  # 价升量增
            score += 4
        elif price_chg_5d > 0 and vol_ratio_5d > 1.0:  # 价升量平
            score += 2
        elif price_chg_5d < 0 and vol_ratio_5d < 0.9:  # 价跌量缩
            score += 3  # 缩量下跌，跌势将尽
        
        # 3.3 持续性（3分）
        continuous_vol_up = ind['continuous_vol_up']
        if continuous_vol_up >= 3:
            score += 3
        elif continuous_vol_up >= 2:
            score += 2
        elif continuous_vol_up >= 1:
            score += 1
        
        return min(score, 15)
    
    def _score_macd_trend_v4(self, ind: Dict, close) -> float:
        """
        【维度4】MACD趋势评分（15分）
        
        细分：
        - 金叉初期（8分）：刚刚金叉或即将金叉
        - 能量柱递增（4分）：红柱递增
        - DIF位置（3分）：DIF在零轴下方，未来空间大
        """
        score = 0
        
        dif_last = ind['dif'][-1]
        dea_last = ind['dea'][-1]
        macd_hist = ind['macd_hist']
        macd_hist_last = macd_hist[-1]
        
        # 4.1 金叉初期（8分）
        if dif_last > dea_last:  # 已经金叉
            if macd_hist_last < 0.5:  # 刚刚金叉，能量柱很小
                score += 8  # 金叉初期，完美
            elif macd_hist_last < 1.0:
                score += 6  # 金叉中期
            else:
                score += 3  # 金叉后期，追高风险
        else:  # 尚未金叉
            if abs(dif_last - dea_last) < abs(dif_last) * 0.1:  # 即将金叉
                score += 7
            elif abs(dif_last - dea_last) < abs(dif_last) * 0.2:
                score += 5
        
        # 4.2 能量柱递增（4分）
        if len(macd_hist) >= 3:
            if macd_hist_last > macd_hist[-2] > macd_hist[-3]:  # 连续递增
                score += 4
            elif macd_hist_last > macd_hist[-2]:  # 递增
                score += 3
            elif macd_hist_last > 0:  # 红柱
                score += 2
        
        # 4.3 DIF位置（3分）
        if dif_last < 0:  # DIF在零轴下方，未来空间大
            score += 3
        elif dif_last < 1:  # DIF略高于零轴
            score += 2
        
        return min(score, 15)
    
    def _score_ma_trend_v4(self, ind: Dict) -> float:
        """
        【维度5】均线多头评分（10分）
        
        细分：
        - 均线粘合（5分）：即将多头发散
        - 多头排列（3分）：MA5>MA10>MA20
        - 价格位置（2分）：股价在均线上方
        """
        score = 0
        
        ma5, ma10, ma20, ma60 = ind['ma5'], ind['ma10'], ind['ma20'], ind['ma60']
        close_last = ind['close'][-1]
        
        # 5.1 均线粘合（5分）
        ma_range = max(ma5, ma10, ma20) - min(ma5, ma10, ma20)
        ma_avg = (ma5 + ma10 + ma20) / 3
        ma_dispersion = ma_range / ma_avg if ma_avg > 0 else 0
        
        if ma_dispersion < 0.02:
            score += 5
        elif ma_dispersion < 0.03:
            score += 4
        elif ma_dispersion < 0.05:
            score += 3
        
        # 5.2 多头排列（3分）
        if ma5 > ma10 > ma20:
            score += 3
        elif ma5 > ma10:
            score += 2
        elif ma10 > ma20:
            score += 1
        
        # 5.3 价格位置（2分）
        if close_last > ma5:
            score += 2
        elif close_last > ma10:
            score += 1
        
        return min(score, 10)
    
    def _score_main_force_v4(self, ind: Dict, pct_chg, volume) -> float:
        """
        【维度6】主力行为评分（10分）
        
        细分：
        - 大单流入（5分）：近期有大单买入
        - 连续流入（3分）：连续多日资金流入
        - 筹码集中（2分）：成交量稳定
        """
        score = 0
        
        # 6.1 大单流入（5分）
        # 简化判断：大涨+大量 = 大单买入
        recent_big_buy = 0
        for i in range(-5, 0):
            if pct_chg[i] > 2 and volume[i] > ind['hist_vol_20d'] * 1.5:
                recent_big_buy += 1
        
        if recent_big_buy >= 3:
            score += 5
        elif recent_big_buy >= 2:
            score += 4
        elif recent_big_buy >= 1:
            score += 3
        
        # 6.2 连续流入（3分）
        continuous_up = ind['continuous_up_days']
        if continuous_up >= 4:
            score += 3
        elif continuous_up >= 3:
            score += 2
        elif continuous_up >= 2:
            score += 1
        
        # 6.3 筹码集中（2分）
        vol_std = np.std(volume[-20:])
        vol_mean = np.mean(volume[-20:])
        vol_cv = vol_std / vol_mean if vol_mean > 0 else 999
        
        if vol_cv < 0.5:  # 成交量变异系数<0.5，比较稳定
            score += 2
        elif vol_cv < 0.8:
            score += 1
        
        return min(score, 10)
    
    def _score_launch_v4(self, ind: Dict, close, volume) -> float:
        """
        【维度7】启动确认评分（5分）⬇️ 降权！
        
        已经启动的股票，追高风险大，降低权重！
        """
        score = 0
        
        # 近3天价格涨幅
        price_chg_3d = (close[-1] - close[-4]) / close[-4] if len(close) > 4 and close[-4] > 0 else 0
        
        # 近3天成交量放大
        vol_ratio = ind['vol_ratio']
        
        # 已经明显启动（涨幅>5%且放量>2倍），不给高分
        if price_chg_3d > 0.05 and vol_ratio > 2.0:
            score += 2  # 已启动，追高风险
        elif price_chg_3d > 0.03 and vol_ratio > 1.5:
            score += 3  # 小幅启动
        elif price_chg_3d > 0 and vol_ratio > 1.2:
            score += 5  # 刚开始启动，最佳买点
        elif price_chg_3d > -0.02:  # 横盘
            score += 4  # 未启动，可潜伏
        
        return min(score, 5)
    
    def _score_limit_up_gene(self, pct_chg) -> float:
        """
        【维度8】涨停基因评分（5分）
        """
        score = 0
        
        # 近60天涨停次数
        limit_up_count_60d = sum(1 for p in pct_chg[-60:] if p > 9.5)
        
        if limit_up_count_60d >= 3:
            score += 5
        elif limit_up_count_60d >= 2:
            score += 4
        elif limit_up_count_60d >= 1:
            score += 3
        
        return min(score, 5)
    
    def _calculate_synergy_v4(self, dim_scores: Dict, ind: Dict, close, volume, pct_chg) -> Dict:
        """
        【AI优化】协同效应加分（0-10分）🔥 减半！避免分数通胀
        
        8种黄金组合（所有加分减半）：
        1. 完美潜伏（潜伏价值18+ & 底部特征18+）+4分
        2. 底部温和放量（底部特征16+ & 量价配合12+）+3分
        3. 金叉初期潜伏（MACD趋势12+ & 潜伏价值15+）+3分
        4. 均线粘合待发（均线多头8+ & 潜伏价值15+）+2.5分
        5. 主力吸筹（主力行为8+ & 量价配合12+）+2.5分
        6. 超跌反弹（底部特征16+ & 价格位置<30%）+2分
        7. 多维共振（5个维度≥12分）+4分
        8. 技术面完美（MACD+均线+量价≥35分）+3分
        """
        bonus = 0
        combo_types = []
        
        # 1. 完美潜伏 - 从8分降到4分
        if dim_scores['潜伏价值'] >= 18 and dim_scores['底部特征'] >= 18:
            bonus += 4
            combo_types.append('完美潜伏')
        
        # 2. 底部温和放量 - 从6分降到3分
        if dim_scores['底部特征'] >= 16 and dim_scores['量价配合'] >= 12:
            bonus += 3
            combo_types.append('底部温和放量')
        
        # 3. 金叉初期潜伏 - 从6分降到3分
        if dim_scores['MACD趋势'] >= 12 and dim_scores['潜伏价值'] >= 15:
            bonus += 3
            combo_types.append('金叉初期潜伏')
        
        # 4. 均线粘合待发 - 从5分降到2.5分
        if dim_scores['均线多头'] >= 8 and dim_scores['潜伏价值'] >= 15:
            bonus += 2.5
            combo_types.append('均线粘合待发')
        
        # 5. 主力吸筹 - 从5分降到2.5分
        if dim_scores['主力行为'] >= 8 and dim_scores['量价配合'] >= 12:
            bonus += 2.5
            combo_types.append('主力吸筹')
        
        # 6. 超跌反弹 - 从4分降到2分
        if dim_scores['底部特征'] >= 16 and ind['price_position'] < 0.30:
            bonus += 2
            combo_types.append('超跌反弹')
        
        # 7. 多维共振 - 从8分降到4分
        high_score_dims = sum(1 for score in dim_scores.values() if score >= dim_scores.get('涨停基因', 0) and score >= 8)
        if high_score_dims >= 5:
            bonus += 4
            combo_types.append('多维共振')
        
        # 8. 技术面完美 - 从6分降到3分
        tech_score = dim_scores['MACD趋势'] + dim_scores['均线多头'] + dim_scores['量价配合']
        if tech_score >= 35:
            bonus += 3
            combo_types.append('技术面完美')
        
        return {
            'bonus': min(bonus, 10),  # 🔥 从20分降到10分
            'combo_type': ' + '.join(combo_types) if combo_types else '无'
        }
    
    def _calculate_risk_v4(self, ind: Dict, close, pct_chg, volume) -> Dict:
        """
        【AI优化】风险扣分（0-30分）🔥 增加中等风险扣分
        
        7大风险因素：
        1. 高位风险（价格位置分级扣分）-最多12分
        2. 暴涨风险（近5天涨超10%）-6分
        3. 连续跌停（近10天有跌停）-8分
        4. 高波动风险（波动率>6%）-5分
        5. 破位风险（跌破MA60）-5分
        6. 缩量下跌（价跌量缩严重）-4分
        """
        penalty = 0
        reasons = []
        
        # 1. 高位风险 - 🔥 分级扣分，避免"劣币驱逐良币"
        price_pos = ind['price_position']
        if price_pos >= 0.80:
            penalty += 12
            reasons.append('极高位风险(-12分)')
        elif price_pos >= 0.70:
            penalty += 8
            reasons.append('高位风险(-8分)')
        elif price_pos >= 0.60:
            penalty += 5
            reasons.append('中高位风险(-5分)')
        elif price_pos >= 0.50:
            penalty += 3
            reasons.append('中位风险(-3分)')
        
        # 2. 暴涨风险
        price_chg_5d = ind['price_chg_5d']
        if price_chg_5d > 0.15:  # 5天涨超15%
            penalty += 6
            reasons.append('暴涨追高风险(-6分)')
        elif price_chg_5d > 0.10:  # 5天涨超10%
            penalty += 4
            reasons.append('涨幅较大(-4分)')
        
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
            reasons.append('高波动风险(-5分)')
        elif volatility > 0.06:
            penalty += 3
            reasons.append('波动较大(-3分)')
        
        # 5. 破位风险
        if close[-1] < ind['ma60']:
            penalty += 5
            reasons.append('跌破MA60(-5分)')
        
        # 6. 缩量下跌
        if ind['price_chg_5d'] < -0.03 and ind['vol_ratio'] < 0.7:
            penalty += 4
            reasons.append('缩量下跌(-4分)')
        
        return {
            'penalty': min(penalty, 30),  # 🔥 从25分提升到30分
            'reasons': reasons
        }
    
    def _recommend_stop_loss_v4(self, close, ind: Dict) -> Dict:
        """
        智能止损止盈推荐（v4.0修复版）
        
        🔥修复：止损价计算逻辑优化，确保合理性
        """
        # 🔥 重要修复：数据是按日期降序排列的，所以close[0]是最新价格，不是close[-1]
        current_price = close[0] if len(close) > 0 else close[-1]
        ma20 = ind['ma20']
        ma60 = ind['ma60']
        price_min_60 = ind['price_min_60']
        price_position = ind['price_position']
        
        # 🔥 修复：根据价格位置动态调整止损幅度
        if price_position < 0.3:  # 低位
            stop_loss_pct = -0.07  # -7%（宽松）
        elif price_position < 0.5:  # 中低位
            stop_loss_pct = -0.06  # -6%
        elif price_position < 0.7:  # 中位
            stop_loss_pct = -0.05  # -5%
        else:  # 高位
            stop_loss_pct = -0.04  # -4%（严格）
        
        # 计算止损价
        stop_loss_1 = current_price * (1 + stop_loss_pct)  # 根据价格位置
        stop_loss_2 = max(ma20, ma60) * 0.97  # 均线支撑-3%
        stop_loss_3 = price_min_60 * 1.02  # 60日最低价+2%
        
        # 🔥 取最高的止损位（最保守），但不能高于当前价-3%
        stop_loss = max(stop_loss_1, stop_loss_2, stop_loss_3)
        stop_loss = min(stop_loss, current_price * 0.97)  # 确保至少-3%
        stop_loss = max(stop_loss, current_price * 0.90)  # 确保不超过-10%
        
        actual_stop_loss_pct = (stop_loss - current_price) / current_price * 100
        
        # 🔥 修复：止盈价也根据价格位置动态调整
        if price_position < 0.3:  # 低位，空间大
            take_profit_pct = 0.10  # +10%
        elif price_position < 0.5:  # 中低位
            take_profit_pct = 0.08  # +8%
        elif price_position < 0.7:  # 中位
            take_profit_pct = 0.06  # +6%
        else:  # 高位，空间小
            take_profit_pct = 0.05  # +5%
        
        take_profit = current_price * (1 + take_profit_pct)
        
        return {
            'stop_loss': round(stop_loss, 2),
            'stop_loss_pct': round(actual_stop_loss_pct, 1),
            'method': f'价格位置{price_position*100:.0f}%动态止损',
            'take_profit': round(take_profit, 2),
            'take_profit_pct': round(take_profit_pct * 100, 1)
        }
    
    def _empty_result(self) -> Dict:
        """返回空结果"""
        return {
            'comprehensive_score': 0,
            'final_score': 0,
            'grade': 'D',
            'description': '数据不足或不符合标准',
            'dimension_scores': {},
            'base_score': 0,
            'synergy_bonus': 0,
            'combo_type': '无',
            'risk_penalty': 0,
            'risk_reasons': [],
            'stop_loss': 0,
            'stop_loss_method': '',
            'take_profit': 0,
            'price_position': 0,
            'vol_ratio': 0,
            'price_chg_5d': 0,
            'version': self.version,
            'success': False
        }


if __name__ == '__main__':
    print("""
    🚀 综合优选评分器 v4.0 - 潜伏为王版
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    核心理念：在启动前潜伏，而不是启动后追高！
    
    🔥 革命性升级：
    1. 新增【潜伏价值】维度（20分）
    2. 【底部特征】提权到20分
    3. 【启动确认】降权到5分
    4. 降低风险扣分（0-25分）
    5. 优化协同加分（8种黄金组合）
    
    💡 预期效果：
    - 平均收益：3-5%（vs v3.0的-0.26%）
    - 胜率：55-60%（vs v3.0的45.2%）
    - 夏普比率：1.0-1.5（vs v3.0的-0.64）
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)


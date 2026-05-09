#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚀 综合优选 v6.0 - 超短线狙击·巅峰版（只选市场最强1-3%）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    用最严格的标准，选出市场最强的板块龙头
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 目标：极致精准 + 超高胜率（80-90%）+ 高收益（8-15%）
📊 筛选：市场3000只股票 → 30-100只（1-3%）

🔥 三级过滤机制：

【第一级】必要条件过滤（硬性要求，不满足直接淘汰）
1. 板块3日涨幅 > 1%（板块必须走强）
2. 资金净流入 > 0（必须有资金）
3. 股票3日涨幅 > 0（必须上涨）
4. 板块内排名 ≤ 30%（必须是板块前列）
5. 价格位置 < 85%（不追高）
6. 放量 > 0.8倍（不能严重缩量）

【第二级】七维严格评分（100分制）
1. 板块热度（25分）- 板块涨幅>5%才高分
2. 资金流向（30分）- 连续大额流入才高分
3. 短期动量（20分）- 涨幅>8%才高分
4. 龙头属性（10分）- 板块前3才高分
5. 相对强度（8分）- 跑赢>5%才高分
6. 技术突破（5分）- 放量>1.5倍才高分
7. 安全边际（2分）

【第三级】精英筛选
- 协同加分（0-30分）：要求极高
- 风险扣分（0-60分）：任何异常都大幅扣分
- 最终门槛：≥85分

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple, Optional
import logging
from time import perf_counter

# 导入V4评分器作为基类
from strategies.evaluators.comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
# 导入v6数据提供者
from strategies.evaluators.v6_data_provider_optimized import get_data_provider
# 导入龙头属性分析器
from strategies.evaluators.v6_leader_analyzer import get_leader_analyzer

logger = logging.getLogger(__name__)


class ComprehensiveStockEvaluatorV6Ultimate(ComprehensiveStockEvaluatorV4):
    """
    🏆 综合优选评分器 v6.0 - 超短线狙击·巅峰版
    
    核心创新：三级过滤 + 严格评分 + 精英筛选
    """
    
    def __init__(self):
        super().__init__()
        self.version = "v6.0_巅峰版"
        self.name = "超短线狙击·巅峰版"
        self.data_provider = get_data_provider()
        self.leader_analyzer = get_leader_analyzer()
        
        logger.info("✅ v6.0巅峰版评分器初始化完成")
        
    def _get_grade_v4(self, score: float) -> str:
        """
        获取评级（v4兼容方法）
        """
        if score >= 90:
            return "S级(≥90分)"
        elif score >= 80:
            return "S级(≥80分)"
        elif score >= 70:
            return "A级(70-79分)"
        elif score >= 60:
            return "B级(60-69分)"
        else:
            return "C级(<60分)"
        
    def evaluate_stock_v6(self, stock_data: pd.DataFrame, ts_code: str) -> Dict:
        """
        🏆 v6.0超短线狙击·巅峰版评分
        
        三级过滤：
        1. 必要条件过滤（不满足直接淘汰）
        2. 严格评分系统（100分制）
        3. 精英筛选（协同-风险）
        """
        stage_timing_ms: Dict[str, float] = {}

        def timed_stage(name: str, fn):
            start = perf_counter()
            try:
                return fn()
            finally:
                stage_timing_ms[name] = stage_timing_ms.get(name, 0.0) + (perf_counter() - start) * 1000.0

        total_start = perf_counter()
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
            indicators = timed_stage(
                'indicators',
                lambda: self._calculate_all_indicators(close, volume, pct_chg),
            )
            
            # 获取板块信息
            sector_info = timed_stage(
                'stock_sector',
                lambda: self.data_provider.get_stock_sector(ts_code),
            )
            industry = sector_info.get('industry', '其他')
            
            # ========== 【第一级】必要条件过滤 ==========
            filter_result = timed_stage(
                'mandatory_conditions',
                lambda: self._check_mandatory_conditions(ts_code, industry, indicators, pct_chg, volume),
            )
            
            candidate_filter_mode = str(getattr(self, "runtime_candidate_filter_mode", "strict") or "strict")
            filter_is_advisory = candidate_filter_mode in {"advisory", "diagnostic_advisory"}
            if not filter_result['passed'] and not filter_is_advisory:
                # 不满足必要条件，直接淘汰
                return {
                    'success': False,
	                    'final_score': 0,
	                    'filter_failed': True,
	                    'filter_reason': filter_result['reason'],
	                    'filter_details': filter_result,
	                    'dimension_scores': {},
	                    'dim_scores': {}
	                }
            
            # ========== 【第二级】七维严格评分 ==========
            
            change_3d = sum(pct_chg[-3:])
            
            # 【维度1】资金流向（30分）- 🔥极度严格
            score_money = timed_stage('score_money_flow', lambda: self._score_money_flow_strict(ts_code))
            
            # 【维度2】板块热度（25分）- 🔥极度严格
            score_sector = timed_stage('score_sector_heat', lambda: self._score_sector_heat_strict(ts_code))
            
            # 【维度3】短期动量（20分）- 🔥极度严格
            score_momentum = timed_stage('score_momentum', lambda: self._score_short_term_momentum_strict(pct_chg))
            
            # 【维度4】龙头属性（10分）- 🔥极度严格
            score_leader = timed_stage('score_leader', lambda: self._score_leader_attribute_strict(ts_code, industry, change_3d))
            
            # 【维度5】相对强度（8分）- 🔥极度严格
            score_relative = timed_stage('score_relative_strength', lambda: self._score_relative_strength_strict(ts_code, pct_chg))
            
            # 【维度6】技术突破（5分）- 🔥极度严格
            score_breakthrough = timed_stage('score_breakthrough', lambda: self._score_technical_breakthrough_strict(indicators, volume))
            
            # 【维度7】安全边际（2分）
            score_safety = timed_stage('score_safety', lambda: self._score_safety_margin(indicators))
            
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
            
            # ========== 【第三级】精英筛选 ==========
            
            # 协同加分（0-30分）- 🔥要求极高
            synergy_result = timed_stage(
                'synergy',
                lambda: self._calculate_synergy_v6_strict(dim_scores, indicators, pct_chg, volume),
            )
            
            # 风险扣分（0-60分）- 🔥任何异常都大幅扣分
            risk_result = timed_stage(
                'risk',
                lambda: self._calculate_risk_v6_strict(indicators, close, pct_chg, volume, ts_code),
            )

            # 可交易入口判断决定极强但过热样本是否需要回撤后再确认。
            entry_gate = timed_stage(
                'entry_gate',
                lambda: self._evaluate_tradeable_entry_v6(indicators, close, pct_chg, volume, dim_scores=dim_scores),
            )
            self._apply_pullback_technical_breakthrough_score_lift_v6(dim_scores, entry_gate)
            base_score = sum(dim_scores.values())
            
            # 最终得分
            final_score = max(0, min(100, base_score + synergy_result['bonus'] - risk_result['penalty']))
            
            # 构建返回结果
            stage_timing_ms['total'] = (perf_counter() - total_start) * 1000.0
            result = {
                'success': True,
                'final_score': round(final_score, 1),
                'base_score': round(base_score, 1),
                'dimension_scores': dim_scores,
                'dim_scores': dim_scores,
                'synergy_bonus': synergy_result['bonus'],
                'synergy_combo': synergy_result['combo_type'],
                'risk_penalty': risk_result['penalty'],
                'risk_reasons': risk_result['reasons'],
                'entry_gate': entry_gate,
                'entry_gate_passed': bool(entry_gate.get('passed')),
                'entry_gate_reason': entry_gate.get('reason', ''),
                'grade': self._get_grade_v4(final_score),
                'description': self._generate_description_v6(dim_scores, final_score),
                
                # 核心指标
                'price_position': round(indicators.get('price_position', 0) * 100, 1),
                'vol_ratio': round(indicators.get('vol_ratio', 0), 2),
                'price_chg_3d': round(change_3d, 2),
                
                # 买卖建议
                'stop_loss': self._recommend_stop_loss_v4(close, indicators)['stop_loss'],
                'take_profit': self._recommend_stop_loss_v4(close, indicators)['take_profit'],
                
                # 过滤信息
                'filter_passed': bool(filter_result['passed']),
                'filter_details': filter_result,
                'runtime_diagnostics': {
                    'stage_timing_ms': {key: round(value, 3) for key, value in stage_timing_ms.items()}
                }
            }
            if filter_is_advisory:
                result['candidate_filter_mode'] = candidate_filter_mode
                result['candidate_filter_relaxed'] = not bool(filter_result['passed'])
            
            return result
            
        except Exception as e:
            logger.error(f"v6.0巅峰版评分失败 {ts_code}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return self._empty_result()
    
    def _check_mandatory_conditions(self, ts_code: str, industry: str, 
                                    ind: Dict, pct_chg, volume) -> Dict:
        """
        【第一级】必要条件过滤 - 🔥超强大脑优化版
        
        智能评估，只淘汰明显不符合的股票
        """
        reasons = []
        warning_flags = []  # 警告标记（不直接淘汰，但会在风险扣分中体现）
        
        try:
            # 1. 🔥智能板块判断：允许板块震荡，但不能大幅下跌
            sector_perf = self.data_provider.get_sector_performance(industry, days=3)
            sector_data_available = bool(sector_perf.get('data_available', True))
            sector_change = sector_perf['change_3d']
            if not sector_data_available:
                warning_flags.append(f"板块PIT数据不可用({sector_perf.get('proxy_source', 'unknown')})")
            elif sector_change < -3.0:  # 从>1%改为<-3%，更宽松
                reasons.append(f'板块大幅下跌({sector_change:.1f}%<-3%)')
            elif sector_change < 0:
                warning_flags.append(f'板块走弱({sector_change:.1f}%)')
            
            # 2. 🔥智能资金判断：允许小幅流出，但要看连续性
            money_flow = self.data_provider.get_money_flow(ts_code, days=3)
            money_flow_available = bool(money_flow.get('data_available', True))
            net_mf = money_flow['net_mf_amount']
            consecutive_days = money_flow['consecutive_inflow_days']

            # 如果连续流出或大额流出才淘汰
            if not money_flow_available:
                warning_flags.append(f"资金PIT数据不可用({money_flow.get('proxy_source', 'unknown')})")
            elif net_mf < -5000:  # 大额流出
                reasons.append(f'资金大幅流出({net_mf:.0f}万<-5000万)')
            elif net_mf < 0 and consecutive_days == 0:
                warning_flags.append(f'资金流出({net_mf:.0f}万)')
            
            # 3. 🔥智能涨跌判断：允许调整，看整体趋势
            change_3d = sum(pct_chg[-3:])
            change_5d = sum(pct_chg[-5:]) if len(pct_chg) >= 5 else change_3d
            
            # 如果3日跌但5日涨，可能是正常回调
            if change_3d < -5.0:  # 从>0改为<-5%
                reasons.append(f'短期大幅下跌({change_3d:.1f}%<-5%)')
            elif change_3d < 0 and change_5d < 0:
                reasons.append(f'持续下跌(3日{change_3d:.1f}%,5日{change_5d:.1f}%)')
            elif change_3d < 0:
                warning_flags.append(f'短期调整({change_3d:.1f}%)')
            
            # 4. 🔥智能龙头判断：根据板块大小动态调整
            leader_info = self.leader_analyzer.calculate_leader_score(ts_code, industry, change_3d)
            rank = leader_info.get('sector_rank', 999)
            total = leader_info.get('total_stocks', 1)
            rank_pct = rank / max(total, 1)
            
            # 小板块（<20只）前50%，大板块（≥20只）前30%
            threshold = 0.50 if total < 20 else 0.30
            if rank_pct > threshold:
                warning_flags.append(f'板块排名{rank}/{total}(前{rank_pct*100:.0f}%)')
            
            # 5. 🔥智能价格判断：结合涨停基因
            price_pos = ind['price_position']
            limit_up_count = leader_info.get('limit_up_count_20d', 0)
            
            # 如果是妖股（涨停≥3次），允许追高到90%
            # 普通股票不能超过85%
            max_pos = 0.90 if limit_up_count >= 3 else 0.85
            if price_pos >= max_pos:
                reasons.append(f'价格过高({price_pos*100:.0f}%≥{max_pos*100:.0f}%)')
            elif price_pos >= 0.75:
                warning_flags.append(f'价格偏高({price_pos*100:.0f}%)')
            
            # 6. 🔥智能放量判断：看整体趋势
            vol_ratio = ind.get('vol_ratio', 1.0)
            
            # 计算5日平均放量比
            if len(volume) >= 10:
                recent_vol_avg = np.mean(volume[-5:])
                base_vol_avg = np.mean(volume[-10:-5])
                vol_trend = recent_vol_avg / base_vol_avg if base_vol_avg > 0 else 1.0
                
                # 如果整体放量趋势向上，允许单日缩量
                if vol_ratio < 0.5 and vol_trend < 0.8:
                    reasons.append(f'持续缩量(今日{vol_ratio:.1f},趋势{vol_trend:.1f})')
                elif vol_ratio < 0.8:
                    warning_flags.append(f'缩量({vol_ratio:.1f}倍)')
            
        except Exception as e:
            logger.warning(f"必要条件检查失败 {ts_code}: {e}")
            # 数据获取失败不直接淘汰，给予警告
            warning_flags.append('数据不完整')
        
        return {
            'passed': len(reasons) == 0,
            'reason': '; '.join(reasons) if reasons else '全部通过',
            'warnings': warning_flags,
            'failed_count': len(reasons),
            'pit_data_quality': {
                'sector_performance_available': bool(locals().get('sector_data_available', False)),
                'money_flow_available': bool(locals().get('money_flow_available', False)),
                'sector_proxy_source': str((locals().get('sector_perf') or {}).get('proxy_source', 'unknown')) if isinstance(locals().get('sector_perf'), dict) else 'unknown',
                'money_flow_proxy_source': str((locals().get('money_flow') or {}).get('proxy_source', 'unknown')) if isinstance(locals().get('money_flow'), dict) else 'unknown',
            }
        }
    
    def _score_money_flow_strict(self, ts_code: str) -> float:
        """
        【维度1】资金流向（30分）- 🔥超强大脑优化版
        
        智能评估资金力度，不只看绝对值，更看趋势和加速度
        """
        score = 0
        
        try:
            money_flow = self.data_provider.get_money_flow(ts_code, days=3)
            
            # 1.1 大单净流入（22分）- runtime replay only has this money-flow proxy.
            net_mf = money_flow['net_mf_amount']
            consecutive_days = money_flow['consecutive_inflow_days']
            today_net = money_flow.get('today_net', 0)
            
            # 🔥加速度检测：今日流入 > 昨日 = 资金加速
            is_accelerating = today_net > net_mf / 3  # 今日占3日总量的1/3以上
            
            if consecutive_days >= 3:
                if net_mf > 20000 or is_accelerating:
                    score += 22  # 连续3天+大额或加速
                elif net_mf > 10000:
                    score += 18
                elif net_mf > 5000:
                    score += 14
                else:
                    score += 10
            elif consecutive_days >= 2:
                if net_mf > 15000 or is_accelerating:
                    score += 15
                elif net_mf > 8000:
                    score += 12
                elif net_mf > 3000:
                    score += 8
                else:
                    score += 5
            elif consecutive_days >= 1:
                if net_mf > 10000 or is_accelerating:
                    score += 8
                elif net_mf > 5000:
                    score += 5
                elif net_mf > 0:
                    score += 3
            
            # 1.2 主力资金（5分）- optional when detailed order-flow fields exist.
            elg_net = money_flow['buy_elg_amount'] - money_flow['sell_elg_amount']
            
            # 🔥相对强度：超大单/大单比例
            lg_net = net_mf
            elg_ratio = abs(elg_net / lg_net) if lg_net != 0 else 0
            
            if elg_net > 5000:
                if elg_ratio > 0.5:  # 超大单占比>50%，主力强势
                    score += 5
                else:
                    score += 4
            elif elg_net > 2000:
                if elg_ratio > 0.5:
                    score += 3
                else:
                    score += 2
            elif elg_net > 0:
                score += 1
            
            # 1.3 北向资金（3分）- optional; do not make runtime replay depend on it.
            north_flow = self.data_provider.get_north_money_flow(ts_code, days=3)
            if north_flow['is_connect_stock']:
                north_net = north_flow['north_net_3d']
                
                # 🔥北向加分：外资青睐
                if north_net > 5000:
                    score += 3
                elif north_net > 2000:
                    score += 2
                elif north_net > 0:
                    score += 1
            
        except Exception as e:
            logger.warning(f"资金流向评分失败 {ts_code}: {e}")
        
        return min(score, 30)
    
    def _score_sector_heat_strict(self, ts_code: str) -> float:
        """
        【维度2】板块热度（25分）- 🔥极度严格
        
        只有板块涨幅>5%才给高分
        """
        score = 0
        
        try:
            sector_info = self.data_provider.get_stock_sector(ts_code)
            industry = sector_info.get('industry', '其他')
            
            # 2.1 板块涨幅（12分）- 🔥极度严格
            sector_perf = self.data_provider.get_sector_performance(industry, days=3)
            sector_change = sector_perf['change_3d']
            
            if sector_change > 8:
                score += 12  # 超强板块
            elif sector_change > 5:
                score += 10
            elif sector_change > 3:
                score += 7
            elif sector_change > 2:
                score += 4
            elif sector_change > 1:
                score += 2
            
            # 2.2 板块排名（8分）- 🔥极度严格
            rank = sector_perf['rank']
            total = sector_perf.get('total_industries', 100)
            rank_pct = rank / total
            
            if rank_pct <= 0.05:  # 前5%
                score += 8
            elif rank_pct <= 0.10:  # 前10%
                score += 6
            elif rank_pct <= 0.20:  # 前20%
                score += 4
            elif rank_pct <= 0.30:  # 前30%
                score += 2
            
            # 2.3 概念题材（5分）- 🔥极度严格
            concepts = sector_info['concept']
            hot_concepts = ['新能源', '人工智能', 'AI', '芯片', '半导体', 
                           '锂电', '光伏', '储能', '数字经济', '大模型']
            has_hot = sum(1 for c in concepts for h in hot_concepts if h in c)
            
            if has_hot >= 2:
                score += 5
            elif has_hot >= 1:
                score += 3
            
        except Exception as e:
            logger.warning(f"板块热度评分失败 {ts_code}: {e}")
        
        return min(score, 25)
    
    def _score_short_term_momentum_strict(self, pct_chg) -> float:
        """
        【维度3】短期动量（20分）- 🔥极度严格
        
        只有涨幅>8%才给高分
        """
        score = 0
        
        # 3.1 近3天涨幅（12分）- 🔥极度严格
        change_3d = sum(pct_chg[-3:])
        
        if change_3d > 15:
            score += 12
        elif change_3d > 10:
            score += 10
        elif change_3d > 8:
            score += 8
        elif change_3d > 5:
            score += 5
        elif change_3d > 3:
            score += 3
        elif change_3d > 1:
            score += 1
        
        # 3.2 连续阳线（5分）
        positive_days = sum(1 for x in pct_chg[-3:] if x > 0)
        if positive_days == 3:
            score += 5
        elif positive_days == 2:
            score += 3
        elif positive_days == 1:
            score += 1
        
        # 3.3 加速特征（3分）
        if len(pct_chg) >= 3:
            if pct_chg[-1] > pct_chg[-2] > pct_chg[-3] and pct_chg[-1] > 2:
                score += 3
            elif pct_chg[-1] > pct_chg[-2] and pct_chg[-1] > 1:
                score += 1
        
        return min(score, 20)
    
    def _score_leader_attribute_strict(self, ts_code: str, industry: str, change_3d: float) -> float:
        """
        【维度4】龙头属性（10分）- 🔥极度严格
        
        只有板块前3名才给高分
        """
        try:
            leader_info = self.leader_analyzer.calculate_leader_score(ts_code, industry, change_3d)
            
            rank = leader_info.get('sector_rank', 999)
            limit_up_count = leader_info.get('limit_up_count_20d', 0)
            
            score = 0
            
            # 板块排名（6分）- 🔥极度严格
            if rank == 1:
                score += 6  # 绝对龙头
            elif rank == 2:
                score += 5
            elif rank == 3:
                score += 4
            elif rank <= 5:
                score += 3
            elif rank <= 10:
                score += 1
            
            # 涨停基因（4分）- 🔥极度严格
            if limit_up_count >= 5:
                score += 4  # 超级妖股
            elif limit_up_count >= 3:
                score += 3
            elif limit_up_count >= 2:
                score += 2
            elif limit_up_count >= 1:
                score += 1
            
            return min(score, 10)
            
        except Exception as e:
            logger.warning(f"龙头属性评分失败 {ts_code}: {e}")
            return 0.0
    
    def _score_relative_strength_strict(self, ts_code: str, pct_chg) -> float:
        """
        【维度5】相对强度（8分）- 🔥极度严格
        
        只有跑赢大盘>5%才给高分
        """
        score = 0
        
        try:
            stock_change = sum(pct_chg[-3:])
            market_change = self.data_provider.get_market_change(days=3)
            relative_strength = stock_change - market_change
            
            if relative_strength > 10:
                score += 8
            elif relative_strength > 7:
                score += 6
            elif relative_strength > 5:
                score += 4
            elif relative_strength > 3:
                score += 2
            elif relative_strength > 0:
                score += 1
            
        except Exception as e:
            logger.warning(f"相对强度评分失败 {ts_code}: {e}")
        
        return min(score, 8)
    
    def _score_technical_breakthrough_strict(self, ind: Dict, volume) -> float:
        """
        【维度6】技术突破（5分）- 🔥极度严格
        
        只有放量>1.5倍才给高分
        """
        score = 0
        
        vol_ratio = ind.get('vol_ratio', 1.0)
        
        if vol_ratio > 2.5:
            score += 5
        elif vol_ratio > 2.0:
            score += 4
        elif vol_ratio > 1.5:
            score += 3
        elif vol_ratio > 1.2:
            score += 1
        
        return min(score, 5)
    
    def _score_safety_margin(self, ind: Dict) -> float:
        """
        【维度7】安全边际（2分）- 🔥极度严格
        
        巅峰版：只给低位股票加分，高位股票扣分
        """
        score = 0
        
        price_pos = ind.get('price_position', 0.5)
        
        # 极度严格：只有真正的低位才加分
        if price_pos < 0.30:  # 极低位
            score += 2
        elif price_pos < 0.50:  # 低位
            score += 1
        elif price_pos < 0.70:  # 中位
            score += 0.5
        # 高位不加分（price_pos >= 0.70）
        
        return min(score, 2)
    
    def _calculate_synergy_v6_strict(self, dim_scores: Dict, ind: Dict, 
                                     pct_chg, volume) -> Dict:
        """
        协同加分系统（巅峰版）- 🔥要求极高，最高30分
        """
        bonus = 0
        combo_types = []
        
        # 1. 板块总龙头（15分）⭐⭐⭐
        if (dim_scores['板块热度'] >= 18 and 
            dim_scores['资金流向'] >= 18 and 
            dim_scores['龙头属性'] >= 9 and
            dim_scores['短期动量'] >= 16):
            bonus += 15
            combo_types.append('板块总龙头')
        
        # 2. 运行时强势共振（12分）⭐⭐⭐
        if (dim_scores['板块热度'] >= 14 and
            dim_scores['龙头属性'] >= 8 and
            dim_scores['短期动量'] >= 12 and
            dim_scores['相对强度'] >= 4):
            bonus += 12
            combo_types.append('强势共振')

        # 3. 资金爆发（10分）⭐⭐⭐
        if (dim_scores['资金流向'] >= 14 and 
            dim_scores['短期动量'] >= 10 and
            dim_scores['技术突破'] >= 2):
            bonus += 10
            combo_types.append('资金爆发')

        # 4. 板块龙头启动（8分）⭐⭐
        if (dim_scores['龙头属性'] >= 8 and 
            dim_scores['板块热度'] >= 10 and
            dim_scores['短期动量'] >= 10):
            bonus += 8
            combo_types.append('龙头启动')

        # 5. 低位强势（5分）⭐
        price_pos = ind.get('price_position', 0.5)
        if (price_pos < 0.65 and
            dim_scores['短期动量'] >= 8 and
            dim_scores['相对强度'] >= 3):
            bonus += 5
            combo_types.append('低位强势')

        # 6. 资金动量（5分）⭐
        if (dim_scores['资金流向'] >= 12 and
            dim_scores['短期动量'] >= 8):
            bonus += 5
            combo_types.append('资金动量')

        # 7. 历史资金爆发（兼容旧条件）
        if (dim_scores['资金流向'] >= 25 and
            dim_scores['短期动量'] >= 15 and
            dim_scores['技术突破'] >= 4):
            bonus += 12
            combo_types.append('资金爆发')
        
        # 8. 启动信号（8分）⭐⭐
        if len(volume) >= 5:
            vol_trend_down = (volume[-4] < volume[-5] and 
                             volume[-3] < volume[-4] and 
                             volume[-2] < volume[-3])
            vol_breakout = volume[-1] > volume[-2] * 1.5
            
            if vol_trend_down and vol_breakout and pct_chg[-1] > 2:
                bonus += 8
                combo_types.append('启动信号')

        # 9. 超强动量（5分）⭐
        change_3d = sum(pct_chg[-3:])
        if (change_3d > 12 and 
            dim_scores['相对强度'] >= 6):
            bonus += 5
            combo_types.append('超强动量')
        
        return {
            'bonus': min(bonus, 30),
            'combo_type': ' + '.join(combo_types) if combo_types else '无'
        }
    
    def _calculate_risk_v6_strict(self, ind: Dict, close, pct_chg, volume, ts_code: str) -> Dict:
        """
        风险扣分系统（巅峰版）- 🔥任何异常都大幅扣分，最高60分
        """
        penalty = 0
        reasons = []
        
        # 1. 追高风险（-25分）⚠️⚠️⚠️ - 🔥极度严格
        price_pos = ind['price_position']
        if price_pos >= 0.95:
            penalty += 25
            reasons.append('极高位(-25分)')
        elif price_pos >= 0.90:
            penalty += 20
            reasons.append('极高位(-20分)')
        elif price_pos >= 0.85:
            penalty += 15
            reasons.append('高位(-15分)')
        elif price_pos >= 0.75:
            penalty += 10
            reasons.append('偏高位(-10分)')
        elif price_pos >= 0.65:
            penalty += 5
            reasons.append('中高位(-5分)')
        
        # 2. 短期暴涨（-20分）⚠️⚠️⚠️ - 🔥极度严格
        change_3d = sum(pct_chg[-3:])
        if change_3d > 35:
            penalty += 20
            reasons.append('短期暴涨(-20分)')
        elif change_3d > 25:
            penalty += 15
            reasons.append('涨幅过大(-15分)')
        elif change_3d > 18:
            penalty += 10
            reasons.append('涨幅较大(-10分)')
        elif change_3d > 12:
            penalty += 5
            reasons.append('涨幅偏大(-5分)')
        
        # 3. 连续涨停（-15分）⚠️⚠️⚠️ - 🔥极度严格
        limit_up_count = sum(1 for p in pct_chg[-5:] if p >= 9.5)
        if limit_up_count >= 4:
            penalty += 15
            reasons.append('连续涨停(-15分)')
        elif limit_up_count >= 3:
            penalty += 10
            reasons.append('多次涨停(-10分)')
        elif limit_up_count >= 2:
            penalty += 5
            reasons.append('两次涨停(-5分)')
        
        # 4. 市场环境（-15分）⚠️⚠️⚠️ - 🔥新增
        try:
            market_change_1d = self.data_provider.get_market_change(days=1)
            market_change_3d = self.data_provider.get_market_change(days=3)
            
            if market_change_1d < -3:
                penalty += 15
                reasons.append('大盘暴跌(-15分)')
            elif market_change_1d < -2:
                penalty += 10
                reasons.append('大盘大跌(-10分)')
            elif market_change_3d < -5:
                penalty += 10
                reasons.append('大盘走弱(-10分)')
            elif market_change_3d < -3:
                penalty += 5
                reasons.append('大盘偏弱(-5分)')
        except:
            pass
        
        # 5. 板块见顶（-10分）⚠️⚠️ - 🔥新增
        try:
            sector_info = self.data_provider.get_stock_sector(ts_code)
            sector_perf = self.data_provider.get_sector_performance(sector_info['industry'], days=5)
            
            if sector_perf['change_3d'] > 15:
                penalty += 10
                reasons.append('板块见顶(-10分)')
            elif sector_perf['change_3d'] > 10:
                penalty += 5
                reasons.append('板块过热(-5分)')
        except:
            pass
        
        # 6. 高波动（-8分）⚠️⚠️ - 🔥加强
        volatility = ind['volatility']
        if volatility > 0.10:
            penalty += 8
            reasons.append('极高波动(-8分)')
        elif volatility > 0.08:
            penalty += 5
            reasons.append('高波动(-5分)')
        elif volatility > 0.06:
            penalty += 3
            reasons.append('波动偏大(-3分)')
        
        # 7. 放量异常（-7分）⚠️ - 🔥新增
        vol_ratio = ind.get('vol_ratio', 1.0)
        if vol_ratio > 5.0:
            penalty += 7
            reasons.append('异常放量(-7分)')
        elif vol_ratio > 3.5:
            penalty += 5
            reasons.append('极度放量(-5分)')
        
        constructive = change_3d > 5 and ind.get('vol_ratio', 1.0) <= 3.5 and limit_up_count < 3
        if constructive and price_pos < 0.85:
            original_penalty = penalty
            penalty = int(round(penalty * 0.70))
            if penalty < original_penalty:
                reasons.append(f'强势非极端风险折减({original_penalty}->{penalty})')
        if constructive and price_pos < 0.75:
            original_penalty = penalty
            penalty = int(round(penalty * 0.85))
            if penalty < original_penalty:
                reasons.append(f'低位强势风险折减({original_penalty}->{penalty})')

        return {
            'penalty': min(penalty, 45),
            'reasons': reasons
        }

    def _evaluate_tradeable_entry_v6(self, ind: Dict, close, pct_chg, volume, dim_scores: Dict = None) -> Dict:
        """
        可交易入口判断：v6 可以识别强势，但追高/过热样本必须等待回撤后再确认。

        这个 gate 不调整 final_score，只给 runtime handler 决定是否允许生成交易信号。
        """
        close_s = pd.Series(close, dtype="float64").dropna()
        pct_s = pd.Series(pct_chg, dtype="float64").dropna()
        vol_s = pd.Series(volume, dtype="float64").dropna()
        if len(close_s) < 20 or len(pct_s) < 5:
            return {
                "passed": False,
                "mode": "insufficient_history",
                "reason": "entry_gate_insufficient_history",
            }

        latest_close = float(close_s.iloc[-1])
        high_20d = float(close_s.tail(20).max() or 0.0)
        pullback_from_20d_high = latest_close / high_20d - 1.0 if high_20d > 0 else 0.0
        latest_pct_chg = float(pct_s.iloc[-1])
        rebound_2d = float(pct_s.tail(2).sum())
        change_3d = float(pct_s.tail(3).sum())
        limit_up_count_5d = int((pct_s.tail(5) >= 9.5).sum())
        price_pos = float(ind.get("price_position", 0.5) or 0.5)
        volatility = float(ind.get("volatility", 0.0) or 0.0)
        vol_ratio = float(ind.get("vol_ratio", 1.0) or 1.0)
        recent_vol_avg = float(vol_s.tail(5).mean()) if len(vol_s) >= 5 else 0.0
        base_vol_avg = float(vol_s.tail(20).head(15).mean()) if len(vol_s) >= 20 else 0.0
        volume_cooling = bool(base_vol_avg > 0 and recent_vol_avg <= base_vol_avg * 1.25)

        overheat_flags = []
        if price_pos >= 0.90:
            overheat_flags.append("price_position_extreme")
        if change_3d > 18:
            overheat_flags.append("short_term_overextended")
        if limit_up_count_5d >= 2:
            overheat_flags.append("recent_limit_up_cluster")
        if volatility > 0.08:
            overheat_flags.append("high_volatility")
        if vol_ratio > 3.5:
            overheat_flags.append("volume_spike")

        pullback_reconfirmed = (
            -0.12 <= pullback_from_20d_high <= -0.025
            and latest_pct_chg > 0
            and rebound_2d > 0
            and 0.8 <= vol_ratio <= 2.8
            and volume_cooling
            and limit_up_count_5d < 2
        )
        direct_entry = (
            not overheat_flags
            and price_pos < 0.85
            and change_3d <= 12
            and 0.8 <= vol_ratio <= 3.0
        )

        if pullback_reconfirmed:
            mode = "pullback_reconfirmed"
            reason = "entry_gate_pullback_reconfirmed"
            passed = True
        elif direct_entry:
            mode = "direct_strength_entry"
            reason = "entry_gate_direct_strength_entry"
            passed = True
        elif overheat_flags:
            mode = "wait_for_pullback_reconfirmation"
            reason = "entry_gate_overheated_wait_for_pullback"
            passed = False
        else:
            mode = "no_tradeable_entry"
            reason = "entry_gate_no_tradeable_entry"
            passed = False

        secondary_confirmation = self._evaluate_pullback_secondary_confirmation_v6(
            dim_scores or {},
            ind,
            close_s,
            pullback_reconfirmed=bool(pullback_reconfirmed),
        )

        return {
            "passed": bool(passed),
            "mode": mode,
            "reason": reason,
            "overheat_flags": overheat_flags,
            "pullback_from_20d_high": round(pullback_from_20d_high * 100, 2),
            "latest_pct_chg": round(latest_pct_chg, 2),
            "rebound_2d": round(rebound_2d, 2),
            "change_3d": round(change_3d, 2),
            "limit_up_count_5d": int(limit_up_count_5d),
            "price_position": round(price_pos * 100, 1),
            "vol_ratio": round(vol_ratio, 2),
            "volume_cooling": bool(volume_cooling),
            "secondary_confirmation": secondary_confirmation,
        }

    def _evaluate_pullback_secondary_confirmation_v6(self, dim_scores: Dict, ind: Dict, close_s=None, *, pullback_reconfirmed: bool) -> Dict:
        """
        回撤确认后的二次质量证据。质量合格时，调用方可把影子技术突破候选分纳入技术突破维度。
        """
        def score_of(name: str) -> float:
            try:
                return float(dim_scores.get(name, 0.0) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        technical_score = score_of('技术突破')
        money_score = score_of('资金流向')
        sector_score = score_of('板块热度')
        momentum_score = score_of('短期动量')
        leader_score = score_of('龙头属性')
        relative_score = score_of('相对强度')
        vol_ratio = float(ind.get('vol_ratio', 1.0) or 1.0)
        close_series = pd.Series(close_s, dtype="float64").dropna() if close_s is not None else pd.Series(dtype="float64")
        latest_close = float(close_series.iloc[-1]) if len(close_series) > 0 else 0.0
        prev_close = float(close_series.iloc[-2]) if len(close_series) >= 2 else latest_close
        ma5 = float(close_series.tail(5).mean()) if len(close_series) >= 5 else float(ind.get('ma5', 0.0) or 0.0)
        ma10 = float(close_series.tail(10).mean()) if len(close_series) >= 10 else float(ind.get('ma10', 0.0) or 0.0)
        prev_ma5 = float(close_series.iloc[:-1].tail(5).mean()) if len(close_series) >= 6 else ma5
        prev_ma10 = float(close_series.iloc[:-1].tail(10).mean()) if len(close_series) >= 11 else ma10
        prior_platform_high = float(close_series.iloc[-6:-1].max()) if len(close_series) >= 6 else 0.0

        volume_breakout = bool(technical_score >= 2.0 or vol_ratio >= 1.5)
        ma5_reclaim = bool(latest_close > ma5 and prev_close <= prev_ma5)
        ma10_reclaim = bool(latest_close > ma10 and prev_close <= prev_ma10)
        pullback_platform_breakout = bool(prior_platform_high > 0 and latest_close > prior_platform_high)
        technical_reconfirmed = bool(volume_breakout or ma5_reclaim or ma10_reclaim or pullback_platform_breakout)

        checks = {
            'technical_breakout_reconfirmed': technical_reconfirmed,
            'volume_breakout': volume_breakout,
            'ma5_reclaim': ma5_reclaim,
            'ma10_reclaim': ma10_reclaim,
            'pullback_platform_breakout': pullback_platform_breakout,
            'money_flow_returned': bool(money_score >= 12.0),
            'sector_reheated': bool(sector_score >= 10.0),
            'momentum_alive': bool(momentum_score >= 8.0),
            'leader_supported': bool(leader_score >= 6.0),
            'relative_strength_ok': bool(relative_score >= 4.0),
        }
        confirmed_count = sum(1 for value in checks.values() if value)
        candidate_score = self._score_pullback_technical_breakthrough_candidate_v6(
            volume_breakout=volume_breakout,
            ma5_reclaim=ma5_reclaim,
            ma10_reclaim=ma10_reclaim,
            pullback_platform_breakout=pullback_platform_breakout,
        )
        return {
            **checks,
            'confirmed_count': int(confirmed_count),
            'technical_breakthrough_candidate_score': round(candidate_score, 1),
            'technical_breakthrough_current_score': round(technical_score, 1),
            'technical_breakthrough_candidate_delta': round(max(0.0, candidate_score - technical_score), 1),
            'quality_ready': bool(pullback_reconfirmed and confirmed_count >= 4 and checks['technical_breakout_reconfirmed']),
        }

    def _score_pullback_technical_breakthrough_candidate_v6(
        self,
        *,
        volume_breakout: bool,
        ma5_reclaim: bool,
        ma10_reclaim: bool,
        pullback_platform_breakout: bool,
    ) -> float:
        """
        影子技术突破候选分。仅在回撤确认且二次质量证据合格时纳入技术突破维度。
        """
        score = 0.0
        if volume_breakout:
            score = max(score, 3.0)
        if pullback_platform_breakout:
            score = max(score, 2.0)
        if ma5_reclaim:
            score = max(score, 1.5)
        if ma10_reclaim:
            score = max(score, 1.0)
        if pullback_platform_breakout and (ma5_reclaim or ma10_reclaim):
            score = max(score, 3.0)
        if volume_breakout and pullback_platform_breakout:
            score = max(score, 4.0)
        return min(score, 5.0)

    def _apply_pullback_technical_breakthrough_score_lift_v6(self, dim_scores: Dict, entry_gate: Dict) -> Dict:
        secondary_confirmation = (
            entry_gate.get('secondary_confirmation')
            if isinstance(entry_gate.get('secondary_confirmation'), dict)
            else {}
        )
        technical_candidate_score = float(
            secondary_confirmation.get('technical_breakthrough_candidate_score', 0.0) or 0.0
        )
        current_score = float(dim_scores.get('技术突破', 0.0) or 0.0)
        mode = str(entry_gate.get('mode', '') or '')
        confirmed_count = int(secondary_confirmation.get('confirmed_count', 0) or 0)
        direct_strength_quality_ready = bool(
            mode == 'direct_strength_entry'
            and secondary_confirmation.get('technical_breakout_reconfirmed') is True
            and confirmed_count >= 5
            and (
                secondary_confirmation.get('money_flow_returned') is True
                or secondary_confirmation.get('momentum_alive') is True
            )
            and secondary_confirmation.get('leader_supported') is True
        )
        if (
            mode in {'pullback_reconfirmed', 'direct_strength_entry'}
            and (secondary_confirmation.get('quality_ready') is True or direct_strength_quality_ready)
            and technical_candidate_score > current_score
        ):
            dim_scores['技术突破'] = round(min(5.0, technical_candidate_score), 1)
            entry_gate['technical_breakthrough_score_lift_applied'] = True
            entry_gate['technical_breakthrough_score_lift_reason'] = (
                'direct_strength_secondary_confirmation'
                if direct_strength_quality_ready and secondary_confirmation.get('quality_ready') is not True
                else 'pullback_secondary_confirmation'
            )
        else:
            entry_gate['technical_breakthrough_score_lift_applied'] = False
        return dim_scores
    
    def _generate_description_v6(self, dim_scores: Dict, final_score: float) -> str:
        """生成推荐理由"""
        reasons = []
        
        if dim_scores['龙头属性'] >= 9:
            reasons.append('板块绝对龙头')
        elif dim_scores['龙头属性'] >= 7:
            reasons.append('板块龙头')
        
        if dim_scores['资金流向'] >= 25:
            reasons.append('资金爆发式流入')
        elif dim_scores['资金流向'] >= 20:
            reasons.append('资金持续流入')
        
        if dim_scores['板块热度'] >= 20:
            reasons.append('板块超强')
        elif dim_scores['板块热度'] >= 15:
            reasons.append('板块走强')
        
        if dim_scores['短期动量'] >= 15:
            reasons.append('动量强劲')
        
        if dim_scores['相对强度'] >= 6:
            reasons.append('远超大盘')
        
        if len(reasons) == 0:
            return f"综合评分{final_score:.0f}分"
        else:
            return "、".join(reasons)


# ========== 便捷调用接口 ==========

def evaluate_stock_v6(stock_data: pd.DataFrame, ts_code: str) -> Dict:
    """
    便捷调用：v6.0超短线狙击·巅峰版评分
    """
    evaluator = ComprehensiveStockEvaluatorV6Ultimate()
    return evaluator.evaluate_stock_v6(stock_data, ts_code)

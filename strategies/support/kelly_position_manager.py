#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎰 凯利公式仓位管理器
Kelly Criterion Position Manager

借鉴：Warren Buffett + Ed Thorp
核心思想：根据胜率和赔率计算数学上最优仓位

凯利公式：f* = (bp - q) / b
其中：
- f*: 最优仓位比例
- b: 盈亏比（平均盈利/平均亏损）
- p: 胜率
- q: 败率 (1-p)
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


class KellyPositionManager:
    """凯利公式仓位管理器"""
    
    def __init__(self, max_single_position: float = 0.25,
                 max_total_position: float = 0.80,
                 kelly_fraction: float = 0.5):
        """
        初始化
        
        Args:
            max_single_position: 单只最大仓位（默认25%）
            max_total_position: 总仓位上限（默认80%，保留20%现金）
            kelly_fraction: 凯利分数（默认0.5，即半凯利）
                          - 1.0 = 完全凯利（激进）
                          - 0.5 = 半凯利（稳健，推荐）
                          - 0.25 = 1/4凯利（保守）
        """
        self.max_single_position = max_single_position
        self.max_total_position = max_total_position
        self.kelly_fraction = kelly_fraction
        
        logger.info(f"凯利仓位管理器初始化: "
                   f"单只上限{max_single_position*100}%, "
                   f"总仓位上限{max_total_position*100}%, "
                   f"凯利分数{kelly_fraction}")
    
    def calculate_kelly_position(self, win_rate: float, 
                                 profit_loss_ratio: float) -> Dict:
        """
        计算凯利公式最优仓位
        
        Args:
            win_rate: 胜率（0-1）
            profit_loss_ratio: 盈亏比（平均盈利/平均亏损）
        
        Returns:
            {'kelly_pct': 凯利比例, 'recommended_pct': 实际建议比例}
        """
        # 凯利公式：f* = (bp - q) / b
        p = win_rate
        q = 1 - p
        b = profit_loss_ratio
        
        if b <= 0:
            return {'kelly_pct': 0, 'recommended_pct': 0, 'reason': '盈亏比无效'}
        
        kelly_pct = (b * p - q) / b
        
        # 应用凯利分数（降低激进度）
        adjusted_kelly = kelly_pct * self.kelly_fraction
        
        # 限制在合理范围
        if adjusted_kelly < 0:
            recommended_pct = 0
            reason = '负凯利，不建议配置'
        elif adjusted_kelly > self.max_single_position:
            recommended_pct = self.max_single_position
            reason = f'凯利建议{adjusted_kelly*100:.1f}%，但限制单只上限{self.max_single_position*100}%'
        else:
            recommended_pct = adjusted_kelly
            reason = '按凯利公式配置'
        
        return {
            'kelly_pct': round(kelly_pct, 4),
            'adjusted_kelly_pct': round(adjusted_kelly, 4),
            'recommended_pct': round(recommended_pct, 4),
            'reason': reason
        }
    
    def calculate_score_based_position(self, score: float, 
                                       star_rating: int) -> Dict:
        """
        基于评分和星级计算仓位
        
        Args:
            score: 评分（0-100）
            star_rating: 星级（1-5）
        
        Returns:
            建议仓位
        """
        # 基础仓位映射
        base_positions = {
            5: 0.25,  # ⭐⭐⭐⭐⭐
            4: 0.20,  # ⭐⭐⭐⭐
            3: 0.15,  # ⭐⭐⭐
            2: 0.10,  # ⭐⭐
            1: 0.05   # ⭐
        }
        
        base_pct = base_positions.get(star_rating, 0.10)
        
        # 根据具体分数微调
        if score >= 95:
            adjustment = 1.1  # +10%
        elif score >= 85:
            adjustment = 1.05  # +5%
        elif score < 65:
            adjustment = 0.9  # -10%
        else:
            adjustment = 1.0
        
        recommended_pct = min(base_pct * adjustment, self.max_single_position)
        
        return {
            'base_pct': base_pct,
            'adjustment': adjustment,
            'recommended_pct': round(recommended_pct, 4),
            'max_allowed': self.max_single_position
        }
    
    def optimize_portfolio_allocation(self, signals: List[Dict]) -> Dict:
        """
        优化整个组合的仓位分配
        
        Args:
            signals: 信号列表，每个信号包含:
                    {
                        'ts_code': 股票代码,
                        'score': 评分,
                        'star_rating': 星级,
                        'win_rate': 历史胜率（可选）,
                        'profit_loss_ratio': 盈亏比（可选）
                    }
        
        Returns:
            优化后的仓位分配方案
        """
        if not signals:
            return {'allocations': [], 'total_position': 0}
        
        allocations = []
        total_position = 0
        
        # 按评分排序（优先配置高分）
        sorted_signals = sorted(signals, key=lambda x: x['score'], reverse=True)
        
        for signal in sorted_signals:
            # 检查总仓位是否已满
            if total_position >= self.max_total_position:
                allocations.append({
                    **signal,
                    'position_pct': 0,
                    'reason': '总仓位已满'
                })
                continue
            
            # 计算建议仓位
            if 'win_rate' in signal and 'profit_loss_ratio' in signal:
                # 使用凯利公式
                kelly_result = self.calculate_kelly_position(
                    signal['win_rate'],
                    signal['profit_loss_ratio']
                )
                recommended_pct = kelly_result['recommended_pct']
                method = 'kelly'
            else:
                # 使用评分方法
                score_result = self.calculate_score_based_position(
                    signal['score'],
                    signal['star_rating']
                )
                recommended_pct = score_result['recommended_pct']
                method = 'score'
            
            # 确保不超过剩余可用仓位
            available_position = self.max_total_position - total_position
            final_pct = min(recommended_pct, available_position)
            
            allocations.append({
                **signal,
                'position_pct': round(final_pct, 4),
                'method': method,
                'reason': f'{"凯利公式" if method == "kelly" else "评分分配"}'
            })
            
            total_position += final_pct
        
        return {
            'allocations': allocations,
            'total_position': round(total_position, 4),
            'cash_reserve': round(1.0 - total_position, 4),
            'max_total': self.max_total_position,
            'summary': {
                'total_signals': len(signals),
                'allocated_signals': len([a for a in allocations if a['position_pct'] > 0]),
                'avg_position': round(total_position / len(signals), 4) if signals else 0
            }
        }
    
    def calculate_historical_kelly_params(self, backtest_results: pd.DataFrame) -> Dict:
        """
        根据回测结果计算凯利参数
        
        Args:
            backtest_results: 回测结果DataFrame，需包含future_return列
        
        Returns:
            {'win_rate': 胜率, 'profit_loss_ratio': 盈亏比}
        """
        if len(backtest_results) == 0:
            return {'win_rate': 0.5, 'profit_loss_ratio': 1.0}
        
        # 计算胜率
        winning_trades = backtest_results[backtest_results['future_return'] > 0]
        losing_trades = backtest_results[backtest_results['future_return'] <= 0]
        
        win_rate = len(winning_trades) / len(backtest_results)
        
        # 计算盈亏比
        if len(winning_trades) > 0 and len(losing_trades) > 0:
            avg_win = winning_trades['future_return'].mean()
            avg_loss = abs(losing_trades['future_return'].mean())
            profit_loss_ratio = avg_win / avg_loss if avg_loss > 0 else 1.5
        else:
            profit_loss_ratio = 1.5  # 默认值
        
        return {
            'win_rate': round(win_rate, 4),
            'profit_loss_ratio': round(profit_loss_ratio, 4),
            'total_trades': len(backtest_results),
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'avg_win': round(winning_trades['future_return'].mean(), 2) if len(winning_trades) > 0 else 0,
            'avg_loss': round(losing_trades['future_return'].mean(), 2) if len(losing_trades) > 0 else 0
        }


# ==================== 测试代码 ====================
if __name__ == "__main__":
    print("🎰 凯利公式仓位管理器测试")
    print("="*60)
    
    manager = KellyPositionManager()
    
    # 测试1: 计算凯利仓位
    print("\n测试1: 凯利公式计算")
    print("-"*60)
    
    test_cases = [
        {'win_rate': 0.65, 'pl_ratio': 1.5, 'name': '高胜率+好盈亏比'},
        {'win_rate': 0.55, 'pl_ratio': 2.0, 'name': '中等胜率+高盈亏比'},
        {'win_rate': 0.70, 'pl_ratio': 1.2, 'name': '极高胜率+低盈亏比'},
    ]
    
    for case in test_cases:
        result = manager.calculate_kelly_position(case['win_rate'], case['pl_ratio'])
        print(f"\n{case['name']}:")
        print(f"  胜率: {case['win_rate']*100}%")
        print(f"  盈亏比: {case['pl_ratio']}")
        print(f"  完全凯利: {result['kelly_pct']*100:.1f}%")
        print(f"  半凯利(推荐): {result['adjusted_kelly_pct']*100:.1f}%")
        print(f"  最终建议: {result['recommended_pct']*100:.1f}%")
        print(f"  说明: {result['reason']}")
    
    # 测试2: 组合优化
    print("\n\n测试2: 组合仓位优化")
    print("-"*60)
    
    signals = [
        {'ts_code': '600519.SH', 'score': 92, 'star_rating': 5, 'win_rate': 0.68, 'profit_loss_ratio': 1.8},
        {'ts_code': '000858.SZ', 'score': 85, 'star_rating': 4, 'win_rate': 0.62, 'profit_loss_ratio': 1.5},
        {'ts_code': '600036.SH', 'score': 78, 'star_rating': 3},
        {'ts_code': '601318.SH', 'score': 72, 'star_rating': 3},
        {'ts_code': '000001.SZ', 'score': 65, 'star_rating': 2},
    ]
    
    portfolio = manager.optimize_portfolio_allocation(signals)
    
    print("\n组合配置方案:")
    for alloc in portfolio['allocations']:
        print(f"\n{alloc['ts_code']} - {'⭐'*alloc['star_rating']} {alloc['score']}分")
        print(f"  建议仓位: {alloc['position_pct']*100:.1f}%")
        print(f"  配置方法: {alloc['method']}")
    
    print(f"\n组合汇总:")
    print(f"  总仓位: {portfolio['total_position']*100:.1f}%")
    print(f"  现金储备: {portfolio['cash_reserve']*100:.1f}%")
    print(f"  配置信号数: {portfolio['summary']['allocated_signals']}/{portfolio['summary']['total_signals']}")
    
    print("\n" + "="*60)
    print("✅ 凯利仓位管理器测试完成！")


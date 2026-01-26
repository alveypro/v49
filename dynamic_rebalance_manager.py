#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔄 动态再平衡管理器
Dynamic Portfolio Rebalancing Manager

核心功能：
1. 利润保护（移动止损）
2. 评分跟踪（信号恶化自动减仓）
3. 机会替换（卖出低分，买入高分）
4. 市场恶化防御（大盘转弱自动减仓）

借鉴：Renaissance Technologies的动态风险管理
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DynamicRebalanceManager:
    """动态再平衡管理器"""
    
    def __init__(self, 
                 profit_protect_threshold: float = 0.08,  # 盈利8%开始保护
                 profit_lock_ratio: float = 0.5,  # 锁定50%利润
                 score_drop_threshold: float = 15,  # 评分下降15分预警
                 score_critical_threshold: float = 60,  # 评分低于60分清仓
                 market_ma_cross_reduce: float = 0.5):  # 大盘死叉减仓50%
        """
        初始化
        
        Args:
            profit_protect_threshold: 盈利多少开始保护（默认8%）
            profit_lock_ratio: 锁定多少比例的利润（默认50%）
            score_drop_threshold: 评分下降多少预警（默认15分）
            score_critical_threshold: 评分低于多少清仓（默认60分）
            market_ma_cross_reduce: 大盘转弱减仓比例（默认50%）
        """
        self.profit_protect_threshold = profit_protect_threshold
        self.profit_lock_ratio = profit_lock_ratio
        self.score_drop_threshold = score_drop_threshold
        self.score_critical_threshold = score_critical_threshold
        self.market_ma_cross_reduce = market_ma_cross_reduce
        
        logger.info("动态再平衡管理器初始化完成")
    
    def check_profit_protection(self, holding: Dict) -> Dict:
        """
        检查是否需要利润保护
        
        Args:
            holding: 持仓信息
                {
                    'ts_code': 股票代码,
                    'buy_price': 买入价,
                    'current_price': 当前价,
                    'highest_price': 持有期最高价（可选）
                }
        
        Returns:
            保护建议
        """
        buy_price = holding['buy_price']
        current_price = holding['current_price']
        profit_pct = (current_price - buy_price) / buy_price
        
        # 使用持有期最高价（如果有）
        highest_price = holding.get('highest_price', current_price)
        max_profit_pct = (highest_price - buy_price) / buy_price
        
        action = 'hold'
        new_stop_loss = None
        reason = ''
        
        if profit_pct >= self.profit_protect_threshold:
            # 盈利达到阈值，启动利润保护
            if profit_pct >= 0.12:
                # 盈利超过12%，锁定80%利润
                locked_profit = max_profit_pct * 0.8
                new_stop_loss = buy_price * (1 + locked_profit)
                reason = f'盈利{profit_pct*100:.1f}%，锁定80%利润，移动止损至{new_stop_loss:.2f}（+{locked_profit*100:.1f}%）'
                action = 'update_stop_loss'
            else:
                # 盈利8-12%，锁定50%利润
                locked_profit = max_profit_pct * self.profit_lock_ratio
                new_stop_loss = buy_price * (1 + locked_profit)
                reason = f'盈利{profit_pct*100:.1f}%，锁定50%利润，移动止损至{new_stop_loss:.2f}（+{locked_profit*100:.1f}%）'
                action = 'update_stop_loss'
        
        # 检查是否触及移动止损
        if new_stop_loss and current_price < new_stop_loss:
            action = 'sell'
            reason += ' | 触及移动止损，建议卖出'
        
        return {
            'action': action,
            'new_stop_loss': new_stop_loss,
            'current_profit_pct': round(profit_pct, 4),
            'max_profit_pct': round(max_profit_pct, 4),
            'reason': reason
        }
    
    def check_score_deterioration(self, holding: Dict) -> Dict:
        """
        检查评分恶化情况
        
        Args:
            holding: 持仓信息
                {
                    'ts_code': 股票代码,
                    'buy_score': 买入时评分,
                    'current_score': 当前评分
                }
        
        Returns:
            调整建议
        """
        buy_score = holding['buy_score']
        current_score = holding['current_score']
        score_drop = buy_score - current_score
        
        action = 'hold'
        reduce_pct = 0
        reason = ''
        
        if current_score < self.score_critical_threshold:
            # 评分低于60分，信号消失
            action = 'sell'
            reduce_pct = 1.0  # 全部卖出
            reason = f'评分从{buy_score}分跌至{current_score}分（低于{self.score_critical_threshold}分），信号消失，建议清仓'
        
        elif score_drop >= 20:
            # 评分大幅下降（>20分）
            action = 'reduce'
            reduce_pct = 0.7  # 减仓70%
            reason = f'评分从{buy_score}分跌至{current_score}分（下降{score_drop}分），信号显著变弱，建议减仓70%'
        
        elif score_drop >= self.score_drop_threshold:
            # 评分中度下降（>15分）
            action = 'reduce'
            reduce_pct = 0.5  # 减仓50%
            reason = f'评分从{buy_score}分跌至{current_score}分（下降{score_drop}分），信号变弱，建议减仓50%'
        
        elif score_drop >= 10:
            # 评分小幅下降（>10分）
            action = 'caution'
            reduce_pct = 0.3  # 减仓30%
            reason = f'评分从{buy_score}分跌至{current_score}分（下降{score_drop}分），密切关注'
        
        return {
            'action': action,
            'reduce_pct': reduce_pct,
            'score_drop': score_drop,
            'reason': reason
        }
    
    def check_opportunity_replacement(self, current_holdings: List[Dict],
                                     new_signals: List[Dict],
                                     max_holdings: int = 5) -> Dict:
        """
        检查是否有更好的机会值得替换
        
        Args:
            current_holdings: 当前持仓列表
            new_signals: 新信号列表
            max_holdings: 最大持仓数
        
        Returns:
            替换建议
        """
        if len(current_holdings) < max_holdings:
            # 还有空位，直接买入
            return {
                'action': 'buy_new',
                'available_slots': max_holdings - len(current_holdings),
                'recommendations': sorted(new_signals, key=lambda x: x['score'], reverse=True)[:max_holdings - len(current_holdings)]
            }
        
        # 找出当前持仓中最弱的
        weakest_holdings = sorted(current_holdings, key=lambda x: x.get('current_score', 0))
        
        # 找出新信号中最强的
        strongest_signals = sorted(new_signals, key=lambda x: x['score'], reverse=True)
        
        replacements = []
        
        for signal in strongest_signals[:3]:  # 只考虑前3个新信号
            new_score = signal['score']
            
            for holding in weakest_holdings:
                old_score = holding.get('current_score', 0)
                score_diff = new_score - old_score
                
                # 如果新信号比旧信号强15分以上，考虑替换
                if score_diff >= 15:
                    # 检查持仓盈亏
                    profit_pct = holding.get('profit_pct', 0)
                    
                    # 如果旧股票亏损且信号弱，更应该替换
                    if profit_pct < 0 or old_score < 65:
                        replacements.append({
                            'sell': holding,
                            'buy': signal,
                            'score_improvement': score_diff,
                            'reason': f'新信号{signal["ts_code"]}({new_score}分)显著强于持仓{holding["ts_code"]}({old_score}分)，建议替换'
                        })
                        break
        
        if replacements:
            return {
                'action': 'replace',
                'replacements': replacements
            }
        else:
            return {
                'action': 'hold',
                'reason': '当前持仓优于新信号，保持不变'
            }
    
    def check_market_regime_defense(self, index_data: pd.DataFrame) -> Dict:
        """
        检查市场环境，决定是否需要防御
        
        Args:
            index_data: 大盘指数数据（需包含close）
        
        Returns:
            防御建议
        """
        if len(index_data) < 20:
            return {'action': 'hold', 'reason': '数据不足'}
        
        close = index_data['close']
        ma5 = close.rolling(window=5).mean()
        ma20 = close.rolling(window=20).mean()
        
        current_ma5 = ma5.iloc[-1]
        current_ma20 = ma20.iloc[-1]
        prev_ma5 = ma5.iloc[-2]
        prev_ma20 = ma20.iloc[-2]
        
        action = 'hold'
        reduce_pct = 0
        reason = ''
        
        # 检查死叉（MA5跌破MA20）
        if prev_ma5 >= prev_ma20 and current_ma5 < current_ma20:
            action = 'reduce'
            reduce_pct = self.market_ma_cross_reduce
            reason = f'大盘MA5跌破MA20（死叉），建议减仓{reduce_pct*100:.0f}%转为防守'
        
        # 检查价格远离均线（超卖或超买）
        elif close.iloc[-1] < current_ma20 * 0.95:
            action = 'reduce'
            reduce_pct = 0.3
            reason = '大盘价格远低于MA20，市场较弱，建议减仓30%'
        
        elif close.iloc[-1] > current_ma20 * 1.05:
            action = 'caution'
            reduce_pct = 0.2
            reason = '大盘价格远高于MA20，谨防回调，可考虑减仓20%'
        
        return {
            'action': action,
            'reduce_pct': reduce_pct,
            'ma5': round(current_ma5, 2),
            'ma20': round(current_ma20, 2),
            'reason': reason
        }
    
    def generate_daily_rebalance_plan(self, 
                                     current_holdings: List[Dict],
                                     new_signals: List[Dict],
                                     index_data: pd.DataFrame) -> Dict:
        """
        生成每日再平衡计划
        
        Args:
            current_holdings: 当前持仓
            new_signals: 新信号
            index_data: 大盘数据
        
        Returns:
            完整的再平衡计划
        """
        plan = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'actions': [],
            'summary': {
                'profit_protections': 0,
                'score_reductions': 0,
                'opportunity_replacements': 0,
                'market_defenses': 0
            }
        }
        
        # 1. 检查每个持仓的利润保护
        for holding in current_holdings:
            profit_check = self.check_profit_protection(holding)
            if profit_check['action'] != 'hold':
                plan['actions'].append({
                    'type': 'profit_protection',
                    'holding': holding,
                    'decision': profit_check
                })
                plan['summary']['profit_protections'] += 1
        
        # 2. 检查评分恶化
        for holding in current_holdings:
            if 'current_score' in holding:
                score_check = self.check_score_deterioration(holding)
                if score_check['action'] != 'hold':
                    plan['actions'].append({
                        'type': 'score_deterioration',
                        'holding': holding,
                        'decision': score_check
                    })
                    plan['summary']['score_reductions'] += 1
        
        # 3. 检查机会替换
        if new_signals:
            replacement_check = self.check_opportunity_replacement(
                current_holdings, new_signals
            )
            if replacement_check['action'] != 'hold':
                plan['actions'].append({
                    'type': 'opportunity_replacement',
                    'decision': replacement_check
                })
                plan['summary']['opportunity_replacements'] += len(
                    replacement_check.get('replacements', [])
                )
        
        # 4. 检查市场防御
        market_check = self.check_market_regime_defense(index_data)
        if market_check['action'] != 'hold':
            plan['actions'].append({
                'type': 'market_defense',
                'decision': market_check
            })
            plan['summary']['market_defenses'] += 1
        
        # 汇总
        plan['total_actions'] = len(plan['actions'])
        plan['needs_action'] = plan['total_actions'] > 0
        
        return plan


# ==================== 测试代码 ====================
if __name__ == "__main__":
    print("🔄 动态再平衡管理器测试")
    print("="*60)
    
    manager = DynamicRebalanceManager()
    
    # 测试1: 利润保护
    print("\n测试1: 利润保护检查")
    print("-"*60)
    
    test_holdings = [
        {'ts_code': '600519.SH', 'buy_price': 100, 'current_price': 109, 'highest_price': 110},
        {'ts_code': '000858.SZ', 'buy_price': 50, 'current_price': 56, 'highest_price': 58},
        {'ts_code': '600036.SH', 'buy_price': 30, 'current_price': 31, 'highest_price': 31},
    ]
    
    for holding in test_holdings:
        result = manager.check_profit_protection(holding)
        print(f"\n{holding['ts_code']}:")
        print(f"  买入价: ¥{holding['buy_price']}, 当前价: ¥{holding['current_price']}")
        print(f"  当前盈利: {result['current_profit_pct']*100:.1f}%")
        print(f"  操作建议: {result['action']}")
        print(f"  说明: {result['reason']}")
    
    # 测试2: 评分恶化
    print("\n\n测试2: 评分恶化检查")
    print("-"*60)
    
    score_test = [
        {'ts_code': '600519.SH', 'buy_score': 85, 'current_score': 82},
        {'ts_code': '000858.SZ', 'buy_score': 90, 'current_score': 72},
        {'ts_code': '600036.SH', 'buy_score': 75, 'current_score': 55},
    ]
    
    for holding in score_test:
        result = manager.check_score_deterioration(holding)
        print(f"\n{holding['ts_code']}:")
        print(f"  买入评分: {holding['buy_score']}, 当前评分: {holding['current_score']}")
        print(f"  评分下降: {result['score_drop']}分")
        print(f"  操作建议: {result['action']}")
        if result['reduce_pct'] > 0:
            print(f"  减仓比例: {result['reduce_pct']*100:.0f}%")
        print(f"  说明: {result['reason']}")
    
    print("\n" + "="*60)
    print("✅ 动态再平衡管理器测试完成！")


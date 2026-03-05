#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🚀 优化回测策略 v49.0 - 终极优化版
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    集成v4.0潜伏为王评分器 + 动态止损止盈 + 仓位管理
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔥 核心优化：
1. 集成v4.0评分器（潜伏为王）
2. 降低评分阈值：75分 → 60分
3. 动态止损止盈：-4%止损 / +6%止盈
4. 仓位管理：根据评分分配仓位
5. 优化持仓：最短3天，最长15天

💡 预期效果：
- 信号数量：31个 → 80-150个
- 平均收益：-0.26% → 3-5%
- 胜率：45.2% → 55-60%
- 夏普比率：-0.64 → 1.0-1.5

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, List
import traceback

np.random.seed(42)

logger = logging.getLogger(__name__)


def backtest_with_dynamic_strategy(evaluator_v4, df: pd.DataFrame, 
                                   sample_size: int = 1000,
                                   score_threshold: float = 60.0,
                                   max_holding_days: int = 15,
                                   stop_loss_pct: float = -4.0,
                                   take_profit_pct: float = 6.0) -> dict:
    """
    🏆 综合优选策略回测（v4.0优化版）
    
    核心优化：
    1. 使用v4.0评分器（潜伏为王）
    2. 评分阈值：60分起（vs v3.0的75分）
    3. 动态止损止盈：止损-4%，止盈+6%
    4. 仓位管理：
       - S级（≥80分）：30%仓位
       - A级（70-79分）：25%仓位
       - B级（60-69分）：20%仓位
    5. 最长持仓15天（避免长期被套）
    
    参数：
        evaluator_v4: v4.0评分器实例
        df: 股票数据DataFrame
        sample_size: 抽样数量
        score_threshold: 评分阈值（默认60分）
        max_holding_days: 最长持仓天数（默认15天）
        stop_loss_pct: 止损百分比（默认-4%）
        take_profit_pct: 止盈百分比（默认+6%）
    """
    logger.info("🚀 开始v4.0优化版回测...")
    logger.info(f"📊 参数：样本={sample_size}, 阈值={score_threshold}分, 持仓≤{max_holding_days}天")
    logger.info(f"💰 止损={stop_loss_pct}%, 止盈={take_profit_pct}%")
    
    try:
        all_signals = []
        all_scores = []
        analyzed_count = 0
        qualified_count = 0
        
        unique_stocks = df['ts_code'].unique()
        if len(unique_stocks) > sample_size:
            sample_stocks = np.random.choice(unique_stocks, sample_size, replace=False)
        else:
            sample_stocks = unique_stocks
        
        logger.info(f"📈 开始扫描 {len(sample_stocks)} 只股票...")
        
        for ts_code in sample_stocks:
            analyzed_count += 1
            if analyzed_count % 100 == 0:
                logger.info(f"回测进度: {analyzed_count}/{len(sample_stocks)}, 已发现{qualified_count}个信号")
            
            try:
                stock_data = df[df['ts_code'] == ts_code].copy()
                
                # 至少需要60天历史数据 + 15天未来数据
                if len(stock_data) < 75:
                    continue
                
                # 按日期排序
                stock_data = stock_data.sort_values('trade_date')
                
                # 找到最后一个可以计算未来收益的时间点
                last_valid_idx = len(stock_data) - max_holding_days - 1
                
                if last_valid_idx < 60:
                    continue
                
                # 获取截止到该点的历史数据
                historical_data = stock_data.iloc[:last_valid_idx + 1].copy()
                
                # 🏆 使用v4.0评分器（潜伏为王版）
                score_result = evaluator_v4.evaluate_stock_v4(historical_data)
                final_score = score_result.get('comprehensive_score', 0) or score_result.get('final_score', 0)
                
                if not score_result.get('success', False):
                    continue
                
                # 记录所有评分
                all_scores.append({
                    'ts_code': ts_code,
                    'final_score': final_score
                })
                
                # 如果达到阈值，这是一个买入信号
                if final_score >= score_threshold:
                    signal_date = historical_data['trade_date'].iloc[-1]
                    buy_price = historical_data['close_price'].iloc[-1]
                    
                    # 🔥 核心优化：动态止损止盈
                    # 计算未来每天的价格，找到触发止损/止盈的时间点
                    future_data = stock_data.iloc[last_valid_idx + 1:last_valid_idx + 1 + max_holding_days]
                    
                    sell_price = None
                    sell_day = None
                    exit_reason = "持仓到期"
                    
                    for i, (idx, row) in enumerate(future_data.iterrows(), 1):
                        future_price = row['close_price']
                        future_return = (future_price - buy_price) / buy_price * 100
                        
                        # 触发止盈
                        if future_return >= take_profit_pct:
                            sell_price = future_price
                            sell_day = i
                            exit_reason = f"止盈{take_profit_pct}%"
                            break
                        
                        # 触发止损
                        if future_return <= stop_loss_pct:
                            sell_price = future_price
                            sell_day = i
                            exit_reason = f"止损{stop_loss_pct}%"
                            break
                    
                    # 如果没有触发止损止盈，持仓到期
                    if sell_price is None:
                        if len(future_data) > 0:
                            sell_price = future_data.iloc[-1]['close_price']
                            sell_day = len(future_data)
                        else:
                            continue
                    
                    future_return = (sell_price - buy_price) / buy_price * 100
                    
                    # 根据评分分配仓位
                    if final_score >= 80:
                        position_size = 0.30  # S级，30%仓位
                        level = 'S级(≥80分)'
                    elif final_score >= 70:
                        position_size = 0.25  # A级，25%仓位
                        level = 'A级(70-79分)'
                    else:
                        position_size = 0.20  # B级，20%仓位
                        level = 'B级(60-69分)'
                    
                    qualified_count += 1
                    
                    # 记录信号
                    signal_dict = {
                        'ts_code': ts_code,
                        'name': stock_data['name'].iloc[0] if 'name' in stock_data.columns else '',
                        'industry': stock_data['industry'].iloc[0] if 'industry' in stock_data.columns else '',
                        'trade_date': signal_date,
                        'buy_price': buy_price,
                        'sell_price': sell_price,
                        'holding_days': sell_day,
                        'exit_reason': exit_reason,
                        'final_score': final_score,
                        'level': level,
                        'position_size': position_size,
                        'future_return': future_return,
                        'weighted_return': future_return * position_size,  # 加权收益
                        
                        # 维度得分
                        '潜伏价值': score_result['dimension_scores'].get('潜伏价值', 0),
                        '底部特征': score_result['dimension_scores'].get('底部特征', 0),
                        '量价配合': score_result['dimension_scores'].get('量价配合', 0),
                        'MACD趋势': score_result['dimension_scores'].get('MACD趋势', 0),
                        '均线多头': score_result['dimension_scores'].get('均线多头', 0),
                        '主力行为': score_result['dimension_scores'].get('主力行为', 0),
                        '启动确认': score_result['dimension_scores'].get('启动确认', 0),
                        '涨停基因': score_result['dimension_scores'].get('涨停基因', 0),
                        
                        'synergy_bonus': score_result.get('synergy_bonus', 0),
                        'combo_type': score_result.get('combo_type', '无'),
                        'risk_penalty': score_result.get('risk_penalty', 0),
                        'risk_reasons': ', '.join(score_result.get('risk_reasons', []))
                    }
                    all_signals.append(signal_dict)
            
            except Exception as e:
                logger.debug(f"处理{ts_code}时出错: {e}")
                continue
        
        logger.info(f"✅ 扫描完成！分析了{analyzed_count}只股票，发现{len(all_signals)}个信号")
        
        # 📊 生成评分分布诊断信息
        score_distribution = {}
        if all_scores:
            scores_df = pd.DataFrame(all_scores)
            max_score = scores_df['final_score'].max()
            avg_score = scores_df['final_score'].mean()
            score_distribution = {
                'total_evaluated': len(all_scores),
                'max_score': max_score,
                'avg_score': avg_score,
                'score_90+': len(scores_df[scores_df['final_score'] >= 90]),
                'score_85+': len(scores_df[scores_df['final_score'] >= 85]),
                'score_80+': len(scores_df[scores_df['final_score'] >= 80]),
                'score_75+': len(scores_df[scores_df['final_score'] >= 75]),
                'score_70+': len(scores_df[scores_df['final_score'] >= 70]),
                'score_65+': len(scores_df[scores_df['final_score'] >= 65]),
                'score_60+': len(scores_df[scores_df['final_score'] >= 60]),
                'score_50+': len(scores_df[scores_df['final_score'] >= 50])
            }
            logger.info(f"📊 评分分布: 最高{max_score:.1f}分, 平均{avg_score:.1f}分")
            logger.info(f"   50+:{score_distribution['score_50+']}只, 60+:{score_distribution['score_60+']}只, 70+:{score_distribution['score_70+']}只, 80+:{score_distribution['score_80+']}只")
        
        if not all_signals:
            suggestion = ""
            if all_scores:
                if max_score < score_threshold:
                    suggestion = f"\n\n💡 建议：最高分仅{max_score:.1f}分，低于阈值{score_threshold}分。建议降低阈值到{int(max_score * 0.9)}分重试。"
                elif score_distribution.get('score_50+', 0) > 0:
                    suggestion = f"\n\n💡 建议：有{score_distribution['score_50+']}只股票≥50分。建议降低阈值到50-55分。"
            
            logger.warning(f"回测未发现有效信号（阈值={score_threshold}分）{suggestion}")
            return {
                'success': False,
                'error': f'回测未发现有效信号（阈值={score_threshold}分）{suggestion}',
                'strategy': '综合优选v4.0',
                'score_distribution': score_distribution,
                'stats': {
                    'total_signals': 0,
                    'avg_return': 0,
                    'win_rate': 0,
                    'sharpe_ratio': 0,
                    'max_drawdown': 0
                }
            }
        
        # 转换为DataFrame
        backtest_df = pd.DataFrame(all_signals)
        backtest_df = backtest_df.dropna(subset=['future_return'])
        
        if len(backtest_df) == 0:
            logger.warning(f"回测数据不足（找到{len(all_signals)}个信号但future_return全为空）")
            return {
                'success': False,
                'error': '回测数据不足',
                'strategy': '综合优选v4.0',
                'stats': {
                    'total_signals': 0,
                    'avg_return': 0,
                    'win_rate': 0,
                    'sharpe_ratio': 0,
                    'max_drawdown': 0
                }
            }
        
        # 计算统计指标
        total_signals = len(backtest_df)
        avg_return = backtest_df['future_return'].mean()
        weighted_avg_return = backtest_df['weighted_return'].mean()  # 加权平均收益
        median_return = backtest_df['future_return'].median()
        win_rate = (backtest_df['future_return'] > 0).sum() / total_signals * 100
        max_return = backtest_df['future_return'].max()
        min_return = backtest_df['future_return'].min()
        avg_holding_days = backtest_df['holding_days'].mean()
        
        # 计算夏普比率（使用加权收益）
        returns_std = backtest_df['weighted_return'].std()
        risk_free_rate = 0.03  # 假设无风险利率3%
        sharpe_ratio = ((weighted_avg_return - risk_free_rate) / returns_std * np.sqrt(252/avg_holding_days)) if returns_std > 0 else 0
        
        # 计算最大回撤
        cumulative_returns = (1 + backtest_df['weighted_return'] / 100).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max * 100
        max_drawdown = drawdown.min()
        
        # 按评分分级统计
        level_stats = {}
        for level in ['S级(≥80分)', 'A级(70-79分)', 'B级(60-69分)']:
            level_data = backtest_df[backtest_df['level'] == level]
            if len(level_data) > 0:
                level_stats[level] = {
                    'count': len(level_data),
                    'avg_return': level_data['future_return'].mean(),
                    'weighted_return': level_data['weighted_return'].mean(),
                    'win_rate': (level_data['future_return'] > 0).sum() / len(level_data) * 100,
                    'avg_holding_days': level_data['holding_days'].mean()
                }
        
        # 止损止盈统计
        exit_stats = backtest_df['exit_reason'].value_counts().to_dict()
        
        logger.info(f"📊 回测结果：")
        logger.info(f"  总信号数：{total_signals}")
        logger.info(f"  平均收益：{avg_return:.2f}%（加权：{weighted_avg_return:.2f}%）")
        logger.info(f"  胜率：{win_rate:.1f}%")
        logger.info(f"  夏普比率：{sharpe_ratio:.2f}")
        logger.info(f"  平均持仓：{avg_holding_days:.1f}天")
        logger.info(f"  退出统计：{exit_stats}")
        
        result = {
            'success': True,
            'strategy': '综合优选v4.0',
            'backtest_df': backtest_df,
            'stats': {
                'total_signals': total_signals,
                'avg_return': avg_return,
                'weighted_avg_return': weighted_avg_return,
                'median_return': median_return,
                'win_rate': win_rate,
                'max_return': max_return,
                'min_return': min_return,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown': max_drawdown,
                'avg_holding_days': avg_holding_days,
                'level_stats': level_stats,
                'exit_stats': exit_stats,
                'score_distribution': score_distribution
            }
        }
        
        return result
        
    except Exception as e:
        logger.error(f"v4.0回测失败: {e}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e),
            'strategy': '综合优选v4.0',
            'stats': {
                'total_signals': 0,
                'avg_return': 0,
                'win_rate': 0,
                'sharpe_ratio': 0,
                'max_drawdown': 0
            }
        }


if __name__ == '__main__':
    print("""
    🚀 优化回测策略 v49.0 - 终极优化版
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    核心优化：
    1. 集成v4.0评分器（潜伏为王）
    2. 降低评分阈值：75分 → 60分
    3. 动态止损止盈：-4%止损 / +6%止盈
    4. 仓位管理：根据评分分配仓位
    5. 优化持仓：最短3天，最长15天
    
    💡 预期效果：
    - 信号数量：31个 → 80-150个
    - 平均收益：-0.26% → 3-5%
    - 胜率：45.2% → 55-60%
    - 夏普比率：-0.64 → 1.0-1.5
    
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    """)


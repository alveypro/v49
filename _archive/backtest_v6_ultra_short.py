#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🔍 v6.0超短线狙击策略回测系统
测试完整版v6.0的实际效果
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
import logging
import sys
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from comprehensive_stock_evaluator_v6 import ComprehensiveStockEvaluatorV6

PERMANENT_DB_PATH = "/Users/mac/QLIB/permanent_stock_database.db"

class V6UltraShortBacktest:
    """v6.0超短线狙击策略回测系统"""
    
    def __init__(self):
        self.evaluator = ComprehensiveStockEvaluatorV6()
        self.conn = sqlite3.connect(PERMANENT_DB_PATH)
        
    def run_backtest(self, 
                     sample_size: int = 500,
                     score_threshold: int = 75,
                     holding_days: int = 5,
                     stop_loss: float = -0.05,
                     take_profit: float = 0.08):
        """运行回测"""
        logger.info("=" * 80)
        logger.info("🔍 v6.0超短线狙击策略回测开始")
        logger.info("=" * 80)
        logger.info(f"📊 回测参数:")
        logger.info(f"   - 样本量: {sample_size}只股票")
        logger.info(f"   - 评分阈值: ≥{score_threshold}分")
        logger.info(f"   - 持仓周期: {holding_days}天")
        logger.info(f"   - 止损: {stop_loss*100:.0f}%")
        logger.info(f"   - 止盈: {take_profit*100:.0f}%")
        
        # 1. 获取股票池
        stocks = self._get_stock_pool(sample_size)
        logger.info(f"\n✅ 获取了{len(stocks)}只股票")
        
        # 2. 回测
        all_signals = []
        
        for idx, (ts_code, name) in enumerate(stocks):
            if (idx + 1) % 50 == 0:
                logger.info(f"进度: {idx+1}/{len(stocks)}")
            
            signals = self._backtest_single_stock(
                ts_code, name, score_threshold, holding_days, stop_loss, take_profit
            )
            all_signals.extend(signals)
        
        logger.info(f"\n✅ 回测完成，共生成{len(all_signals)}个交易信号")
        
        # 3. 分析结果
        if len(all_signals) > 0:
            results = self._analyze_results(all_signals, holding_days, stop_loss, take_profit)
            return results
        else:
            logger.warning("⚠️ 没有生成任何交易信号")
            return None
    
    def _get_stock_pool(self, sample_size: int) -> List:
        """获取股票池"""
        query = """
            SELECT ts_code, name
            FROM stock_basic
            WHERE is_active = 1
              AND circ_mv >= 1000000
              AND circ_mv <= 5000000
            ORDER BY RANDOM()
            LIMIT ?
        """
        df = pd.read_sql_query(query, self.conn, params=(sample_size,))
        return list(zip(df['ts_code'], df['name']))
    
    def _backtest_single_stock(self, ts_code: str, name: str,
                               score_threshold: int, holding_days: int,
                               stop_loss: float, take_profit: float) -> List[Dict]:
        """对单只股票进行回测"""
        signals = []
        
        try:
            # 获取历史数据
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=150)).strftime('%Y%m%d')
            
            query = """
                SELECT trade_date, close_price, vol, pct_chg
                FROM daily_trading_data
                WHERE ts_code = ?
                  AND trade_date >= ?
                  AND trade_date <= ?
                ORDER BY trade_date
            """
            
            data = pd.read_sql_query(query, self.conn, params=(ts_code, start_date, end_date))
            
            if len(data) < 80:
                return signals
            
            # 滑动窗口回测
            for i in range(60, len(data) - holding_days - 1):
                window_data = data.iloc[:i+1].copy()
                window_data['name'] = name
                
                # v6.0评分
                evaluation = self.evaluator.evaluate_stock_v6(window_data, ts_code)
                
                if not evaluation['success']:
                    continue
                
                final_score = evaluation['final_score']
                
                # 生成买入信号
                if final_score >= score_threshold:
                    buy_date = data.iloc[i]['trade_date']
                    buy_price = data.iloc[i]['close_price']
                    
                    # 模拟持仓
                    holding_result = self._simulate_holding(
                        data.iloc[i+1:i+1+holding_days+5],
                        buy_price,
                        holding_days,
                        stop_loss,
                        take_profit
                    )
                    
                    if holding_result:
                        signal = {
                            'ts_code': ts_code,
                            'name': name,
                            'buy_date': buy_date,
                            'buy_price': buy_price,
                            'sell_date': holding_result['sell_date'],
                            'sell_price': holding_result['sell_price'],
                            'return_pct': holding_result['return_pct'],
                            'holding_days': holding_result['holding_days'],
                            'exit_reason': holding_result['exit_reason'],
                            'score': final_score,
                            'sector_heat': evaluation['dim_scores']['板块热度'],
                            'money_flow': evaluation['dim_scores']['资金流向'],
                            'tech_breakthrough': evaluation['dim_scores']['技术突破']
                        }
                        signals.append(signal)
                        i += holding_result['holding_days']
        
        except Exception as e:
            logger.debug(f"回测{name}({ts_code})失败: {e}")
        
        return signals
    
    def _simulate_holding(self, future_data: pd.DataFrame, buy_price: float,
                         holding_days: int, stop_loss: float, take_profit: float) -> Dict:
        """模拟持仓"""
        if len(future_data) == 0:
            return None
        
        for day_idx, row in future_data.iterrows():
            current_price = row['close_price']
            return_pct = (current_price - buy_price) / buy_price
            actual_holding_days = day_idx - future_data.index[0] + 1
            
            # 止盈
            if return_pct >= take_profit:
                return {
                    'sell_date': row['trade_date'],
                    'sell_price': current_price,
                    'return_pct': return_pct,
                    'holding_days': actual_holding_days,
                    'exit_reason': '止盈'
                }
            
            # 止损
            if return_pct <= stop_loss:
                return {
                    'sell_date': row['trade_date'],
                    'sell_price': current_price,
                    'return_pct': return_pct,
                    'holding_days': actual_holding_days,
                    'exit_reason': '止损'
                }
            
            # 到期
            if actual_holding_days >= holding_days:
                return {
                    'sell_date': row['trade_date'],
                    'sell_price': current_price,
                    'return_pct': return_pct,
                    'holding_days': actual_holding_days,
                    'exit_reason': '到期'
                }
        
        # 数据不足
        if len(future_data) > 0:
            last_row = future_data.iloc[-1]
            return {
                'sell_date': last_row['trade_date'],
                'sell_price': last_row['close_price'],
                'return_pct': (last_row['close_price'] - buy_price) / buy_price,
                'holding_days': len(future_data),
                'exit_reason': '数据不足'
            }
        
        return None
    
    def _analyze_results(self, signals: List[Dict], holding_days: int,
                        stop_loss: float, take_profit: float) -> Dict:
        """分析回测结果"""
        df = pd.DataFrame(signals)
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 v6.0回测结果分析")
        logger.info("=" * 80)
        
        # 基础统计
        total_trades = len(df)
        winning_trades = len(df[df['return_pct'] > 0])
        losing_trades = len(df[df['return_pct'] <= 0])
        win_rate = winning_trades / total_trades * 100 if total_trades > 0 else 0
        
        avg_return = df['return_pct'].mean() * 100
        median_return = df['return_pct'].median() * 100
        max_return = df['return_pct'].max() * 100
        min_return = df['return_pct'].min() * 100
        
        avg_holding = df['holding_days'].mean()
        
        logger.info(f"\n📈 基础统计:")
        logger.info(f"   - 总交易次数: {total_trades}")
        logger.info(f"   - 盈利次数: {winning_trades}")
        logger.info(f"   - 亏损次数: {losing_trades}")
        logger.info(f"   - 胜率: {win_rate:.1f}% {'✅' if win_rate >= 60 else '⚠️' if win_rate >= 55 else '❌'}")
        logger.info(f"   - 平均收益率: {avg_return:.2f}%")
        logger.info(f"   - 中位数收益率: {median_return:.2f}%")
        logger.info(f"   - 最大收益: {max_return:.2f}%")
        logger.info(f"   - 最大亏损: {min_return:.2f}%")
        logger.info(f"   - 平均持仓天数: {avg_holding:.1f}天")
        
        # 退出原因统计
        logger.info(f"\n📊 退出原因统计:")
        exit_reasons = df['exit_reason'].value_counts()
        for reason, count in exit_reasons.items():
            pct = count / total_trades * 100
            avg_ret = df[df['exit_reason'] == reason]['return_pct'].mean() * 100
            logger.info(f"   - {reason}: {count}次 ({pct:.1f}%), 平均收益{avg_ret:.2f}%")
        
        # 不同评分区间的表现
        logger.info(f"\n🎯 不同评分区间的表现:")
        df['score_range'] = pd.cut(df['score'], bins=[0, 75, 80, 85, 90, 100],
                                    labels=['75-80', '80-85', '85-90', '90-95', '95+'])
        for score_range in ['75-80', '80-85', '85-90', '90-95', '95+']:
            subset = df[df['score_range'] == score_range]
            if len(subset) > 0:
                win_rate_range = len(subset[subset['return_pct'] > 0]) / len(subset) * 100
                avg_return_range = subset['return_pct'].mean() * 100
                logger.info(f"   - {score_range}分: {len(subset)}次, 胜率{win_rate_range:.1f}%, 平均收益{avg_return_range:.2f}%")
        
        # 夏普比率
        if df['return_pct'].std() > 0:
            sharpe_ratio = df['return_pct'].mean() / df['return_pct'].std()
            logger.info(f"\n📐 风险指标:")
            logger.info(f"   - 夏普比率: {sharpe_ratio:.2f}")
            logger.info(f"   - 收益标准差: {df['return_pct'].std()*100:.2f}%")
        else:
            sharpe_ratio = 0
        
        # 最大回撤
        cumulative_returns = (1 + df['return_pct']).cumprod()
        running_max = cumulative_returns.expanding().max()
        drawdown = (cumulative_returns - running_max) / running_max
        max_drawdown = drawdown.min() * 100
        logger.info(f"   - 最大回撤: {max_drawdown:.2f}%")
        
        # 年化收益率
        if avg_holding > 0:
            trades_per_year = 250 / avg_holding
            annual_return = (1 + df['return_pct'].mean()) ** trades_per_year - 1
            logger.info(f"\n💰 年化收益率估算: {annual_return*100:.1f}%")
        else:
            annual_return = 0
        
        # 保存结果
        output_file = f"v6_backtest_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"\n💾 详细结果已保存到: {output_file}")
        
        # 评估
        logger.info("\n" + "=" * 80)
        logger.info("🎯 v6.0策略评估")
        logger.info("=" * 80)
        
        if win_rate >= 60 and avg_return >= 1.5 and sharpe_ratio >= 0.6:
            logger.info("✅ **v6.0策略表现优秀！强烈建议集成到超级回测模块**")
            logger.info(f"   - 胜率{win_rate:.1f}% ≥ 60% ✅")
            logger.info(f"   - 平均收益{avg_return:.2f}% ≥ 1.5% ✅")
            logger.info(f"   - 夏普比率{sharpe_ratio:.2f} ≥ 0.6 ✅")
            logger.info(f"   - 年化收益{annual_return*100:.1f}% ✅")
        elif win_rate >= 55 and avg_return >= 0.5:
            logger.info("⚠️ **v6.0策略表现良好，建议优化后集成**")
            logger.info(f"   - 胜率{win_rate:.1f}%")
            logger.info(f"   - 平均收益{avg_return:.2f}%")
            logger.info(f"   - 夏普比率{sharpe_ratio:.2f}")
        else:
            logger.info("❌ **v6.0策略表现不佳，需要优化**")
            logger.info(f"   - 胜率{win_rate:.1f}%")
            logger.info(f"   - 平均收益{avg_return:.2f}%")
        
        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'avg_return': avg_return,
            'sharpe_ratio': sharpe_ratio,
            'annual_return': annual_return * 100,
            'output_file': output_file
        }
    
    def close(self):
        """关闭连接"""
        self.conn.close()


def main():
    """主函数"""
    backtest = V6UltraShortBacktest()
    
    try:
        results = backtest.run_backtest(
            sample_size=500,       # 测试500只股票
            score_threshold=75,    # 评分≥75分
            holding_days=5,        # 持仓5天
            stop_loss=-0.05,       # 止损-5%
            take_profit=0.08       # 止盈+8%
        )
    finally:
        backtest.close()
    
    logger.info("\n" + "=" * 80)
    logger.info("🎉 v6.0回测完成！")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()











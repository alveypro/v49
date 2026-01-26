#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能交易助手 v1.0
作者：AI量化专家
创建时间：2026-01-06

功能：
1. 每日自动选股
2. 持仓监控管理
3. 止盈止损提醒
4. 交易记录管理
5. 每日报告生成
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict
import json
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_assistant.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 导入通知服务
try:
    from notification_service import NotificationService
    NOTIFICATION_AVAILABLE = True
except ImportError:
    logger.warning("⚠️ 通知服务模块未找到，通知功能将被禁用")
    NOTIFICATION_AVAILABLE = False


class TradingAssistant:
    """智能交易助手"""
    
    def __init__(self, db_path: str = "permanent_stock_database.db"):
        """
        初始化交易助手
        
        Args:
            db_path: 主数据库路径
        """
        self.db_path = db_path
        self.assistant_db = "trading_assistant.db"
        self._init_database()
        
        # 初始化通知服务
        self.notifier = None
        if NOTIFICATION_AVAILABLE:
            try:
                self.notifier = NotificationService()
                logger.info("📧 通知服务已启用")
            except Exception as e:
                logger.warning(f"⚠️ 通知服务初始化失败: {e}")
        
        logger.info("🚀 智能交易助手初始化完成")
    
    def _init_database(self):
        """初始化助手数据库"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        # 创建持仓表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                stock_name TEXT,
                buy_date TEXT,
                buy_price REAL,
                quantity INTEGER,
                cost_total REAL,
                current_price REAL,
                current_value REAL,
                profit_loss REAL,
                profit_loss_pct REAL,
                status TEXT DEFAULT 'holding',
                strategy TEXT DEFAULT 'v4.0',
                score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建交易记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_code TEXT NOT NULL,
                stock_name TEXT,
                action TEXT,  -- 'buy' or 'sell'
                trade_date TEXT,
                price REAL,
                quantity INTEGER,
                amount REAL,
                reason TEXT,
                profit_loss REAL,
                profit_loss_pct REAL,
                strategy TEXT DEFAULT 'v4.0',
                score REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 创建每日选股表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_recommendations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommend_date TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                stock_name TEXT,
                score REAL,
                price REAL,
                reason TEXT,
                strategy TEXT DEFAULT 'v4.0',
                market_cap REAL,
                industry TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(recommend_date, ts_code)
            )
        """)
        
        # 创建配置表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT,
                description TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
        
        # 初始化默认配置
        self._init_default_config()
        
        logger.info("✅ 数据库初始化完成")
    
    def _init_default_config(self):
        """初始化默认配置"""
        default_config = {
            'strategy': 'v4.0',
            'min_score': '65',  # 🔧 优化：基于回测数据，65分以上期望值为正
            'max_score': '90',  # 🔧 新增：最高分数，避免过度筛选
            'market_cap_min': '10000000000',  # 100亿
            'market_cap_max': '50000000000',  # 500亿
            'recommend_count': '5',
            'single_position_pct': '0.2',  # 单只20%
            'max_position_pct': '0.8',  # 最多80%仓位
            'take_profit_pct': '0.06',  # 6%止盈
            'stop_loss_pct': '0.04',  # 4%止损
            'holding_days': '5',  # 建议持仓天数
            'notification_enabled': 'true',
            'notification_email': '',
            'auto_scan_time': '09:15',  # 每天9:15扫描
            'score_analysis': 'v4_65-90分最优'  # 分析结论备注
        }
        
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        for key, value in default_config.items():
            cursor.execute("""
                INSERT OR IGNORE INTO config (key, value, description)
                VALUES (?, ?, ?)
            """, (key, value, f'默认{key}'))
        
        conn.commit()
        conn.close()
    
    def get_config(self, key: str) -> str:
        """获取配置"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    
    def update_config(self, key: str, value: str):
        """更新配置"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE config 
            SET value = ?, updated_at = CURRENT_TIMESTAMP
            WHERE key = ?
        """, (value, key))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 配置更新: {key} = {value}")
    
    def daily_stock_scan(self, top_n: int = 5) -> List[Dict]:
        """
        每日自动选股
        
        Args:
            top_n: 推荐股票数量
            
        Returns:
            推荐股票列表
        """
        logger.info("🔍 开始每日选股扫描...")
        
        try:
            # 获取配置
            min_score = float(self.get_config('min_score'))
            market_cap_min = float(self.get_config('market_cap_min'))
            market_cap_max = float(self.get_config('market_cap_max'))
            
            # 使用v4.0策略选股
            from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
            evaluator = ComprehensiveStockEvaluatorV4()
            
            # 获取候选股票
            conn = sqlite3.connect(self.db_path)
            query = """
                SELECT DISTINCT sb.ts_code, sb.name, sb.industry, sb.circ_mv
                FROM stock_basic sb
                WHERE sb.circ_mv >= ? AND sb.circ_mv <= ?
                ORDER BY RANDOM()
                LIMIT 200
            """
            
            candidates = pd.read_sql_query(
                query, 
                conn, 
                params=(market_cap_min/10000, market_cap_max/10000)
            )
            conn.close()
            
            logger.info(f"📊 候选股票: {len(candidates)}只")
            
            # 评分筛选
            recommendations = []
            
            for idx, row in candidates.iterrows():
                ts_code = row['ts_code']
                stock_name = row['name']
                
                # 获取最近数据
                conn = sqlite3.connect(self.db_path)
                stock_data = pd.read_sql_query(f"""
                    SELECT * FROM daily_trading_data
                    WHERE ts_code = '{ts_code}'
                    ORDER BY trade_date DESC
                    LIMIT 100
                """, conn)
                conn.close()
                
                if len(stock_data) < 60:
                    continue
                
                # 评分
                result = evaluator.evaluate_stock_v4(stock_data)
                
                if result['success'] and result['final_score'] >= min_score:
                    latest_price = stock_data.iloc[0]['close_price']
                    
                    # ✅ 生成详细推荐理由
                    reason_parts = []
                    reason_parts.append(result.get('description', '优质标的'))
                    
                    # 添加关键维度信息
                    dim_scores = result.get('dimension_scores', {})
                    if dim_scores:
                        top_dims = sorted(dim_scores.items(), key=lambda x: x[1], reverse=True)[:3]
                        reason_parts.append(f"核心优势: {', '.join([f'{k}({v:.0f}分)' for k, v in top_dims])}")
                    
                    recommendations.append({
                        'ts_code': ts_code,
                        'stock_name': stock_name,
                        'score': result['final_score'],
                        'price': latest_price,
                        'reason': ' | '.join(reason_parts),
                        'market_cap': row['circ_mv'] * 10000,
                        'industry': row['industry'],
                        'grade': result.get('grade', ''),
                        'dimension_scores': dim_scores
                    })
                
                if len(recommendations) >= top_n * 2:
                    break
            
            # 按分数排序，取Top N
            recommendations.sort(key=lambda x: x['score'], reverse=True)
            top_recommendations = recommendations[:top_n]
            
            # 保存到数据库
            today = datetime.now().strftime('%Y-%m-%d')
            self._save_daily_recommendations(today, top_recommendations)
            
            logger.info(f"✅ 选股完成，推荐{len(top_recommendations)}只")
            
            # 🆕 发送选股通知
            self._send_stock_selection_notification(top_recommendations)
            
            return top_recommendations
            
        except Exception as e:
            logger.error(f"❌ 选股失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def _save_daily_recommendations(self, date: str, recommendations: List[Dict]):
        """保存每日推荐"""
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        for rec in recommendations:
            cursor.execute("""
                INSERT OR REPLACE INTO daily_recommendations
                (recommend_date, ts_code, stock_name, score, price, reason, 
                 strategy, market_cap, industry)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date, rec['ts_code'], rec['stock_name'], rec['score'],
                rec['price'], rec['reason'], 'v4.0',
                rec['market_cap'], rec['industry']
            ))
        
        conn.commit()
        conn.close()
    
    def add_holding(self, ts_code: str, buy_price: float, quantity: int, 
                   score: float = 0, strategy: str = 'v4.0'):
        """
        添加持仓
        
        Args:
            ts_code: 股票代码
            buy_price: 买入价格
            quantity: 买入数量
            score: 评分
            strategy: 策略
        """
        # 获取股票名称
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM stock_basic WHERE ts_code = ?", (ts_code,))
        result = cursor.fetchone()
        conn.close()
        
        stock_name = result[0] if result else ''
        
        # 计算成本
        cost_total = buy_price * quantity
        
        # 保存到持仓表
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO holdings 
            (ts_code, stock_name, buy_date, buy_price, quantity, cost_total, 
             current_price, current_value, profit_loss, profit_loss_pct, 
             status, strategy, score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'holding', ?, ?)
        """, (
            ts_code, stock_name, datetime.now().strftime('%Y-%m-%d'),
            buy_price, quantity, cost_total, buy_price, cost_total, 
            0.0, 0.0,  # 初始化profit_loss和profit_loss_pct为0
            strategy, score
        ))
        
        # 保存到交易记录
        cursor.execute("""
            INSERT INTO trade_history
            (ts_code, stock_name, action, trade_date, price, quantity, amount, 
             reason, strategy, score)
            VALUES (?, ?, 'buy', ?, ?, ?, ?, '手动买入', ?, ?)
        """, (
            ts_code, stock_name, datetime.now().strftime('%Y-%m-%d'),
            buy_price, quantity, cost_total, strategy, score
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 添加持仓: {stock_name}({ts_code}), {quantity}股 @ ¥{buy_price}")
    
    def update_holdings(self):
        """更新持仓信息"""
        logger.info("🔄 更新持仓信息...")
        
        conn_assistant = sqlite3.connect(self.assistant_db)
        holdings = pd.read_sql_query(
            "SELECT * FROM holdings WHERE status = 'holding'",
            conn_assistant
        )
        
        if holdings.empty:
            logger.info("📊 当前无持仓")
            conn_assistant.close()
            return
        
        # 获取最新价格
        conn_main = sqlite3.connect(self.db_path)
        
        for idx, holding in holdings.iterrows():
            ts_code = holding['ts_code']
            
            # 获取最新价格
            latest_data = pd.read_sql_query(f"""
                SELECT close_price FROM daily_trading_data
                WHERE ts_code = '{ts_code}'
                ORDER BY trade_date DESC
                LIMIT 1
            """, conn_main)
            
            if not latest_data.empty:
                current_price = latest_data.iloc[0]['close_price']
                current_value = current_price * holding['quantity']
                profit_loss = current_value - holding['cost_total']
                profit_loss_pct = profit_loss / holding['cost_total'] if holding['cost_total'] > 0 else 0
                
                # 更新数据库
                cursor = conn_assistant.cursor()
                cursor.execute("""
                    UPDATE holdings
                    SET current_price = ?,
                        current_value = ?,
                        profit_loss = ?,
                        profit_loss_pct = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (current_price, current_value, profit_loss, 
                     profit_loss_pct, holding['id']))
                
                conn_assistant.commit()
                
                logger.info(f"📊 {holding['stock_name']}: ¥{current_price:.2f}, "
                          f"盈亏{profit_loss_pct*100:.2f}%")
            else:
                # 没有找到数据，确保字段不为None
                logger.warning(f"⚠️ 未找到{holding['stock_name']}的最新数据")
                cursor = conn_assistant.cursor()
                cursor.execute("""
                    UPDATE holdings
                    SET current_price = COALESCE(current_price, buy_price),
                        current_value = COALESCE(current_value, cost_total),
                        profit_loss = COALESCE(profit_loss, 0),
                        profit_loss_pct = COALESCE(profit_loss_pct, 0),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (holding['id'],))
                conn_assistant.commit()
        
        conn_main.close()
        conn_assistant.close()
        
        logger.info("✅ 持仓更新完成")
    
    def check_stop_conditions(self) -> List[Dict]:
        """
        检查止盈止损条件
        
        Returns:
            需要提醒的持仓列表
        """
        logger.info("🔍 检查止盈止损条件...")
        
        take_profit_pct = float(self.get_config('take_profit_pct'))
        stop_loss_pct = float(self.get_config('stop_loss_pct'))
        
        conn = sqlite3.connect(self.assistant_db)
        holdings = pd.read_sql_query(
            "SELECT * FROM holdings WHERE status = 'holding'",
            conn
        )
        conn.close()
        
        alerts = []
        
        for idx, holding in holdings.iterrows():
            profit_pct = holding['profit_loss_pct']
            
            # 跳过无效数据
            if profit_pct is None or pd.isna(profit_pct):
                continue
            
            if profit_pct >= take_profit_pct:
                alerts.append({
                    'type': 'take_profit',
                    'ts_code': holding['ts_code'],
                    'stock_name': holding['stock_name'],
                    'buy_price': holding['buy_price'],
                    'current_price': holding['current_price'],
                    'profit_pct': profit_pct,
                    'message': f"🎉 {holding['stock_name']} 已达止盈条件！"
                               f"盈利{profit_pct*100:.2f}%，建议卖出！"
                })
                logger.warning(f"🎉 止盈提醒: {holding['stock_name']} +{profit_pct*100:.2f}%")
                
            elif profit_pct <= -stop_loss_pct:
                alerts.append({
                    'type': 'stop_loss',
                    'ts_code': holding['ts_code'],
                    'stock_name': holding['stock_name'],
                    'buy_price': holding['buy_price'],
                    'current_price': holding['current_price'],
                    'profit_pct': profit_pct,
                    'message': f"⚠️ {holding['stock_name']} 触发止损！"
                               f"亏损{abs(profit_pct)*100:.2f}%，建议止损！"
                })
                logger.warning(f"⚠️ 止损预警: {holding['stock_name']} {profit_pct*100:.2f}%")
        
        # 🆕 发送止盈止损通知
        if alerts:
            self._send_stop_condition_notification(alerts)
        
        return alerts
    
    def sell_holding(self, ts_code: str, sell_price: float, reason: str = '手动卖出'):
        """
        卖出持仓
        
        Args:
            ts_code: 股票代码
            sell_price: 卖出价格
            reason: 卖出原因
        """
        conn = sqlite3.connect(self.assistant_db)
        cursor = conn.cursor()
        
        # 获取持仓信息
        cursor.execute("""
            SELECT * FROM holdings 
            WHERE ts_code = ? AND status = 'holding'
        """, (ts_code,))
        
        holding = cursor.fetchone()
        
        if not holding:
            logger.warning(f"⚠️ 未找到持仓: {ts_code}")
            conn.close()
            return
        
        # 计算盈亏
        quantity = holding[5]  # quantity字段
        cost_total = holding[6]  # cost_total字段
        sell_amount = sell_price * quantity
        profit_loss = sell_amount - cost_total
        profit_loss_pct = profit_loss / cost_total
        
        # 更新持仓状态
        cursor.execute("""
            UPDATE holdings
            SET status = 'sold',
                current_price = ?,
                current_value = ?,
                profit_loss = ?,
                profit_loss_pct = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE ts_code = ? AND status = 'holding'
        """, (sell_price, sell_amount, profit_loss, profit_loss_pct, ts_code))
        
        # 记录交易
        cursor.execute("""
            INSERT INTO trade_history
            (ts_code, stock_name, action, trade_date, price, quantity, amount,
             reason, profit_loss, profit_loss_pct, strategy)
            VALUES (?, ?, 'sell', ?, ?, ?, ?, ?, ?, ?, 'v4.0')
        """, (
            ts_code, holding[2], datetime.now().strftime('%Y-%m-%d'),
            sell_price, quantity, sell_amount, reason, profit_loss, profit_loss_pct
        ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ 卖出成功: {holding[2]}({ts_code}), "
                   f"盈亏{profit_loss_pct*100:.2f}%")
    
    def generate_daily_report(self) -> str:
        """
        生成每日报告
        
        Returns:
            报告文本
        """
        logger.info("📝 生成每日报告...")
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 获取今日推荐
        conn = sqlite3.connect(self.assistant_db)
        recommendations = pd.read_sql_query(f"""
            SELECT * FROM daily_recommendations
            WHERE recommend_date = '{today}'
            ORDER BY score DESC
        """, conn)
        
        # 获取当前持仓
        holdings = pd.read_sql_query("""
            SELECT * FROM holdings WHERE status = 'holding'
        """, conn)
        
        # 获取今日交易
        trades_today = pd.read_sql_query(f"""
            SELECT * FROM trade_history
            WHERE trade_date = '{today}'
            ORDER BY created_at DESC
        """, conn)
        
        conn.close()
        
        # 生成报告
        report = f"""
{'='*80}
📊 智能交易助手 - 每日报告
{'='*80}

📅 日期: {today}
🕐 生成时间: {datetime.now().strftime('%H:%M:%S')}

{'='*80}
🎯 【今日选股推荐】
{'='*80}

"""
        
        if not recommendations.empty:
            for idx, rec in recommendations.iterrows():
                report += f"""
{idx+1}. {rec['stock_name']} ({rec['ts_code']})
   ⭐ 评分: {rec['score']:.1f}分 ({rec['grade'] if 'grade' in rec else ''})
   💰 价格: ¥{rec['price']:.2f}
   🏭 行业: {rec['industry']}
   💎 市值: {rec['market_cap']/100000000:.1f}亿
   📝 理由: {rec['reason'][:100]}...
   
"""
        else:
            report += "\n   暂无推荐股票\n\n"
        
        report += f"""
{'='*80}
📊 【当前持仓】
{'='*80}

"""
        
        if not holdings.empty:
            total_cost = 0
            total_value = 0
            total_profit = 0
            
            for idx, holding in holdings.iterrows():
                total_cost += holding['cost_total']
                total_value += holding['current_value']
                total_profit += holding['profit_loss']
                
                report += f"""
{idx+1}. {holding['stock_name']} ({holding['ts_code']})
   📅 买入日期: {holding['buy_date']}
   💰 买入价格: ¥{holding['buy_price']:.2f}
   📊 当前价格: ¥{holding['current_price']:.2f}
   📈 数量: {holding['quantity']}股
   💵 成本: ¥{holding['cost_total']:.2f}
   💰 市值: ¥{holding['current_value']:.2f}
   {"📈" if holding['profit_loss'] > 0 else "📉"} 盈亏: ¥{holding['profit_loss']:.2f} ({holding['profit_loss_pct']*100:.2f}%)
   
"""
            
            total_profit_pct = total_profit / total_cost if total_cost > 0 else 0
            
            report += f"""
【持仓汇总】
   总成本: ¥{total_cost:.2f}
   总市值: ¥{total_value:.2f}
   总盈亏: ¥{total_profit:.2f} ({total_profit_pct*100:.2f}%)
   
"""
        else:
            report += "\n   当前无持仓\n\n"
        
        report += f"""
{'='*80}
📝 【今日交易】
{'='*80}

"""
        
        if not trades_today.empty:
            for idx, trade in trades_today.iterrows():
                action_emoji = "🟢" if trade['action'] == 'buy' else "🔴"
                action_text = "买入" if trade['action'] == 'buy' else "卖出"
                
                report += f"""
{action_emoji} {action_text}: {trade['stock_name']} ({trade['ts_code']})
   💰 价格: ¥{trade['price']:.2f}
   📊 数量: {trade['quantity']}股
   💵 金额: ¥{trade['amount']:.2f}
"""
                
                if trade['action'] == 'sell' and trade['profit_loss']:
                    report += f"   {'📈' if trade['profit_loss'] > 0 else '📉'} 盈亏: ¥{trade['profit_loss']:.2f} ({trade['profit_loss_pct']*100:.2f}%)\n"
                
                report += f"   📝 原因: {trade['reason']}\n\n"
        else:
            report += "\n   今日无交易\n\n"
        
        report += f"""
{'='*80}
💡 【风险提示】
{'='*80}

⚠️ 本系统仅供参考，不构成投资建议
⚠️ 股市有风险，投资需谨慎
⚠️ 请严格执行止盈止损纪律
⚠️ 建议单只股票仓位不超过20%

{'='*80}
📊 报告结束
{'='*80}
"""
        
        # 🆕 发送每日报告通知
        self._send_daily_report_notification(report)
        
        return report
    
    def _send_stock_selection_notification(self, recommendations: List[Dict]):
        """
        发送选股通知
        
        Args:
            recommendations: 推荐股票列表
        """
        if not self.notifier:
            return
        
        if not recommendations:
            return
        
        try:
            # 构建通知内容
            title = f"📊 每日选股推荐 ({datetime.now().strftime('%Y-%m-%d')})"
            
            content = f"✅ 今日选出 {len(recommendations)} 只优质股票：\n\n"
            
            for i, rec in enumerate(recommendations, 1):
                content += f"{i}. {rec['stock_name']} ({rec['ts_code']})\n"
                content += f"   ⭐ 评分: {rec['score']:.1f}分\n"
                content += f"   💰 价格: ¥{rec['price']:.2f}\n"
                content += f"   🏭 行业: {rec.get('industry', 'N/A')}\n"
                content += f"   📝 理由: {rec.get('reason', 'N/A')[:50]}...\n\n"
            
            content += "\n⚠️ 请人工审核后决策，不构成投资建议！"
            
            # 发送通知
            self.notifier.send_notification(title, content, urgent=False)
            logger.info("📧 选股通知已发送")
            
        except Exception as e:
            logger.error(f"❌ 发送选股通知失败: {e}")
    
    def _send_stop_condition_notification(self, alerts: List[Dict]):
        """
        发送止盈止损通知
        
        Args:
            alerts: 提醒列表
        """
        if not self.notifier:
            return
        
        if not alerts:
            return
        
        try:
            title = "⚠️ 止盈止损提醒"
            
            content = f"检测到 {len(alerts)} 只股票触发条件：\n\n"
            
            for alert in alerts:
                if alert['type'] == 'take_profit':
                    content += f"🎉 止盈: {alert['stock_name']} ({alert['ts_code']})\n"
                    content += f"   买入: ¥{alert['buy_price']:.2f}\n"
                    content += f"   当前: ¥{alert['current_price']:.2f}\n"
                    content += f"   盈利: {alert['profit_pct']*100:.2f}%\n"
                    content += f"   建议: 考虑止盈卖出\n\n"
                else:
                    content += f"⚠️ 止损: {alert['stock_name']} ({alert['ts_code']})\n"
                    content += f"   买入: ¥{alert['buy_price']:.2f}\n"
                    content += f"   当前: ¥{alert['current_price']:.2f}\n"
                    content += f"   亏损: {abs(alert['profit_pct'])*100:.2f}%\n"
                    content += f"   建议: 及时止损！\n\n"
            
            content += "\n⚠️ 请及时处理，严格执行交易纪律！"
            
            # 发送通知（紧急）
            self.notifier.send_notification(title, content, urgent=True)
            logger.info("📧 止盈止损通知已发送")
            
        except Exception as e:
            logger.error(f"❌ 发送止盈止损通知失败: {e}")
    
    def _send_daily_report_notification(self, report: str):
        """
        发送每日报告通知
        
        Args:
            report: 报告内容
        """
        if not self.notifier:
            return
        
        try:
            title = f"📊 每日交易报告 ({datetime.now().strftime('%Y-%m-%d')})"
            
            # 报告内容可能很长，只发送摘要
            lines = report.split('\n')
            summary_lines = []
            in_summary = False
            
            for line in lines:
                if '【今日选股推荐】' in line or '【当前持仓】' in line or '【持仓汇总】' in line:
                    in_summary = True
                    summary_lines.append(line)
                elif in_summary and len(summary_lines) < 30:
                    summary_lines.append(line)
                elif '【今日交易】' in line:
                    break
            
            content = '\n'.join(summary_lines[:30])
            content += "\n\n📱 完整报告请登录系统查看"
            
            # 发送通知
            self.notifier.send_notification(title, content, urgent=False)
            logger.info("📧 每日报告通知已发送")
            
        except Exception as e:
            logger.error(f"❌ 发送每日报告通知失败: {e}")
    
    def setup_email_notification(self, receiver_emails: List[str], 
                                 sender_email: str = None, 
                                 sender_password: str = None):
        """
        快速配置邮件通知
        
        Args:
            receiver_emails: 接收邮箱列表（必填）
            sender_email: 发件邮箱（可选，使用默认）
            sender_password: 发件邮箱授权码（可选，使用默认）
        """
        # 加载或创建配置
        config_file = "notification_config.json"
        
        if Path(config_file).exists():
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
        else:
            config = {
                "enabled": True,
                "email": {
                    "enabled": True,
                    "smtp_server": "smtp.qq.com",
                    "smtp_port": 587,
                    "sender_email": "",
                    "sender_password": "",
                    "receiver_emails": []
                },
                "wechat_work": {"enabled": False, "webhook_url": ""},
                "dingtalk": {"enabled": False, "webhook_url": "", "secret": ""},
                "serverchan": {"enabled": False, "sendkey": ""},
                "bark": {"enabled": False, "device_key": ""}
            }
        
        # 更新接收邮箱
        config['enabled'] = True
        config['email']['enabled'] = True
        config['email']['receiver_emails'] = receiver_emails
        
        # 如果提供了发件邮箱信息，更新它
        if sender_email:
            config['email']['sender_email'] = sender_email
        if sender_password:
            config['email']['sender_password'] = sender_password
        
        # 保存配置
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        
        # 重新初始化通知服务
        if NOTIFICATION_AVAILABLE:
            try:
                self.notifier = NotificationService(config_file)
                logger.info(f"✅ 邮件通知配置成功！接收邮箱: {len(receiver_emails)}个")
                for email in receiver_emails:
                    logger.info(f"   📧 {email}")
                return True
            except Exception as e:
                logger.error(f"❌ 通知服务初始化失败: {e}")
                return False
        return False
    
    def add_receiver_email(self, email: str):
        """
        添加接收邮箱
        
        Args:
            email: 邮箱地址
        """
        config_file = "notification_config.json"
        
        if not Path(config_file).exists():
            logger.error("❌ 请先配置邮件通知")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        receivers = config['email']['receiver_emails']
        if email not in receivers:
            receivers.append(email)
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 已添加接收邮箱: {email}")
            return True
        else:
            logger.info(f"ℹ️ 邮箱已存在: {email}")
            return False
    
    def remove_receiver_email(self, email: str):
        """
        删除接收邮箱
        
        Args:
            email: 邮箱地址
        """
        config_file = "notification_config.json"
        
        if not Path(config_file).exists():
            logger.error("❌ 配置文件不存在")
            return False
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        receivers = config['email']['receiver_emails']
        if email in receivers:
            receivers.remove(email)
            
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2, ensure_ascii=False)
            
            logger.info(f"✅ 已删除接收邮箱: {email}")
            return True
        else:
            logger.info(f"ℹ️ 邮箱不存在: {email}")
            return False
    
    def list_receiver_emails(self) -> List[str]:
        """
        列出所有接收邮箱
        
        Returns:
            接收邮箱列表
        """
        config_file = "notification_config.json"
        
        if not Path(config_file).exists():
            logger.warning("⚠️ 配置文件不存在")
            return []
        
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        return config.get('email', {}).get('receiver_emails', [])
    
    def get_statistics(self) -> Dict:
        """获取统计数据"""
        conn = sqlite3.connect(self.assistant_db)
        
        # 总交易次数
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trade_history")
        total_trades = cursor.fetchone()[0]
        
        # 买入次数
        cursor.execute("SELECT COUNT(*) FROM trade_history WHERE action = 'buy'")
        buy_count = cursor.fetchone()[0]
        
        # 卖出次数
        cursor.execute("SELECT COUNT(*) FROM trade_history WHERE action = 'sell'")
        sell_count = cursor.fetchone()[0]
        
        # 盈利次数
        cursor.execute("SELECT COUNT(*) FROM trade_history WHERE action = 'sell' AND profit_loss > 0")
        profit_count = cursor.fetchone()[0]
        
        # 总盈亏
        cursor.execute("SELECT SUM(profit_loss) FROM trade_history WHERE action = 'sell'")
        total_profit = cursor.fetchone()[0] or 0
        
        # 胜率
        win_rate = profit_count / sell_count if sell_count > 0 else 0
        
        # 当前持仓数
        cursor.execute("SELECT COUNT(*) FROM holdings WHERE status = 'holding'")
        holding_count = cursor.fetchone()[0]
        
        # 持仓总市值
        cursor.execute("SELECT SUM(current_value) FROM holdings WHERE status = 'holding'")
        holding_value = cursor.fetchone()[0] or 0
        
        # 持仓总盈亏
        cursor.execute("SELECT SUM(profit_loss) FROM holdings WHERE status = 'holding'")
        holding_profit = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return {
            'total_trades': total_trades,
            'buy_count': buy_count,
            'sell_count': sell_count,
            'profit_count': profit_count,
            'win_rate': win_rate,
            'total_profit': total_profit,
            'holding_count': holding_count,
            'holding_value': holding_value,
            'holding_profit': holding_profit
        }


# 便捷函数
def create_assistant() -> TradingAssistant:
    """创建交易助手实例"""
    return TradingAssistant()


if __name__ == "__main__":
    # 测试代码
    assistant = TradingAssistant()
    
    print("🚀 智能交易助手测试")
    print("="*80)
    
    # 测试每日选股
    print("\n📊 测试每日选股...")
    recommendations = assistant.daily_stock_scan(top_n=5)
    
    if recommendations:
        print(f"\n✅ 找到{len(recommendations)}个推荐:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec['stock_name']}({rec['ts_code']}): {rec['score']:.1f}分")
    
    # 生成报告
    print("\n📝 生成每日报告...")
    report = assistant.generate_daily_report()
    print(report)
    
    # 统计
    stats = assistant.get_statistics()
    print("\n📊 统计数据:")
    print(f"   总交易: {stats['total_trades']}次")
    print(f"   当前持仓: {stats['holding_count']}只")
    print(f"   胜率: {stats['win_rate']*100:.1f}%")
    
    print("\n✅ 测试完成！")


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
机构级V13系统_v068_永久版
中国股市专业分析系统 - 永久稳定版本
作者: 中国股市专业人士
版本: v068 永久版
"""

import streamlit as st
import pandas as pd
import numpy as np
import tushare as ts
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import time
import warnings
import os
from typing import Dict, List, Tuple, Optional

warnings.filterwarnings('ignore')

class 机构级V13系统:
    """机构级V13系统核心类"""
    
    def __init__(self):
        self.token = self.load_tushare_token()
        if self.token:
            ts.set_token(self.token)
            self.pro = ts.pro_api()
        else:
            st.error("❌ Tushare Token未配置，请检查配置文件")
            self.pro = None
        
        self.cache = {}
        self.system_name = "机构级V13系统_v068_永久版"
        
    def load_tushare_token(self) -> Optional[str]:
        """加载Tushare Token"""
        token_files = ['.tushare_token', 'tushare_token.txt', '.env']
        
        for token_file in token_files:
            if os.path.exists(token_file):
                try:
                    with open(token_file, 'r', encoding='utf-8') as f:
                        content = f.read().strip()
                        if content and len(content) > 20:
                            return content
                except Exception:
                    continue
        
        # 尝试从环境变量获取
        token = os.getenv('TUSHARE_TOKEN')
        if token:
            return token
            
        return None
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if not self.pro:
            return pd.DataFrame()
            
        try:
            # 获取A股列表
            stock_list = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market'
            )
            return stock_list
        except Exception as e:
            st.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_stock_data(self, ts_code: str, days: int = 30) -> pd.DataFrame:
        """获取股票数据"""
        if not self.pro:
            return pd.DataFrame()
            
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df = df.sort_values('trade_date')
                
                # 计算技术指标
                df = self.calculate_technical_indicators(df)
                
            return df
        except Exception as e:
            st.error(f"获取股票数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """计算技术指标"""
        if df.empty:
            return df
            
        try:
            # 移动平均线
            df['ma5'] = df['close'].rolling(window=5).mean()
            df['ma10'] = df['close'].rolling(window=10).mean()
            df['ma20'] = df['close'].rolling(window=20).mean()
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # MACD
            exp1 = df['close'].ewm(span=12).mean()
            exp2 = df['close'].ewm(span=26).mean()
            df['macd'] = exp1 - exp2
            df['signal'] = df['macd'].ewm(span=9).mean()
            df['histogram'] = df['macd'] - df['signal']
            
            # 布林带
            df['bb_middle'] = df['close'].rolling(window=20).mean()
            bb_std = df['close'].rolling(window=20).std()
            df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
            df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
            
            return df
        except Exception as e:
            st.error(f"计算技术指标失败: {e}")
            return df
    
    def analyze_stock(self, ts_code: str) -> Dict:
        """分析股票"""
        df = self.get_stock_data(ts_code, 60)
        if df.empty:
            return {}
            
        try:
            latest = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else latest
            
            analysis = {
                'ts_code': ts_code,
                'current_price': latest['close'],
                'change_pct': ((latest['close'] - prev['close']) / prev['close']) * 100,
                'volume': latest['vol'],
                'turnover': latest['amount'],
                'ma5': latest.get('ma5', 0),
                'ma10': latest.get('ma10', 0),
                'ma20': latest.get('ma20', 0),
                'rsi': latest.get('rsi', 50),
                'macd': latest.get('macd', 0),
                'signal': latest.get('signal', 0)
            }
            
            # 技术分析评分
            score = self.calculate_technical_score(latest)
            analysis['technical_score'] = score
            
            # 交易信号
            signal = self.generate_trading_signal(df)
            analysis['trading_signal'] = signal
            
            return analysis
        except Exception as e:
            st.error(f"分析股票失败: {e}")
            return {}
    
    def calculate_technical_score(self, data: pd.Series) -> float:
        """计算技术分析评分"""
        score = 50  # 基础分数
        
        try:
            # MA趋势分析
            if data.get('ma5', 0) > data.get('ma10', 0) > data.get('ma20', 0):
                score += 20
            elif data.get('ma5', 0) > data.get('ma10', 0):
                score += 10
            
            # RSI分析
            rsi = data.get('rsi', 50)
            if 30 < rsi < 70:
                score += 15
            elif rsi < 30:
                score += 25  # 超卖
            elif rsi > 70:
                score -= 15  # 超买
            
            # MACD分析
            macd = data.get('macd', 0)
            signal = data.get('signal', 0)
            if macd > signal and macd > 0:
                score += 15
            elif macd > signal:
                score += 10
            
            return min(max(score, 0), 100)
        except Exception:
            return 50
    
    def generate_trading_signal(self, df: pd.DataFrame) -> str:
        """生成交易信号"""
        if df.empty or len(df) < 5:
            return "数据不足"
            
        try:
            latest = df.iloc[-1]
            
            # 多重条件判断
            signals = []
            
            # MA信号
            if latest.get('ma5', 0) > latest.get('ma10', 0) > latest.get('ma20', 0):
                signals.append("买入")
            elif latest.get('ma5', 0) < latest.get('ma10', 0) < latest.get('ma20', 0):
                signals.append("卖出")
            
            # RSI信号
            rsi = latest.get('rsi', 50)
            if rsi < 30:
                signals.append("买入")
            elif rsi > 70:
                signals.append("卖出")
            
            # MACD信号
            if latest.get('macd', 0) > latest.get('signal', 0):
                signals.append("买入")
            else:
                signals.append("卖出")
            
            # 综合判断
            buy_count = signals.count("买入")
            sell_count = signals.count("卖出")
            
            if buy_count > sell_count:
                return "🟢 买入信号"
            elif sell_count > buy_count:
                return "🔴 卖出信号"
            else:
                return "🟡 观望"
                
        except Exception:
            return "🟡 观望"
    
    def create_stock_chart(self, ts_code: str) -> go.Figure:
        """创建股票图表"""
        df = self.get_stock_data(ts_code, 60)
        if df.empty:
            return go.Figure()
            
        try:
            fig = go.Figure()
            
            # K线图
            fig.add_trace(go.Candlestick(
                x=df['trade_date'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='K线'
            ))
            
            # 移动平均线
            if 'ma5' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['trade_date'],
                    y=df['ma5'],
                    mode='lines',
                    name='MA5',
                    line=dict(color='orange', width=1)
                ))
            
            if 'ma20' in df.columns:
                fig.add_trace(go.Scatter(
                    x=df['trade_date'],
                    y=df['ma20'],
                    mode='lines',
                    name='MA20',
                    line=dict(color='blue', width=1)
                ))
            
            fig.update_layout(
                title=f'{ts_code} 股价走势图',
                xaxis_title='日期',
                yaxis_title='价格',
                height=500,
                showlegend=True
            )
            
            return fig
        except Exception as e:
            st.error(f"创建图表失败: {e}")
            return go.Figure()

def main():
    """主函数"""
    st.set_page_config(
        page_title="机构级V13系统_v068_永久版",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # 初始化系统
    if 'system' not in st.session_state:
        st.session_state.system = 机构级V13系统()
    
    system = st.session_state.system
    
    # 页面标题
    st.title("📈 机构级V13系统_v068_永久版")
    st.markdown("---")
    
    # 侧边栏
    with st.sidebar:
        st.header("🎯 系统控制")
        
        # 功能选择
        function = st.selectbox(
            "选择功能",
            ["股票分析", "市场扫描", "技术指标", "系统状态"]
        )
        
        st.markdown("---")
        st.info("💡 v068永久版特性:\n- 稳定的数据接口\n- 优化的技术指标\n- 智能交易信号\n- 实时市场分析")
    
    # 主要内容区域
    if function == "股票分析":
        st.header("📊 股票分析")
        
        # 股票代码输入
        col1, col2 = st.columns([3, 1])
        with col1:
            stock_code = st.text_input("输入股票代码 (如: 000001.SZ)", value="000001.SZ")
        with col2:
            analyze_btn = st.button("🔍 分析", type="primary")
        
        if analyze_btn and stock_code:
            with st.spinner("正在分析股票..."):
                analysis = system.analyze_stock(stock_code)
                
                if analysis:
                    # 显示分析结果
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("当前价格", f"{analysis['current_price']:.2f}")
                    with col2:
                        change_pct = analysis['change_pct']
                        st.metric("涨跌幅", f"{change_pct:.2f}%%", delta=f"{change_pct:.2f}%%")
                    with col3:
                        st.metric("技术评分", f"{analysis['technical_score']:.1f}")
                    with col4:
                        st.metric("交易信号", analysis['trading_signal'])
                    
                    # 股票图表
                    st.subheader("📈 价格走势")
                    chart = system.create_stock_chart(stock_code)
                    if chart.data:
                        st.plotly_chart(chart, use_container_width=True)
                    
                    # 技术指标详情
                    st.subheader("📋 技术指标详情")
                    tech_col1, tech_col2 = st.columns(2)
                    
                    with tech_col1:
                        st.write("**移动平均线**")
                        st.write(f"MA5: {analysis.get('ma5', 0):.2f}")
                        st.write(f"MA10: {analysis.get('ma10', 0):.2f}")
                        st.write(f"MA20: {analysis.get('ma20', 0):.2f}")
                    
                    with tech_col2:
                        st.write("**技术指标**")
                        st.write(f"RSI: {analysis.get('rsi', 0):.2f}")
                        st.write(f"MACD: {analysis.get('macd', 0):.4f}")
                        st.write(f"Signal: {analysis.get('signal', 0):.4f}")
                else:
                    st.error("❌ 分析失败，请检查股票代码")
    
    elif function == "市场扫描":
        st.header("🔍 市场扫描")
        
        scan_btn = st.button("🚀 开始扫描", type="primary")
        
        if scan_btn:
            with st.spinner("正在扫描市场..."):
                stock_list = system.get_stock_list()
                
                if not stock_list.empty:
                    # 随机选择一些股票进行分析
                    sample_stocks = stock_list.sample(min(10, len(stock_list)))
                    
                    results = []
                    progress_bar = st.progress(0)
                    
                    for i, (_, stock) in enumerate(sample_stocks.iterrows()):
                        analysis = system.analyze_stock(stock['ts_code'])
                        if analysis:
                            results.append({
                                '股票代码': stock['ts_code'],
                                '股票名称': stock['name'],
                                '当前价格': analysis['current_price'],
                                '涨跌幅': f"{analysis['change_pct']:.2f}%%",
                                '技术评分': analysis['technical_score'],
                                '交易信号': analysis['trading_signal']
                            })
                        
                        progress_bar.progress((i + 1) / len(sample_stocks))
                        time.sleep(0.1)  # 避免请求过快
                    
                    if results:
                        df_results = pd.DataFrame(results)
                        st.subheader("📊 扫描结果")
                        st.dataframe(df_results, use_container_width=True)
                        
                        # 按技术评分排序
                        top_stocks = df_results.nlargest(5, '技术评分')
                        st.subheader("🏆 技术评分TOP5")
                        st.dataframe(top_stocks, use_container_width=True)
                    else:
                        st.warning("⚠️ 未获取到有效数据")
                else:
                    st.error("❌ 获取股票列表失败")
    
    elif function == "技术指标":
        st.header("📈 技术指标说明")
        
        st.markdown("""
        ### 🎯 技术指标解释
        
        **移动平均线 (MA)**
        - MA5: 5日移动平均线，短期趋势
        - MA10: 10日移动平均线，中短期趋势
        - MA20: 20日移动平均线，中期趋势
        
        **相对强弱指数 (RSI)**
        - RSI < 30: 超卖区域，可能反弹
        - 30 < RSI < 70: 正常区域
        - RSI > 70: 超买区域，可能回调
        
        **MACD指标**
        - MACD > Signal: 多头信号
        - MACD < Signal: 空头信号
        - 柱状图: 动量变化
        
        **技术评分**
        - 0-30: 弱势
        - 30-70: 中性
        - 70-100: 强势
        """)
    
    elif function == "系统状态":
        st.header("⚙️ 系统状态")
        
        # 系统信息
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📋 系统信息")
            st.write(f"**系统名称**: {system.system_name}")
            st.write(f"**版本**: v068 永久版")
            st.write(f"**状态**: {'🟢 正常' if system.pro else '🔴 异常'}")
            st.write(f"**Token状态**: {'🟢 已配置' if system.token else '🔴 未配置'}")
        
        with col2:
            st.subheader("🔧 功能模块")
            st.write("✅ 股票数据获取")
            st.write("✅ 技术指标计算")
            st.write("✅ 交易信号生成")
            st.write("✅ 图表可视化")
            st.write("✅ 市场扫描")
        
        # Token配置
        st.subheader("🔑 Token配置")
        if not system.token:
            st.error("❌ Tushare Token未配置")
            st.info("请在项目根目录创建 .tushare_token 文件并填入您的Token")
        else:
            st.success("✅ Token配置正常")
            
        # 测试连接
        if st.button("🔍 测试连接"):
            if system.pro:
                try:
                    test_data = system.pro.stock_basic(list_status='L', limit=1)
                    if not test_data.empty:
                        st.success("✅ 连接测试成功")
                    else:
                        st.error("❌ 连接测试失败")
                except Exception as e:
                    st.error(f"❌ 连接测试失败: {e}")
            else:
                st.error("❌ 无法测试连接，请检查Token配置")
    
    # 页脚
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #666;'>" +
        "机构级V13系统_v068_永久版 | 中国股市专业分析平台" +
        "</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()

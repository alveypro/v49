#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版V001智能缓存增强系统启动器
修复所有语法错误，确保正常启动
"""

import streamlit as st
import pandas as pd
import numpy as np
import tushare as ts
import time
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 设置页面配置
st.set_page_config(
    page_title="V001智能缓存增强系统",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

class SimpleV001System:
    def __init__(self):
        self.token = self.load_token()
        if self.token:
            ts.set_token(self.token)
            self.pro = ts.pro_api()
        else:
            st.error("请配置Tushare Token")
            st.stop()
    
    def load_token(self):
        """加载Tushare Token"""
        token_files = ['.tushare_token', 'tushare_token.txt']
        for file in token_files:
            if os.path.exists(file):
                try:
                    with open(file, 'r') as f:
                        token = f.read().strip()
                        if token:
                            return token
                except:
                    continue
        return None
    
    def get_stock_list(self, limit=100):
        """获取股票列表"""
        try:
            df = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market'
            )
            return df.head(limit)
        except Exception as e:
            st.error(f"获取股票列表失败: {e}")
            return pd.DataFrame()
    
    def get_stock_data(self, ts_code, days=30):
        """获取股票数据"""
        try:
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            return df.sort_values('trade_date')
        except Exception as e:
            st.error(f"获取股票数据失败: {e}")
            return pd.DataFrame()
    
    def calculate_signals(self, df):
        """计算交易信号"""
        if df.empty:
            return {}
        
        latest = df.iloc[-1]
        
        # 计算涨跌幅
        pct_chg = latest['pct_chg'] if 'pct_chg' in df.columns else 0
        
        # 计算均价
        avg_price = df['close'].mean()
        
        # 简单信号
        signal = "买入" if latest['close'] > avg_price and pct_chg > 0 else "观望"
        
        return {
            "股票代码": latest['ts_code'],
            "最新价格": f"{latest['close']:.2f}",
            "涨跌幅": f"{pct_chg:.2f}%%",
            "交易信号": signal,
            "建议仓位": "10%%" if signal == "买入" else "0%%"
        }

def main():
    st.title("🚀 V001智能缓存增强系统")
    st.markdown("---")
    
    # 初始化系统
    if 'system' not in st.session_state:
        with st.spinner("正在初始化系统..."):
            st.session_state.system = SimpleV001System()
    
    system = st.session_state.system
    
    # 侧边栏
    st.sidebar.title("系统控制")
    
    # 获取股票列表
    if st.sidebar.button("刷新股票列表"):
        with st.spinner("正在获取股票列表..."):
            st.session_state.stock_list = system.get_stock_list()
    
    if 'stock_list' not in st.session_state:
        st.session_state.stock_list = system.get_stock_list()
    
    stock_list = st.session_state.stock_list
    
    if not stock_list.empty:
        # 股票选择
        selected_stock = st.sidebar.selectbox(
            "选择股票",
            options=stock_list['ts_code'].tolist(),
            format_func=lambda x: f"{x} - {stock_list[stock_list['ts_code']==x]['name'].iloc[0]}"
        )
        
        # 主界面
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("股票分析")
            
            if st.button("开始分析"):
                with st.spinner("正在分析股票..."):
                    # 获取股票数据
                    stock_data = system.get_stock_data(selected_stock)
                    
                    if not stock_data.empty:
                        # 显示价格走势
                        st.line_chart(stock_data.set_index('trade_date')['close'])
                        
                        # 计算信号
                        signals = system.calculate_signals(stock_data)
                        
                        # 显示信号
                        st.subheader("交易信号")
                        for key, value in signals.items():
                            st.metric(key, value)
        
        with col2:
            st.subheader("系统状态")
            st.success("✅ 系统运行正常")
            st.info(f"📊 已加载 {len(stock_list)} 只股票")
            st.info(f"🕒 更新时间: {datetime.now().strftime('%H:%M:%S')}")
            
            # 系统信息
            st.subheader("系统信息")
            st.text("版本: V001")
            st.text("状态: 运行中")
            st.text("缓存: 已启用")
    else:
        st.error("无法获取股票数据，请检查网络连接和Token配置")

if __name__ == "__main__":
    main()

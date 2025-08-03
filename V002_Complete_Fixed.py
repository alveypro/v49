# coding: utf-8
"""
V002 Optimized Trading System

Author: Trae AI
Date: 2023-10-27

An enhanced and refactored version of the trading system, focusing on performance, 
error handling, and a modular structure.
"""

import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime, timedelta
import logging

# ==============================================================================
# 1. System Configuration & Constants
# ==============================================================================

# --- System Information ---
SYSTEM_NAME = "V002 高性能交易决策系统"
SYSTEM_VERSION = "2.0.0"
BASE_ARCHITECTURE = "V001 Complete Enhanced"

# --- File & Directory Paths ---
CACHE_DIR = "cache_v002"
LOG_FILE = "system_v002.log"

# --- Tushare API Configuration ---
TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN', 'YOUR_TUSHARE_TOKEN')
TUSHARE_AVAILABLE = False
pro = None

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# 2. Tushare Pro API Initialization
# ==============================================================================

def initialize_tushare():
    """Initialize Tushare Pro API connection."""
    global TUSHARE_AVAILABLE, pro
    
    try:
        import tushare as ts
        if TUSHARE_TOKEN and TUSHARE_TOKEN != 'YOUR_TUSHARE_TOKEN':
            pro = ts.pro_api(TUSHARE_TOKEN)
            # Test connection
            test_data = pro.trade_cal(exchange='', start_date='20240101', end_date='20240102')
            if not test_data.empty:
                TUSHARE_AVAILABLE = True
                logger.info("Tushare Pro API initialized successfully")
            else:
                logger.warning("Tushare Pro API test failed")
        else:
            logger.warning("Tushare token not configured")
    except Exception as e:
        logger.error(f"Failed to initialize Tushare Pro API: {e}")
        TUSHARE_AVAILABLE = False

# ==============================================================================
# 3. Caching System
# ==============================================================================

if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def get_cache_path(key):
    """Generates a cache file path for a given key."""
    return os.path.join(CACHE_DIR, f"{key}.json")

def save_to_cache(key, data, ttl_minutes=60):
    """Saves data to a JSON cache file with a TTL."""
    if data is None or (hasattr(data, 'empty') and data.empty):
        return
    
    cache_data = {
        'timestamp': datetime.now().isoformat(),
        'ttl_minutes': ttl_minutes,
        'data': data.to_dict() if hasattr(data, 'to_dict') else data
    }
    
    try:
        with open(get_cache_path(key), 'w', encoding='utf-8') as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Data cached successfully: {key}")
    except Exception as e:
        logger.error(f"Failed to save cache {key}: {e}")

def load_from_cache(key):
    """Loads data from cache if it exists and is not expired."""
    cache_file = get_cache_path(key)
    
    if not os.path.exists(cache_file):
        return None
    
    try:
        with open(cache_file, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        # Check if cache is expired
        cache_time = datetime.fromisoformat(cache_data['timestamp'])
        ttl = timedelta(minutes=cache_data.get('ttl_minutes', 60))
        
        if datetime.now() - cache_time > ttl:
            logger.info(f"Cache expired: {key}")
            return None
        
        logger.info(f"Cache hit: {key}")
        return cache_data['data']
    
    except Exception as e:
        logger.error(f"Failed to load cache {key}: {e}")
        return None

# ==============================================================================
# 4. Core Data Fetching Functions
# ==============================================================================

def get_latest_trade_date():
    """Get the latest trading date."""
    if not TUSHARE_AVAILABLE:
        return datetime.now().date()
    
    try:
        cal_data = pro.trade_cal(exchange='', start_date='20240101', end_date=datetime.now().strftime('%Y%m%d'))
        latest_trade = cal_data[cal_data['is_open'] == 1]['cal_date'].max()
        return datetime.strptime(latest_trade, '%Y%m%d').date()
    except Exception as e:
        logger.error(f"Failed to get latest trade date: {e}")
        return datetime.now().date()

def fetch_daily_basic(trade_date=None):
    """Fetch daily basic data for all stocks."""
    if trade_date is None:
        trade_date = get_latest_trade_date().strftime('%Y%m%d')
    
    cache_key = f"daily_basic_{trade_date}"
    cached_data = load_from_cache(cache_key)
    
    if cached_data is not None:
        return pd.DataFrame(cached_data)
    
    if not TUSHARE_AVAILABLE:
        logger.warning("Tushare not available, returning empty DataFrame")
        return pd.DataFrame()
    
    try:
        data = pro.daily_basic(trade_date=trade_date)
        save_to_cache(cache_key, data, ttl_minutes=240)  # Cache for 4 hours
        return data
    except Exception as e:
        logger.error(f"Failed to fetch daily basic data: {e}")
        return pd.DataFrame()

# ==============================================================================
# 5. Data Processing Modules
# ==============================================================================

class DataModules:
    """Core data processing modules for the trading system."""
    
    def __init__(self):
        self.latest_trade_date = get_latest_trade_date()
    
    def short_term_surge(self, limit=20):
        """Identify stocks with short-term surge potential."""
        try:
            daily_data = fetch_daily_basic()
            if daily_data.empty:
                return pd.DataFrame()
            
            # Filter for stocks with high turnover and price change
            surge_stocks = daily_data[
                (daily_data['turnover_rate'] > 5) &
                (daily_data['pe'] > 0) &
                (daily_data['pe'] < 50)
            ].sort_values('turnover_rate', ascending=False).head(limit)
            
            return surge_stocks[['ts_code', 'turnover_rate', 'pe', 'pb', 'total_mv']]
        
        except Exception as e:
            logger.error(f"Error in short_term_surge: {e}")
            return pd.DataFrame()
    
    def value_investing(self, limit=20):
        """Identify value investment opportunities."""
        try:
            daily_data = fetch_daily_basic()
            if daily_data.empty:
                return pd.DataFrame()
            
            # Filter for value stocks (low PE, low PB)
            value_stocks = daily_data[
                (daily_data['pe'] > 0) &
                (daily_data['pe'] < 15) &
                (daily_data['pb'] > 0) &
                (daily_data['pb'] < 2)
            ].sort_values('pe', ascending=True).head(limit)
            
            return value_stocks[['ts_code', 'pe', 'pb', 'total_mv', 'turnover_rate']]
        
        except Exception as e:
            logger.error(f"Error in value_investing: {e}")
            return pd.DataFrame()

# ==============================================================================
# 6. Streamlit UI Management
# ==============================================================================

class TradingSystemUI:
    """Main UI class for the trading system."""
    
    def __init__(self):
        self.system_name = SYSTEM_NAME
        self.version = SYSTEM_VERSION
        self.base_architecture = BASE_ARCHITECTURE
        self.latest_trade_date = get_latest_trade_date()
        self.data_modules = DataModules()
        self.selected_module = None
        
        # Define available modules
        self.modules = {
            "🏠 系统首页": self.render_home,
            "📊 市场总览": self.render_market_overview,
            "🚀 短线暴涨": self.render_short_term_surge,
            "💎 价值挖掘": self.render_value_investing,
        }
    
    def run(self):
        st.set_page_config(page_title=self.system_name, layout="wide")
        self.apply_custom_css()
        self.render_header()
        self.render_sidebar()
        self.render_main_content()
        self.render_footer()
    
    def apply_custom_css(self):
        st.markdown("""
        <style>
        .main-header { 
            padding: 2rem; 
            background: linear-gradient(90deg, #1e3c72 0%, #2a5298 100%); 
            color: white; 
            text-align: center; 
            border-radius: 10px; 
            margin-bottom: 2rem; 
        }
        .stApp { background-color: #f0f2f6; }
        .metric-card {
            background: white;
            padding: 1rem;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin: 0.5rem 0;
        }
        </style>
        """, unsafe_allow_html=True)
    
    def render_header(self):
        st.markdown(f'''
        <div class="main-header">
            <h1>🚀 {self.system_name}</h1>
            <p>版本: {self.version} | 架构: {self.base_architecture}</p>
        </div>
        ''', unsafe_allow_html=True)
    
    def render_sidebar(self):
        with st.sidebar:
            st.image("https://via.placeholder.com/200x100/1e3c72/white?text=V002", width=200)
            st.markdown("---")
            
            self.selected_module = st.selectbox("选择功能模块", list(self.modules.keys()))
            
            st.markdown("---")
            st.subheader("📊 系统状态")
            st.info(f"最新交易日: {self.latest_trade_date.strftime('%Y-%m-%d')}")
            
            if TUSHARE_AVAILABLE:
                st.success("🟢 Tushare: 已连接")
            else:
                st.error("🔴 Tushare: 未连接")
            
            st.markdown("---")
            st.subheader("💾 缓存状态")
            cache_files = len([f for f in os.listdir(CACHE_DIR) if f.endswith('.json')]) if os.path.exists(CACHE_DIR) else 0
            st.metric("缓存文件数", cache_files)
    
    def render_main_content(self):
        if self.selected_module in self.modules:
            self.modules[self.selected_module]()
    
    def render_footer(self):
        st.markdown("---")
        st.markdown(f"<p style='text-align: center; color: #666;'>© {datetime.now().year} {self.system_name}</p>", unsafe_allow_html=True)
    
    # Module rendering methods
    def render_home(self):
        st.header("🏠 系统首页")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("系统版本", self.version)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("数据源状态", "已连接" if TUSHARE_AVAILABLE else "未连接")
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("最新交易日", self.latest_trade_date.strftime('%Y-%m-%d'))
            st.markdown('</div>', unsafe_allow_html=True)
        
        st.subheader("📋 系统功能")
        st.info("""
        **V002 高性能交易决策系统** 主要功能：
        
        - 🚀 **短线暴涨**: 基于换手率和技术指标识别短期机会
        - 💎 **价值挖掘**: 低估值股票筛选和分析
        - 📊 **市场总览**: 全市场数据概览和统计
        - 💾 **智能缓存**: 高效数据缓存机制，提升响应速度
        """)
    
    def render_market_overview(self):
        st.header("📊 市场总览")
        
        if not TUSHARE_AVAILABLE:
            st.error("数据源未连接，无法获取市场数据")
            return
        
        with st.spinner("正在获取市场数据..."):
            daily_data = fetch_daily_basic()
        
        if daily_data.empty:
            st.warning("暂无市场数据")
            return
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("总股票数", len(daily_data))
        
        with col2:
            avg_pe = daily_data[daily_data['pe'] > 0]['pe'].mean()
            st.metric("平均市盈率", f"{avg_pe:.2f}")
        
        with col3:
            avg_pb = daily_data[daily_data['pb'] > 0]['pb'].mean()
            st.metric("平均市净率", f"{avg_pb:.2f}")
        
        with col4:
            avg_turnover = daily_data['turnover_rate'].mean()
            st.metric("平均换手率", f"{avg_turnover:.2f}%")
        
        st.subheader("📈 市场数据详情")
        st.dataframe(daily_data.head(100), use_container_width=True)
    
    def render_short_term_surge(self):
        st.header("🚀 短线暴涨")
        st.info("基于换手率和估值指标筛选短期暴涨潜力股")
        
        if not TUSHARE_AVAILABLE:
            st.error("数据源未连接，无法获取数据")
            return
        
        limit = st.slider("显示数量", 10, 50, 20)
        
        with st.spinner("正在分析短线暴涨机会..."):
            surge_data = self.data_modules.short_term_surge(limit)
        
        if surge_data.empty:
            st.warning("暂无符合条件的股票")
            return
        
        st.subheader("🎯 短线暴涨候选股")
        st.dataframe(surge_data, use_container_width=True)
        
        # 显示筛选条件
        with st.expander("📋 筛选条件"):
            st.write("""
            - 换手率 > 5%
            - 市盈率 > 0 且 < 50
            - 按换手率降序排列
            """)
    
    def render_value_investing(self):
        st.header("💎 价值挖掘")
        st.info("基于估值指标筛选价值投资机会")
        
        if not TUSHARE_AVAILABLE:
            st.error("数据源未连接，无法获取数据")
            return
        
        limit = st.slider("显示数量", 10, 50, 20)
        
        with st.spinner("正在挖掘价值投资机会..."):
            value_data = self.data_modules.value_investing(limit)
        
        if value_data.empty:
            st.warning("暂无符合条件的股票")
            return
        
        st.subheader("💰 价值投资候选股")
        st.dataframe(value_data, use_container_width=True)
        
        # 显示筛选条件
        with st.expander("📋 筛选条件"):
            st.write("""
            - 市盈率 > 0 且 < 15
            - 市净率 > 0 且 < 2
            - 按市盈率升序排列
            """)

# ==============================================================================
# 7. Main Execution
# ==============================================================================

def main():
    """Main function to initialize and run the system."""
    initialize_tushare()
    ui = TradingSystemUI()
    ui.run()

if __name__ == "__main__":
    main()

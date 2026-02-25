#!/bin/bash

echo "🚀 启动v068永久版 - 13模块完整系统"
echo "======================================"

# 停止之前的v068进程（仅本系统）
echo "🛑 停止之前的v068进程..."
pkill -f "streamlit run 机构级V13系统_v068_永久版.py" 2>/dev/null || true
if lsof -ti:8501 >/dev/null 2>&1; then
    lsof -ti:8501 | xargs kill -9 2>/dev/null || true
fi

# 检查Python环境
echo "🔍 检查Python环境..."
if ! command -v python &> /dev/null; then
    echo "❌ Python未安装"
    exit 1
fi

# 检查依赖
echo "📦 检查依赖..."
python -c "import streamlit, pandas, numpy, tushare" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "❌ 缺少必要依赖，请安装：pip install streamlit pandas numpy tushare"
    exit 1
fi

# 检查Tushare token
echo "🔑 检查Tushare token..."
if [ ! -f "versions/v068/src/tushare_token.txt" ]; then
    echo "❌ 未找到tushare_token.txt文件"
    exit 1
fi

echo "✅ 环境检查完成"

# 启动系统
echo "🌟 启动v068永久版系统..."
echo "📊 系统特性："
echo "   - 13模块完整系统"
echo "   - 机构级V13系统架构"
echo "   - Tushare Pro真实数据"
echo "   - 智能缓存增强"
echo "   - 专业股票分析"

cd versions/v068/src
streamlit run 机构级V13系统_v068_永久版.py --server.port 8501 
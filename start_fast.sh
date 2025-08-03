#!/bin/bash

echo "🚀 快速启动股票分析系统"
echo "========================"

# 停止之前的进程
pkill -f streamlit

# 启动v1.2版本（最稳定的版本）
cd /Users/mac/QLIB/versions/v1.2_complete
streamlit run V001_v1.2_完整V001_13模块_智能缓存增强系统_fixed.py --server.port 8501 
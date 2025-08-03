#!/usr/bin/env python3
"""
V001 v1.2 完整版本启动器
"""

import subprocess
import sys
import os

def main():
    print("🚀 启动V001 v1.2完整版本...")
    print("版本: v1.2")
    print("发布日期: 2025-07-24")
    print("状态: 完整版本")
    print()
    
    main_file = "V001_v1.2_完整V001_13模块_智能缓存增强系统.py"
    if not os.path.exists(main_file):
        print(f"❌ 找不到主文件: {main_file}")
        return
    
    try:
        subprocess.run([sys.executable, "-m", "streamlit", "run", main_file], check=True)
    except KeyboardInterrupt:
        print("\n👋 系统已停止")
    except Exception as e:
        print(f"❌ 启动失败: {e}")

if __name__ == "__main__":
    main()

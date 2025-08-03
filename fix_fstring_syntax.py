#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复f-string中的语法错误
"""

import re
import shutil

def fix_fstring_syntax(file_path):
    # 备份原文件
    backup_path = file_path + '.backup_fstring_fix'
    shutil.copy2(file_path, backup_path)
    print(f"已备份原文件到: {backup_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 修复f-string中的百分号问题
    # 将 `{expression:.1f}`% 改为 `{expression:.1f}%`
    content = re.sub(r'`\{([^}]+):.1f\}`%', r'`{\1:.1f}%`', content)
    
    # 修复其他可能的emoji字符
    emoji_replacements = {
        '🎯': '目标',
        '🛡️': '盾牌',
        '¥': '￥',
        '📈': '上涨',
        '📉': '下跌',
        '💰': '金钱',
        '⭐': '星星',
        '🔥': '火',
        '💎': '钻石',
        '🚨': '警报',
        '✅': '✓',
        '❌': '✗',
        '⚠️': '警告',
        '🔍': '搜索',
        '📊': '图表',
        '⚡': '闪电',
        '🚀': '火箭',
        '🧠': '大脑',
        '💡': '灯泡'
    }
    
    for emoji, replacement in emoji_replacements.items():
        content = content.replace(emoji, replacement)
    
    # 写入修复后的内容
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("f-string语法修复完成")
    
    # 验证语法
    import subprocess
    try:
        result = subprocess.run(['python3', '-m', 'py_compile', file_path], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ 语法检查通过")
            return True
        else:
            print(f"❌ 语法错误: {result.stderr}")
            return False
    except Exception as e:
        print(f"验证过程出错: {e}")
        return False

if __name__ == "__main__":
    file_path = "机构级V13系统_v730_改进版.py"
    fix_fstring_syntax(file_path)

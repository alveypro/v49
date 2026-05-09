#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复机构级V13系统文件中的f-string语法错误
"""

import os
import re
import shutil
from datetime import datetime

def fix_fstring_syntax():
    """修复f-string中的百分号语法错误"""
    
    # 查找所有可能的文件
    all_files = os.listdir('.')
    target_files = []
    
    for filename in all_files:
        if filename.endswith('.py') and 'backup' not in filename:
            # 检查文件名是否包含机构级相关内容
            if any(keyword in filename for keyword in ['机构级', 'V13', 'v730']):
                target_files.append(filename)
    
    print(f"找到 {len(target_files)} 个目标文件:")
    for f in target_files:
        print(f"  - {f}")
    
    for filename in target_files:
        try:
            print(f"\n处理文件: {filename}")
            
            # 读取文件内容
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 检查文件大小
            if len(content) < 100:
                print(f"  跳过 {filename} - 文件太小 ({len(content)} 字符)")
                continue
            
            original_content = content
            
            # 修复各种f-string百分号问题
            patterns = [
                (r'(:\.[0-9]+f})%(?!%)', r'\1%%'),  # :.1f}% -> :.1f}%%
                (r'(:[0-9]*\.[0-9]+f})%(?!%)', r'\1%%'),  # :2.1f}% -> :2.1f}%%
                (r'(:[0-9]+f})%(?!%)', r'\1%%'),  # :1f}% -> :1f}%%
                (r'(}[^}]*?)%(?!%)', r'\1%%'),  # 其他可能的百分号问题
            ]
            
            changes_made = 0
            for pattern, replacement in patterns:
                new_content = re.sub(pattern, replacement, content)
                if new_content != content:
                    changes_made += 1
                    content = new_content
            
            if content != original_content:
                # 备份原文件
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_name = f"{filename}.backup_fstring_fix_{timestamp}"
                shutil.copy2(filename, backup_name)
                
                # 写入修复后的内容
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"  ✅ 修复了 {changes_made} 个f-string问题")
                print(f"  📁 备份保存为: {backup_name}")
                
                # 验证语法
                try:
                    compile(content, filename, 'exec')
                    print(f"  ✅ 语法检查通过")
                except SyntaxError as e:
                    print(f"  ❌ 语法错误仍然存在: {e}")
                    print(f"     行号: {e.lineno}, 位置: {e.offset}")
                    if e.lineno:
                        lines = content.split('\n')
                        if e.lineno <= len(lines):
                            print(f"     问题行: {lines[e.lineno-1]}")
            else:
                print(f"  ℹ️  未发现f-string问题")
                
        except Exception as e:
            print(f"  ❌ 处理 {filename} 时出错: {e}")

if __name__ == "__main__":
    print("🔧 开始修复f-string语法错误...")
    fix_fstring_syntax()
    print("\n🎉 修复完成!")

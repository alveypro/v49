#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import shutil
import time

def fix_fstring_syntax():
    print('🔧 开始修复f-string语法错误...')
    
    # 直接列出所有文件
    all_files = os.listdir('.')
    print('\n所有Python文件:')
    for f in all_files:
        if f.endswith('.py'):
            print(f'  {f}')
    
    # 查找目标文件
    target_files = []
    for filename in all_files:
        if filename.endswith('.py') and 'backup' not in filename:
            if '机构级' in filename or 'V13' in filename or 'v730' in filename:
                target_files.append(filename)
    
    print(f'\n找到目标文件: {target_files}')
    
    # 处理每个文件
    for filename in target_files:
        try:
            print(f'\n处理文件: {filename}')
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.read()
            
            if len(content) < 100:
                print(f'  跳过 - 文件太小')
                continue
                
            original_content = content
            
            # 修复f-string百分号问题 - 更精确的正则表达式
            # 匹配 :.1f}% 这样的模式并替换为 :.1f}%%
            content = re.sub(r'(:\.[0-9]+f})%(?!%)', r'\1%%', content)
            content = re.sub(r'(:[0-9]*\.[0-9]+f})%(?!%)', r'\1%%', content)
            content = re.sub(r'(:[0-9]+f})%(?!%)', r'\1%%', content)
            
            # 特别处理可能的问题行
            content = re.sub(r'(trading_signals\["position_size"\]:\.[0-9]+f})%(?!%)', r'\1%%', content)
            
            if content != original_content:
                # 备份
                backup_name = f'{filename}.backup_fstring_{int(time.time())}'
                shutil.copy2(filename, backup_name)
                
                # 写入修复内容
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f'  ✅ 已修复并备份为 {backup_name}')
                
                # 语法检查
                try:
                    compile(content, filename, 'exec')
                    print(f'  ✅ 语法检查通过')
                except SyntaxError as e:
                    print(f'  ❌ 语法错误: 行{e.lineno}: {e.msg}')
            else:
                print(f'  ℹ️ 未发现需要修复的问题')
                
        except Exception as e:
            print(f'  ❌ 错误: {e}')
    
    print('\n🎉 修复完成!')

if __name__ == '__main__':
    fix_fstring_syntax()

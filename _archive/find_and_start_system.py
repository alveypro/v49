#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import py_compile
import subprocess
import sys

def find_working_files():
    """查找可以正常启动的系统文件"""
    files = [f for f in os.listdir('.') if f.endswith('.py') and 
             ('V001' in f or 'v730' in f or '机构级' in f) and 'backup' not in f]
    
    print('检查系统文件语法...')
    working_files = []
    
    for f in files:
        try:
            py_compile.compile(f, doraise=True)
            working_files.append(f)
            print(f'✅ {f}')
        except Exception as e:
            print(f'❌ {f} - {str(e)[:50]}...')
    
    return working_files

def main():
    print('正在查找可启动的系统文件...')
    working_files = find_working_files()
    
    if not working_files:
        print('\n❌ 没有找到可启动的系统文件')
        return
    
    print(f'\n找到 {len(working_files)} 个可启动文件:')
    for i, f in enumerate(working_files[:5], 1):
        print(f'{i}. {f}')
    
    # 优先启动包含V001的文件
    v001_files = [f for f in working_files if 'V001' in f]
    if v001_files:
        target_file = v001_files[0]
        print(f'\n🚀 启动V001系统: {target_file}')
    else:
        target_file = working_files[0]
        print(f'\n🚀 启动系统: {target_file}')
    
    # 启动文件
    try:
        subprocess.run([sys.executable, target_file], check=True)
    except subprocess.CalledProcessError as e:
        print(f'启动失败: {e}')
    except KeyboardInterrupt:
        print('\n用户中断启动')

if __name__ == '__main__':
    main()

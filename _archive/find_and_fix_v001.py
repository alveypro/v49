#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re

def find_v001_file():
    """查找V001文件"""
    for f in os.listdir('.'):
        if 'V001' in f and '智能缓存增强系统.py' in f and 'backup' not in f:
            return f
    return None

def remove_special_chars(content):
    """移除特殊符号，特别是emoji"""
    # 移除emoji和其他特殊Unicode字符
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    
    content = emoji_pattern.sub('', content)
    
    # 移除其他可能的特殊符号
    special_chars = ['🚀', '⚡', '🔥', '💎', '📈', '📊', '🎯', '💰', '🏆', '⭐']
    for char in special_chars:
        content = content.replace(char, '')
    
    return content

def main():
    # 查找V001文件
    filename = find_v001_file()
    if not filename:
        print("未找到V001文件")
        return
    
    print(f"找到文件: {filename}")
    
    # 备份原文件
    backup_name = f"{filename}.backup_emoji_fix"
    os.system(f"cp '{filename}' '{backup_name}'")
    print(f"已备份到: {backup_name}")
    
    # 读取文件内容
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 移除特殊符号
        fixed_content = remove_special_chars(content)
        
        # 写回文件
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print("已移除所有特殊符号")
        
        # 验证语法
        import py_compile
        try:
            py_compile.compile(filename, doraise=True)
            print("语法检查通过！")
        except py_compile.PyCompileError as e:
            print(f"语法错误: {e}")
            
    except Exception as e:
        print(f"处理文件时出错: {e}")

if __name__ == "__main__":
    main()

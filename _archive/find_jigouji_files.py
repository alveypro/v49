#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

print("查找包含'机构级'的Python文件:")
for filename in os.listdir('.'):
    if '机构级' in filename and filename.endswith('.py'):
        print(f"  {filename}")
        if os.path.exists(filename):
            print(f"    ✅ 文件存在")
        else:
            print(f"    ❌ 文件不存在")

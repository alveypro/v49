#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证市值数据的完整性
"""

import sqlite3
import pandas as pd

DB_PATH = '/Users/mac/QLIB/permanent_stock_database.db'

print("🔍 验证市值数据完整性")
print("="*80)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# 1. 总体统计
print("\n1️⃣ 总体数据统计:")
cursor.execute("SELECT COUNT(*) FROM stock_basic")
total_stocks = cursor.fetchone()[0]
print(f"   股票总数: {total_stocks}")

cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE circ_mv > 0")
with_mv = cursor.fetchone()[0]
print(f"   有市值数据: {with_mv} ({with_mv/total_stocks*100:.1f}%)")

cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE circ_mv IS NULL OR circ_mv = 0")
no_mv = cursor.fetchone()[0]
print(f"   无市值数据: {no_mv} ({no_mv/total_stocks*100:.1f}%)")

# 2. 市值分布
print("\n2️⃣ 市值分布详情:")
ranges = [
    ("< 30亿", 0, 30*10000),
    ("30-50亿", 30*10000, 50*10000),
    ("50-100亿", 50*10000, 100*10000),
    ("100-200亿", 100*10000, 200*10000),
    ("200-500亿", 200*10000, 500*10000),
    ("500-1000亿", 500*10000, 1000*10000),
    ("1000-2000亿", 1000*10000, 2000*10000),
    ("> 2000亿", 2000*10000, 999999*10000),
]

total_counted = 0
for label, min_mv, max_mv in ranges:
    if max_mv > 900000 * 10000:
        cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE circ_mv >= ?", (min_mv,))
    else:
        cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE circ_mv >= ? AND circ_mv < ?", (min_mv, max_mv))
    count = cursor.fetchone()[0]
    total_counted += count
    print(f"   {label:15s}: {count:5d} 只")

print(f"   {'合计':15s}: {total_counted:5d} 只")

# 3. 重点检查100-1000亿
print("\n3️⃣ 重点检查100-1000亿市值:")
cap_min = 100 * 10000
cap_max = 1000 * 10000

cursor.execute("""
    SELECT COUNT(*) FROM stock_basic 
    WHERE circ_mv >= ? AND circ_mv <= ?
""", (cap_min, cap_max))
count_100_1000 = cursor.fetchone()[0]
print(f"   100-1000亿总数: {count_100_1000} 只")

# 细分
sub_ranges = [
    ("100-200亿", 100*10000, 200*10000),
    ("200-300亿", 200*10000, 300*10000),
    ("300-500亿", 300*10000, 500*10000),
    ("500-800亿", 500*10000, 800*10000),
    ("800-1000亿", 800*10000, 1000*10000),
]

print("\n   细分统计:")
for label, min_mv, max_mv in sub_ranges:
    cursor.execute("SELECT COUNT(*) FROM stock_basic WHERE circ_mv >= ? AND circ_mv <= ?", (min_mv, max_mv))
    count = cursor.fetchone()[0]
    print(f"   {label:15s}: {count:5d} 只")

# 4. 查看市值数据的时效性
print("\n4️⃣ 检查市值数据的来源和时效:")
query = """
    SELECT ts_code, name, circ_mv, circ_mv/10000 as circ_mv_yi, total_mv
    FROM stock_basic 
    WHERE circ_mv > 0
    ORDER BY circ_mv DESC
    LIMIT 10
"""
df = pd.read_sql_query(query, conn)
print("\n   市值最大的10只股票:")
for idx, row in df.iterrows():
    print(f"   {idx+1}. {row['ts_code']} {row['name']:10s} "
          f"流通: {row['circ_mv_yi']:8.1f}亿 "
          f"总市值: {row['total_mv']/10000 if row['total_mv'] else 0:8.1f}亿")

# 5. 检查是否有异常值
print("\n5️⃣ 检查异常数据:")
cursor.execute("""
    SELECT COUNT(*) FROM stock_basic 
    WHERE circ_mv > 0 AND circ_mv < 1000
""")
too_small = cursor.fetchone()[0]
print(f"   市值<0.01亿（可能是错误数据）: {too_small} 只")

cursor.execute("""
    SELECT COUNT(*) FROM stock_basic 
    WHERE circ_mv > 100000*10000
""")
too_large = cursor.fetchone()[0]
print(f"   市值>10万亿（可能是错误数据）: {too_large} 只")

# 6. 与行情数据对比
print("\n6️⃣ 与行情数据对比:")
cursor.execute("""
    SELECT COUNT(DISTINCT ts_code) FROM daily_trading_data
""")
stocks_with_data = cursor.fetchone()[0]
print(f"   有行情数据的股票: {stocks_with_data} 只")

cursor.execute("""
    SELECT COUNT(DISTINCT sb.ts_code)
    FROM stock_basic sb
    INNER JOIN daily_trading_data dtd ON sb.ts_code = dtd.ts_code
    WHERE sb.circ_mv >= ? AND sb.circ_mv <= ?
""", (cap_min, cap_max))
count_with_both = cursor.fetchone()[0]
print(f"   100-1000亿且有行情数据: {count_with_both} 只")
print(f"   数据完整性: {count_with_both/count_100_1000*100:.1f}%")

# 7. 抽样检查
print("\n7️⃣ 随机抽样检查（100-1000亿）:")
query = """
    SELECT ts_code, name, industry, circ_mv/10000 as circ_mv_yi
    FROM stock_basic 
    WHERE circ_mv >= ? AND circ_mv <= ?
    ORDER BY RANDOM()
    LIMIT 10
"""
df = pd.read_sql_query(query, conn, params=(cap_min, cap_max))
for idx, row in df.iterrows():
    print(f"   {row['ts_code']} {row['name']:10s} {row['industry']:10s} {row['circ_mv_yi']:8.1f}亿")

conn.close()

print("\n" + "="*80)
print("✅ 验证完成")
print("\n💡 判断标准:")
print("   - A股总数约5000只，100-1000亿约1500只是合理的（约30%）")
print("   - 如果无市值数据的股票很多，需要更新市值数据")
print("   - 如果数据完整性<90%，建议到Tab1更新市值数据")


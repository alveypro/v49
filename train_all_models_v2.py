"""
train_all_models_v2.py
改进版：真实信号优先 + 历史模拟补充
- 从 strategy_signal_tracking + strategy_signal_performance 提取真实样本
- 真实样本不足500条时，用历史模拟补充到目标数量
- 每月自动重训时真实样本占比会越来越高
"""
import sys, os, sqlite3, pickle, shutil
sys.path.insert(0, '/opt/openclaw/app/v49')
import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import TimeSeriesSplit

DB = '/opt/openclaw/permanent_stock_database.db'
LABEL_THRESH = 2.0
SAMPLE_STOCKS = 500
MIN_REAL_SAMPLES = 50   # 真实样本低于此数时完全用模拟
TARGET_SAMPLES = 90000  # 目标总样本数

def get_hist(ts_code, conn):
    return pd.read_sql(f"""
        SELECT ts_code, trade_date, open_price, high_price, low_price,
               close_price, vol, pct_chg, amount
        FROM daily_trading_data
        WHERE ts_code='{ts_code}' ORDER BY trade_date ASC
    """, conn)

def get_stocks(n=SAMPLE_STOCKS):
    conn = sqlite3.connect(DB)
    stocks = pd.read_sql("SELECT DISTINCT ts_code FROM daily_trading_data", conn)['ts_code'].tolist()
    conn.close()
    np.random.seed(42)
    return np.random.choice(stocks, min(n, len(stocks)), replace=False)

def make_label_from_hist(df, i, days=5, thresh=2.0):
    if i + 2 >= len(df): return None, None
    buy = df.iloc[i+1]['open_price']
    if buy <= 0: return None, None
    if i + days >= len(df): return None, None
    sell = df.iloc[i+days]['close_price']
    ret = (sell / buy - 1) * 100 - 0.4
    return ret, 1 if ret > thresh else 0

def get_real_samples_v4(conn):
    """从真实信号记录提取v4特征+标签"""
    try:
        df = pd.read_sql("""
            SELECT t.ts_code, t.signal_trade_date, t.score,
                   p.ret_pct, p.horizon_days
            FROM strategy_signal_tracking t
            JOIN strategy_signal_performance p
              ON t.id = p.signal_id
            WHERE t.strategy IN ('v4','v5','v6')
              AND p.ret_pct IS NOT NULL
        """, conn)
        if df.empty:
            return []
        from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
        ev = ComprehensiveStockEvaluatorV4()
        records = []
        for _, row in df.iterrows():
            hist = get_hist(row['ts_code'], conn)
            # 找到信号日期对应的行
            idx = hist[hist['trade_date'] == row['signal_trade_date']].index
            if len(idx) == 0:
                continue
            i = idx[0]
            if i < 80:
                continue
            r = ev.evaluate_stock_v4(hist.iloc[:i])
            if not r.get('success'):
                continue
            dim = r.get('dimension_scores', {})
            thresh = 2.0 if row.get('horizon_days', 5) <= 5 else 3.0
            records.append({
                'trade_date': row['signal_trade_date'],
                'final_score': r.get('final_score', 0),
                '潜伏价值': dim.get('潜伏价值', 0),
                '底部特征': dim.get('底部特征', 0),
                '量价配合': dim.get('量价配合', 0),
                'MACD趋势': dim.get('MACD趋势', 0),
                '均线多头': dim.get('均线多头', 0),
                '主力行为': dim.get('主力行为', 0),
                '启动确认': dim.get('启动确认', 0),
                '涨停基因': dim.get('涨停基因', 0),
                'label': 1 if row['ret_pct'] > thresh else 0,
                'is_real': 1,
            })
        print(f"  真实v4样本: {len(records)}条")
        return records
    except Exception as e:
        print(f"  获取真实v4样本失败: {e}")
        return []

def get_real_samples_v8(conn):
    """从真实信号记录提取v8因子+标签"""
    try:
        df = pd.read_sql("""
            SELECT t.ts_code, t.signal_trade_date, p.ret_pct
            FROM strategy_signal_tracking t
            JOIN strategy_signal_performance p ON t.id = p.signal_id
            WHERE t.strategy = 'v8' AND p.ret_pct IS NOT NULL
        """, conn)
        if df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            hist = get_hist(row['ts_code'], conn)
            idx = hist[hist['trade_date'] == row['signal_trade_date']].index
            if len(idx) == 0:
                continue
            i = idx[0]
            if i < 80:
                continue
            close = hist['close_price'].values.astype(float)
            high = hist['high_price'].values.astype(float)
            low = hist['low_price'].values.astype(float)
            vol = hist['vol'].values.astype(float)
            pct = hist['pct_chg'].values.astype(float)
            amount = hist['amount'].values.astype(float)
            fac = calc_v8_factors(close, high, low, vol, pct, amount, i)
            if fac is None:
                continue
            fac['trade_date'] = row['signal_trade_date']
            fac['label'] = 1 if row['ret_pct'] > 2.0 else 0
            fac['is_real'] = 1
            records.append(fac)
        print(f"  真实v8样本: {len(records)}条")
        return records
    except Exception as e:
        print(f"  获取真实v8样本失败: {e}")
        return []

def get_real_samples_v9(conn):
    """从真实信号记录提取v9特征+标签"""
    try:
        df = pd.read_sql("""
            SELECT t.ts_code, t.signal_trade_date, p.ret_pct
            FROM strategy_signal_tracking t
            JOIN strategy_signal_performance p ON t.id = p.signal_id
            WHERE t.strategy = 'v9' AND p.ret_pct IS NOT NULL
        """, conn)
        if df.empty:
            return []
        records = []
        for _, row in df.iterrows():
            hist = get_hist(row['ts_code'], conn)
            idx = hist[hist['trade_date'] == row['signal_trade_date']].index
            if len(idx) == 0:
                continue
            i = idx[0]
            if i < 80:
                continue
            close = hist['close_price'].values.astype(float)
            vol = hist['vol'].values.astype(float)
            pct = hist['pct_chg'].values.astype(float)
            amount = hist['amount'].values.astype(float)
            s = max(0, i-60)
            c, v, p, a = close[s:i], vol[s:i], pct[s:i], amount[s:i]
            n = len(c)
            if n < 20:
                continue
            ma20 = np.mean(c[-20:])
            ma60 = np.mean(c) if n >= 60 else ma20
            records.append({
                'trade_date': row['signal_trade_date'],
                'vol_ratio': float(np.clip(np.mean(v[-5:])/(np.mean(v[-20:])+1e-9), 0, 10)),
                'momentum_20': float(np.clip((c[-1]-c[-20])/(c[-20]+1e-9), -0.5, 0.5)),
                'momentum_60': float(np.clip((c[-1]-c[0])/(c[0]+1e-9), -1, 1)),
                'volatility': float(np.clip(np.std(p[-20:])/100, 0, 0.1)),
                'price_pos': float((c[-1]-np.min(c))/(np.max(c)-np.min(c)+1e-9)),
                'ma_trend': float(np.clip((ma20-ma60)/(ma60+1e-9), -0.3, 0.3)),
                'label': 1 if row['ret_pct'] > 3.0 else 0,
                'is_real': 1,
            })
        print(f"  真实v9样本: {len(records)}条")
        return records
    except Exception as e:
        print(f"  获取真实v9样本失败: {e}")
        return []

def calc_v8_factors(close, high, low, vol, pct, amount, i):
    w = 60
    s = max(0, i - w)
    c, h, v, p, a = close[s:i], high[s:i], vol[s:i], pct[s:i], amount[s:i]
    n = len(c)
    if n < 20:
        return None
    eps = 1e-9
    r5 = (c[-1]-c[-5])/(c[-5]+eps) if n >= 5 else 0.0
    r10 = (c[-5]-c[-10])/(c[-10]+eps) if n >= 10 else 0.0
    acceleration = (r5 - r10) * 100
    streak = 0
    for k in range(1, min(21, n)):
        if c[-k] > c[-(k+1)]: streak += 1
        else: break
    persistence = streak / 20.0
    obv = sum(v[k] if c[k]>c[k-1] else (-v[k] if c[k]<c[k-1] else 0) for k in range(1, n))
    obv_norm = obv / (np.mean(v)*n + eps)
    price_std = np.std(c[-20:]) / (np.mean(c[-20:]) + eps)
    chip_concentration = 1.0 / (price_std + 0.01)
    vol_ma5 = np.mean(v[-5:]) if n >= 5 else v[-1]
    vol_ma20 = np.mean(v[-20:]) if n >= 20 else np.mean(v)
    turnover_momentum = vol_ma5 / (vol_ma20 + eps)
    valuation_repair = c[-1] / (np.max(h) + eps)
    roe_trend = np.mean(p[-5:]) - np.mean(p[-20:]) if n >= 20 else 0.0
    capital_flow = np.sum(p[-5:]*a[-5:])/(np.sum(a[-5:])+eps) if n >= 5 else 0.0
    sector_resonance = np.sum(p[-20:]>0)/20.0 if n >= 20 else 0.5
    smart_money = (np.mean(v[-5:]*c[-5:])-np.mean(v[-10:-5]*c[-10:-5]))/(np.mean(v[-10:-5]*c[-10:-5])+eps) if n >= 10 else 0.0
    ma20 = np.mean(c[-20:])
    ma60 = np.mean(c) if n >= 60 else ma20
    final_score = float(np.clip(50+(c[-1]/ma20-1)*100+(c[-1]/ma60-1)*50, 0, 100))
    return {
        'final_score': final_score,
        'v7_score': float(np.clip(final_score*0.8, 0, 100)),
        'advanced_score': float(np.clip((acceleration+persistence*10+turnover_momentum*5)/3, 0, 100)),
        'acceleration': float(np.clip(acceleration, -10, 10)),
        'persistence': float(persistence),
        'obv': float(np.clip(obv_norm, -5, 5)),
        'chip_concentration': float(np.clip(chip_concentration, 0, 10)),
        'turnover_momentum': float(np.clip(turnover_momentum, 0, 10)),
        'valuation_repair': float(valuation_repair),
        'roe_trend': float(np.clip(roe_trend, -5, 5)),
        'capital_flow': float(np.clip(capital_flow, -5, 5)),
        'sector_resonance': float(sector_resonance),
        'smart_money': float(np.clip(smart_money, -5, 5)),
    }

def train_and_save(records, features, model_name, label_days=5):
    if len(records) < 100:
        print(f"  样本不足({len(records)})，跳过")
        return
    df = pd.DataFrame(records).sort_values('trade_date').reset_index(drop=True)
    pos = df['label'].sum()
    real_count = df.get('is_real', pd.Series([0]*len(df))).sum() if 'is_real' in df.columns else 0
    print(f"  总样本: {len(df)} (真实:{real_count} 模拟:{len(df)-real_count}), 正样本: {pos} ({pos/len(df)*100:.1f}%)")
    X = df[features].values.astype(np.float32)
    y = df['label'].values
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    # 真实样本权重加倍
    sample_weight = np.ones(len(y))
    if 'is_real' in df.columns:
        sample_weight[df['is_real'].values == 1] = 3.0
    tscv = TimeSeriesSplit(n_splits=5)
    scores = []
    for train_idx, val_idx in tscv.split(X):
        m = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05, max_depth=4,
                               num_leaves=15, min_child_samples=20,
                               class_weight='balanced', random_state=42, verbose=-1)
        m.fit(X[train_idx], y[train_idx], sample_weight=sample_weight[train_idx])
        scores.append((m.predict(X[val_idx]) == y[val_idx]).mean())
    print(f"  CV准确率: {np.mean(scores):.3f} +/- {np.std(scores):.3f}")
    final = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05, max_depth=4,
                               num_leaves=15, min_child_samples=20,
                               class_weight='balanced', random_state=42, verbose=-1)
    final.fit(X, y, sample_weight=sample_weight)
    path = f'/opt/openclaw/ml_model_{model_name}.pkl'
    with open(path, 'wb') as f:
        pickle.dump({'model': final, 'features': features,
                     'label_days': label_days, 'label_thresh': LABEL_THRESH,
                     'real_samples': int(real_count), 'total_samples': len(df)}, f)
    print(f"  已保存: {path}")

# ============ 主训练流程 ============
conn = sqlite3.connect(DB)
stocks = get_stocks()

# ── v4/v5/v6 ──
print("\n[v4/v5/v6] 训练中...")
real_records = get_real_samples_v4(conn)
feats_v4 = ['final_score','潜伏价值','底部特征','量价配合','MACD趋势','均线多头','主力行为','启动确认','涨停基因']

# 历史模拟补充
sim_records = []
from comprehensive_stock_evaluator_v4 import ComprehensiveStockEvaluatorV4
ev4 = ComprehensiveStockEvaluatorV4()
need_sim = max(0, TARGET_SAMPLES - len(real_records) * 3)  # 真实样本权重3倍，折算
sim_stocks = get_stocks(min(SAMPLE_STOCKS, need_sim // 180 + 1))
for idx, ts in enumerate(sim_stocks):
    if idx % 100 == 0: print(f"  模拟进度: {idx}/{len(sim_stocks)}, 已收集: {len(sim_records)}")
    df = get_hist(ts, conn)
    df['name'] = ''
    if len(df) < 85: continue
    for i in range(80, len(df)-5, 5):
        ret, label = make_label_from_hist(df, i, days=5)
        if label is None: continue
        r = ev4.evaluate_stock_v4(df.iloc[:i])
        if not r.get('success'): continue
        dim = r.get('dimension_scores', {})
        sim_records.append({
            'trade_date': df.iloc[i]['trade_date'],
            'final_score': r.get('final_score', 0),
            '潜伏价值': dim.get('潜伏价值', 0), '底部特征': dim.get('底部特征', 0),
            '量价配合': dim.get('量价配合', 0), 'MACD趋势': dim.get('MACD趋势', 0),
            '均线多头': dim.get('均线多头', 0), '主力行为': dim.get('主力行为', 0),
            '启动确认': dim.get('启动确认', 0), '涨停基因': dim.get('涨停基因', 0),
            'label': label, 'is_real': 0,
        })

all_v4 = real_records + sim_records
train_and_save(all_v4, feats_v4, 'v1')
for name in ['v4', 'v5', 'v6']:
    shutil.copy('/opt/openclaw/ml_model_v1.pkl', f'/opt/openclaw/ml_model_{name}.pkl')
    print(f"  已复制 -> ml_model_{name}.pkl")

# ── v7 ──
print("\n[v7] 训练中（复用v4特征）...")
shutil.copy('/opt/openclaw/ml_model_v1.pkl', '/opt/openclaw/ml_model_v7.pkl')
print("  已复制 -> ml_model_v7.pkl")

# ── v8 ──
print("\n[v8] 训练中...")
real_v8 = get_real_samples_v8(conn)
sim_v8 = []
for idx, ts in enumerate(stocks):
    if idx % 50 == 0: print(f"  v8模拟进度: {idx}/{len(stocks)}, 已收集: {len(sim_v8)}")
    df = get_hist(ts, conn)
    if len(df) < 85: continue
    close = df['close_price'].values.astype(float)
    high = df['high_price'].values.astype(float)
    low = df['low_price'].values.astype(float)
    vol = df['vol'].values.astype(float)
    pct = df['pct_chg'].values.astype(float)
    amount = df['amount'].values.astype(float)
    for i in range(80, len(df)-5, 5):
        ret, label = make_label_from_hist(df, i, days=5)
        if label is None: continue
        fac = calc_v8_factors(close, high, low, vol, pct, amount, i)
        if fac is None: continue
        fac['trade_date'] = df.iloc[i]['trade_date']
        fac['label'] = label
        fac['is_real'] = 0
        sim_v8.append(fac)

feats_v8 = ['final_score','v7_score','advanced_score','acceleration','persistence','obv',
            'chip_concentration','turnover_momentum','valuation_repair','roe_trend',
            'capital_flow','sector_resonance','smart_money']
train_and_save(real_v8 + sim_v8, feats_v8, 'v8')

# ── v9 ──
print("\n[v9] 训练中...")
real_v9 = get_real_samples_v9(conn)
sim_v9 = []
for idx, ts in enumerate(stocks):
    if idx % 100 == 0: print(f"  v9模拟进度: {idx}/{len(stocks)}, 已收集: {len(sim_v9)}")
    df = get_hist(ts, conn)
    if len(df) < 100: continue
    close = df['close_price'].values.astype(float)
    vol = df['vol'].values.astype(float)
    pct = df['pct_chg'].values.astype(float)
    for i in range(80, len(df)-15, 5):
        if i+2 >= len(df): continue
        buy = df.iloc[i+1]['open_price']
        if buy <= 0: continue
        sell_i = min(i+15, len(df)-1)
        ret = (df.iloc[sell_i]['close_price']/buy - 1)*100 - 0.4
        label = 1 if ret > 3.0 else 0
        s = max(0, i-60)
        c, v, p = close[s:i], vol[s:i], pct[s:i]
        n = len(c)
        if n < 20: continue
        ma20 = np.mean(c[-20:])
        ma60 = np.mean(c) if n >= 60 else ma20
        sim_v9.append({
            'trade_date': df.iloc[i]['trade_date'],
            'vol_ratio': float(np.clip(np.mean(v[-5:])/(np.mean(v[-20:])+1e-9), 0, 10)),
            'momentum_20': float(np.clip((c[-1]-c[-20])/(c[-20]+1e-9), -0.5, 0.5)),
            'momentum_60': float(np.clip((c[-1]-c[0])/(c[0]+1e-9), -1, 1)),
            'volatility': float(np.clip(np.std(p[-20:])/100, 0, 0.1)),
            'price_pos': float((c[-1]-np.min(c))/(np.max(c)-np.min(c)+1e-9)),
            'ma_trend': float(np.clip((ma20-ma60)/(ma60+1e-9), -0.3, 0.3)),
            'label': label, 'is_real': 0,
        })

feats_v9 = ['vol_ratio','momentum_20','momentum_60','volatility','price_pos','ma_trend']
train_and_save(real_v9 + sim_v9, feats_v9, 'v9', label_days=15)

# ── stable/combo ──
print("\n[稳定上涨/组合] 复用v4模型...")
for name in ['stable', 'combo']:
    shutil.copy('/opt/openclaw/ml_model_v1.pkl', f'/opt/openclaw/ml_model_{name}.pkl')
    print(f"  已复制 -> ml_model_{name}.pkl")

conn.close()
print("\n=== 全部训练完成 ===")

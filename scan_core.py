# -*- coding: utf-8 -*-
"""7入结构 核心扫描器（Baostock 纯血极速稳定版）"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POOL_FILE = os.path.join(BASE_DIR, "pool.json")

CFG = dict(n_break=3, m_enter=7, break_max_pct=0.15, t_max=20)
FILTER = dict(min_amount_yi=0.5, exclude_st=True)

def get_bj_time():
    return datetime.now(timezone(timedelta(hours=8)))

def bs_code(code):
    """转为 baostock 格式：sh.600000 或 sz.000001"""
    if code.startswith(("60", "68", "90", "11")):
        return f"sh.{code}"
    if code.startswith(("00", "30", "20", "12", "15")):
        return f"sz.{code}"
    return f"sz.{code}"


# ---------------- 1. 股票池拉取（直接用 Baostock） ----------------
def _fetch_all_a_from_baostock():
    import baostock as bs
    bs.login()
    rs = bs.query_stock_basic()
    out = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        code_str = row[0].split(".")[-1]
        name = row[1]
        # 只要沪深A股（排除北交所，排除退市）
        if len(code_str) == 6 and code_str.startswith(("60", "68", "00", "30")):
            out.append({"code": code_str, "name": name})
    bs.logout()
    return out


def ensure_pool(force=False, max_age_hours=24):
    if os.path.exists(POOL_FILE) and not force:
        try:
            with open(POOL_FILE, encoding="utf-8") as f:
                arr = json.load(f)
            if len(arr) > 3000:
                print(f"→ 使用本地缓存股票池：{len(arr)} 只")
                return
        except Exception:
            pass

    print("→ 使用 Baostock 拉取全市场股票池...")
    arr = _fetch_all_a_from_baostock()
    if len(arr) > 3000:
        with open(POOL_FILE, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, indent=1)
        print(f"→ 股票池更新成功：{len(arr)} 只")
    else:
        raise RuntimeError("Baostock 拉取股票池失败！")


def _load_pool():
    with open(POOL_FILE, encoding="utf-8") as f:
        return json.load(f)


# ---------------- 2. 获取K线（Baostock 极其稳定） ----------------
def fetch_kline_baostock(code, days=45):
    import baostock as bs
    end_date = get_bj_time().strftime("%Y-%m-%d")
    start_date = (get_bj_time() - timedelta(days=90)).strftime("%Y-%m-%d")
    
    sym = bs_code(code)
    # adjustflag="3" 代表不复权，这对于精确判断涨停价至关重要
    rs = bs.query_history_k_data_plus(
        sym, "date,open,high,low,close,volume", 
        start_date=start_date, end_date=end_date, 
        frequency="d", adjustflag="3"
    )
    
    if rs.error_code != '0':
        return None
        
    data = []
    while rs.next():
        data.append(rs.get_row_data())
        
    if len(data) < 20:
        return None
        
    df = pd.DataFrame(data, columns=["date", "open", "high", "low", "close", "volume"])
    df["open"] = df["open"].astype(float)
    df["close"] = df["close"].astype(float)
    df["high"] = df["high"].astype(float)
    df["low"] = df["low"].astype(float)
    df["volume"] = df["volume"].astype(float) # 单位：股
    
    # 截取最近 45 天
    return df.tail(days).reset_index(drop=True)


# ---------------- 3. 信号判定 ----------------
def eval_signal(code, df):
    d = df.copy()
    
    limit_pct = 1.20 if code.startswith(("30", "68")) else 1.10
    d["prev_close"] = d["close"].shift(1)
    d["limit_price"] = (d["prev_close"] * limit_pct).round(2)
    
    d["is_up"] = (d["close"] >= d["limit_price"] - 0.01) & (d["close"] > 0)
    d["fb"] = d["is_up"] & (~d["is_up"].shift(1).fillna(False))
    
    d["ma5v"] = d["volume"].rolling(5).mean()
    d["ma10v"] = d["volume"].rolling(10).mean()
    n = len(d)
    
    for t0 in range(n - 1, max(n - 15, 10), -1):
        if not d.at[t0, "fb"]:
            continue
            
        P = d.at[t0, "open"]
        is_yizi = bool(d.at[t0, "open"] == d.at[t0, "close"] == d.at[t0, "high"])
        
        brk = None
        for j in range(t0 + 1, min(t0 + 1 + CFG["n_break"], n)):
            if d.at[j, "close"] < P:
                if (P - d.at[j, "close"]) / P <= CFG["break_max_pct"]:
                    brk = j
                break
        if brk is None:
            continue
            
        last = n - 1
        if not (brk < last <= brk + CFG["m_enter"]):
            continue
        if d.at[last, "close"] <= P:
            continue
        if not (d.at[last, "volume"] > d.at[last, "ma5v"] and
                d.at[last, "volume"] > d.at[last, "ma10v"]):
            continue
            
        if d.loc[t0+1:last, "fb"].any():
            continue
            
        bro_low = d.loc[brk:last, "low"].min()
        return {
            "t0_date": d.at[t0, "date"], 
            "P": round(P, 2),
            "break_date": d.at[brk, "date"], 
            "break_low": round(bro_low, 2),
            "days_after_break": int(last - brk),
            "vol_ratio_ma5": round(d.at[last, "volume"] / d.at[last, "ma5v"], 2),
            "vol_ratio_ma10": round(d.at[last, "volume"] / d.at[last, "ma10v"], 2),
            "vol_vs_t0": round(d.at[last, "volume"] / d.at[t0, "volume"], 2),
            "t0_pct": round((d.at[t0, "close"] / d.at[t0, "prev_close"] - 1) * 100, 2),
            "close": round(d.at[last, "close"], 2),
            "pct_today": round((d.at[last, "close"] / d.at[last, "prev_close"] - 1) * 100, 2),
            "t0_yizi": is_yizi
        }
    return None


# ---------------- 4. 主扫描 ----------------
def scan_all(progress=None):
    import baostock as bs
    t_start = time.time()
    ensure_pool()
    
    # 登录 Baostock
    bs.login()
    
    pool = _load_pool()
    pool_dict = {x["code"]: x["name"] for x in pool}
    codes = list(pool_dict.keys())

    results = []
    scanned = 0
    skipped_stale = 0
    skipped_liq = 0

    def work(code):
        df = fetch_kline_baostock(code, 45)
        if df is None or df.empty:
            return code, None, "stale"
            
        last_date = datetime.strptime(df.iloc[-1]["date"], "%Y-%m-%d")
        if (get_bj_time().replace(tzinfo=None) - last_date).days > 10:
            return code, None, "stale"
            
        last_row = df.iloc[-1]
        # 成交额估算：收盘价 * 成交量（股）
        amount = float(last_row['close']) * float(last_row['volume'])
        if amount < FILTER["min_amount_yi"] * 1e8:
            return code, None, "liq"
            
        sig = eval_signal(code, df)
        if sig:
            sig["code"] = code
            sig["name"] = pool_dict.get(code, code)
            sig["price"] = float(last_row['close'])
            sig["amount_yi"] = round(amount / 1e8, 2)
            return code, sig, None
        return code, None, None

    # Baostock 接口很稳，但并发别太高，8 线程完美
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(work, c): c for c in codes}
        for i, fu in enumerate(as_completed(futs)):
            try:
                code, sig, skip_reason = fu.result()
                if skip_reason == "stale":
                    skipped_stale += 1
                elif skip_reason == "liq":
                    skipped_liq += 1
                else:
                    scanned += 1
                    
                if sig:
                    results.append(sig)
            except Exception:
                pass
            
            if progress and (i + 1) % 200 == 0:
                progress(i + 1, len(codes), len(results))

    bs.logout()
    
    results.sort(key=lambda x: x["amount_yi"], reverse=True)
    
    return {
        "trade_date": get_bj_time().strftime("%Y-%m-%d"),
        "updated_at": get_bj_time().strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "skipped": f"僵尸/超时 {skipped_stale} 只, 流动性差 {skipped_liq} 只",
        "elapsed": round(time.time() - t_start, 1),
        "params": CFG,
        "filters": FILTER,
        "signals": results,
        "pushed": False,
    }

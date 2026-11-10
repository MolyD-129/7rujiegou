# -*- coding: utf-8 -*-
"""7入结构 核心扫描器（涨停池驱动版 - 极速高稳定）"""
import json
import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

HDR = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

CFG = dict(n_break=3, m_enter=7, break_max_pct=0.15, t_max=20)
FILTER = dict(min_amount_yi=0.5, exclude_st=True)

def get_bj_time():
    return datetime.now(timezone(timedelta(hours=8)))


# ---------------- 1. 获取近 20 个交易日的涨停池 ----------------
def fetch_recent_limit_pool(days=20):
    """从东方财富拉取最近20个交易日的涨停股票代码，去重返回"""
    print(f"→ 正在拉取近 {days} 个交易日的涨停池...")
    pool_codes = set()
    
    for i in range(days):
        # 倒推日期
        d = get_bj_time() - timedelta(days=i)
        date_str = d.strftime("%Y%m%d")
        
        url = f"https://push2ex.eastmoney.com/getTopicZTPool?ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wz.ztzt&Pageindex=0&pagesize=1000&sort=fbt%3Aasc&date={date_str}"
        try:
            r = requests.get(url, headers=HDR, timeout=5)
            js = r.json()
            if js.get("data") and js["data"].get("pool"):
                for item in js["data"]["pool"]:
                    pool_codes.add(item["c"]) # 股票代码
        except Exception:
            pass
        time.sleep(0.1)
        
    print(f"→ 涨停池拉取完毕，共找到 {len(pool_codes)} 只近期涨停过的股票")
    return list(pool_codes)


# ---------------- 2. 获取K线（东方财富极速不复权接口） ----------------
def fetch_kline_eastmoney(code, days=45):
    secid = f"1.{code}" if code.startswith(("60", "68", "90", "11")) else f"0.{code}"
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56",  # 日期,开盘,收盘,最高,最低,成交量
        "klt": "101", "fqt": "0", "end": "20500101", "lmt": str(days)
    }

    for a in range(3):
        try:
            r = requests.get(url, params=params, headers=HDR, timeout=8)
            js = r.json()
            if not js.get("data") or not js["data"].get("klines"):
                return None, None
            
            name = js["data"].get("name", "")
            recs = []
            for x in js["data"]["klines"]:
                parts = x.split(",")
                recs.append({
                    "date": parts[0], "open": float(parts[1]), "close": float(parts[2]),
                    "high": float(parts[3]), "low": float(parts[4]), "volume": float(parts[5]) * 100
                })
            return name, pd.DataFrame(recs)
        except Exception:
            time.sleep(0.5)
    return None, None


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
            
        # 中间出新首板，废弃老信号（解决 603657 问题）
        if d.loc[t0+1:last, "fb"].any():
            continue
            
        bro_low = d.loc[brk:last, "low"].min()
        return {
            "t0_date": d.at[t0, "date"], "P": round(P, 2),
            "break_date": d.at[brk, "date"], "break_low": round(bro_low, 2),
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
    t_start = time.time()
    
    # 获取涨停池
    candidate_codes = fetch_recent_limit_pool(20)
    
    results = []
    scanned = 0
    skipped_liq = 0

    def work(code):
        name, df = fetch_kline_eastmoney(code, 45)
        if df is None or df.empty:
            return code, None, None, "stale"
            
        last_row = df.iloc[-1]
        amount = float(last_row['close']) * float(last_row['volume'])
        if amount < FILTER["min_amount_yi"] * 1e8:
            return code, None, None, "liq"
            
        sig = eval_signal(code, df)
        if sig:
            sig["code"] = code
            sig["name"] = name
            sig["price"] = float(last_row['close'])
            sig["amount_yi"] = round(amount / 1e8, 2)
            return code, sig, name, None
        return code, None, name, None

    # 线程可以开到 12，因为只有几百只
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(work, c): c for c in candidate_codes}
        for i, fu in enumerate(as_completed(futs)):
            try:
                code, sig, name, skip_reason = fu.result()
                if skip_reason == "liq":
                    skipped_liq += 1
                else:
                    scanned += 1
                if sig:
                    results.append(sig)
            except Exception:
                pass
            
            if progress and (i + 1) % 100 == 0:
                progress(i + 1, len(candidate_codes), len(results))

    # 兜底校验：如果扫描数量太少，说明被反爬了，直接抛错不推消息
    if len(candidate_codes) > 300 and scanned < len(candidate_codes) * 0.8:
        raise RuntimeError(f"接口严重超时，仅扫描 {scanned} 只，放弃本次推送。")

    results.sort(key=lambda x: x["amount_yi"], reverse=True)
    
    return {
        "trade_date": get_bj_time().strftime("%Y-%m-%d"),
        "updated_at": get_bj_time().strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "skipped": f"流动性差 {skipped_liq} 只",
        "elapsed": round(time.time() - t_start, 1),
        "params": CFG, "filters": FILTER, "signals": results, "pushed": False,
    }

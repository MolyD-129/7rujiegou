# -*- coding: utf-8 -*-
"""7入结构 核心扫描器（终极修复版）"""
import json
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HDR = {"User-Agent": UA, "Referer": "https://gu.qq.com/"}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POOL_FILE = os.path.join(BASE_DIR, "pool.json")

CFG = dict(n_break=3, m_enter=7, break_max_pct=0.15, t_max=20)
FILTER = dict(min_amount_yi=0.5, exclude_st=True)


def qq(code):
    if code.startswith(("60", "68", "90", "11")):
        return "sh" + code
    if code.startswith(("00", "30", "20", "12", "15")):
        return "sz" + code
    return "sz" + code


# ---------------- 1. 股票池拉取 ----------------
def _fetch_all_a_from_eastmoney():
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1", "pz": "6000", "po": "1", "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2", "invt": "2", "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
        "fields": "f12,f14",
    }
    for a in range(3):
        try:
            r = requests.get(url, params=params, headers=HDR, timeout=30)
            js = r.json()
            diff = js["data"]["diff"]
            out = []
            for it in diff:
                code = str(it.get("f12", "")).strip()
                name = str(it.get("f14", "")).strip()
                if len(code) == 6:
                    out.append({"code": code, "name": name})
            if len(out) > 3000:
                return out
            else:
                print(f"  ⚠️ 股票池过小({len(out)}只)，重试...")
        except Exception as e:
            print(f"拉股票池失败(重试{a+1}): {e}")
            time.sleep(2 * (a + 1))
    raise RuntimeError("股票池拉取失败")


def ensure_pool(force=False, max_age_hours=24):
    if not force and os.path.exists(POOL_FILE):
        age_h = (time.time() - os.path.getmtime(POOL_FILE)) / 3600
        if age_h < max_age_hours:
            with open(POOL_FILE, encoding="utf-8") as f:
                if len(json.load(f)) > 3000:
                    return
    print("→ 拉取全市场股票池...")
    arr = _fetch_all_a_from_eastmoney()
    with open(POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, indent=1)
    print(f"→ 股票池已更新：{len(arr)} 只")


def _load_pool():
    with open(POOL_FILE, encoding="utf-8") as f:
        return json.load(f)


# ---------------- 2. 获取K线（不复权） ----------------
def fetch_kline_tencent(code, days=45):
    sym = qq(code)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,,,{days},"
    for a in range(3):
        try:
            r = requests.get(url, headers=HDR, timeout=10)
            js = r.json()
            if js.get("code") != 0:
                return None
            data_key = "day" if "day" in js["data"][sym] else "qfqday"
            raw = js["data"][sym][data_key]
            if not raw or len(raw) < 20:
                return None
            recs = []
            for x in raw:
                recs.append({
                    "date": x[0], 
                    "open": float(x[1]), 
                    "close": float(x[2]),
                    "high": float(x[3]), 
                    "low": float(x[4]), 
                    "volume": float(x[5]) * 100
                })
            return pd.DataFrame(recs)
        except Exception:
            time.sleep(0.5)
    return None


# ---------------- 3. 信号判定（真实涨停价 + 洗盘废弃老信号） ----------------
def eval_signal(code, df):
    d = df.copy()
    
    # 算真实涨停价（昨收 * 1.1或1.2，保留两位）
    limit_pct = 1.20 if code.startswith(("30", "68")) else 1.10
    d["prev_close"] = d["close"].shift(1)
    d["limit_price"] = (d["prev_close"] * limit_pct).round(2)
    
    # 兼容T字、一字、烂板
    d["is_up"] = (d["close"] >= d["limit_price"] - 0.01) & (d["close"] > 0)
    d["fb"] = d["is_up"] & (~d["is_up"].shift(1).fillna(False))
    
    d["ma5v"] = d["volume"].rolling(5).mean()
    d["ma10v"] = d["volume"].rolling(10).mean()
    n = len(d)
    
    for t0 in range(n - 1, max(n - 15, 10), -1):
        if not d.at[t0, "fb"]:
            continue
            
        P = d.at[t0, "open"]
        # 如果开盘价 == 收盘价 == 最高价，判定为一字板/T字板
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
            
        # 边界处理：破位到站回期间，如果出现新首板，老信号作废（解决 603657 问题）
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
    t_start = time.time()
    ensure_pool()
    pool = _load_pool()
    pool_dict = {x["code"]: x["name"] for x in pool}
    codes = list(pool_dict.keys())

    results = []
    scanned = 0
    skipped_stale = 0
    skipped_liq = 0

    def work(code):
        df = fetch_kline_tencent(code, 45)
        if df is None:
            return code, None, "stale"
            
        last_date = datetime.strptime(df.iloc[-1]["date"], "%Y-%m-%d")
        if (datetime.now() - last_date).days > 10:
            return code, None, "stale"
            
        last_row = df.iloc[-1]
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

    with ThreadPoolExecutor(max_workers=12) as ex:
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

    results.sort(key=lambda x: x["amount_yi"], reverse=True)
    
    return {
        "trade_date": time.strftime("%Y-%m-%d"),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "skipped": f"僵尸股 {skipped_stale} 只, 流动性差 {skipped_liq} 只",
        "elapsed": round(time.time() - t_start, 1),
        "params": CFG,
        "filters": FILTER,
        "signals": results,
        "pushed": False,
    }

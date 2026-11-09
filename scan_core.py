# -*- coding: utf-8 -*-
"""7入结构 核心扫描器（腾讯 HTTP 多线程高容错版）"""
import json
import os
import time
from datetime import datetime, timedelta, timezone
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

def get_bj_time():
    return datetime.now(timezone(timedelta(hours=8)))


def qq(code):
    if code.startswith(("60", "68", "90", "11")):
        return "sh" + code
    if code.startswith(("00", "30", "20", "12", "15")):
        return "sz" + code
    return "sz" + code


# ---------------- 1. 股票池拉取（直接用 Baostock，拉一次存本地就不会再断了） ----------------
def _fetch_all_a_from_baostock():
    import baostock as bs
    bs.login()
    rs = bs.query_stock_basic()
    out = []
    while (rs.error_code == '0') & rs.next():
        row = rs.get_row_data()
        code_str = row[0].split(".")[-1]
        name = row[1]
        if len(code_str) == 6 and code_str.startswith(("60", "68", "00", "30")):
            out.append({"code": code_str, "name": name})
    bs.logout()
    return out


def ensure_pool(force=False):
    if os.path.exists(POOL_FILE) and not force:
        try:
            with open(POOL_FILE, encoding="utf-8") as f:
                arr = json.load(f)
            if len(arr) > 3000:
                print(f"→ 使用本地缓存股票池：{len(arr)} 只")
                return
        except Exception:
            pass

    print("→ 首次运行，拉取全市场股票池...")
    arr = _fetch_all_a_from_baostock()
    if len(arr) > 3000:
        with open(POOL_FILE, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, indent=1)
        print(f"→ 股票池更新成功：{len(arr)} 只")
    else:
        raise RuntimeError("拉取股票池失败！")


def _load_pool():
    with open(POOL_FILE, encoding="utf-8") as f:
        data = json.load(f)
        # 兼容东财或baostock的结构
        if isinstance(data, dict) and "data" in data and "diff" in data["data"]:
            arr = data["data"]["diff"]
        else:
            arr = data
            
    out = []
    for x in arr:
        code = str(x.get("f12", x.get("code", ""))).strip()
        name = str(x.get("f14", x.get("name", ""))).strip()
        if len(code) == 6:
            out.append({"code": code, "name": name})
    return out


# ---------------- 2. 获取K线（腾讯 HTTP 高容错） ----------------
def fetch_kline_tencent(code, days=45):
    sym = qq(code)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,,,{days},"
    
    # 重试 3 次，每次超时 12 秒，给海外网络留足时间
    for a in range(3):
        try:
            r = requests.get(url, headers=HDR, timeout=12)
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
            time.sleep(1) # 失败等 1 秒再重试
            
    return None


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
        if (get_bj_time().replace(tzinfo=None) - last_date).days > 10:
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

    # 线程数设为 6，防止被封，但依然能较快跑完
    with ThreadPoolExecutor(max_workers=6) as ex:
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

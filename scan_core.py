# -*- coding: utf-8 -*-
"""
7入结构 扫描核心 (腾讯前复权版)
- 解决连板识别错误（改用前复权数据，无断层）
- 解决只扫84只的问题（放弃实时快照，直接靠K线数据过滤）
- 完美支持一字板/T字板连板
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
HDR = {"User-Agent": UA, "Referer": "https://gu.qq.com/"}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POOL_FILE = os.path.join(BASE_DIR, "pool.json")

CFG = dict(n_break=3, m_enter=7, break_max_pct=0.15, t_max=20)
# 流动性过滤：直接用最后一天收盘价*成交量估算，不再依赖快照
FILTER = dict(min_amount_yi=0.5, exclude_st=True)


def qq(code):
    if code.startswith(("60", "68", "90", "11")):
        return "sh" + code
    if code.startswith(("00", "30", "20", "12", "15")):
        return "sz" + code
    return "sz" + code


# ---------------- 股票池 ----------------
def _fetch_all_a_from_eastmoney():
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1, "pz": 8000, "po": 1, "np": 1, "fltt": 2, "invt": 2,
        "fid": "f3",
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
            if out:
                return out
        except Exception as e:
            print(f"拉股票池失败(重试{a+1}): {e}")
            time.sleep(2 * (a + 1))
    raise RuntimeError("东方财富股票池拉取失败")


def ensure_pool(force=False, max_age_hours=24):
    if not force and os.path.exists(POOL_FILE):
        age_h = (time.time() - os.path.getmtime(POOL_FILE)) / 3600
        if age_h < max_age_hours:
            return
    print("→ 拉取全市场股票池（东方财富）...")
    arr = _fetch_all_a_from_eastmoney()
    with open(POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, indent=1)
    print(f"→ 股票池已更新：{len(arr)} 只")


def _load_pool():
    with open(POOL_FILE, encoding="utf-8") as f:
        arr = json.load(f)
    return [str(x["code"]).zfill(6) for x in arr]


# ---------------- K线 (腾讯前复权，彻底解决连板断层) ----------------
def fetch_kline_tencent(code, days=45):
    sym = qq(code)
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,,,{days},qfq"
    for a in range(3):
        try:
            r = requests.get(url, headers=HDR, timeout=10)
            js = r.json()
            if js.get("code") != 0:
                return None
            data_key = "qfqday" if "qfqday" in js["data"][sym] else "day"
            raw = js["data"][sym][data_key]
            if not raw or len(raw) < 20:
                return None
            recs = []
            for x in raw:
                recs.append({
                    "date": x[0], "open": float(x[1]), "close": float(x[2]),
                    "high": float(x[3]), "low": float(x[4]), "volume": float(x[5]) * 100
                })
            return pd.DataFrame(recs)
        except Exception:
            time.sleep(0.5)
    return None


# ---------------- 信号判定 ----------------
def eval_signal(code, df):
    d = df.copy()
    d["pct"] = d["close"].pct_change()
    
    # 严格根据前复权涨幅判断，主板9.5%，双创19.5%（兼容一字板）
    thr = 0.195 if code.startswith(("30", "68")) else 0.095
    d["is_up"] = (d["pct"] >= thr) & (d["close"] > 0)
    d["fb"] = d["is_up"] & (~d["is_up"].shift(1).fillna(False))
    
    d["ma5v"] = d["volume"].rolling(5).mean()
    d["ma10v"] = d["volume"].rolling(10).mean()
    n = len(d)
    
    # 从最近几天往回找首板
    for t0 in range(n - 1, max(n - 15, 10), -1):
        if not d.at[t0, "fb"]:
            continue
        P = d.at[t0, "open"]
        
        brk = None
        # 找破位点（T0后3天内，收盘跌破P）
        for j in range(t0 + 1, min(t0 + 1 + CFG["n_break"], n)):
            if d.at[j, "close"] < P:
                if (P - d.at[j, "close"]) / P <= CFG["break_max_pct"]:
                    brk = j
                break
        if brk is None:
            continue
            
        last = n - 1
        # 站回周期：破位后7天内
        if not (brk < last <= brk + CFG["m_enter"]):
            continue
        # 价格站回
        if d.at[last, "close"] <= P:
            continue
        # 放量确认
        if not (d.at[last, "volume"] > d.at[last, "ma5v"] and
                d.at[last, "volume"] > d.at[last, "ma10v"]):
            continue
            
        bro_low = d.loc[brk:last, "low"].min()
        return {
            "t0_date": d.at[t0, "date"], "P": round(P, 2),
            "break_date": d.at[brk, "date"], "break_low": round(bro_low, 2),
            "days_after_break": int(last - brk),
            "vol_ratio_ma5": round(d.at[last, "volume"] / d.at[last, "ma5v"], 2),
            "vol_ratio_ma10": round(d.at[last, "volume"] / d.at[last, "ma10v"], 2),
            "vol_vs_t0": round(d.at[last, "volume"] / d.at[t0, "volume"], 2),
            "t0_pct": round(d.at[t0, "pct"] * 100, 2),
            "close": round(d.at[last, "close"], 2),
            "pct_today": round(d.at[last, "pct"] * 100, 2),
        }
    return None


# ---------------- 主扫描 ----------------
def scan_all(progress=None):
    t_start = time.time()
    ensure_pool()

    codes = _load_pool()
    print(f"→ 股票池总数：{len(codes)} 只，开始并发拉取K线...")

    results = []
    scanned = 0
    failed = 0

    def work(code):
        # 获取最近 45 天前复权数据
        df = fetch_kline_tencent(code, 45)
        if df is None:
            return code, None, None
        
        # 提取基础信息用于过滤和展示
        last_row = df.iloc[-1]
        price = float(last_row['close'])
        volume = float(last_row['volume'])
        amount = price * volume  # 估算成交额（元）
        trade_date = str(last_row['date']).replace("-", "")
        
        # 流动性过滤：成交额小于 5000 万直接跳过
        if amount < FILTER["min_amount_yi"] * 1e8:
            return code, None, None
            
        sig = eval_signal(code, df)
        info = {
            "price": price,
            "amount_yi": round(amount / 1e8, 2),
            "mktcap": 0, # 这里不再获取市值，网页显示 0
            "trade_date": trade_date
        }
        return code, sig, info

    # 12 线程并发拉取
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(work, c): c for c in codes}
        for i, fu in enumerate(as_completed(futs)):
            try:
                code, sig, info = fu.result()
                if info is not None:
                    scanned += 1
                if sig:
                    results.append({
                        "code": code, 
                        "name": next((x["name"] for x in json.load(open(POOL_FILE, encoding="utf-8")) if x["code"] == code), code),
                        "price": info["price"], 
                        "mktcap": info["mktcap"],
                        "amount_yi": info["amount_yi"], 
                        **sig
                    })
            except Exception:
                failed += 1
            if progress and (i + 1) % 200 == 0:
                progress(i + 1, len(codes), len(results))

    results.sort(key=lambda x: x["amount_yi"], reverse=True)
    
    # 从拉到的最后一只股票中取日期作为交易日
    final_date = results[0].get("trade_date") if results else time.strftime("%Y-%m-%d")
    if "trade_date" in locals():
        pass
        
    return {
        "trade_date": time.strftime("%Y-%m-%d"), # 统一用今天
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "failed": failed,
        "elapsed": round(time.time() - t_start, 1),
        "params": CFG,
        "filters": FILTER,
        "signals": results,
        "pushed": False,
    }

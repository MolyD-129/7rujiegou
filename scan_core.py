# -*- coding: utf-8 -*-
"""7入结构 扫描核心（修复连板判定、空数据报错、日期错位）"""
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
FILTER = dict(min_amount_yi=0.5, min_mktcap_yi=20.0, min_price=1.0, exclude_st=True)


def qq(code):
    if code.startswith(("60", "68", "90", "11")):
        return "sh" + code
    if code.startswith(("00", "30", "20", "12", "15")):
        return "sz" + code
    return "sz" + code


def limit_thr(code):
    # 阈值稍微放宽，防止浮点误差漏掉连板
    if code.startswith(("30", "68")):
        return 0.19
    return 0.09


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
    raise RuntimeError("东方财富股票池拉取失败，请检查网络")


def ensure_pool(force=False, max_age_hours=24):
    if not force and os.path.exists(POOL_FILE):
        age_h = (time.time() - os.path.getmtime(POOL_FILE)) / 3600
        if age_h < max_age_hours:
            return
    print("→ 拉取全市场股票池（东方财富，不含北交所）...")
    arr = _fetch_all_a_from_eastmoney()
    with open(POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(arr, f, ensure_ascii=False, indent=1)
    print(f"→ 股票池已更新：{len(arr)} 只")


def _load_pool():
    with open(POOL_FILE, encoding="utf-8") as f:
        arr = json.load(f)
    return [str(x["code"]).zfill(6) for x in arr]


# ---------------- 快照 ----------------
def fetch_snapshot(batch=60):
    codes = _load_pool()
    snap = {}
    for i in range(0, len(codes), batch):
        chunk = codes[i:i + batch]
        q = ",".join(qq(c) for c in chunk)
        txt = ""
        for a in range(3):
            try:
                r = requests.get(f"https://qt.gtimg.cn/q={q}", headers=HDR, timeout=12)
                r.encoding = "gbk"
                txt = r.text
                break
            except Exception:
                time.sleep(0.5 * (a + 1))
        for line in txt.split(";"):
            line = line.strip()
            if not line.startswith("v_"):
                continue
            try:
                body = line.split('="', 1)[1].rstrip('"')
                f = body.split("~")
                if len(f) < 46:
                    continue
                code = f[2]
                price = float(f[3])
                if price <= 0:
                    continue
                
                amount_str = f[37].strip() if len(f) > 37 else "0"
                mktcap_str = f[45].strip() if len(f) > 45 else "0"
                amount = float(amount_str) * 1e4 if amount_str else 0.0
                mktcap = float(mktcap_str) if mktcap_str else 0.0
                
                date_str = f[30][:8] if len(f) > 30 and len(f[30]) >= 8 else time.strftime("%Y%m%d")

                snap[code] = {
                    "name": f[1], "price": price, "prev_close": float(f[4]),
                    "open": float(f[5]), "volume": float(f[6]) * 100,
                    "high": float(f[33]), "low": float(f[34]),
                    "amount": amount, "mktcap": mktcap, "date": date_str,
                }
            except Exception as e:
                continue
        time.sleep(0.04)
    return snap


# ---------------- K线 ----------------
SINA_URL = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            "CN_MarketData.getKLineData?symbol={sym}&scale=240&ma=no&datalen={n}")


def fetch_kline(code, days=45):
    sym = qq(code)
    url = SINA_URL.format(sym=sym, n=days)
    for a in range(3):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=10)
            if r.status_code != 200 or not r.text.strip().startswith("["):
                time.sleep(0.5 * (a + 1))
                continue
            js = json.loads(r.text)
            if not js or len(js) < 25:
                return None
            recs = [{"date": x["day"], "open": float(x["open"]), "close": float(x["close"]),
                     "high": float(x["high"]), "low": float(x["low"]),
                     "volume": float(x["volume"])} for x in js]
            return pd.DataFrame(recs).drop_duplicates("date").sort_values("date").reset_index(drop=True)
        except Exception:
            time.sleep(0.5 * (a + 1))
    return None


# ---------------- 信号判定 (纯靠涨幅判定涨停) ----------------
def eval_signal(code, df, snap_row=None):
    d = df.copy()
    d["pct"] = d["close"].pct_change()
    thr = limit_thr(code)
    
    # 不再判断 close 和 open 的关系，只要涨幅达标就认作涨停
    d["is_up"] = (d["pct"] >= thr) & (d["close"] > 0)
    
    d["fb"] = d["is_up"] & (~d["is_up"].shift(1).fillna(False))
    d["ma5v"] = d["volume"].rolling(5).mean()
    d["ma10v"] = d["volume"].rolling(10).mean()
    n = len(d)
    if n < 20:
        return None
    
    for t0 in range(n - 1, max(n - 13, 10), -1):
        if not d.at[t0, "fb"]:
            continue
        P = d.at[t0, "open"]
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

    print("→ 拉取全市场快照...")
    snap = fetch_snapshot()
    if not snap:
        raise RuntimeError("快照为空")
    trade_date = list(snap.values())[0]["date"]
    print(f"→ 快照 {len(snap)} 只，交易日 {trade_date}")

    cand = []
    for code, s in snap.items():
        if FILTER["exclude_st"] and ("ST" in s["name"].upper() or "退" in s["name"]):
            continue
        if s["amount"] < FILTER["min_amount_yi"] * 1e8:
            continue
        if s["mktcap"] < FILTER["min_mktcap_yi"]:
            continue
        if s["price"] < FILTER["min_price"]:
            continue
        cand.append(code)
    print(f"→ 流动性过滤后 {len(cand)} 只进入 K 线扫描")

    results, scanned, failed = [], 0, 0

    def work(code):
        df = fetch_kline(code, 45)
        if df is None:
            return code, None
        return code, eval_signal(code, df, snap.get(code))

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(work, c): c for c in cand}
        for i, fu in enumerate(as_completed(futs)):
            try:
                code, sig = fu.result()
                scanned += 1
                if sig:
                    s = snap[code]
                    results.append({"code": code, "name": s["name"],
                                    "price": s["price"], "mktcap": round(s["mktcap"], 1),
                                    "amount_yi": round(s["amount"] / 1e8, 2), **sig})
            except Exception:
                failed += 1
            if progress and (i + 1) % 100 == 0:
                progress(i + 1, len(cand), len(results))

    results.sort(key=lambda x: x["amount_yi"], reverse=True)
    return {
        "trade_date": trade_date,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_scanned": scanned,
        "failed": failed,
        "elapsed": round(time.time() - t_start, 1),
        "params": CFG,
        "filters": FILTER,
        "signals": results,
        "pushed": False,
    }

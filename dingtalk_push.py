# -*- coding: utf-8 -*-
"""钉钉群机器人推送（支持加签；关键词「股票」已内置于标题与正文）"""
import base64
import hashlib
import hmac
import os
import time
import urllib.parse

import requests

WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "").strip()
SECRET = os.getenv("DINGTALK_SECRET", "").strip()


def _signed_url():
    if not SECRET:
        return WEBHOOK
    ts = str(round(time.time() * 1000))
    string_to_sign = f"{ts}\n{SECRET}"
    code = hmac.new(SECRET.encode(), string_to_sign.encode(), hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(code))
    sep = "&" if "?" in WEBHOOK else "?"
    return f"{WEBHOOK}{sep}timestamp={ts}&sign={sign}"


def send_markdown(title, content):
    if not WEBHOOK:
        print("⚠️ 未配置 DINGTALK_WEBHOOK，跳过推送")
        return False
    url = _signed_url()
    payload = {"msgtype": "markdown", "markdown": {"title": title, "text": content}}
    try:
        r = requests.post(url, json=payload, timeout=15)
        j = r.json()
        if j.get("errcode") == 0:
            print("✅ 钉钉推送成功")
            return True
        print(f"❌ 钉钉推送失败: {j}")
    except Exception as e:
        print(f"❌ 钉钉推送异常: {e}")
    return False


def build_markdown(data):
    """注意：钉钉自定义关键词为「股票」，标题与正文都包含该词"""
    sig = data["signals"]
    if not sig:
        return (f"### {data['trade_date']} 股票扫描 · 无信号\n\n"
                f"扫描 {data['total_scanned']} 只，今日无符合条件的股票。\n\n"
                f"空仓也是一种结果。")
    lines = [f"### {data['trade_date']} 股票信号 · 共 {len(sig)} 只", ""]
    for i, s in enumerate(sig, 1):
        lines.append(
            f"**{i}. {s['name']}（{s['code']}）** {s['price']}\n\n"
            f"> 涨幅 {s['pct_today']:+.2f}% · 启动线 {s['P']} · 破位 {s['break_date'][5:]}\n\n"
            f"> 站回第{s['days_after_break']}天 · 量/5日均 {s['vol_ratio_ma5']} · 成交 {s['amount_yi']}亿\n"
        )
        lines.append("")
    lines.append(f"---\n扫描 {data['total_scanned']} 只 · 用时 {data['elapsed']}s")
    return "\n".join(lines)
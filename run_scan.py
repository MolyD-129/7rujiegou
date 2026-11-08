# -*- coding: utf-8 -*-
"""
入口：
  - 今天已推送    -> 跳过
  - 今天已扫描未推送 -> 只重推（不重扫）
  - 其他          -> 扫描 + 写 data.json + 推送 + 标记 pushed=true
"""
import json
import os
import sys
import time

from scan_core import scan_all
from dingtalk_push import send_markdown, build_markdown

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DATA = os.path.join(BASE_DIR, "web", "data.json")


def _load_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


def _is_today(updated_at: str) -> bool:
    return str(updated_at or "").startswith(time.strftime("%Y-%m-%d"))


def _push(data):
    if data["signals"]:
        title = f"7入结构 · 股票扫描 · {data['trade_date']} · {len(data['signals'])}只"
    else:
        title = f"7入结构 · 股票扫描 · {data['trade_date']} · 无信号"
    return send_markdown(title, build_markdown(data))


def main():
    force = "--force" in sys.argv
    today_data = None if force else _load_json(WEB_DATA)

    if today_data and _is_today(today_data.get("updated_at", "")):
        if today_data.get("pushed"):
            print("✅ 今天已扫描并推送成功，跳过。")
            return
        print("→ 今天已扫描但尚未推送，只重新推送，不重扫。")
        ok = _push(today_data)
        if ok:
            today_data["pushed"] = True
            _save_json(WEB_DATA, today_data)
            print("✅ 补推成功，已标记 pushed=true")
        else:
            print("❌ 补推失败，等下一次触发再试")
        return

    print("开始扫描…")
    data = scan_all(progress=lambda d, t, h: print(f"  进度 {d}/{t} 命中 {h}"))
    data["pushed"] = False
    _save_json(WEB_DATA, data)
    print(f"扫描完成：{len(data['signals'])} 只命中 / {data['total_scanned']} 只 / {data['elapsed']}s")

    ok = _push(data)
    if ok:
        data["pushed"] = True
        _save_json(WEB_DATA, data)
        print("✅ 已推送并标记 pushed=true")


if __name__ == "__main__":
    main()
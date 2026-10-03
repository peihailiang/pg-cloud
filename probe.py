#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
境外可达性探测（只读，不采集、不部署）

逐个站点发一个最轻的请求，只判断「网络能不能通」：
  - 200            通
  - 4xx            通了（服务器回应了，只是我们没按它的规矩请求）
  - 5xx            源站自己有问题（不是网络不通）
  - 超时 / DNS 失败 / 连接被拒   境外访问不通 ← 这才是我们要排查的

输出一份汇总，用于判断 GitHub Actions 方案是否可行。
"""

import json
import os
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.abspath(__file__))
SITES_JSON = os.environ.get("PG_SITES_JSON") or os.path.join(ROOT, "sites.json")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# 每类站，挑一个最轻的探测地址
PROBE_PATH = {
    "yunbao":    ("POST", "http://{host}/ajax/item"),
    "haoquanyi": ("GET",  "http://{host}/requestservic/homepage"),
    "spa":       ("GET",  "https://{host}/server.json"),
    "sdfaka":    ("GET",  "http://{host}/"),
    "qingtian":  ("GET",  "http://{host}/"),
}


def probe_one(site):
    host = site["host"]
    prog = site.get("program") or "yunbao"
    method, tpl = PROBE_PATH.get(prog, ("GET", "http://{host}/"))
    url = tpl.format(host=host)

    t0 = time.time()
    try:
        req = urllib.request.Request(url, method=method)
        req.add_header("User-Agent", UA)
        req.add_header("Accept", "*/*")
        body = b"{}" if method == "POST" else None
        r = urllib.request.urlopen(req, data=body, timeout=15)
        code = r.status
        n = len(r.read(4096))
        return (host, prog, "OK", code, n, time.time() - t0, "")
    except urllib.error.HTTPError as e:
        # 服务器回话了 —— 网络是通的
        return (host, prog, "OK", e.code, 0, time.time() - t0, "")
    except urllib.error.URLError as e:
        return (host, prog, "FAIL", 0, 0, time.time() - t0, str(e.reason)[:80])
    except socket.timeout:
        return (host, prog, "FAIL", 0, 0, time.time() - t0, "timeout")
    except Exception as e:
        return (host, prog, "FAIL", 0, 0, time.time() - t0, type(e).__name__)


def main():
    sites = json.load(open(SITES_JSON, encoding="utf-8"))
    print("=== 境外可达性探测 ===")
    print("运行环境:", sys.platform, "| Python", sys.version.split()[0])
    print("站点总数: %d" % len(sites))
    print("-" * 72)

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=12) as ex:
        rows = list(ex.map(probe_one, sites))

    ok = [r for r in rows if r[2] == "OK"]
    bad = [r for r in rows if r[2] != "OK"]

    for host, prog, st, code, n, dt, err in rows:
        mark = "✓" if st == "OK" else "✗"
        detail = ("HTTP %s" % code) if st == "OK" else err
        print("  %s %-24s %-10s %-12s %.1fs" % (mark, host, prog, detail, dt))

    print("-" * 72)
    print("用时 %.1fs" % (time.time() - t0))
    print("可达 %d / 不通 %d" % (len(ok), len(bad)))

    if bad:
        print("\n不通的站点：")
        for host, prog, st, code, n, dt, err in bad:
            print("  %-24s %-10s %s" % (host, prog, err))

    # 结论：只要过半可达，方案就基本成立
    ratio = len(ok) / max(1, len(rows))
    print("\n=== 结论 ===")
    if ratio >= 0.8:
        print("可达率 %.0f%%，GitHub Actions 方案可行。" % (ratio * 100))
    elif ratio >= 0.5:
        print("可达率 %.0f%%，部分站点境外不通，但主体可用。" % (ratio * 100))
    else:
        print("可达率 %.0f%%，境外访问困难，建议改用国内平台。" % (ratio * 100))

    # 打印一个醒目总结，方便在 Actions 页面直接看到
    print("\n::notice title=可达率::%d/%d (%.0f%%)" % (len(ok), len(rows), ratio * 100))


if __name__ == "__main__":
    main()

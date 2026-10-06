# -*- coding: utf-8 -*-
# 2026-10-06：本采集器同时供两处运行 ——
#   · 本机（services/crawl_all.py，由 sh_sites_server.js 每 30 分钟 spawn）
#   · 云端（pg-cloud/crawler.py，GitHub Actions 每 30 分钟，本机关机不影响）
# 两处内容必须保持一致（云端版另有几处「境外网络」专属参数，见下面标注）。
"""
瓶盖比价网 —— 全站采集器（69 站，纯实时抓源站，不用任何第三方聚合源）

2026-10-04 逐类接口实测定稿：

  1) yunbao（40 站）
     POST http://<host>/ajax/item        body: {}
       -> 返回 base64 串，解码后 .data.good = 商品数组
       库存口径：type=='k' 取 goodStock，否则取 stock   （瑶瑶实测 stock=0 但 goodStock=948）
     GET  http://<host>/get_api_stock?gid=<id>   -> 单商品精确库存（可选）

  2) haoquanyi（13 站，含大水网 wx.walo888.com）
     GET  http://<host>/requestservic/homepage
       -> 返回 base64 串，解码后 .yybshoplist = 分类数组，
          每类 .list[].{id,title,money,stock,api}
     POST http://<host>/requestservic/readingstcok   data: pid=<id>
       -> {"kucun":"3907"}   精确库存（api=1 的代销商品必须走它）
     注意：https 会 TLSV1_ALERT_INTERNAL_ERROR，必须走 http

  3) spa（14 站，同一套系统，共用一个后端网关）
     GET  https://<host>/server.json   -> {"serverhost": N}
       N=1 或 3 -> https://niu2.eaqian.cn
       N=2 或 4 -> https://xg2.eayous1.com
     POST <网关>/home/index/siteConfig            body {}  （拿站点名）
     POST <网关>/home/index/CateListSimpleGoods   body {}
       -> .data.list[].goodsList[].{id,name,price,stock,...}
     ★ 关键：必须带两个自定义头，网关靠它们识别站点：
          serverhost: <N>
          myhost: <该站域名>
       缺了这两个头会返回默认演示数据！

  4) sdfaka（1 站 nnfka.cn）
     GET  http://<host>/            -> 页面内嵌 _token=xxxx
     POST http://<host>/ajax/index  data: _token=<token>
       -> 返回 base64（js base64 变体），解码后 JSON：
          .data.cat[]   分类数组（abridge 作为分组键）
          .data.goodList{ abridge: [ {id,name,price,stock,apistr,...} ] }
     GET  http://<host>/get_api_stock?gid=<id> -> {"kucun":"..."}  单商品精确库存

  5) qingtian（1 站 a11.a6qt.cn，晴天）
     传统 PHP 模板站，商品与价格直接渲染在 HTML（"商品名 ￥ 价格 已售N件"）。
     ★ 该站不公开库存数字（只展示销量），故库存一律标记 -1（前端识别为"不公开"，
       不参与缺货判定，避免把有货误判成没货）。
     商品链接形如 /pbg/<gid>.html，但那是分类页；这里直接从首页/分类页文本解析商品名与价格。

用法：
  python crawl_all.py                # 全量采集，输出 data/live_all.json
  python crawl_all.py --only spa     # 只跑某一类（yunbao/haoquanyi/spa）
  python crawl_all.py --host yao27.com    # 只跑某个站
  python crawl_all.py --verify       # 对 haoquanyi 逐商品复核精确库存（慢）
"""
import base64
import http.cookiejar
import json
import os
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---- 2026-10-04 关键修复：强制 stdout/stderr 用 UTF-8 ----
# 本脚本会被「守护进程拉起的服务」在无人值守环境下 spawn，那时 stdout 继承的是
# 中文 Windows 默认编码 GBK(cp936)。脚本里打印的 ✓ ✗ ⟳ ¥ 四个符号 GBK 编不出来，
# 会抛 UnicodeEncodeError 直接退出（exit 1），且发生在「写文件」之前 →
# live_all.json 永不更新、也不会部署。实测：PYTHONIOENCODING=gbk 时必崩。
# 这里显式重设编码，无论被谁、在什么环境下调用都安全。
for _s in ("stdout", "stderr"):
    try:
        getattr(sys, _s).reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT = os.path.dirname(os.path.abspath(__file__))
# 路径可用环境变量覆盖，便于在云端（GitHub Actions）跑：
#   PG_SITES_JSON=./sites.json   PG_OUT_FILE=./dist/live_all.json
SITES_JSON = os.environ.get("PG_SITES_JSON") or os.path.join(ROOT, "lib", "sites.json")
OUT_FILE = os.environ.get("PG_OUT_FILE") or os.path.join(ROOT, "bijia-site", "live_all.json")
OUT_DIR = os.path.dirname(OUT_FILE)
# 云端仓库把站点清单放在根目录，lib/ 不存在时自动回退
if not os.path.isfile(SITES_JSON):
    _alt = os.path.join(ROOT, "sites.json")
    if os.path.isfile(_alt):
        SITES_JSON = _alt

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")

# 等待型任务（纯 HTTP）用线程池。
# 2026-10-06：境外 runner 出口（美国）到国内源站往返明显更慢，
# 20 并发会互相抢带宽、批量超时（实测商品数掉到本机的 78%）。
# 云端因此降到 14 并发 + 更长的单次等待，并对失败站点再做一轮低并发重试。
CLOUD = (os.environ.get("PG_CLOUD") == "1")
SITE_WORKERS = 14 if CLOUD else 20
ITEM_WORKERS = 12 if CLOUD else 16
RETRY_WORKERS = 4          # 二次重试：低并发，避免再次互相拖垮
HTTP_TIMEOUT = 30 if CLOUD else 20
HTTP_TRIES = 4 if CLOUD else 3

_ctx = ssl.create_default_context()
_ctx.check_hostname = False
_ctx.verify_mode = ssl.CERT_NONE

GATEWAYS = {1: "https://niu2.eaqian.cn", 2: "https://xg2.eayous1.com",
            3: "https://niu2.eaqian.cn", 4: "https://xg2.eayous1.com"}


def _open(req, timeout=HTTP_TIMEOUT):
    return urllib.request.urlopen(req, timeout=timeout, context=_ctx)


def http_get(url, timeout=HTTP_TIMEOUT, extra_headers=None, tries=HTTP_TRIES):
    """GET，带退避重试。源站（尤其大水）偶发 502/超时，重试一次往往就好了，
    否则整站 100+ 商品会因一次抖动全丢。"""
    h = {"User-Agent": UA}
    h.update(extra_headers or {})
    last = None
    for i in range(tries):
        try:
            with _open(urllib.request.Request(url, headers=h), timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if i < tries - 1:
                time.sleep(1.5 * (i + 1))
    raise last


def http_post(url, data=None, timeout=HTTP_TIMEOUT, json_body=None, extra_headers=None):
    h = {"User-Agent": UA}
    h.update(extra_headers or {})
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        h["Content-Type"] = "application/json;charset=UTF-8"
    else:
        body = urllib.parse.urlencode(data or {}).encode("utf-8")
        h["Content-Type"] = "application/x-www-form-urlencoded"
    with _open(urllib.request.Request(url, data=body, headers=h), timeout) as r:
        return r.read().decode("utf-8", "ignore")


def _b64_json(raw):
    """源站常见的 base64 包装 JSON"""
    s = raw.decode("utf-8", "ignore").strip() if isinstance(raw, bytes) else raw.strip()
    if s.startswith("{") or s.startswith("["):
        return json.loads(s)
    return json.loads(base64.b64decode(s).decode("utf-8", "ignore"))


# ---------------------------------------------------------------- yunbao
def crawl_yunbao(site):
    """40 站：/ajax/item 返回 base64 包装的 {data:{good:[...]}}"""
    host, proto = site["host"], site.get("proto") or "http"
    url = "%s://%s/ajax/item" % (proto, host)
    raw = http_post(url, json_body={})
    j = _b64_json(raw)
    goods = (j.get("data") or {}).get("good") or []
    out = []
    for g in goods:
        try:
            gid = str(g.get("id") or "")
            if not gid:
                continue
            # 库存口径：type=='k' 用 goodStock，否则 stock
            st = g.get("goodStock") if str(g.get("type")) == "k" else g.get("stock")
            if st in (None, ""):
                st = g.get("stock") or g.get("goodStock") or 0
            out.append({
                "gid": gid,
                "title": g.get("name") or "",
                "price": float(g.get("price") or 0),
                "stock": int(float(st or 0)),
                "status": int(g.get("status") or 0),
                "url": "%s://%s/goods/%s" % (proto, host, gid),
            })
        except Exception:
            continue
    name = ((j.get("webInfo") or {}).get("name")
            or (j.get("webInfo") or {}).get("title") or site["name"])
    return out, name


# ------------------------------------------------------------- haoquanyi
def crawl_haoquanyi(site, verify=False):
    """13 站（含大水）：/requestservic/homepage 返回 base64 的 yybshoplist

    verify=True 时对每个商品再打一次 /requestservic/readingstcok 复核精确库存
    （homepage 的 stock 字段历史上出现过脏值，如大水安慕希曾返回 1189 而商品页为 0）。
    """
    host = site["host"]
    raw = http_get("http://%s/requestservic/homepage" % host)
    j = _b64_json(raw)
    classes = j.get("yybshoplist") or []
    out = []
    for cls in classes:
        cname = cls.get("classname") or ""
        for it in cls.get("list") or []:
            pid = str(it.get("id") or "")
            if not pid:
                continue
            out.append({
                "gid": pid,
                "title": it.get("title") or "",
                "price": float(it.get("money") or 0),
                "stock": int(float(it.get("stock") or 0)),
                "api": int(it.get("api") or 0),
                "status": int(it.get("status") or 0),
                "class": cname,
                "url": "http://%s/index/p/id/%s" % (host, pid),
            })
    if verify and out:
        # 并发压到 3 且串行分批：源站（大水）对并发敏感，猛打会 502。
        # 复核失败的条目保留 homepage 原值（fixed 里没有就不覆盖）。
        with ThreadPoolExecutor(max_workers=3) as ex:
            futs = [ex.submit(haoquanyi_verify_stock, host, it["gid"]) for it in out]
            fixed = {}
            for fu in as_completed(futs):
                pid, k = fu.result()
                if k is not None:
                    fixed[pid] = k
        for it in out:
            if it["gid"] in fixed:
                it["stock"] = fixed[it["gid"]]
    return out, None


def haoquanyi_verify_stock(host, pid, tries=2):
    """逐商品复核精确库存。带重试，容错源站偶发 502。"""
    for _ in range(tries):
        try:
            txt = http_post("http://%s/requestservic/readingstcok" % host, {"pid": pid}, timeout=15)
            j = json.loads(txt)
            k = j.get("kucun")
            return pid, (int(k) if str(k).isdigit() else None)
        except Exception:
            time.sleep(0.3)
    return pid, None


# ------------------------------------------------------------------- spa
def crawl_spa(site):
    """14 站：server.json 定 serverhost → 网关 POST，必须带 serverhost/myhost 头"""
    host, proto = site["host"], site.get("proto") or "https"
    try:
        sh = json.loads(http_get("%s://%s/server.json" % (proto, host), timeout=12).decode("utf-8", "ignore"))
        sh = int(sh.get("serverhost") or 0)
    except Exception:
        sh = 0
    gw = GATEWAYS.get(sh)
    if not gw:
        return [], None
    hdr = {"serverhost": str(sh), "myhost": host,
           "Referer": "%s://%s/" % (proto, host),
           "Origin": "%s://%s" % (proto, host)}
    site_name = None
    try:
        cfg = json.loads(http_post(gw + "/home/index/siteConfig", json_body={}, extra_headers=hdr, timeout=20))
        site_name = (cfg.get("data") or {}).get("site_name")
        sh_dom = (cfg.get("data") or {}).get("site_domain")
        if sh_dom:
            site_name = site_name or host
    except Exception:
        pass
    raw = http_post(gw + "/home/index/CateListSimpleGoods", json_body={}, extra_headers=hdr, timeout=25)
    j = json.loads(raw)
    cats = (j.get("data") or {}).get("list") or []
    out = []
    for c in cats:
        for g in c.get("goodsList") or []:
            gid = str(g.get("id") or "")
            if not gid:
                continue
            out.append({
                "gid": gid,
                "title": g.get("name") or "",
                "price": float(g.get("price") or 0),
                "stock": int(float(g.get("stock") or 0)),
                "status": int(g.get("status") or 0),
                "class": c.get("name") or "",
                "url": "%s://%s/goods/%s" % (proto, host, gid),
            })
    return out, site_name


# ---------------------------------------------------------------- sdfaka
def crawl_sdfaka(site):
    """1 站：首页取 _token + session cookie → POST /ajax/index → base64 解码 → data.goodList
       注意：必须用同一个 cookie 会话（XSRF-TOKEN + *_pro_session），否则返回空。"""
    host, proto = site["host"], "https"
    base = "%s://%s" % (proto, host)
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cj),
        urllib.request.HTTPSHandler(context=_ctx))
    opener.addheaders = [("User-Agent", UA)]
    html = opener.open(base + "/", timeout=20).read().decode("utf-8", "ignore")
    m = re.search(r"_token=([A-Za-z0-9]+)", html)
    token = m.group(1) if m else ""
    req = urllib.request.Request(
        base + "/ajax/index", data=("_token=" + token).encode("utf-8"),
        headers={"User-Agent": UA,
                 "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                 "X-Requested-With": "XMLHttpRequest",
                 "Referer": base + "/", "Origin": base})
    raw = opener.open(req, timeout=30).read()
    if not raw:
        raise RuntimeError("empty response（token/会话失效）")
    j = _b64_json(raw)
    data = j.get("data") or {}
    goodlist = data.get("goodList") or {}
    flat = []
    if isinstance(goodlist, dict):
        for _k, arr in goodlist.items():
            if isinstance(arr, list):
                flat.extend(arr)
    elif isinstance(goodlist, list):
        flat = goodlist
    if not flat and isinstance(data.get("good"), list):
        flat = data["good"]
    if not flat and isinstance(j.get("good"), list):
        flat = j["good"]
    out = []
    for g in flat:
        try:
            gid = str(g.get("id") or "")
            if not gid:
                continue
            st = g.get("goodStock") if str(g.get("type")) == "k" else g.get("stock")
            if st in (None, ""):
                st = g.get("stock") or g.get("goodStock") or 0
            out.append({
                "gid": gid,
                "title": g.get("name") or g.get("title") or "",
                "price": float(g.get("price") or g.get("money") or 0),
                "stock": int(float(st or 0)),
                "status": int(g.get("status") or 0),
                "url": "%s/goods/%s" % (base, gid),
            })
        except Exception:
            continue
    return out, None


# --------------------------------------------------------------- qingtian
def crawl_qingtian(site):
    """1 站（晴天 a11.a6qt.cn）：商品清单在首页，**库存只在商品详情页**。

    2026-10-04 两处修正（原实现有 bug）：
      1) gid 原用 `str(abs(hash(商品名)) % 1e8)` —— CPython 字符串 hash 每进程带随机盐，
         gid **每轮采集都会变** → 前端 live_all.json 快照永远匹配不上底包/上一轮的数据。
         现改用商品自己的 id（首页卡片里的 /pg/<id>.html），稳定且与底包里的编号一致。
      2) 原实现直接给 stock = -1「不公开」—— 实测**详情页明确写着**「库存：<span>N</span>个」。
         现逐商品抓详情页取真实库存（65 商品并行约 6 秒，可接受）。
    """
    host, proto = site["host"], "http"
    base = "%s://%s" % (proto, host)
    html = http_get(base + "/pbn.html", timeout=20).decode("utf-8", "ignore")

    cards = []
    seen = set()
    for m in re.finditer(r"<li\b[^>]*>(.*?)</li>", html, re.S):
        c = m.group(1)
        mid = re.search(r"/pg/(\d+)\.html", c)
        if not mid:
            continue                      # /pbg/ 之类不是普通商品，跳过
        pid = mid.group(1)
        if pid in seen:
            continue
        mn = re.search(r'class="elli"[^>]*>\s*<a[^>]*>(.*?)</a>', c, re.S)
        name = re.sub(r"<[^>]+>", "", mn.group(1)).strip() if mn else ""
        name = re.sub(r"\s+", " ", name)
        if len(name) < 2:
            continue
        mp = re.search(r'class="price"[^>]*>\s*[￥¥]\s*<span>\s*([0-9]+(?:\.[0-9]{1,2})?)\s*</span>', c)
        try:
            price = float(mp.group(1)) if mp else 0.0
        except Exception:
            price = 0.0
        if price <= 0 or price > 99999:
            continue
        ms = re.search(r"已售\s*(\d+)\s*件", c)
        seen.add(pid)
        cards.append({
            "gid": pid, "title": name, "price": price,
            "sales": int(ms.group(1) or 0) if ms else 0,
            "url": "%s/pg/%s.html" % (base, pid),
            "stock": -1,
        })

    def _stock(it):
        """抓商品详情页库存。实测页面有 **3 种展示形态**（2026-10-04 全量核对）：

          · 「库存：<span>5041</b>个」                    → 真实数字
          · 「库存：<span>0</b>个」                       → 售罄（0）
          · 「库存：<span style=...>库存充足</span>」      → 超过阈值不显示数字

        第 3 种若只写 `\\d+` 会匹配失败 → 退成 -1 → 前端显示「库存不公开」；
        而底包 data.js 里这类商品标的是 **9999**（语义=库存充足），且基本都是虚拟商品
        （流量卡/会员/交流群/售后须知/代看服务）。所以这里同样映射为 9999，保持口径一致。
        """
        try:
            page = http_get(it["url"], timeout=20, tries=2).decode("utf-8", "ignore")
            m = re.search(r"库存：\s*<span[^>]*>\s*(\d+)", page)
            if m:
                it["stock"] = int(m.group(1))
            elif "库存充足" in page:
                it["stock"] = 9999
        except Exception:
            pass
        return it

    if cards:
        with ThreadPoolExecutor(max_workers=ITEM_WORKERS) as ex:
            list(ex.map(_stock, cards))

    out = []
    for it in cards:
        out.append({
            "gid": it["gid"], "title": it["title"], "price": it["price"],
            "stock": it["stock"], "sales": it["sales"], "status": 1,
            "url": it["url"],
        })
    return out, "晴天云商城"


# ----------------------------------------------------------------- 分派
HANDLERS = {
    "yunbao": crawl_yunbao,
    "haoquanyi": crawl_haoquanyi,
    "spa": crawl_spa,
    "sdfaka": crawl_sdfaka,
    "qingtian": crawl_qingtian,
}


def crawl_site(site, verify=False):
    """返回 (site_dict, items, site_name, err)"""
    prog = site.get("program")
    fn = HANDLERS.get(prog)
    if not fn:
        return site, [], None, "未支持类型 %s" % prog
    t0 = time.time()
    try:
        # haoquanyi 支持 verify（逐商品复核精确库存）；其余类型签名不带该参数
        items, sname = fn(site, verify) if prog == "haoquanyi" else fn(site)
        return site, items, sname, None
    except Exception as e:
        return site, [], None, "%s: %s" % (type(e).__name__, str(e)[:90])


def load_sites():
    with open(SITES_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def load_prev():
    """读上一轮产物，用于「本站这轮失败/为空时沿用旧值」。
    2026-10-04 加的护栏：源站会偶发整体 502（如大水 wx.walo888.com），
    若直接把空数组写进产物，前端 applyLiveIndex 虽不会新增行，
    但该站所有行都失去实时覆盖、退回 data.js 陈旧快照（如 1189）。
    宁可给略旧的实时值，也不要退回错误快照。"""
    try:
        with open(OUT_FILE, "r", encoding="utf-8") as f:
            return (json.load(f) or {}).get("data") or {}
    except Exception:
        return {}


def main():
    args = sys.argv[1:]
    only_prog = None
    only_host = None
    verify = "--verify" in args
    for i, a in enumerate(args):
        if a == "--only" and i + 1 < len(args):
            only_prog = args[i + 1]
        if a == "--host" and i + 1 < len(args):
            only_host = args[i + 1]

    sites = load_sites()
    if only_prog:
        sites = [s for s in sites if s["program"] == only_prog]
    if only_host:
        sites = [s for s in sites if s["host"] == only_host]
    print("待采集 %d 站" % len(sites))

    prev = load_prev()
    t0 = time.time()
    result = {}
    stats = {}
    reused = [0]                       # 用列表承载，便于嵌套函数里累加
    bad_sites = []                     # 本批「既没采到、也没有上一轮可沿用」的站点

    def run_batch(targets, workers, is_retry=False):
        """采一批站点。

        2026-10-06：抽成函数是为了支持失败站点的二次重试 ——
        云端 runner 每轮都是全新环境，dist/ 不进仓库，load_prev() 恒为空，
        于是「沿用上一轮」这条护栏在云端完全失效，失败的站点直接整站丢失。
        补一轮低并发重试能把大部分临时性超时救回来。
        本机同样受益（源站偶发 502 时不必等到下一轮）。
        """
        local_bad = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(crawl_site, s, verify): s for s in targets}
            for fu in as_completed(futs):
                site, items, sname, err = fu.result()
                key = site["host"]
                # 2026-10-04 瘦身：页面只按 host|gid 匹配，取 price/stock 两个值。
                # 原样存 title/api/class/url 会让产物膨胀到 1.1MB+，经 8777 的 chunked
                # 传输会被截断（实测浏览器只收到 842KB → JSON 解析失败 → 前端静默跳过），
                # 因此这里只落最小字段集。
                slim = []
                for it in items:
                    g = it.get("gid")
                    if g is None or g == "":
                        continue
                    slim.append({
                        "gid": str(g),
                        "price": it.get("price"),
                        "stock": it.get("stock"),
                    })
                # 失败或采到空 → 沿用上一轮（宁可给略旧的实时值，也不退回 data.js 陈旧快照）
                if not slim:
                    old = prev.get(key)
                    if old and old.get("items"):
                        result[key] = old
                        reused[0] += 1
                        p = site["program"]
                        stats.setdefault(p, {"ok": 0, "fail": 0, "items": 0})
                        stats[p]["ok"] += 1
                        stats[p]["items"] += len(old["items"])
                        print("  ⟳ %-20s %-10s %5d 条  （沿用上一轮）%s"
                              % (site["host"], p, len(old["items"]), err or ""))
                        continue
                    local_bad.append(site)      # 既没数据也没旧值 → 待重试
                result[key] = {
                    "site": sname or site["name"],
                    "program": site["program"],
                    "count": len(slim),
                    "items": slim,
                }
                p = site["program"]
                stats.setdefault(p, {"ok": 0, "fail": 0, "items": 0})
                if err:
                    stats[p]["fail"] += 1
                    if not slim:
                        print("  ✗ %-20s %-10s %s" % (site["host"], p, err))
                else:
                    stats[p]["ok"] += 1
                    stats[p]["items"] += len(items)
                    print("  ✓ %-20s %-10s %5d 条  %s" % (site["host"], p, len(items), sname or ""))
        return local_bad

    bad_sites = run_batch(sites, SITE_WORKERS)
    if bad_sites:
        print("\n=== 二次重试：%d 个失败站点（低并发 + 更宽松的等待）===" % len(bad_sites))
        still = run_batch(bad_sites, RETRY_WORKERS, is_retry=True)
        if still:
            print("  仍失败 %d 站：%s" % (len(still), ", ".join(s["host"] for s in still)))

    print("\n=== 汇总（%.1fs）===" % (time.time() - t0))
    total = 0
    for p, s in sorted(stats.items()):
        print("  %-10s 成功 %d / 失败 %d   商品 %d" % (p, s["ok"], s["fail"], s["items"]))
        total += s["items"]
    print("  合计商品 %d 条" % total)
    if reused[0]:
        print("  其中 %d 站沿用上一轮（源站临时不可用）" % reused[0])

    out = {
        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "ts": int(time.time() * 1000),
        "sites": len(result),
        "total_items": total,
        "data": result,
    }
    if not os.path.isdir(OUT_DIR):
        os.makedirs(OUT_DIR)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print("已写入 %s（%.1f KB）" % (OUT_FILE, os.path.getsize(OUT_FILE) / 1024.0))


if __name__ == "__main__":
    main()

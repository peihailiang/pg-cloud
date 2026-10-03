# 瓶盖比价网 · 云端采集（GitHub Actions）

每 30 分钟自动采集 68 个来源站的实时库存与价格，产出 `live_all.json`
并部署到 [pinggai-bijia.surge.sh](https://pinggai-bijia.surge.sh)。

**跑在 GitHub 的服务器上，本机关机也照常运行。**

---

## 一次性配置（三步）

### 1. 建仓库

在 GitHub 上新建一个 **Private** 仓库（私有，避免 token 风险），
名字随意，例如 `pg-bijia-cloud`。

### 2. 加一个 Secret

仓库 → Settings → Secrets and variables → Actions → New repository secret

| Name | Value |
|---|---|
| `SURGE_TOKEN` | `1597874172653166e4f6467f74630d49` |

### 3. 推代码

在本目录执行：

```bash
git init
git add .
git commit -m "init: 云端采集"
git branch -M main
git remote add origin https://github.com/<你的用户名>/pg-bijia-cloud.git
git push -u origin main
```

推上去后，Actions 会自动开始跑第一轮。

---

## 日常

- **手动跑一次**：仓库 → Actions → crawl-and-deploy → Run workflow
- **看日志**：Actions 页面点进任意一次 run
- **改采集频率**：编辑 `.github/workflows/crawl-and-deploy.yml` 里的 `cron`
- **改来源站清单**：编辑 `sites.json`
- **改页面**：编辑 `site-src/index.html`

---

## 文件说明

| 文件 | 作用 |
|---|---|
| `crawler.py` | 采集器，纯标准库，五类站各一套解析 |
| `sites.json` | 68 个来源站清单（host / name / program） |
| `site-src/` | 静态站资源（页面、商品库、图标） |
| `.github/workflows/crawl-and-deploy.yml` | 定时任务：采集 → 校验 → 部署 |

---

## 两个注意点

1. **GitHub 的 cron 有延迟**。官方说明是高峰期可能排队 5–15 分钟，
   所以实际间隔会略大于 30 分钟，这是平台特性，改不了。
2. **源站挂掉时的处理**：采集器遇到某站采不到（如 502），
   会沿用上一轮该站的数据，不会把空值写进产物。
   只有采集整体异常（站点 < 40 或商品 < 2000）时才会中止部署，
   保证线上永远是可用数据。

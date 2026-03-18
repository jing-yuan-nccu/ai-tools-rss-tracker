"""
RSS Tracker - 追蹤 AI 工具更新資訊
目標來源：Codex, Claude (Anthropic), OpenCode, OpenClaw
"""

import sys
import io
import feedparser
import sqlite3
from datetime import datetime
from pathlib import Path

# 修正 Windows 終端機中文顯示問題
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── 設定 RSS 來源 ────────────────────────────────────────────────
FEEDS = {
    "codex_changelog": {
        "name": "OpenAI Codex Changelog",
        "url": "https://developers.openai.com/codex/changelog/rss.xml",
    },
    "openai_status": {
        "name": "OpenAI Status",
        "url": "https://status.openai.com/feed.rss",
    },
    "anthropic_news": {
        "name": "Anthropic News (community)",
        "url": "https://raw.githubusercontent.com/taobojlen/anthropic-rss-feed/main/anthropic_news_rss.xml",
    },
    "opencode_releases": {
        "name": "OpenCode GitHub Releases",
        "url": "https://github.com/opencode-ai/opencode/releases.atom",
    },
    "openclaw_feeds": {
        "name": "OpenClaw Feeds (aggregator)",
        "url": "https://github.com/arc-claw-bot/openclaw-feeds/releases.atom",
    },
    "agents_radar": {
        "name": "Agents Radar (Claude/Codex/OpenClaw digest)",
        "url": "https://github.com/duanyytop/agents-radar/releases.atom",
    },
}

# ── 資料庫設定 ───────────────────────────────────────────────────
DB_PATH = Path(__file__).parent / "rss_data.db"


def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_key    TEXT NOT NULL,
            feed_name   TEXT NOT NULL,
            title       TEXT NOT NULL,
            link        TEXT UNIQUE,
            published   TEXT,
            summary     TEXT,
            content     TEXT,
            fetched_at  TEXT NOT NULL
        )
    """)
    # 舊資料庫補欄位
    cols = [r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()]
    if "content" not in cols:
        conn.execute("ALTER TABLE articles ADD COLUMN content TEXT")
    conn.commit()


def parse_published(entry) -> str:
    """統一轉換發布時間為 ISO 格式字串"""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6]).isoformat()
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6]).isoformat()
    return ""


def fetch_feed(feed_key: str, feed_cfg: dict, conn: sqlite3.Connection) -> dict:
    """抓取單一 RSS feed，回傳統計結果"""
    print(f"\n[{feed_cfg['name']}]")
    print(f"  URL: {feed_cfg['url']}")

    result = {"new": 0, "duplicate": 0, "error": None}

    try:
        parsed = feedparser.parse(feed_cfg["url"])

        if parsed.bozo and not parsed.entries:
            result["error"] = str(parsed.bozo_exception)
            print(f"  ERROR 錯誤: {result['error']}")
            return result

        print(f"  找到 {len(parsed.entries)} 筆文章")

        for entry in parsed.entries:
            title     = getattr(entry, "title", "(無標題)")
            link      = getattr(entry, "link", "")
            summary   = getattr(entry, "summary", "")
            published = parse_published(entry)

            # content 欄位通常比 summary 有更完整的內容
            content = ""
            if hasattr(entry, "content") and entry.content:
                content = entry.content[0].get("value", "")

            try:
                conn.execute("""
                    INSERT INTO articles (feed_key, feed_name, title, link, published, summary, content, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    feed_key,
                    feed_cfg["name"],
                    title,
                    link,
                    published,
                    summary[:500],
                    content,
                    datetime.now().isoformat(),
                ))
                result["new"] += 1
            except sqlite3.IntegrityError:
                result["duplicate"] += 1

        conn.commit()
        print(f"  OK 新增 {result['new']} 筆 / 重複 {result['duplicate']} 筆")

    except Exception as e:
        result["error"] = str(e)
        print(f"  ERROR 例外: {e}")

    return result


def fetch_all():
    """抓取所有 feeds 並存入資料庫"""
    print("=" * 50)
    print(f"RSS Tracker 開始執行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)

        summary = {}
        for key, cfg in FEEDS.items():
            summary[key] = fetch_feed(key, cfg, conn)

    # 印出本次摘要
    print("\n" + "=" * 50)
    print("執行完畢摘要：")
    total_new = sum(r["new"] for r in summary.values())
    total_dup = sum(r["duplicate"] for r in summary.values())
    errors    = [k for k, r in summary.items() if r["error"]]
    print(f"  新增文章：{total_new} 筆")
    print(f"  已存在（跳過）：{total_dup} 筆")
    if errors:
        print(f"  失敗來源：{', '.join(errors)}")
    print(f"  資料庫位置：{DB_PATH}")
    print("=" * 50)


def show_latest(n: int = 10):
    """顯示最新 n 筆文章"""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT feed_name, title, published, link
            FROM articles
            ORDER BY published DESC, id DESC
            LIMIT ?
        """, (n,)).fetchall()

    print(f"\n最新 {n} 筆文章：")
    print("-" * 60)
    for feed_name, title, published, link in rows:
        print(f"[{feed_name}]")
        print(f"  {title}")
        print(f"  {published}  {link}")
        print()


if __name__ == "__main__":
    fetch_all()
    show_latest(10)

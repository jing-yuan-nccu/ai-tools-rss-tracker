"""
push_to_notion.py
從 SQLite 讀取未推送的文章，寫入 Notion Database
"""

import sys
import io
import sqlite3
import os
from datetime import datetime
from pathlib import Path

from notion_client import Client
from notion_client.errors import APIResponseError
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv()

DB_PATH     = Path(__file__).parent / "rss_data.db"
NOTION_DB   = os.getenv("NOTION_DATABASE_ID")
notion      = Client(auth=os.getenv("NOTION_TOKEN"))


def init_pushed_column(conn: sqlite3.Connection):
    """確保 articles table 有 notion_pushed 欄位"""
    cols = [r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()]
    if "notion_pushed" not in cols:
        conn.execute("ALTER TABLE articles ADD COLUMN notion_pushed INTEGER DEFAULT 0")
        conn.commit()


def push_article(row: tuple) -> bool:
    """把單筆文章寫入 Notion，回傳是否成功"""
    article_id, feed_key, feed_name, title, link, published, summary, content, fetched_at = row

    props = {
        "Name": {
            "title": [{"text": {"content": title[:200]}}]
        },
        "Source": {
            "select": {"name": feed_name}
        },
        "Summary": {
            "rich_text": [{"text": {"content": (summary or "")[:2000]}}]
        },
    }

    if link:
        props["URL"] = {"url": link}

    if published:
        try:
            # Notion date 格式需要 ISO 8601
            dt = published[:19]  # 只取 YYYY-MM-DDTHH:MM:SS
            props["Published"] = {"date": {"start": dt}}
        except Exception:
            pass

    if fetched_at:
        try:
            dt = fetched_at[:19]
            props["Fetched At"] = {"date": {"start": dt}}
        except Exception:
            pass

    # 頁面 body：優先用 content，沒有則用 summary
    body_text = content or summary or ""
    children = []

    if body_text:
        # Notion 單一 rich_text block 上限 2000 字，超過要切塊
        for i in range(0, min(len(body_text), 6000), 2000):
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": body_text[i:i+2000]}}]
                },
            })

    if link:
        children.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{
                    "type": "text",
                    "text": {"content": "原文連結", "link": {"url": link}},
                    "annotations": {"bold": True, "color": "blue"},
                }]
            },
        })

    try:
        notion.pages.create(
            parent={"database_id": NOTION_DB},
            properties=props,
            children=children,
        )
        return True
    except APIResponseError as e:
        print(f"  Notion API 錯誤: {e}")
        return False


def push_all():
    print("=" * 50)
    print(f"推送至 Notion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    with sqlite3.connect(DB_PATH) as conn:
        init_pushed_column(conn)

        rows = conn.execute("""
            SELECT id, feed_key, feed_name, title, link, published, summary, content, fetched_at
            FROM articles
            WHERE notion_pushed = 0
            ORDER BY published ASC
        """).fetchall()

        print(f"待推送文章：{len(rows)} 筆")

        success = 0
        fail    = 0

        for row in rows:
            article_id = row[0]
            title      = row[3]  # id, feed_key, feed_name, title, ...
            print(f"  推送: {title[:60]}...")

            if push_article(row):
                conn.execute(
                    "UPDATE articles SET notion_pushed = 1 WHERE id = ?",
                    (article_id,)
                )
                conn.commit()
                success += 1
            else:
                fail += 1

    print()
    print(f"完成：成功 {success} 筆 / 失敗 {fail} 筆")
    print("=" * 50)


if __name__ == "__main__":
    push_all()

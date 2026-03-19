"""
update_notion_pages.py
用 SQLite 中清洗過的資料，更新 Notion 已有 page 的 Summary 和 body 內容
"""

import sys
import io
import os
import time
import sqlite3
from datetime import datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

load_dotenv()

DB_PATH = Path(__file__).parent / "rss_data.db"
TOKEN = os.getenv("NOTION_TOKEN")
DB_ID = os.getenv("NOTION_DATABASE_ID")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


def fetch_all_notion_pages(client: httpx.Client) -> list:
    """取得 Notion 資料庫所有 page（含 id 和 URL）"""
    pages = []
    has_more = True
    start_cursor = None

    while has_more:
        body = {"page_size": 100}
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = client.post(
            f"https://api.notion.com/v1/databases/{DB_ID}/query",
            headers=HEADERS, json=body,
        )
        data = resp.json()
        if resp.status_code != 200:
            print(f"查詢失敗: {data}")
            break
        pages.extend(data["results"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return pages


def get_children_ids(client: httpx.Client, page_id: str) -> list:
    """取得 page 下所有 children block 的 id"""
    block_ids = []
    has_more = True
    start_cursor = None

    while has_more:
        url = f"https://api.notion.com/v1/blocks/{page_id}/children"
        params = {"page_size": 100}
        if start_cursor:
            params["start_cursor"] = start_cursor
        resp = client.get(url, headers=HEADERS, params=params)
        data = resp.json()
        for block in data.get("results", []):
            block_ids.append(block["id"])
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return block_ids


def delete_block(client: httpx.Client, block_id: str):
    """刪除單一 block"""
    client.delete(f"https://api.notion.com/v1/blocks/{block_id}", headers=HEADERS)


def build_children(body_text: str, link: str) -> list:
    """建立新的 children blocks"""
    children = []

    if body_text:
        # Notion 單一 rich_text block 上限 2000 字
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

    return children


def update_page(client: httpx.Client, page_id: str, summary: str,
                content: str, link: str) -> bool:
    """更新單一 page：更新 Summary property + 替換 body blocks"""
    # 1. 更新 Summary property
    props = {
        "Summary": {
            "rich_text": [{"text": {"content": (summary or "")[:2000]}}]
        },
    }
    resp = client.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=HEADERS,
        json={"properties": props},
    )
    if resp.status_code != 200:
        print(f"    更新 properties 失敗: {resp.json().get('message', '')}")
        return False

    # 2. 刪除舊的 children blocks
    old_block_ids = get_children_ids(client, page_id)
    for bid in old_block_ids:
        delete_block(client, bid)

    # 3. 寫入新的 children blocks
    body_text = content or summary or ""
    children = build_children(body_text, link)

    if children:
        resp = client.patch(
            f"https://api.notion.com/v1/blocks/{page_id}/children",
            headers=HEADERS,
            json={"children": children},
        )
        if resp.status_code != 200:
            print(f"    寫入 children 失敗: {resp.json().get('message', '')}")
            return False

    return True


def main():
    print("=" * 50)
    print(f"更新 Notion 頁面內容: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    client = httpx.Client(timeout=30)

    # 1. 取得 Notion 所有 page，建立 URL -> page_id 對照表
    print("正在查詢 Notion 所有頁面...")
    pages = fetch_all_notion_pages(client)
    url_to_page = {}
    for page in pages:
        url = page["properties"].get("URL", {}).get("url", "")
        if url:
            url_to_page[url] = page["id"]
    print(f"Notion 共 {len(url_to_page)} 個有 URL 的頁面")

    # 2. 從 SQLite 讀取清洗過的資料
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT title, link, summary, content
        FROM articles
        ORDER BY id
    """).fetchall()
    print(f"SQLite 共 {len(rows)} 筆文章")
    print()

    # 3. 逐筆比對更新
    updated = 0
    skipped = 0
    failed = 0

    for title, link, summary, content in rows:
        if not link or link not in url_to_page:
            skipped += 1
            continue

        page_id = url_to_page[link]
        print(f"  更新: {title[:60]}...")

        if update_page(client, page_id, summary, content, link):
            updated += 1
        else:
            failed += 1

        # 避免觸發 Notion API rate limit (3 requests/sec)
        time.sleep(0.5)

    print()
    print(f"完成：更新 {updated} 筆 / 跳過 {skipped} 筆 / 失敗 {failed} 筆")
    print("=" * 50)

    conn.close()
    client.close()


if __name__ == "__main__":
    main()

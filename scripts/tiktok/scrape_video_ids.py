"""
TikTok Video ID Scraper Task
============================
ใช้ Playwright ดึง video IDs จาก hashtag
"""

import asyncio
import re
import json
import os
from datetime import datetime
from urllib.parse import quote
from typing import List, Dict, Set

# Import config
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.tiktok.config.hashtag_config import (
    HASHTAG_CATEGORIES,
    VIDEOS_PER_HASHTAG,
    SCROLL_COUNT,
)


async def get_video_ids_from_search(page, query: str, max_videos: int = 25) -> Set[str]:
    """
    ดึง video IDs จากหน้า search ของ TikTok
    Includes retry with refresh for TikTok's anti-bot blocking
    """
    search_url = f"https://www.tiktok.com/search?q={quote(query)}"
    print(f"   🔍 กำลังค้นหา: #{query}")

    # Try loading page with retry on failure
    page_loaded = False
    for attempt in range(2):  # Try twice - initial load and one refresh
        try:
            if attempt == 0:
                await page.goto(search_url, timeout=45000, wait_until="networkidle")
            else:
                print(f"   🔄 Retrying with page refresh...")
                await page.reload(timeout=45000, wait_until="networkidle")
            page_loaded = True
            break
        except Exception as e:
            if attempt == 0:
                print(f"   ⚠️ Initial load timeout, will retry: {str(e)[:80]}")
                await asyncio.sleep(3)  # Short delay before retry
            else:
                print(f"   ❌ Page load failed after retry: {str(e)[:80]}")
                return set()

    if not page_loaded:
        return set()

    # Wait for video elements to appear (TikTok dynamically loads content)
    try:
        # Wait for video containers to appear
        await page.wait_for_selector('a[href*="/video/"]', timeout=15000)
        print(f"   ✅ Video elements found, scrolling...")
    except Exception as e:
        # Try clicking on "Videos" tab if search results show other content first
        try:
            videos_tab = await page.query_selector('span:has-text("Videos")')
            if videos_tab:
                await videos_tab.click()
                await asyncio.sleep(2)
                await page.wait_for_selector('a[href*="/video/"]', timeout=10000)
                print(f"   ✅ Clicked Videos tab, found content...")
        except:
            print(f"   ⚠️ No video elements found on page")
            return set()

    # Scroll down to load more videos (ปรับตามที่ผู้ใช้แนะนำ: เลื่อน 1 ครั้ง รอสักพักให้โหลดเสร็จ ทำซ้ำ 5-10 ครั้ง)
    scroll_count = min(10, max(5, max_videos // 5))
    for i in range(scroll_count):
        await page.keyboard.press("End")
        print(f"   ⬇️ Scrolling ({i + 1}/{scroll_count})... waiting for videos to load")
        await asyncio.sleep(4.0)  # หน่วงเวลาเพิ่มขึ้นเป็น 4 วินาทีต่อการ scroll

    # Wait a bit more for dynamic content at the very end
    await asyncio.sleep(3)

    # Extract video links
    try:
        hrefs = await page.evaluate("""() => {
            const anchors = Array.from(document.querySelectorAll('a'));
            return anchors.map(a => a.href).filter(href => href.includes('/video/'));
        }""")
    except Exception as e:
        print(f"   ⚠️ Failed to extract links: {e}")
        return set()

    # Extract video IDs
    video_ids = set()
    for link in hrefs:
        match = re.search(r"/video/(\d+)", link)
        if match:
            video_ids.add(match.group(1))
            if len(video_ids) >= max_videos:
                break

    print(f"   ✅ พบ {len(video_ids)} video IDs")
    return video_ids


async def scrape_single_hashtag(
    hashtag: str, category: str = "general", max_videos: int = 25, headless: bool = True
) -> Dict:
    """
    Scrape video IDs from a single hashtag

    Parameters:
    -----------
    hashtag : str
        Hashtag to scrape (without # symbol)
    category : str
        Category name for grouping
    max_videos : int
        Maximum videos to scrape
    headless : bool
        Run browser in headless mode

    Returns:
    --------
    dict : {
        "status": "success" or "error",
        "hashtag": str,
        "category": str,
        "video_count": int,
        "video_ids": list,
        "error": str (if error)
    }
    """
    from playwright.async_api import async_playwright

    print(f"🔍 Scraping #{hashtag} (category: {category})")

    browser = None
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # Scrape video IDs with timeout (3 minutes max per hashtag)
            try:
                video_ids = await asyncio.wait_for(
                    get_video_ids_from_search(page, hashtag, max_videos),
                    timeout=180,  # 3 minute timeout for scraping
                )
            except asyncio.TimeoutError:
                print(f"   ⏰ Scraping timed out for #{hashtag}")
                video_ids = set()

            await browser.close()
            browser = None  # Mark as closed

            print(f"✅ #{hashtag}: Found {len(video_ids)} videos")

            return {
                "status": "success",
                "hashtag": hashtag,
                "category": category,
                "video_count": len(video_ids),
                "video_ids": list(video_ids),
                "scraped_at": datetime.now().isoformat(),
            }

    except asyncio.TimeoutError:
        error_msg = "Scraping timed out after 5 minutes"
        print(f"⏰ #{hashtag}: {error_msg}")
        return {
            "status": "error",
            "hashtag": hashtag,
            "category": category,
            "video_count": 0,
            "video_ids": [],
            "error": error_msg,
            "scraped_at": datetime.now().isoformat(),
        }

    except Exception as e:
        error_msg = str(e)
        print(f"❌ #{hashtag}: Error - {error_msg}")

        return {
            "status": "error",
            "hashtag": hashtag,
            "category": category,
            "video_count": 0,
            "video_ids": [],
            "error": error_msg,
            "scraped_at": datetime.now().isoformat(),
        }


async def scrape_category(
    category_name: str,
    hashtags: List[str],
    videos_per_hashtag: int = 25,
    headless: bool = True,
) -> Dict:
    """
    Scrape video IDs จาก category
    """
    from playwright.async_api import async_playwright

    print(f"\n{'=' * 60}")
    print(f"📂 Category: {category_name}")
    print(f"   Hashtags: {len(hashtags)}")
    print(f"   Videos per hashtag: {videos_per_hashtag}")
    print("=" * 60)

    all_videos = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for hashtag in hashtags:
            video_ids = await get_video_ids_from_search(
                page, hashtag, videos_per_hashtag
            )

            for vid in video_ids:
                if vid not in all_videos:
                    all_videos[vid] = {
                        "category": category_name,
                        "hashtags": [hashtag],
                        "scraped_at": datetime.now().isoformat(),
                    }
                else:
                    if hashtag not in all_videos[vid]["hashtags"]:
                        all_videos[vid]["hashtags"].append(hashtag)

        await browser.close()

    print(f"\n📊 Category {category_name}: พบ {len(all_videos)} unique videos")
    return all_videos


async def scrape_all_categories(
    headless: bool = True, output_dir: str = "data/video_ids"
) -> Dict:
    """
    Scrape ทุก category และบันทึกลงไฟล์
    """
    os.makedirs(output_dir, exist_ok=True)

    all_results = {}
    today = datetime.now().strftime("%Y-%m-%d")

    for category_name, category_data in HASHTAG_CATEGORIES.items():
        hashtags = category_data["hashtags"]

        videos = await scrape_category(
            category_name=category_name,
            hashtags=hashtags,
            videos_per_hashtag=VIDEOS_PER_HASHTAG,
            headless=headless,
        )

        all_results[category_name] = videos

        # บันทึกแยกตาม category
        category_file = os.path.join(output_dir, f"{category_name}_{today}.json")
        with open(category_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "category": category_name,
                    "description": category_data["description"],
                    "date": today,
                    "total_videos": len(videos),
                    "video_ids": list(videos.keys()),
                    "video_details": videos,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"💾 บันทึก: {category_file}")

    # บันทึกรวมทั้งหมด
    combined_file = os.path.join(output_dir, f"all_categories_{today}.json")

    # รวม video IDs ทั้งหมด (ไม่ซ้ำ)
    all_video_ids = {}
    for category_name, videos in all_results.items():
        for vid, data in videos.items():
            if vid not in all_video_ids:
                all_video_ids[vid] = {
                    "categories": [data["category"]],
                    "hashtags": data["hashtags"],
                    "scraped_at": data["scraped_at"],
                }
            else:
                if data["category"] not in all_video_ids[vid]["categories"]:
                    all_video_ids[vid]["categories"].append(data["category"])
                for h in data["hashtags"]:
                    if h not in all_video_ids[vid]["hashtags"]:
                        all_video_ids[vid]["hashtags"].append(h)

    with open(combined_file, "w", encoding="utf-8") as f:
        json.dump(
            {
                "date": today,
                "total_unique_videos": len(all_video_ids),
                "categories": list(HASHTAG_CATEGORIES.keys()),
                "video_ids": list(all_video_ids.keys()),
                "video_details": all_video_ids,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    print(f"\n{'=' * 60}")
    print(f"✅ Scraping เสร็จสิ้น!")
    print(f"📊 Total unique videos: {len(all_video_ids)}")
    print(f"💾 Combined file: {combined_file}")
    print("=" * 60)

    return all_video_ids


def run_scraper(headless: bool = True, output_dir: str = "data/video_ids"):
    """
    Entry point สำหรับ Airflow task
    """
    return asyncio.run(scrape_all_categories(headless=headless, output_dir=output_dir))


if __name__ == "__main__":
    # Test run
    run_scraper(headless=True)

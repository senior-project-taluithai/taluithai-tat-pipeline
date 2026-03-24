import asyncio
import os
import sys
import time
from datetime import datetime
from typing import List, Dict

from dotenv import load_dotenv
from pymongo import MongoClient

# Add project path for local imports if needed
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_PATH)

# Load env
load_dotenv(os.path.join(PROJECT_PATH, ".env"))

# Import playwright setup from scrape_video_ids
from scripts.tiktok.scrape_video_ids import get_video_ids_from_search
from playwright.async_api import async_playwright

MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB = os.environ.get("MONGODB_DATABASE", "taluithai")

async def scrape_trending_videos(keyword="สถานที่ไทยไทย เทรน", max_videos=20):
    """
    Search TikTok for a keyword and return video IDs.
    """
    print(f"🚀 Starting TikTok trend search for: '{keyword}'")
    video_ids = set()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            # We reuse the logic from your existing scraper
            video_ids = await get_video_ids_from_search(page, keyword, max_videos=max_videos)
            print(f"✅ Found {len(video_ids)} videos for keyword '{keyword}'")
        except Exception as e:
            print(f"❌ Error scraping keyword '{keyword}': {e}")
            
        await browser.close()
        
    return list(video_ids)

if __name__ == "__main__":
    asyncio.run(scrape_trending_videos())

import os
import sys
import asyncio
from datetime import datetime
from prefect import flow, task
from pymongo import MongoClient

# Add project root to python path
PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_PATH)

from scripts.tiktok.scrape_trends import scrape_trending_videos
from scripts.tiktok.llm_mapper import extract_place_name, find_place_id

# Assume you have a base scraper logic for metadata
# For now we mock the metadata extraction based on your scrape_video_metadata
from scripts.tiktok.scrape_video_metadata import scrape_video_metadata


TIKTOK_SEARCH_KEYWORDS = [
    "ที่เที่ยวฮิตตอนนี้",
    "ที่เที่ยวเปิดใหม่",
    "รีวิวที่เที่ยวไทย",
    "พิกัดที่เที่ยว",
    "จุดเช็คอิน",
    "เที่ยวตามรีวิว",
    "เทรนด์ที่เที่ยว",
    "รวมที่เที่ยว",
    "เที่ยวไทย",
    "เที่ยววันหยุด",
    "พิกัดถ่ายรูป",
    "คาเฟ่เปิดใหม่",
    "มุมถ่ายรูปปังๆ",
    "ที่เที่ยวลับ",
    "พิกัดลับ",
    "UnseenThailand",
    "ที่เที่ยวฮีลใจ",
    "เที่ยวธรรมชาติ",
    "ทะเลสวยบอกต่อ",
    "จุดกางเต็นท์",
    "เที่ยวเขา",
    "สายมูห้ามพลาด",
    "วัดสวยบอกต่อ",
    "เที่ยวเมืองเก่า",
    "ตลาดกลางคืน",
    "สตรีทฟู้ด",
    "เที่ยวสายลุย",
    "ที่เที่ยวใกล้กรุงเทพ",
    "OneDayTrip",
    "เที่ยวเมืองรอง",
    "VLOGพาเที่ยว",
]


@task(retries=2, retry_delay_seconds=60)
async def get_trending_video_ids(keyword: str, max_videos: int) -> list:
    """Scrape TikTok for trending video IDs."""
    return await scrape_trending_videos(keyword=keyword, max_videos=max_videos)


@task
def extract_metadata_and_map_places(video_ids: list, search_keyword: str):
    """
    For each video:
    1. Scrape metadata (caption, views, etc)
    2. Use LLM to extract place name
    3. Look up place_id in Postgres
    4. Save to MongoDB
    """
    if not video_ids:
        print("No videos to process")
        return []

    print(f"Processing {len(video_ids)} videos...")

    pg_conn_str = os.environ.get(
        "PUBLIC_PG_CONNECTION_STRING",
        "postgresql://postgres:uRv0%7CRVoo%21%3C1y1%7DX%3C%25G9W%26-NcLw%28H15y@34.87.52.21:5432/taluithai",
    )

    mongo_uri = "mongodb://taluithai:FUvv3%5E%5ETfmeMSTX7t%2BEjx6s8bHXBEJng@db-taluithai.oswinfalk.xyz:27017/?authSource=admin"
    mongo_db_name = os.environ.get("MONGODB_DATABASE", "taluithai")

    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    trends_col = db["tiktok_trends"]

    results = []

    for vid in video_ids:
        try:
            # 1. Scrape metadata
            print(f"Scraping metadata for video {vid}...")
            # Real metadata extraction via oEmbed
            metadata = scrape_video_metadata(vid)
            if not metadata or "desc" not in metadata:
                print(f"Skipping video {vid}: No desc/caption found")
                continue

            caption = metadata.get("desc", "")
            if not caption:
                continue

            # 2. Extract Place Name using LLM
            place_name = extract_place_name(caption)
            print(f"LLM extracted place: {place_name} from video {vid}")

            if not place_name:
                continue

            # 3. Find Place ID in Database
            place_id = find_place_id(place_name, pg_conn_str)

            if not place_id:
                print(f"Could not map '{place_name}' to a DB place_id")
                continue

            print(f"Successfully mapped '{place_name}' to place_id {place_id}")

            # 4. Prepare data for MongoDB
            trend_data = {
                "placeId": str(place_id),
                "placeName": place_name,
                "tiktokMetadata": {
                    "videoId": vid,
                    "keyword": search_keyword,
                    "videoUrl": f"https://www.tiktok.com/@user/video/{vid}",
                    "caption": caption,
                    "views": metadata.get("statistics", {}).get("playCount", 0),
                    "likes": metadata.get("statistics", {}).get("diggCount", 0),
                },
                "trendScore": metadata.get("statistics", {}).get("playCount", 0)
                + (metadata.get("statistics", {}).get("diggCount", 0) * 10),
                "scrapedAt": datetime.now(),
            }

            # 5. Upsert to MongoDB
            trends_col.update_one(
                {"tiktokMetadata.videoId": vid}, {"$set": trend_data}, upsert=True
            )

            results.append(trend_data)

        except Exception as e:
            print(f"Error processing video {vid}: {e}")

    client.close()
    return results


@flow(name="TikTok Trending Places Sync")
def tiktok_trends_pipeline(keyword: str = "ที่เที่ยวฮิต", max_videos: int = 20):
    """
    Main Prefect Flow:
    Scrapes TikTok for trending places, extracts locations via LLM,
    and stores them in MongoDB for the API to consume.
    """
    # Create event loop for async task
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Determine keyword based on day of month if not provided
    if not keyword:
        day_of_month = datetime.now().day
        # Index is 0-based, day is 1-based. Modulo just in case list is shorter than month days.
        keyword_idx = (day_of_month - 1) % len(TIKTOK_SEARCH_KEYWORDS)
        keyword = TIKTOK_SEARCH_KEYWORDS[keyword_idx]
        print(f"📅 Day {day_of_month}: Selected keyword '{keyword}'")

    # 1. Get Video IDs (using a higher max_videos to get as many as possible)
    # The actual playwright scraper has limits based on scroll counts,
    # but setting max_videos=100 will allow it to fetch many more clips.
    video_ids = loop.run_until_complete(get_trending_video_ids(keyword, max_videos))

    # 2. Extract and Store
    results = extract_metadata_and_map_places(video_ids, keyword)

    print(f"Flow completed. Successfully processed {len(results)} trending places.")
    return results


if __name__ == "__main__":
    tiktok_trends_pipeline()

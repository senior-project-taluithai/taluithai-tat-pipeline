import os
import sys
import random
import asyncio
from datetime import datetime, timedelta
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


# Split into two distinct groups to prevent mainstream places showing up in hidden gems
MAINSTREAM_KEYWORDS = [
    "ที่เที่ยวฮิตตอนนี้",
    "พิกัดที่เที่ยว",
    "จุดเช็คอิน",
    "เที่ยวตามรีวิว",
    "เทรนด์ที่เที่ยว",
    "รวมที่เที่ยว",
    "เที่ยวไทย",
    "เที่ยววันหยุด",
    "มุมถ่ายรูปปังๆ",
    "วัดสวยบอกต่อ",
    "ตลาดกลางคืน",
    "ที่เที่ยวใกล้กรุงเทพ",
    "ที่เที่ยวยอดฮิต",
    "คนเยอะมาก",
]

HIDDEN_GEM_KEYWORDS = [
    "ที่เที่ยวลับ",
    "พิกัดลับ",
    "UnseenThailand",
    "ซ่อนตัว",
    "คนยังไม่ค่อยรู้",
    "ที่เที่ยวเปิดใหม่",
    "คาเฟ่เปิดใหม่",
    "ที่เที่ยวฮีลใจ",
    "เที่ยวเมืองรอง",
    "ที่เที่ยวแปลกใหม่",
    "คาเฟ่ลับ",
    "ที่พักลับ",
    "จุดถ่ายรูปลับ",
    "หนีความวุ่นวาย",
    "คนน้อย",
    "สงบๆ",
    "ฟีลธรรมชาติ",
]


@task(retries=2, retry_delay_seconds=60)
async def get_trending_video_ids(keyword: str, max_videos: int) -> list:
    """Scrape TikTok for trending video IDs."""
    return await scrape_trending_videos(keyword=keyword, max_videos=max_videos)


@task
def extract_metadata_and_map_places(
    video_ids: list, search_keyword: str, source_type: str = "mainstream"
):
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
                "source_type": source_type,
                "tiktokMetadata": {
                    "videoId": vid,
                    "keyword": search_keyword,
                    "videoUrl": f"https://www.tiktok.com/@user/video/{vid}",
                    "caption": caption,
                    "views": metadata.get("statistics", {}).get("playCount", 0),
                    "likes": metadata.get("statistics", {}).get("diggCount", 0),
                    "collectCount": metadata.get("statistics", {}).get(
                        "collectCount", 0
                    ),
                    "shareCount": metadata.get("statistics", {}).get("shareCount", 0),
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

    # 6. Cleanup old trends (older than 7 days)
    seven_days_ago = datetime.now() - timedelta(days=7)
    deleted = trends_col.delete_many({"scrapedAt": {"$lt": seven_days_ago}})
    print(f"🧹 Cleaned up {deleted.deleted_count} old trends from MongoDB.")

    client.close()
    return results


@flow(name="TikTok Trending Places Sync")
async def tiktok_trends_pipeline(max_videos: int = 40):
    """
    Main Prefect Flow:
    Scrapes TikTok for trending places, extracts locations via LLM,
    and stores them in MongoDB for the API to consume.
    """
    # Randomly select keywords to scrape in this run to ensure diversity
    num_mainstream = random.randint(2, 3)
    num_hidden = random.randint(3, 4)

    selected_mainstream = random.sample(MAINSTREAM_KEYWORDS, num_mainstream)
    selected_hidden = random.sample(HIDDEN_GEM_KEYWORDS, num_hidden)

    print(f"🎲 Selected mainstream keywords for this run: {selected_mainstream}")
    print(f"🎲 Selected hidden gem keywords for this run: {selected_hidden}")

    all_results = []

    # Process Mainstream Keywords
    for keyword in selected_mainstream:
        print(f"\n--- 🚀 Processing Mainstream Keyword: {keyword} ---")
        # 1. Get Video IDs
        video_ids = await get_trending_video_ids(keyword, max_videos)

        # 2. Extract and Store (explicitly tag as mainstream)
        results = extract_metadata_and_map_places(
            video_ids, keyword, source_type="mainstream"
        )
        all_results.extend(results)

    # Process Hidden Gem Keywords
    for keyword in selected_hidden:
        print(f"\n--- 💎 Processing Hidden Gem Keyword: {keyword} ---")
        # 1. Get Video IDs
        video_ids = await get_trending_video_ids(keyword, max_videos)

        # 2. Extract and Store (explicitly tag as hidden_gem)
        results = extract_metadata_and_map_places(
            video_ids, keyword, source_type="hidden_gem"
        )
        all_results.extend(results)

    print(
        f"\n✅ Flow completed. Successfully processed {len(all_results)} trending places in total."
    )
    return all_results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        # Long-running mode: serve the flow on a cron schedule (local)
        tiktok_trends_pipeline.serve(
            name="tiktok-trends-12h-sync",
            cron="0 2,14 * * *",
            tags=["tiktok", "trends"],
            description="Scrapes TikTok for trending tourist places every 12 hours",
        )
    else:
        # One-shot: run immediately
        asyncio.run(tiktok_trends_pipeline())

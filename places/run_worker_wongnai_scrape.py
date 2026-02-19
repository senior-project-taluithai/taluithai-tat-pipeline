#!/usr/bin/env python
"""
Wongnai Review Scraper - Single Worker (Simple)
================================================
Worker process for parallel Wongnai scraping.
Uses random selection to distribute work across workers.

Usage:
    python run_worker_wongnai_scrape.py --worker-id 1
"""

import argparse
import logging
import time
import random
import sys
import os
from datetime import datetime

# Adjust path to find modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tasks.wongnai.wongnai_scraper import get_wongnai_reviews
from tasks.wongnai.loader import get_connection, update_wongnai_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Worker%(worker_id)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_places_batch(batch_size: int = 50) -> list:
    """
    Get random batch of places to scrape.
    Uses ORDER BY RANDOM() to naturally distribute work across workers.
    """
    sql = """
        SELECT
            p.place_id,
            p.name,
            pr.name as province_name
        FROM tat.places p
        LEFT JOIN tat.provinces pr ON p.province_id = pr.province_id
        WHERE p.wongnai_genres IS NULL 
           OR array_length(p.wongnai_genres, 1) IS NULL
        ORDER BY RANDOM()
        LIMIT %s
    """
    
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [batch_size])
                columns = [desc[0] for desc in cur.description]
                places = [dict(zip(columns, row)) for row in cur.fetchall()]
            return places
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")
        return []


def run_worker(
    worker_id: int,
    batch_size: int = 50,
    delay_min: float = 1.0,
    delay_max: float = 2.5,
    max_places: int = 0,  # 0 = unlimited
    genres_only: bool = False,  # If True, only update genres, not ratings
):
    """Run Wongnai scraper"""
    # Add worker_id to log format
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.worker_id = worker_id
        return record
    logging.setLogRecordFactory(record_factory)
    
    logger.info("=" * 60)
    logger.info(f"🔧 Wongnai Worker {worker_id} Starting")
    logger.info("=" * 60)
    logger.info(f"   Batch size: {batch_size}")
    logger.info(f"   Delay: {delay_min}-{delay_max}s")
    if max_places > 0:
        logger.info(f"   Max places: {max_places}")
    if genres_only:
        logger.info(f"   ⚡ GENRES ONLY MODE - ratings will NOT be updated")
    logger.info("")
    
    total_scraped = 0
    total_success = 0
    total_with_genres = 0
    start_time = datetime.now()
    batch_num = 0
    
    try:
        while True:
            # Check if we've reached max places
            if max_places > 0 and total_scraped >= max_places:
                logger.info(f"✅ Worker {worker_id}: Reached max places limit ({max_places})")
                break
            
            batch_num += 1
            
            # Get random batch of places
            places = get_places_batch(batch_size)
            
            if not places:
                logger.info(f"✅ Worker {worker_id}: No more places to scrape!")
                break
            
            logger.info(f"\n📦 Batch {batch_num}: {len(places)} places")
            
            results = []
            for idx, place in enumerate(places, 1):
                place_id = place.get('place_id')
                name = place.get('name', '')
                
                logger.info(f"[{idx}/{len(places)}] {name}")
                
                result = get_wongnai_reviews(
                    place_name=name,
                    place_id=place_id,
                )
                
                result_dict = result.to_dict()
                if result.wongnai_url:
                    result_dict['wongnai_url'] = result.wongnai_url
                results.append(result_dict)
                
                if idx < len(places):
                    delay = random.uniform(delay_min, delay_max)
                    time.sleep(delay)
            
            # Save batch immediately after processing
            if results:
                try:
                    stats = update_wongnai_batch(results, genres_only=genres_only)
                    
                    success = sum(1 for r in results if r.get('status') == 'success_w')
                    with_genres = sum(1 for r in results if r.get('wongnai_genres'))
                    rate_limited = sum(1 for r in results if r.get('status') == 'rate_limited')
                    
                    total_scraped += len(results)
                    total_success += success
                    total_with_genres += with_genres
                    
                    log_msg = f"💾 Genres: {stats.get('genres_only', 0)} | Ratings: {stats.get('updated', 0)} | Marked: {stats.get('marked', 0)}"
                    if rate_limited > 0:
                        log_msg += f" | ⏳ Rate Limited: {rate_limited} (will retry)"
                    logger.info(log_msg)
                except Exception as e:
                    logger.error(f"❌ Error saving batch: {e}")
            
            # Brief pause between batches
            time.sleep(random.uniform(2, 4))
            
    except KeyboardInterrupt:
        logger.info(f"\n⚠️ Worker {worker_id} interrupted")
    except Exception as e:
        logger.error(f"\n❌ Worker {worker_id} error: {e}")
        import traceback
        traceback.print_exc()
    
    elapsed = datetime.now() - start_time
    rate = total_scraped / elapsed.total_seconds() * 60 if elapsed.total_seconds() > 0 else 0
    
    logger.info(f"\n{'=' * 60}")
    logger.info(f"📋 Worker {worker_id} Summary")
    logger.info(f"{'=' * 60}")
    logger.info(f"   Total scraped: {total_scraped:,}")
    logger.info(f"   Success: {total_success:,}")
    logger.info(f"   With genres: {total_with_genres:,}")
    logger.info(f"   Elapsed: {elapsed}")
    logger.info(f"   Rate: {rate:.1f} places/min")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--delay-min", type=float, default=1.0)
    parser.add_argument("--delay-max", type=float, default=2.5)
    parser.add_argument("--max-places", type=int, default=0, help="Max places to scrape (0=unlimited)")
    parser.add_argument("--genres-only", action="store_true", help="Only update genres, not ratings")
    
    args = parser.parse_args()
    
    run_worker(
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        max_places=args.max_places,
        genres_only=args.genres_only,
    )

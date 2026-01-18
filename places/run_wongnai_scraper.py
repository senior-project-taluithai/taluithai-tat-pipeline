#!/usr/bin/env python
"""
Wongnai Batch Scraper
=====================
Scrape Wongnai data (genres, neighborhoods, price range) for all places.

Usage:
    python run_wongnai_scraper.py [--batch-size 50] [--delay-min 0.5] [--delay-max 1.5]
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_places_to_scrape(limit: int = 50) -> list:
    """
    Get places that don't have Wongnai data yet.
    """
    sql = """
        SELECT
            p.place_id,
            p.name,
            pr.name as province_name
        FROM tat.places p
        LEFT JOIN tat.provinces pr ON p.province_id = pr.province_id
        WHERE p.wongnai_genres IS NULL
        ORDER BY RANDOM()
        LIMIT %s
    """
    
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [limit])
                columns = [desc[0] for desc in cur.description]
                places = [dict(zip(columns, row)) for row in cur.fetchall()]
            return places
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")
        return []


def run_scraper(
    batch_size: int = 50,
    delay_min: float = 0.5,
    delay_max: float = 1.5,
    max_batches: int = 0,  # 0 = unlimited
):
    """Run the Wongnai scraper for all places"""
    logger.info("=" * 60)
    logger.info("🔍 Wongnai Batch Scraper Starting")
    logger.info("=" * 60)
    logger.info(f"   Batch size: {batch_size}")
    logger.info(f"   Delay: {delay_min}-{delay_max}s")
    logger.info("")
    
    total_scraped = 0
    total_success = 0
    total_with_genres = 0
    start_time = datetime.now()
    batch_num = 0
    
    try:
        while True:
            batch_num += 1
            
            if max_batches > 0 and batch_num > max_batches:
                logger.info(f"✅ Reached max batches limit ({max_batches})")
                break
            
            places = get_places_to_scrape(limit=batch_size)
            
            if not places:
                logger.info("✅ No more places to scrape!")
                break
            
            logger.info(f"\n📦 Batch {batch_num}: {len(places)} places")
            
            results = []
            for idx, place in enumerate(places, 1):
                place_id = place.get('place_id')
                name = place.get('name', '')
                province = place.get('province_name', '')
                
                logger.info(f"[{idx}/{len(places)}] {name}")
                
                result = get_wongnai_reviews(
                    place_name=name,
                    place_id=place_id,
                )
                
                # Add wongnai_url to result
                if result.wongnai_url:
                    result_dict = result.to_dict()
                    result_dict['wongnai_url'] = result.wongnai_url
                    results.append(result_dict)
                else:
                    results.append(result.to_dict())
                
                if idx < len(places):
                    delay = random.uniform(delay_min, delay_max)
                    time.sleep(delay)
            
            # Save batch
            if results:
                stats = update_wongnai_batch(results)
                
                success = sum(1 for r in results if r.get('status') == 'success_w')
                with_genres = sum(1 for r in results if r.get('wongnai_genres'))
                
                total_scraped += len(results)
                total_success += success
                total_with_genres += with_genres
                
                logger.info(f"💾 Saved: {stats.get('updated', 0)} | Success: {success}/{len(results)} | With Genres: {with_genres}")
            
            # Brief pause between batches
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        logger.info("\n⚠️ Scraper interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    elapsed = datetime.now() - start_time
    logger.info(f"\n{'=' * 60}")
    logger.info(f"📋 Summary")
    logger.info(f"{'=' * 60}")
    logger.info(f"   Total scraped: {total_scraped:,}")
    logger.info(f"   Success: {total_success:,}")
    logger.info(f"   With genres: {total_with_genres:,}")
    logger.info(f"   Elapsed: {elapsed}")
    if total_scraped > 0:
        logger.info(f"   Rate: {total_scraped / elapsed.total_seconds() * 60:.1f} places/min")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wongnai Batch Scraper")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of places per batch")
    parser.add_argument("--delay-min", type=float, default=0.5, help="Minimum delay between requests (seconds)")
    parser.add_argument("--delay-max", type=float, default=1.5, help="Maximum delay between requests (seconds)")
    parser.add_argument("--max-batches", type=int, default=0, help="Maximum number of batches (0 = unlimited)")
    
    args = parser.parse_args()
    
    run_scraper(
        batch_size=args.batch_size,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        max_batches=args.max_batches,
    )

#!/usr/bin/env python
"""
Google Maps Review - Single Worker Scraper
==========================================
Worker process for parallel scraping.
Handles specific provinces assigned by the coordinator.

Usage:
    python run_worker_google_scrape.py --worker-id 1 --provinces "1,2,3"
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

from tasks.google_maps.review_scraper import get_google_maps_reviews
from tasks.google_maps.loader import (
    get_connection,
    update_places_batch,
    log_scrape_run,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_places_for_provinces(province_ids: list, limit: int = 50) -> list:
    """
    Get places to scrape for specific provinces.
    Uses FOR UPDATE SKIP LOCKED to prevent multiple workers from getting the same places.
    """
    if not province_ids:
        return []
    
    placeholders = ','.join(['%s'] * len(province_ids))
    
    # Use FOR UPDATE SKIP LOCKED to lock selected rows
    # This prevents other workers from selecting the same places
    sql = f"""
        SELECT
            p.place_id,
            p.name,
            d.name as district_name,
            sd.name as sub_district_name,
            pr.name as province_name
        FROM tat.places p
        JOIN tat.provinces pr ON p.province_id = pr.province_id
        LEFT JOIN tat.districts d ON p.district_id = d.district_id
        LEFT JOIN tat.sub_districts sd ON p.sub_district_id = sd.sub_district_id
        WHERE p.google_scraped_at IS NULL
          AND p.province_id IN ({placeholders})
        ORDER BY RANDOM()
        LIMIT %s
        FOR UPDATE OF p SKIP LOCKED
    """
    
    try:
        conn = get_connection()
        conn.autocommit = False  # Need transaction for FOR UPDATE
        try:
            with conn.cursor() as cur:
                cur.execute(sql, province_ids + [limit])
                columns = [desc[0] for desc in cur.description]
                places = [dict(zip(columns, row)) for row in cur.fetchall()]
            conn.commit()  # Release the lock after fetching
            return places
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")
        return []


def run_worker(
    worker_id: int,
    province_ids: list,
    batch_size: int = 50,
    delay_min: float = 2.0,
    delay_max: float = 4.0,
    max_retries: int = 1,
):
    """Run scraper for assigned provinces"""
    logger.info(f"=" * 60)
    logger.info(f"🔧 Worker {worker_id} Starting")
    logger.info(f"=" * 60)
    logger.info(f"   Provinces: {province_ids[:5]}... ({len(province_ids)} total)")
    logger.info(f"   Delay: {delay_min}-{delay_max}s, Retries: {max_retries}")
    logger.info("")
    
    total_scraped = 0
    total_success = 0
    start_time = datetime.now()
    batch_num = 0
    
    try:
        while True:
            batch_num += 1
            places = get_places_for_provinces(province_ids, limit=batch_size)
            
            if not places:
                logger.info(f"✅ Worker {worker_id}: No more places for assigned provinces!")
                break
            
            logger.info(f"\n📦 Worker {worker_id} Batch {batch_num}: {len(places)} places")
            
            results = []
            for idx, place in enumerate(places, 1):
                place_id = place.get('place_id')
                name = place.get('name', '')
                province = place.get('province_name', '')
                district = place.get('district_name', '')
                sub_district = place.get('sub_district_name', '')
                
                logger.info(f"[{idx}/{len(places)}] {name} ({province})")
                
                result = None
                for attempt in range(max_retries + 1):
                    result = get_google_maps_reviews(
                        place_name=name,
                        province=province,
                        district=district,
                        sub_district=sub_district,
                        place_id=place_id,
                        headless=True,
                    )
                    
                    if result.status == "success":
                        break
                    elif attempt < max_retries:
                        logger.info(f"   🔄 Retry ({attempt+1}/{max_retries})...")
                        time.sleep(random.uniform(1, 2))
                
                if result:
                    results.append(result)
                
                if idx < len(places):
                    delay = random.uniform(delay_min, delay_max)
                    time.sleep(delay)
            
            # Save batch
            if results:
                # Convert objects to dicts if needed
                results_data = [r.to_dict() if hasattr(r, 'to_dict') else r for r in results]
                stats = update_places_batch(results_data)
                
                success = sum(1 for r in results if r.status == 'success')
                total_scraped += len(results)
                total_success += success
                
                logger.info(f"💾 Saved: {stats.get('updated', 0)} | Success: {success}/{len(results)}")
            
            # Brief pause between batches
            time.sleep(random.uniform(2, 5))
            
    except KeyboardInterrupt:
        logger.info(f"\n⚠️ Worker {worker_id} interrupted")
    except Exception as e:
        logger.error(f"\n❌ Worker {worker_id} error: {e}")
    
    elapsed = datetime.now() - start_time
    logger.info(f"\n📋 Worker {worker_id} Summary: {total_scraped} scraped, {total_success} success, {elapsed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--provinces", type=str, required=True, help="Comma-separated province IDs")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=4.0)
    parser.add_argument("--max-retries", type=int, default=1)
    
    args = parser.parse_args()
    province_ids = [int(x.strip()) for x in args.provinces.split(",") if x.strip()]
    
    run_worker(
        worker_id=args.worker_id,
        province_ids=province_ids,
        batch_size=args.batch_size,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        max_retries=args.max_retries,
    )

#!/usr/bin/env python
"""
Wongnai Review Worker
=====================
Scrapes Wongnai for places that were not found on Google Maps

Usage:
    python run_worker_wongnai.py --worker-id 1 --batch-size 20
"""

import argparse
import logging
import time
import random
import sys
import os
from datetime import datetime

# Adjust path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tasks.wongnai.wongnai_scraper import get_wongnai_reviews
from tasks.wongnai.loader import get_not_found_places, update_wongnai_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def run_worker(
    worker_id: int,
    batch_size: int = 20,
    delay_min: float = 1.0,
    delay_max: float = 2.0,
):
    """Run Wongnai scraper for not_found places"""
    logger.info("=" * 60)
    logger.info(f"🟡 Wongnai Worker {worker_id} Starting")
    logger.info("=" * 60)
    logger.info(f"   Batch size: {batch_size}")
    logger.info(f"   Delay: {delay_min}-{delay_max}s")
    logger.info("")
    
    total_scraped = 0
    total_success = 0
    start_time = datetime.now()
    batch_num = 0
    
    try:
        while True:
            batch_num += 1
            places = get_not_found_places(limit=batch_size)
            
            if not places:
                logger.info(f"✅ Worker {worker_id}: No more not_found places!")
                break
            
            logger.info(f"\n📦 Batch {batch_num}: {len(places)} places")
            
            results = []
            for idx, place in enumerate(places, 1):
                place_id = place.get('place_id')
                name = place.get('name', '')
                province = place.get('province_name', '')
                
                logger.info(f"[{idx}/{len(places)}] {name} ({province})")
                
                result = get_wongnai_reviews(
                    place_name=name,
                    place_id=place_id,
                )
                
                results.append(result)
                
                if idx < len(places):
                    delay = random.uniform(delay_min, delay_max)
                    time.sleep(delay)
            
            # Save batch
            if results:
                results_data = [r.to_dict() if hasattr(r, 'to_dict') else r for r in results]
                stats = update_wongnai_batch(results_data)
                
                success = sum(1 for r in results if r.status == 'success_w')
                total_scraped += len(results)
                total_success += success
                
                logger.info(f"💾 Updated: {stats.get('updated', 0)} | Marked: {stats.get('marked', 0)}")
            
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        logger.info(f"\n⚠️ Worker {worker_id} interrupted")
    except Exception as e:
        logger.error(f"\n❌ Worker {worker_id} error: {e}")
    
    elapsed = datetime.now() - start_time
    logger.info(f"\n📋 Worker {worker_id} Summary: {total_scraped} scraped, {total_success} success_w, {elapsed}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--delay-min", type=float, default=1.0)
    parser.add_argument("--delay-max", type=float, default=2.0)
    
    args = parser.parse_args()
    
    run_worker(
        worker_id=args.worker_id,
        batch_size=args.batch_size,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
    )

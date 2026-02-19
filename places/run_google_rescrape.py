#!/usr/bin/env python
"""
Google Maps Re-scrape for Not Found / Error Places
===================================================
Re-attempts Google Maps scraping for places that previously got
'not_found' or 'error' status.

Uses Playwright (headless browser) to search Google Maps and extract
star distribution data.

Usage:
    # Single worker:
    python run_google_rescrape.py --batch-size 10

    # Multiple workers (run in separate terminals):
    python run_google_rescrape.py --worker-id 1 --batch-size 10
    python run_google_rescrape.py --worker-id 2 --batch-size 10
    python run_google_rescrape.py --worker-id 3 --batch-size 10

    # Or use the launcher:
    python run_parallel_google_rescrape.py --workers 3
"""

import argparse
import logging
import time
import random
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tasks.google_maps.review_scraper import get_google_maps_reviews
from tasks.google_maps.loader import get_connection, update_places_batch

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_places_to_rescrape(limit: int = 20) -> list:
    """Get places with not_found or error status for Google re-scrape.
    Uses FOR UPDATE SKIP LOCKED so multiple workers won't pick the same rows."""
    sql = """
        SELECT
            p.place_id,
            p.name,
            COALESCE(d.name, '') as district_name,
            COALESCE(sd.name, '') as sub_district_name,
            pr.name as province_name,
            p.google_scrape_status
        FROM tat.places p
        JOIN tat.provinces pr ON p.province_id = pr.province_id
        LEFT JOIN tat.districts d ON p.district_id = d.district_id
        LEFT JOIN tat.sub_districts sd ON p.sub_district_id = sd.sub_district_id
        WHERE p.google_scrape_status IN ('not_found', 'error')
        ORDER BY RANDOM()
        LIMIT %s
        FOR UPDATE OF p SKIP LOCKED
    """
    try:
        conn = get_connection()
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [limit])
                columns = [desc[0] for desc in cur.description]
                places = [dict(zip(columns, row)) for row in cur.fetchall()]
            # Commit to release the advisory lock — rows are "claimed" by
            # updating status to 'scraping' so other workers skip them
            if places:
                place_ids = [p['place_id'] for p in places]
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tat.places SET google_scrape_status = 'scraping' WHERE place_id = ANY(%s)",
                        (place_ids,)
                    )
            conn.commit()
            return places
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"DB Error: {e}")
        return []


def get_remaining_count() -> dict:
    """Get counts of places needing re-scrape."""
    sql = """
        SELECT
            google_scrape_status,
            COUNT(*) as cnt
        FROM tat.places
        WHERE google_scrape_status IN ('not_found', 'error')
        GROUP BY google_scrape_status
        ORDER BY google_scrape_status
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            result = {}
            for row in cur.fetchall():
                result[row[0]] = row[1]
            return result
    finally:
        conn.close()


def run(worker_id: int = 1, batch_size: int = 20, delay_min: float = 3.0, delay_max: float = 6.0):
    counts = get_remaining_count()
    total_remaining = sum(counts.values())

    logger.info("=" * 60)
    logger.info(f"Google Maps Re-scrape — Worker {worker_id}")
    logger.info("=" * 60)
    for status, cnt in counts.items():
        logger.info(f"   {status}: {cnt:,}")
    logger.info(f"   Total: {total_remaining:,}")
    logger.info(f"   Batch size: {batch_size}, Delay: {delay_min}-{delay_max}s")
    est_minutes = total_remaining * 12 / 60  # ~12 sec/place with Playwright
    logger.info(f"   Estimated time: ~{est_minutes:.0f} minutes")
    logger.info("")

    total_processed = 0
    total_success = 0
    total_partial = 0
    start_time = datetime.now()
    batch_num = 0
    consecutive_errors = 0

    try:
        while True:
            batch_num += 1
            places = get_places_to_rescrape(limit=batch_size)

            if not places:
                logger.info("No more places to re-scrape!")
                break

            logger.info(f"\n[W{worker_id}] Batch {batch_num}: {len(places)} places")

            results = []
            for idx, place in enumerate(places, 1):
                place_id = place["place_id"]
                name = place["name"]
                province = place["province_name"]
                district = place.get("district_name", "")
                sub_district = place.get("sub_district_name", "")

                logger.info(f"[{idx}/{len(places)}] {name} ({province})")

                try:
                    result = get_google_maps_reviews(
                        place_name=name,
                        province=province,
                        district=district,
                        sub_district=sub_district,
                        place_id=place_id,
                        headless=True,
                    )
                    results.append(result)

                    if result.status == "success":
                        consecutive_errors = 0
                        logger.info(f"   Rating: {result.google_avg_rating}, Reviews: {result.google_review_count}")
                    elif result.status == "partial":
                        consecutive_errors = 0
                        logger.info(f"   Partial: {result.google_avg_rating} stars")
                    elif result.status == "not_found":
                        logger.info(f"   Not found on Google Maps")
                    else:
                        consecutive_errors += 1
                        logger.info(f"   Error: {result.error_message}")

                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"   Exception: {e}")

                # If too many consecutive errors, pause longer
                if consecutive_errors >= 5:
                    logger.warning(f"   {consecutive_errors} consecutive errors, pausing 60s...")
                    time.sleep(60)
                    consecutive_errors = 0

                if idx < len(places):
                    time.sleep(random.uniform(delay_min, delay_max))

            # Save batch
            if results:
                results_data = [r.to_dict() if hasattr(r, 'to_dict') else r for r in results]
                stats = update_places_batch(results_data)

                batch_success = sum(1 for r in results if r.status == 'success')
                batch_partial = sum(1 for r in results if r.status == 'partial')
                total_processed += len(results)
                total_success += batch_success
                total_partial += batch_partial

                logger.info(f"Saved: {stats.get('updated', 0)} success | {stats.get('marked', 0)} marked | "
                           f"Batch: {batch_success} success, {batch_partial} partial")

            # Pause between batches
            time.sleep(random.uniform(3, 6))

    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()

    elapsed = datetime.now() - start_time
    rate = total_processed / elapsed.total_seconds() * 60 if elapsed.total_seconds() > 0 else 0
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Summary")
    logger.info(f"{'=' * 60}")
    logger.info(f"   Processed: {total_processed:,}")
    logger.info(f"   Success (full): {total_success:,}")
    logger.info(f"   Partial: {total_partial:,}")
    logger.info(f"   Elapsed: {elapsed}")
    logger.info(f"   Rate: {rate:.1f} places/min")

    # Show final counts
    final_counts = get_remaining_count()
    logger.info(f"\n   Remaining not_found: {final_counts.get('not_found', 0):,}")
    logger.info(f"   Remaining error: {final_counts.get('error', 0):,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Re-scrape Google Maps for not_found/error places")
    parser.add_argument("--worker-id", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--delay-min", type=float, default=3.0)
    parser.add_argument("--delay-max", type=float, default=6.0)

    args = parser.parse_args()
    run(worker_id=args.worker_id, batch_size=args.batch_size, delay_min=args.delay_min, delay_max=args.delay_max)

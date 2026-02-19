#!/usr/bin/env python
"""
Fill Ratings from Stored Wongnai URLs
======================================
Optimized script: uses existing wongnai_url to fetch ratings directly
(skips the search step entirely).

Targets places with:
- google_scrape_status IN ('not_found', 'error')
- wongnai_url IS NOT NULL
- google_avg_rating IS NULL

Usage:
    python run_fill_ratings_from_wongnai_url.py [--batch-size 50] [--delay-min 0.5] [--delay-max 1.5]
"""

import argparse
import logging
import time
import random
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tasks.wongnai.wongnai_scraper import extract_business_json, extract_wongnai_metadata, HEADERS
from tasks.wongnai.loader import get_connection

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def get_places_with_url_no_rating(limit: int = 50) -> list:
    """Get places that have wongnai_url but no rating yet."""
    sql = """
        SELECT
            p.place_id,
            p.name,
            p.wongnai_url,
            p.google_scrape_status
        FROM tat.places p
        WHERE p.google_scrape_status IN ('not_found', 'error')
          AND p.wongnai_url IS NOT NULL
          AND p.wongnai_url != ''
          AND p.google_avg_rating IS NULL
        ORDER BY RANDOM()
        LIMIT %s
    """
    try:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, [limit])
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")
        return []


def get_remaining_count() -> dict:
    """Get counts of places needing rating fill."""
    sql = """
        SELECT
            COUNT(*) FILTER (WHERE wongnai_url IS NOT NULL AND wongnai_url != '' AND google_avg_rating IS NULL) as with_url,
            COUNT(*) FILTER (WHERE (wongnai_url IS NULL OR wongnai_url = '') AND google_avg_rating IS NULL) as without_url
        FROM tat.places
        WHERE google_scrape_status IN ('not_found', 'error')
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return {"with_url": row[0], "without_url": row[1]}
    finally:
        conn.close()


def fetch_rating_from_url(wongnai_url: str) -> dict:
    """Fetch a Wongnai page and extract rating data."""
    try:
        response = requests.get(wongnai_url, headers=HEADERS, timeout=15)

        if response.status_code == 403:
            return {"status": "rate_limited"}

        if response.status_code != 200:
            return {"status": "error", "error": f"HTTP {response.status_code}"}

        business_data = extract_business_json(response.text)
        metadata = extract_wongnai_metadata(response.text)

        if not business_data:
            return {"status": "no_data"}

        statistic = business_data.get("statistic", business_data)
        dist = statistic.get("ratingDistribution", {})

        return {
            "status": "success_w",
            "avg_rating": statistic.get("rating"),
            "review_count": statistic.get("numberOfReviews", 0),
            "star_5": dist.get("five", 0),
            "star_4": dist.get("four", 0),
            "star_3": dist.get("three", 0),
            "star_2": dist.get("two", 0),
            "star_1": dist.get("one", 0),
            "genres": metadata.get("genres", []),
            "neighborhoods": metadata.get("neighborhoods", []),
            "price_range": metadata.get("price_range"),
        }
    except requests.Timeout:
        return {"status": "error", "error": "timeout"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def save_ratings_batch(results: list):
    """Save rating results to database."""
    success_data = []
    failed_place_ids = []  # Places that returned error/no_data/404
    no_data_count = 0

    for r in results:
        if r["fetch_status"] == "success_w" and r.get("avg_rating") is not None:
            success_data.append((
                r["avg_rating"],
                r["review_count"],
                r.get("star_5", 0),
                r.get("star_4", 0),
                r.get("star_3", 0),
                r.get("star_2", 0),
                r.get("star_1", 0),
                "success_w",
                datetime.now(),
                r.get("genres", []),
                r.get("neighborhoods", []),
                r.get("price_range"),
                r["place_id"],
            ))
        elif r["fetch_status"] in ("no_data", "error"):
            failed_place_ids.append((r["place_id"],))
            if r["fetch_status"] == "no_data":
                no_data_count += 1

    updated = 0
    if success_data:
        sql = """
            UPDATE tat.places AS t
            SET
                google_avg_rating = CAST(v.avg_rating AS FLOAT),
                google_review_count = CAST(v.review_count AS INTEGER),
                google_star_5 = CAST(v.star_5 AS INTEGER),
                google_star_4 = CAST(v.star_4 AS INTEGER),
                google_star_3 = CAST(v.star_3 AS INTEGER),
                google_star_2 = CAST(v.star_2 AS INTEGER),
                google_star_1 = CAST(v.star_1 AS INTEGER),
                google_scrape_status = v.status,
                google_scraped_at = CAST(v.scraped_at AS TIMESTAMP),
                wongnai_genres = v.genres::text[],
                wongnai_neighborhoods = v.neighborhoods::text[],
                wongnai_price_range = CAST(v.price_range AS INTEGER)
            FROM (VALUES %s) AS v(
                avg_rating, review_count,
                star_5, star_4, star_3, star_2, star_1,
                status, scraped_at,
                genres, neighborhoods, price_range,
                place_id
            )
            WHERE t.place_id = CAST(v.place_id AS INTEGER)
        """
        try:
            from psycopg2.extras import execute_values
            with get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, success_data)
                    updated = cur.rowcount
                conn.commit()
        except Exception as e:
            logger.error(f"❌ DB update error: {e}")

    # Mark failed places (404, no_data, error) with google_avg_rating=0
    # so they don't reappear in the queue
    marked = 0
    if failed_place_ids:
        sql_mark = """
            UPDATE tat.places AS t
            SET
                google_avg_rating = 0,
                google_scraped_at = CAST(v.scraped_at AS TIMESTAMP)
            FROM (VALUES %s) AS v(place_id)
            CROSS JOIN (SELECT %s::timestamp AS scraped_at) ts
            WHERE t.place_id = CAST(v.place_id AS INTEGER)
              AND t.google_avg_rating IS NULL
        """
        try:
            from psycopg2.extras import execute_values
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # execute_values only handles the VALUES part, so use a simpler approach
                    place_ids = [p[0] for p in failed_place_ids]
                    cur.execute(
                        """UPDATE tat.places
                           SET google_avg_rating = 0, google_scraped_at = %s
                           WHERE place_id = ANY(%s) AND google_avg_rating IS NULL""",
                        (datetime.now(), place_ids)
                    )
                    marked = cur.rowcount
                conn.commit()
        except Exception as e:
            logger.error(f"❌ DB mark-failed error: {e}")

    return {"updated": updated, "no_data": no_data_count, "marked_failed": marked}


def run(batch_size: int = 50, delay_min: float = 2.0, delay_max: float = 4.0):
    counts = get_remaining_count()
    logger.info("=" * 60)
    logger.info("⚡ Fill Ratings from Stored Wongnai URLs")
    logger.info("=" * 60)
    logger.info(f"   Places with URL, no rating: {counts['with_url']:,}")
    logger.info(f"   Places without URL (need full scrape): {counts['without_url']:,}")
    logger.info(f"   Batch size: {batch_size}, Delay: {delay_min}-{delay_max}s")
    logger.info("")

    total_processed = 0
    total_updated = 0
    start_time = datetime.now()
    batch_num = 0
    consecutive_limited_batches = 0  # Track consecutive rate-limited batches
    COOLDOWN_BASE = 300  # 5 minutes base cooldown
    COOLDOWN_MAX = 900   # 15 minutes max cooldown
    RATE_LIMIT_THRESHOLD = 0.3  # If >30% of batch is rate limited, trigger cooldown

    try:
        while True:
            batch_num += 1
            places = get_places_with_url_no_rating(limit=batch_size)

            if not places:
                logger.info("✅ No more places with URL needing rating!")
                break

            logger.info(f"\n📦 Batch {batch_num}: {len(places)} places")

            results = []
            rate_limited_count = 0

            for idx, place in enumerate(places, 1):
                place_id = place["place_id"]
                name = place["name"]
                url = place["wongnai_url"]

                logger.info(f"[{idx}/{len(places)}] {name}")

                data = fetch_rating_from_url(url)

                if data["status"] == "rate_limited":
                    rate_limited_count += 1
                    logger.warning("   ⏳ Rate limited")
                    # If 3+ consecutive rate limits in this batch, stop batch early
                    if rate_limited_count >= 3 and rate_limited_count >= idx * RATE_LIMIT_THRESHOLD:
                        logger.warning(f"   🛑 Too many rate limits ({rate_limited_count}/{idx}), stopping batch early")
                        break
                    time.sleep(random.uniform(8, 15))
                    continue

                result = {"place_id": place_id, "fetch_status": data["status"]}
                if data["status"] == "success_w":
                    result.update(data)
                    logger.info(f"   ✅ Rating: {data.get('avg_rating')}, Reviews: {data.get('review_count')}")
                elif data["status"] == "no_data":
                    logger.info("   ⚠️ Page loaded but no rating data")
                else:
                    logger.info(f"   ❌ {data.get('error', 'unknown error')}")

                results.append(result)

                if idx < len(places):
                    time.sleep(random.uniform(delay_min, delay_max))

            # Save batch
            if results:
                stats = save_ratings_batch(results)
                total_processed += len(results)
                total_updated += stats["updated"]
                logger.info(f"💾 Updated: {stats['updated']} | No data: {stats['no_data']} | Failed marked: {stats.get('marked_failed', 0)} | Rate limited: {rate_limited_count}")

            # Batch-level rate limit detection
            batch_total = len(results) + rate_limited_count
            if batch_total > 0 and rate_limited_count / batch_total > RATE_LIMIT_THRESHOLD:
                consecutive_limited_batches += 1
                cooldown = min(COOLDOWN_BASE * consecutive_limited_batches, COOLDOWN_MAX)
                logger.warning(f"🔴 Batch was heavily rate limited ({rate_limited_count}/{batch_total})")
                logger.warning(f"   Cooling down for {cooldown // 60} minutes (streak: {consecutive_limited_batches})...")
                time.sleep(cooldown)
            else:
                consecutive_limited_batches = 0  # Reset streak on successful batch
                time.sleep(random.uniform(3, 6))  # Normal pause between batches

    except KeyboardInterrupt:
        logger.info("\n⚠️ Interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    elapsed = datetime.now() - start_time
    rate = total_processed / elapsed.total_seconds() * 60 if elapsed.total_seconds() > 0 else 0
    logger.info(f"\n{'=' * 60}")
    logger.info(f"📋 Summary")
    logger.info(f"{'=' * 60}")
    logger.info(f"   Processed: {total_processed:,}")
    logger.info(f"   Updated with rating: {total_updated:,}")
    logger.info(f"   Elapsed: {elapsed}")
    logger.info(f"   Rate: {rate:.1f} places/min")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fill ratings from stored Wongnai URLs")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=4.0)

    args = parser.parse_args()
    run(batch_size=args.batch_size, delay_min=args.delay_min, delay_max=args.delay_max)

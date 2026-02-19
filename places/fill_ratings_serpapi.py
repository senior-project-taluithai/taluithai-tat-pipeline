#!/usr/bin/env python3.11
"""
Fill missing Google ratings using SerpAPI Google Maps search.
Targets places with 'not_found' or 'error' google_scrape_status.

SerpAPI returns: rating, reviews count, place_id, gps_coordinates
(No star distribution from search API — only overall rating + count)
"""

import os
import sys
import time
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from serpapi import GoogleSearch

sys.stdout.reconfigure(line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SERPAPI_KEY = os.environ.get("SERPAPI_KEY")

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_places_to_fill(limit: int = 100) -> list:
    """Get places with not_found or error status."""
    sql = """
        SELECT
            p.place_id,
            p.name,
            COALESCE(d.name, '') as district_name,
            COALESCE(sd.name, '') as sub_district_name,
            pr.name as province_name
        FROM tat.places p
        JOIN tat.provinces pr ON p.province_id = pr.province_id
        LEFT JOIN tat.districts d ON p.district_id = d.district_id
        LEFT JOIN tat.sub_districts sd ON p.sub_district_id = sd.sub_district_id
        WHERE p.google_scrape_status IN ('not_found', 'error')
        ORDER BY p.place_id
        LIMIT %s
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [limit])
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def search_place_serpapi(name: str, province: str, district: str = "", sub_district: str = "") -> dict:
    """Search for a place on Google Maps via SerpAPI."""
    # Build queries: try specific first, then broader
    queries = []
    # Query 1: name + sub_district + district + province (most specific)
    parts = [name]
    if sub_district:
        parts.append(sub_district)
    if district:
        parts.append(district)
    parts.append(province)
    queries.append(" ".join(parts))
    # Query 2: name + district + province (without sub_district)
    if sub_district:
        parts2 = [name]
        if district:
            parts2.append(district)
        parts2.append(province)
        queries.append(" ".join(parts2))

    for query in queries:
        params = {
            "engine": "google_maps",
            "q": query,
            "hl": "th",
            "type": "search",
            "api_key": SERPAPI_KEY,
        }

        try:
            search = GoogleSearch(params)
            results = search.get_dict()

            # Case 1: Direct match (auto-redirect to place page)
            pr = results.get("place_results", {})
            if pr and pr.get("rating"):
                return {
                    "rating": pr["rating"],
                    "reviews": pr.get("reviews", 0),
                    "google_place_id": pr.get("place_id"),
                    "title": pr.get("title", ""),
                }

            # Case 2: Multiple results (search results list)
            local = results.get("local_results", [])
            if local:
                # Take first result that has a rating
                for item in local:
                    if item.get("rating"):
                        return {
                            "rating": item["rating"],
                            "reviews": item.get("reviews", 0),
                            "google_place_id": item.get("place_id"),
                            "title": item.get("title", ""),
                        }
                # If no rated result, take first one anyway
                item = local[0]
                return {
                    "rating": item.get("rating"),
                    "reviews": item.get("reviews", 0),
                    "google_place_id": item.get("place_id"),
                    "title": item.get("title", ""),
                }

            # Direct match but no rating
            if pr and pr.get("title"):
                return {
                    "rating": pr.get("rating"),
                    "reviews": pr.get("reviews", 0),
                    "google_place_id": pr.get("place_id"),
                    "title": pr.get("title", ""),
                }

        except Exception as e:
            logger.error(f"    SerpAPI error: {e}")
            return None

    return None


def update_place_rating(place_id: int, data: dict):
    """Update a single place with rating data from SerpAPI."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if data.get("rating"):
                cur.execute("""
                    UPDATE tat.places SET
                        google_avg_rating = %s,
                        google_review_count = %s,
                        google_place_id = %s,
                        google_scrape_status = 'success',
                        google_scraped_at = %s
                    WHERE place_id = %s
                """, (
                    data["rating"],
                    data.get("reviews", 0),
                    data.get("google_place_id"),
                    datetime.now(),
                    place_id,
                ))
            else:
                # Found on maps but no rating
                cur.execute("""
                    UPDATE tat.places SET
                        google_place_id = %s,
                        google_scrape_status = 'not_found',
                        google_scraped_at = %s
                    WHERE place_id = %s
                """, (
                    data.get("google_place_id"),
                    datetime.now(),
                    place_id,
                ))
        conn.commit()
    finally:
        conn.close()


def mark_not_found(place_id: int):
    """Mark a place as confirmed not found."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE tat.places SET
                    google_scrape_status = 'not_found',
                    google_scraped_at = %s
                WHERE place_id = %s
            """, (datetime.now(), place_id))
        conn.commit()
    finally:
        conn.close()


def main():
    places = get_places_to_fill(limit=1000)
    total = len(places)
    logger.info(f"Places to fill: {total}")

    found = 0
    found_no_rating = 0
    not_found = 0
    errors = 0
    start = datetime.now()

    for i, place in enumerate(places, 1):
        pid = place["place_id"]
        name = place["name"]
        province = place["province_name"]
        district = place.get("district_name", "")
        sub_district = place.get("sub_district_name", "")

        logger.info(f"[{i}/{total}] {name} ({province})")

        try:
            result = search_place_serpapi(name, province, district, sub_district)

            if result:
                if result.get("rating"):
                    update_place_rating(pid, result)
                    found += 1
                    logger.info(f"    ✅ {result['title']} — {result['rating']}⭐ ({result['reviews']} reviews)")
                else:
                    update_place_rating(pid, result)
                    found_no_rating += 1
                    logger.info(f"    ⚠️ Found but no rating: {result['title']}")
            else:
                mark_not_found(pid)
                not_found += 1
                logger.info(f"    ❌ Not found")
        except Exception as e:
            errors += 1
            logger.error(f"    💥 Error: {e}")

        # SerpAPI rate limit
        time.sleep(1.2)

        # Progress every 50
        if i % 50 == 0:
            elapsed = (datetime.now() - start).total_seconds()
            rate = i / elapsed * 60
            remaining = (total - i) / rate if rate > 0 else 0
            logger.info(f"\n--- Progress: {i}/{total} | Found: {found} | "
                        f"Rate: {rate:.0f}/min | ETA: {remaining:.0f}min ---\n")

    elapsed = datetime.now() - start
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Done! Elapsed: {elapsed}")
    logger.info(f"  Found with rating: {found}")
    logger.info(f"  Found no rating:   {found_no_rating}")
    logger.info(f"  Not found:         {not_found}")
    logger.info(f"  Errors:            {errors}")

    # Final DB check
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT google_scrape_status, COUNT(*) 
            FROM tat.places 
            GROUP BY google_scrape_status 
            ORDER BY COUNT(*) DESC
        """)
        logger.info(f"\nFinal status breakdown:")
        for row in cur.fetchall():
            logger.info(f"  {row[0]}: {row[1]:,}")
    conn.close()


if __name__ == "__main__":
    main()

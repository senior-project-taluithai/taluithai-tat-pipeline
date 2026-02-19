"""
Wongnai Loader
==============
Updates database with Wongnai scraped data
Uses existing Google columns but marks status as 'success_w'
"""

import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from datetime import datetime

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env'))

# DB Config
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_not_found_places(limit: int = 50) -> list:
    """
    Get places that were not found on Google Maps
    """
    sql = """
        SELECT
            p.place_id,
            p.name,
            pr.name as province_name
        FROM tat.places p
        JOIN tat.provinces pr ON p.province_id = pr.province_id
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
            conn.commit()
            return places
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    except Exception as e:
        print(f"❌ DB Error: {e}")
        return []


def update_wongnai_batch(results: list, genres_only: bool = False) -> dict:
    """
    Update database with Wongnai results.
    
    Args:
        results: List of WongnaiResult dicts
        genres_only: If True, only update wongnai_* columns (genres, neighborhoods, price_range, url)
                     and do NOT touch rating columns or google_scrape_status
    
    Logic:
    - Places WITH existing Google rating: Only update wongnai_* columns (genres, neighborhoods, price_range, url)
    - Places WITHOUT Google rating: Update both rating AND wongnai_* columns, set status to 'success_w'
      (unless genres_only=True, then skip rating update)
    """
    if not results:
        return {"updated": 0, "marked": 0, "genres_only": 0}
    
    # Separate results by type
    has_wongnai_data = []  # All places with Wongnai data (for genres)
    needs_rating = []      # Places that need rating from Wongnai (Google not_found)
    still_not_found = []   # Places not found on Wongnai either
    
    for r in results:
        if hasattr(r, 'to_dict'):
            data = r.to_dict()
        else:
            data = r
        
        status = data.get('status', 'error')
        place_id = data.get('place_id')
        
        if status == 'success_w':
            # Has data from Wongnai - prepare genres update for ALL
            has_wongnai_data.append((
                data.get('wongnai_genres', []),
                data.get('wongnai_neighborhoods', []),
                data.get('wongnai_price_range'),
                data.get('wongnai_url'),
                datetime.now(),
                place_id
            ))
            
            # Also prepare rating update (will be applied conditionally)
            if not genres_only:
                needs_rating.append((
                    data.get('google_avg_rating'),
                    data.get('google_review_count'),
                    data.get('google_star_5', 0),
                    data.get('google_star_4', 0),
                    data.get('google_star_3', 0),
                    data.get('google_star_2', 0),
                    data.get('google_star_1', 0),
                    'success_w',
                    datetime.now(),
                    place_id
                ))
        elif status == 'rate_limited':
            # Rate limited - DO NOT mark, so it can be retried later
            pass
        else:
            # Not found or error - mark with empty genres so it doesn't reappear in queue
            still_not_found.append((
                [],  # Empty genres array to mark as processed
                [],  # Empty neighborhoods
                datetime.now(),
                place_id
            ))
    
    genres_count = 0
    rating_count = 0
    marked_count = 0
    
    # Step 1: Update wongnai_* columns for ALL places with Wongnai data
    # This does NOT touch rating columns or status
    if has_wongnai_data:
        sql_genres = """
            UPDATE tat.places AS t
            SET
                wongnai_genres = v.wongnai_genres::text[],
                wongnai_neighborhoods = v.wongnai_neighborhoods::text[],
                wongnai_price_range = CAST(v.wongnai_price_range AS INTEGER),
                wongnai_url = v.wongnai_url,
                wongnai_scraped_at = CAST(v.wongnai_scraped_at AS TIMESTAMP)
            FROM (VALUES %s) AS v(
                wongnai_genres,
                wongnai_neighborhoods,
                wongnai_price_range,
                wongnai_url,
                wongnai_scraped_at, 
                place_id
            )
            WHERE t.place_id = CAST(v.place_id AS INTEGER)
        """
        
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql_genres, has_wongnai_data)
                conn.commit()
                genres_count = len(has_wongnai_data)
        except Exception as e:
            print(f"❌ Database update error (genres): {e}")
    
    # Step 2: Update rating ONLY for places that don't have Google rating
    # Skipped if genres_only=True
    if needs_rating and not genres_only:
        sql_rating = """
            UPDATE tat.places AS t
            SET
                google_avg_rating = CAST(v.google_avg_rating AS FLOAT),
                google_review_count = CAST(v.google_review_count AS INTEGER),
                google_star_5 = CAST(v.google_star_5 AS INTEGER),
                google_star_4 = CAST(v.google_star_4 AS INTEGER),
                google_star_3 = CAST(v.google_star_3 AS INTEGER),
                google_star_2 = CAST(v.google_star_2 AS INTEGER),
                google_star_1 = CAST(v.google_star_1 AS INTEGER),
                google_scrape_status = v.status,
                google_scraped_at = CAST(v.google_scraped_at AS TIMESTAMP)
            FROM (VALUES %s) AS v(
                google_avg_rating, 
                google_review_count, 
                google_star_5, 
                google_star_4, 
                google_star_3, 
                google_star_2, 
                google_star_1, 
                status,
                google_scraped_at, 
                place_id
            )
            WHERE t.place_id = CAST(v.place_id AS INTEGER)
              AND (t.google_scrape_status IS NULL 
                   OR t.google_scrape_status IN ('not_found', 'error')
                   OR t.google_avg_rating IS NULL)
        """
        
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql_rating, needs_rating)
                    rating_count = cur.rowcount
                conn.commit()
        except Exception as e:
            print(f"❌ Database update error (rating): {e}")
    
    # Step 3: Mark not-found places with empty genres
    if still_not_found:
        sql_not_found = """
            UPDATE tat.places AS t
            SET
                wongnai_genres = v.wongnai_genres::text[],
                wongnai_neighborhoods = v.wongnai_neighborhoods::text[],
                wongnai_scraped_at = CAST(v.wongnai_scraped_at AS TIMESTAMP)
            FROM (VALUES %s) AS v(
                wongnai_genres,
                wongnai_neighborhoods,
                wongnai_scraped_at, 
                place_id
            )
            WHERE t.place_id = CAST(v.place_id AS INTEGER)
        """
        
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql_not_found, still_not_found)
                conn.commit()
                marked_count = len(still_not_found)
        except Exception as e:
            print(f"❌ Database update error (not_found): {e}")
    
    return {"updated": rating_count, "marked": marked_count, "genres_only": genres_count}



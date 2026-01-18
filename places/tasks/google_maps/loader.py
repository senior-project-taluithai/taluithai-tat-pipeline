"""
Database Loader and Batch Updater
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

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

def update_places_batch(results: list) -> dict:
    """
    Update database with scraped results.
    Updates ALL results (success, partial, not_found, error) to track what has been attempted.
    """
    if not results:
        return {"updated": 0, "marked": 0}
    
    # Separate success/partial (with rating data) from not_found/error (no rating)
    success_data = []
    no_rating_data = []
    
    for r in results:
        # Check if r is dict or object
        if hasattr(r, 'to_dict'):
            data = r.to_dict()
        else:
            data = r
        
        status = data.get('status', 'error')
        place_id = data.get('place_id')
        
        if status in ('success', 'partial'):
            # Has rating data
            success_data.append((
                data.get('google_place_id'),
                data.get('google_avg_rating'),
                data.get('google_review_count'),
                data.get('google_star_5', 0),
                data.get('google_star_4', 0),
                data.get('google_star_3', 0),
                data.get('google_star_2', 0),
                data.get('google_star_1', 0),
                status,
                datetime.now(),
                place_id
            ))
        else:
            # No rating data (not_found, error)
            no_rating_data.append((
                status,
                datetime.now(),
                place_id
            ))
    
    updated_count = 0
    marked_count = 0
    
    # Update places with rating data
    if success_data:
        sql_success = """
            UPDATE tat.places AS t
            SET
                google_place_id = v.google_place_id,
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
                google_place_id, 
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
        """
        
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql_success, success_data)
                conn.commit()
                updated_count = len(success_data)
        except Exception as e:
            print(f"❌ Database update error (success): {e}")
    
    # Mark places with no rating (not_found, error) so we don't retry them
    if no_rating_data:
        sql_no_rating = """
            UPDATE tat.places AS t
            SET
                google_scrape_status = v.status,
                google_scraped_at = CAST(v.google_scraped_at AS TIMESTAMP)
            FROM (VALUES %s) AS v(
                status,
                google_scraped_at, 
                place_id
            )
            WHERE t.place_id = CAST(v.place_id AS INTEGER)
        """
        
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql_no_rating, no_rating_data)
                conn.commit()
                marked_count = len(no_rating_data)
        except Exception as e:
            print(f"❌ Database update error (no_rating): {e}")
    
    return {"updated": updated_count, "marked": marked_count}

def log_scrape_run(worker_id, stats):
    """Log run stats (placeholder)"""
    pass

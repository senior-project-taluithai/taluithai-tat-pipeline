#!/usr/bin/env python
"""
TAT Events Ingester
====================
Fetches events from TAT Data API and stores in PostgreSQL.

Usage:
    python ingest_events.py [--lang th] [--lang en]
"""

import argparse
import os
import requests
import time
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from datetime import datetime

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# API Configuration
API_BASE = "https://tatdataapi.io/api/v2/events"
API_KEY = os.environ.get("TAT_API_KEY")
PAGE_SIZE = 1000

# Database
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_events_page(page: int, lang: str = "th") -> dict:
    """Fetch a single page of events from API"""
    headers = {
        "accept": "application/json",
        "Accept-Language": lang,
        "x-api-key": API_KEY
    }
    params = {
        "limit": PAGE_SIZE,
        "page": page
    }
    
    response = requests.get(API_BASE, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_all_events(lang: str = "th") -> list:
    """Fetch all events from API with pagination"""
    all_events = []
    page = 1
    
    # First request to get total
    print(f"📡 Fetching events (lang={lang})...")
    data = fetch_events_page(page, lang)
    total = data["pagination"]["total"]
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    
    print(f"   Total events: {total:,}")
    print(f"   Total pages: {total_pages}")
    
    all_events.extend(data["data"])
    print(f"   Page {page}/{total_pages}: {len(data['data'])} events")
    
    # Fetch remaining pages
    for page in range(2, total_pages + 1):
        time.sleep(0.5)  # Rate limiting
        data = fetch_events_page(page, lang)
        all_events.extend(data["data"])
        print(f"   Page {page}/{total_pages}: {len(data['data'])} events")
    
    print(f"✅ Fetched {len(all_events):,} events total")
    return all_events


def upsert_events_th(events: list):
    """Insert/update Thai events into database"""
    conn = get_connection()
    cur = conn.cursor()
    
    insert_data = []
    for e in events:
        province_id = None
        if e.get("location") and e["location"].get("province"):
            province_id = e["location"]["province"].get("provinceId")
        
        insert_data.append((
            e["eventId"],
            e.get("name"),
            e.get("introduction"),
            e.get("startDate"),
            e.get("endDate"),
            e.get("latitude"),
            e.get("longitude"),
            province_id,
            e.get("thumbnailUrl"),
            e.get("tags", []),
            e.get("createdAt"),
            e.get("updatedAt"),
        ))
    
    sql = """
        INSERT INTO tat.events (
            event_id, name, introduction, start_date, end_date,
            latitude, longitude, province_id, thumbnail_url, tags,
            created_at, updated_at
        ) VALUES %s
        ON CONFLICT (event_id) DO UPDATE SET
            name = EXCLUDED.name,
            introduction = EXCLUDED.introduction,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            province_id = EXCLUDED.province_id,
            thumbnail_url = EXCLUDED.thumbnail_url,
            tags = EXCLUDED.tags,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at
    """
    
    execute_values(cur, sql, insert_data, page_size=1000)
    conn.commit()
    print(f"💾 Upserted {len(insert_data):,} Thai events")
    
    cur.close()
    conn.close()


def upsert_events_en(events: list):
    """Update English fields for existing events"""
    conn = get_connection()
    cur = conn.cursor()
    
    update_data = [(e.get("name"), e.get("introduction"), e["eventId"]) for e in events]
    
    sql = """
        UPDATE tat.events 
        SET name_en = %s, introduction_en = %s
        WHERE event_id = %s
    """
    
    cur.executemany(sql, update_data)
    conn.commit()
    print(f"💾 Updated {cur.rowcount:,} English event names")
    
    cur.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Ingest TAT Events")
    parser.add_argument("--lang", choices=["th", "en", "both"], default="both",
                        help="Language to fetch (default: both)")
    args = parser.parse_args()
    
    print("=" * 60)
    print("🎉 TAT Events Ingester")
    print("=" * 60)
    
    if args.lang in ("th", "both"):
        events_th = fetch_all_events("th")
        upsert_events_th(events_th)
    
    if args.lang in ("en", "both"):
        events_en = fetch_all_events("en")
        upsert_events_en(events_en)
    
    # Summary
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tat.events")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tat.events WHERE name_en IS NOT NULL")
    with_en = cur.fetchone()[0]
    conn.close()
    
    print()
    print("=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"   Total events: {total:,}")
    print(f"   With English: {with_en:,}")


if __name__ == "__main__":
    main()

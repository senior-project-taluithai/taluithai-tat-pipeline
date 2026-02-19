#!/usr/bin/env python3.11
"""
Fill missing station geo data using SerpAPI Google Maps search.
Targets 116 stations that have no lat/lng.
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv
from serpapi import GoogleSearch

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

SERPAPI_KEY = os.environ.get("SERPAPI_KEY")

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}


def search_station(station_name_en: str) -> dict:
    """Search for a Thai railway station on Google Maps via SerpAPI."""
    query = f"สถานีรถไฟ {station_name_en} ประเทศไทย"

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

        local_results = results.get("local_results", [])
        if not local_results:
            # Try English query
            params["q"] = f"{station_name_en} railway station Thailand"
            search = GoogleSearch(params)
            results = search.get_dict()
            local_results = results.get("local_results", [])

        if not local_results:
            return None

        # Take first result
        place = local_results[0]
        gps = place.get("gps_coordinates", {})
        lat = gps.get("latitude")
        lng = gps.get("longitude")

        if lat is None or lng is None:
            return None

        # Extract Thai name from title
        title = place.get("title", "")

        # Try to get address for province info
        address = place.get("address", "")

        return {
            "lat": lat,
            "lng": lng,
            "name_th": title,
            "address": address,
        }

    except Exception as e:
        print(f"    SerpAPI error: {e}")
        return None


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Get stations without geo
    cur.execute("""
        SELECT station_code, name_en
        FROM railway.stations
        WHERE lat IS NULL
        ORDER BY station_code
    """)
    stations = cur.fetchall()
    print(f"Stations missing geo: {len(stations)}")
    print()

    updated = 0
    not_found = 0

    for i, (code, name_en) in enumerate(stations, 1):
        print(f"[{i}/{len(stations)}] {code} - {name_en}")

        result = search_station(name_en)

        if result:
            cur.execute("""
                UPDATE railway.stations SET
                    lat = %s, lng = %s, name_th = %s
                WHERE station_code = %s
            """, (result["lat"], result["lng"], result["name_th"], code))
            conn.commit()
            updated += 1
            print(f"    ✅ {result['name_th']} ({result['lat']}, {result['lng']})")
        else:
            not_found += 1
            print(f"    ❌ Not found")

        # Rate limit: SerpAPI allows 100 searches/month on free tier
        time.sleep(1.5)

    print(f"\n{'=' * 50}")
    print(f"Updated: {updated}")
    print(f"Not found: {not_found}")

    cur.execute("SELECT COUNT(*) FROM railway.stations WHERE lat IS NOT NULL")
    print(f"Total stations with geo: {cur.fetchone()[0]}/648")

    conn.close()


if __name__ == "__main__":
    main()

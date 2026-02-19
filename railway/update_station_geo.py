#!/usr/bin/env python3.11
"""
Update railway.stations with geo data (lat, lng, Thai name, province, etc.)
by chaining: CSV english name -> RailStation english -> RailStation thai -> geocoded data
"""

import os
import re
import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}


def strip_en(name):
    if pd.isna(name): return ''
    return re.sub(
        r'\s*(RAILWAY STATION|UNMANNED STATION|FLAG STATION|STATION|HALT)\s*$',
        '', str(name).strip(), flags=re.IGNORECASE
    ).strip()


def strip_th(name):
    if pd.isna(name): return name
    return re.sub(
        r'^(สถานีรถไฟ|ที่หยุดรถไฟ|ที่หยุดรถ|ป้ายหยุดรถ)\s*',
        '', name.strip()
    ).strip()


def main():
    print("Loading data files...")
    rail = pd.read_excel('railway/data/RailStation.xls')
    geo = pd.read_excel('railway/data/stations_geocoded.xlsx')
    csv_df = pd.read_excel('railway/data/after32.csv')

    # Build mappings
    rail['en_short'] = rail['NAMEE'].apply(strip_en)
    rail['th_short'] = rail['NAMET'].apply(strip_th)

    csv_stations = csv_df[['อักษรย่อ', 'สถานี']].drop_duplicates()
    csv_en_to_code = {v.strip().lower(): code for v, code in zip(csv_stations['สถานี'], csv_stations['อักษรย่อ'])}

    rail_en_to_th = {}
    rail_en_to_xy = {}
    for _, r in rail.iterrows():
        en = strip_en(r['NAMEE']).lower() if pd.notna(r['NAMEE']) else ''
        th = r['th_short']
        if en and th:
            rail_en_to_th[en] = th
            if pd.notna(r['X']) and pd.notna(r['Y']):
                rail_en_to_xy[en] = (r['Y'], r['X'])  # Y=lat, X=lng

    geo_by_th = {}
    for _, g in geo.iterrows():
        geo_by_th[g['ชื่อสถานีรถไฟ'].strip()] = g

    # Chain match: code -> geo data
    updates = []
    for csv_en_lower, code in csv_en_to_code.items():
        if csv_en_lower in rail_en_to_th:
            th_name = rail_en_to_th[csv_en_lower]
            if th_name in geo_by_th:
                g = geo_by_th[th_name]
                updates.append({
                    'code': code,
                    'name_th': th_name,
                    'lat': float(g['ค่าพิกัด_Lat']) if pd.notna(g['ค่าพิกัด_Lat']) else None,
                    'lng': float(g['ค่าพิกัด_Long']) if pd.notna(g['ค่าพิกัด_Long']) else None,
                    'province': str(g['จังหวัด']) if pd.notna(g['จังหวัด']) else None,
                    'district': str(g['อำเภอ']) if pd.notna(g['อำเภอ']) else None,
                    'subdistrict': str(g['ตำบล']) if pd.notna(g['ตำบล']) else None,
                    'postal_code': str(g['รหัสไปรษณีย์']) if pd.notna(g['รหัสไปรษณีย์']) else None,
                    'station_type': str(g['รายละเอียดเพิ่มเติม']) if pd.notna(g['รายละเอียดเพิ่มเติม']) else None,
                })
            elif csv_en_lower in rail_en_to_xy:
                # No geocoded match but RailStation has coordinates
                lat, lng = rail_en_to_xy[csv_en_lower]
                updates.append({
                    'code': code,
                    'name_th': th_name,
                    'lat': float(lat),
                    'lng': float(lng),
                    'province': None, 'district': None, 'subdistrict': None,
                    'postal_code': None, 'station_type': None,
                })

    print(f"Stations to update with geo: {len(updates)}/648")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Step 1: Drop unique constraint on name_th temporarily
    try:
        cur.execute("ALTER TABLE railway.stations DROP CONSTRAINT IF EXISTS stations_name_th_key")
        conn.commit()
    except Exception:
        conn.rollback()

    # Step 2: Update new stations (with code) with geo data + Thai name
    updated = 0
    for u in updates:
        cur.execute("""
            UPDATE railway.stations SET
                name_th = %s,
                lat = %s,
                lng = %s,
                province = %s,
                district = %s,
                subdistrict = %s,
                postal_code = %s,
                station_type = %s
            WHERE station_code = %s
        """, (
            u['name_th'], u['lat'], u['lng'],
            u['province'], u['district'], u['subdistrict'],
            u['postal_code'], u['station_type'],
            u['code']
        ))
        updated += cur.rowcount
    conn.commit()
    print(f"Updated with geo: {updated}")

    # Step 3: Delete old stations (without station_code) — they are now redundant
    cur.execute("DELETE FROM railway.stations WHERE station_code IS NULL")
    deleted = cur.rowcount
    conn.commit()
    print(f"Deleted old stations without code: {deleted}")

    # Step 4: Re-add unique constraint on name_th (allow NULLs/duplicates since some are English-only)
    # Actually skip this — station_code is the real PK now
    print("Skipping name_th unique constraint (station_code is the identifier)")

    # Check results
    cur.execute("SELECT COUNT(*) FROM railway.stations WHERE station_code IS NOT NULL")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM railway.stations WHERE station_code IS NOT NULL AND lat IS NOT NULL")
    with_geo = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM railway.stations WHERE name_th != name_en")
    with_th = cur.fetchone()[0]

    print(f"\nStations total: {total}")
    print(f"Stations with geo: {with_geo} ({with_geo/total*100:.1f}%)")
    print(f"Stations with Thai name: {with_th} ({with_th/total*100:.1f}%)")

    # Show sample
    cur.execute("""
        SELECT station_code, name_th, name_en, lat, lng, province
        FROM railway.stations
        WHERE lat IS NOT NULL
        ORDER BY station_code
        LIMIT 5
    """)
    print("\nSample stations:")
    for row in cur.fetchall():
        print(f"  {row[0]} | {row[1]} | {row[2]} | ({row[3]}, {row[4]}) | {row[5]}")

    conn.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3.11
"""
Railway Data Import v2
======================
Reads after32.csv (xlsx format) and populates:
  1. railway.stations   — add station_code mapping
  2. railway.trains     — train metadata
  3. railway.train_schedules — full schedule with platform, stop_type, sequence
  4. railway.route_topology  — parsed Train route column

Usage:
    python3.11 railway/import_railway_v2.py
"""

import os
import re
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}

DATA_FILE = "railway/data/after32.csv"

# Regex for Train route column
# Pattern: [NodeA]-[LinkA]-[Track]-[NodeB]-[LinkB] (Index)
ROUTE_RE = re.compile(
    r'\[([^\]]*)\]-\[([^\]]*)\]-\[([^\]]*)\]-\[([^\]]*)\]-\[([^\]]*)\]\s*\((\d+)\)'
)


def parse_time(val) -> str:
    """Convert time string like '1:36:00' or '1  0:05:00' to TIME compatible string.
    The '1  H:MM:SS' format means day 1 (next day), so add 24 to hours and wrap."""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    # Handle "1  H:MM:SS" format (day prefix)
    day_match = re.match(r'^(\d+)\s+(\d+):(\d+):(\d+)$', s)
    if day_match:
        day = int(day_match.group(1))
        h = (day * 24 + int(day_match.group(2))) % 24
        return f"{h:02d}:{day_match.group(3)}:{day_match.group(4)}"
    # Handle normal "H:MM:SS" format
    parts = s.split(':')
    if len(parts) == 3:
        h = int(parts[0]) % 24
        return f"{h:02d}:{parts[1]}:{parts[2]}"
    return s


def parse_train_route(route_str: str) -> dict:
    """Parse Train route string into topology components."""
    if pd.isna(route_str) or not route_str:
        return None
    m = ROUTE_RE.match(str(route_str).strip())
    if not m:
        return None
    return {
        "node_from": m.group(1) or None,
        "link_in": m.group(2) or None,
        "track_used": m.group(3) or None,
        "node_to": m.group(4) or None,
        "link_out": m.group(5) or None,
        "variant_index": int(m.group(6)),
    }


def main():
    print("=" * 60)
    print("Railway Data Import v2")
    print("=" * 60)

    # Load data
    print(f"\nLoading {DATA_FILE}...")
    df = pd.read_excel(DATA_FILE)
    print(f"  Rows: {len(df):,}")
    print(f"  Trains: {df['หมายเลขขบวนรถ'].nunique()}")
    print(f"  Stations: {df['อักษรย่อ'].nunique()}")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        cur = conn.cursor()

        # ============================================================
        # Step 1: Update stations with station_code
        # ============================================================
        print("\n[1/4] Updating stations with station_code...")

        stations_df = df[['อักษรย่อ', 'สถานี']].drop_duplicates()
        stations_map = dict(zip(stations_df['อักษรย่อ'], stations_df['สถานี']))

        # First, try to match existing stations by English name
        cur.execute("SELECT id, name_th, name_en, station_code FROM railway.stations")
        existing = cur.fetchall()
        existing_by_name_en = {}
        for row in existing:
            if row[2]:  # name_en
                existing_by_name_en[row[2].strip().lower()] = row[0]

        matched = 0
        new_stations = []

        for code, name_en in stations_map.items():
            name_en_lower = name_en.strip().lower()
            if name_en_lower in existing_by_name_en:
                # Update existing station with code
                cur.execute(
                    "UPDATE railway.stations SET station_code = %s, name_en = %s WHERE id = %s AND (station_code IS NULL OR station_code = '')",
                    (code, name_en, existing_by_name_en[name_en_lower])
                )
                matched += 1
            else:
                new_stations.append((code, name_en))

        # For stations not matched by name_en, try inserting them
        # (they might be new stations not in the original geo data)
        inserted = 0
        for code, name_en in new_stations:
            try:
                cur.execute(
                    "INSERT INTO railway.stations (name_th, name_en, station_code) VALUES (%s, %s, %s) ON CONFLICT (station_code) DO NOTHING",
                    (name_en, name_en, code)  # Use English name as Thai name fallback
                )
                if cur.rowcount > 0:
                    inserted += 1
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                conn.autocommit = False
                cur = conn.cursor()

        conn.commit()
        print(f"  Matched existing: {matched}")
        print(f"  Inserted new: {inserted}")
        print(f"  Skipped (already had code or duplicate): {len(new_stations) - inserted}")

        # ============================================================
        # Step 2: Insert trains
        # ============================================================
        print("\n[2/4] Inserting trains...")

        trains_df = df.groupby('หมายเลขขบวนรถ').agg({
            'route เส้นทางแต่ละสาย': 'first',
            'Name of train type': 'first',
            'Train length': 'first',
            'Used traction unit types': 'first',
        }).reset_index()

        train_data = []
        for _, row in trains_df.iterrows():
            train_data.append((
                str(row['หมายเลขขบวนรถ']),
                str(row['route เส้นทางแต่ละสาย']) if pd.notna(row['route เส้นทางแต่ละสาย']) else None,
                str(row['Name of train type']) if pd.notna(row['Name of train type']) else None,
                int(row['Train length']) if pd.notna(row['Train length']) else None,
                str(row['Used traction unit types']) if pd.notna(row['Used traction unit types']) else None,
            ))

        execute_values(
            cur,
            """INSERT INTO railway.trains (train_number, route_group_id, train_type, train_length, traction_unit)
               VALUES %s ON CONFLICT (train_number) DO UPDATE SET
                 route_group_id = EXCLUDED.route_group_id,
                 train_type = EXCLUDED.train_type,
                 train_length = EXCLUDED.train_length,
                 traction_unit = EXCLUDED.traction_unit""",
            train_data
        )
        conn.commit()
        print(f"  Trains upserted: {len(train_data)}")

        # ============================================================
        # Step 3: Insert train_schedules
        # ============================================================
        print("\n[3/4] Inserting train_schedules...")

        schedule_data = []
        for train_no, group in df.groupby('หมายเลขขบวนรถ'):
            group = group.sort_index()  # Keep original order (already sequential)
            for seq, (_, row) in enumerate(group.iterrows()):
                schedule_data.append((
                    str(train_no),
                    str(row['อักษรย่อ']),
                    parse_time(row['Arrival time']),
                    parse_time(row['Departure time']),
                    str(row['Plf']) if pd.notna(row['Plf']) else None,
                    str(row['Stop type']) if pd.notna(row['Stop type']) else None,
                    seq,
                    str(row['Train route']) if pd.notna(row['Train route']) else None,
                ))

        execute_values(
            cur,
            """INSERT INTO railway.train_schedules
               (train_number, station_code, arrival_time, departure_time, platform, stop_type, sequence, train_route_raw)
               VALUES %s""",
            schedule_data
        )
        conn.commit()
        print(f"  Schedules inserted: {len(schedule_data)}")

        # ============================================================
        # Step 4: Parse Train route → route_topology
        # ============================================================
        print("\n[4/4] Parsing route_topology...")

        # Get schedule_ids back
        cur.execute("SELECT schedule_id, train_route_raw FROM railway.train_schedules WHERE train_route_raw IS NOT NULL")
        rows = cur.fetchall()

        topology_data = []
        parse_ok = 0
        parse_fail = 0
        for schedule_id, route_raw in rows:
            parsed = parse_train_route(route_raw)
            if parsed:
                topology_data.append((
                    schedule_id,
                    parsed["node_from"],
                    parsed["link_in"],
                    parsed["track_used"],
                    parsed["node_to"],
                    parsed["link_out"],
                    parsed["variant_index"],
                ))
                parse_ok += 1
            else:
                parse_fail += 1

        if topology_data:
            execute_values(
                cur,
                """INSERT INTO railway.route_topology
                   (schedule_id, node_from, link_in, track_used, node_to, link_out, variant_index)
                   VALUES %s""",
                topology_data
            )
        conn.commit()
        print(f"  Topology parsed: {parse_ok}")
        print(f"  Parse failures: {parse_fail}")

        # ============================================================
        # Summary
        # ============================================================
        print(f"\n{'=' * 60}")
        print("Import complete!")
        print(f"{'=' * 60}")
        cur.execute("SELECT COUNT(*) FROM railway.stations WHERE station_code IS NOT NULL")
        print(f"  Stations with code: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM railway.trains")
        print(f"  Trains: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM railway.train_schedules")
        print(f"  Schedules: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM railway.route_topology")
        print(f"  Topology entries: {cur.fetchone()[0]}")

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

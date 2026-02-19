#!/usr/bin/env python3
"""
Load railway station and schedule data into PostgreSQL database.
Usage: python load_railway_data.py
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import warnings
from dotenv import load_dotenv
warnings.filterwarnings('ignore')

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

# Database connection settings
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5434'),
    'database': os.getenv('POSTGRES_DB', 'taluithai'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
}

def get_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)

def init_schema(conn):
    """Run the schema creation script."""
    # Schema is in repo root (for docker-compose mount)
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    schema_file = os.path.join(base_dir, '02-railway-schema.sql')
    if os.path.exists(schema_file):
        with open(schema_file, 'r') as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("✅ Schema created successfully")
    else:
        print("⚠️ Schema file not found, creating basic schema...")
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS railway")
        conn.commit()

def load_stations(conn, filepath=None):
    """Load stations data into database."""
    if filepath is None:
        # Data is in railway/data/
        script_dir = os.path.dirname(__file__)
        filepath = os.path.join(script_dir, '..', 'data', 'stations_geocoded.xlsx')
    print(f"📊 Loading stations from {filepath}...")
    df = pd.read_excel(filepath)
    
    # Map column names
    column_mapping = {
        'ชื่อสถานีรถไฟ': 'name_th',
        'จังหวัด': 'province',
        'ค่าพิกัด_Lat': 'lat',
        'ค่าพิกัด_Long': 'lng',
        'ตำบล': 'subdistrict',
        'อำเภอ': 'district',
        'รหัสไปรษณีย์': 'postal_code',
        'รายละเอียดเพิ่มเติม': 'station_type',
    }
    
    # Prepare data
    stations = []
    for _, row in df.iterrows():
        station = {
            'name_th': row.get('ชื่อสถานีรถไฟ'),
            'province': row.get('จังหวัด'),
            'district': row.get('อำเภอ'),
            'subdistrict': row.get('ตำบล'),
            'postal_code': str(row.get('รหัสไปรษณีย์', '')),
            'lat': row.get('ค่าพิกัด_Lat'),
            'lng': row.get('ค่าพิกัด_Long'),
            'station_type': row.get('รายละเอียดเพิ่มเติม'),
        }
        if pd.notna(station['name_th']):
            stations.append(station)
    
    # Insert into database
    with conn.cursor() as cur:
        # Clear existing data
        cur.execute("TRUNCATE TABLE railway.stations RESTART IDENTITY CASCADE")
        
        # Insert new data
        insert_sql = """
            INSERT INTO railway.stations 
            (name_th, province, district, subdistrict, postal_code, lat, lng, station_type)
            VALUES %s
            ON CONFLICT (name_th) DO UPDATE SET
                lat = EXCLUDED.lat,
                lng = EXCLUDED.lng,
                station_type = EXCLUDED.station_type
        """
        values = [
            (s['name_th'], s['province'], s['district'], s['subdistrict'], 
             s['postal_code'], s['lat'], s['lng'], s['station_type'])
            for s in stations
        ]
        execute_values(cur, insert_sql, values)
    
    conn.commit()
    print(f"✅ Loaded {len(stations)} stations")
    return len(stations)

def load_schedules(conn, filepath=None):
    """Load train schedules into database."""
    if filepath is None:
        # Data is in railway/data/
        script_dir = os.path.dirname(__file__)
        filepath = os.path.join(script_dir, '..', 'data', 'after32.csv')
    print(f"📊 Loading schedules from {filepath}...")
    
    # The file is actually Excel despite .csv extension
    df = pd.read_excel(filepath, engine='openpyxl')
    
    # Prepare data
    schedules = []
    for idx, row in df.iterrows():
        schedule = {
            'train_no': str(row.get('หมายเลขขบวนรถ', '')),
            'station_name_en': row.get('สถานี'),
            'station_abbr': row.get('อักษรย่อ'),
            'arrival_time': row.get('Arrival time'),
            'departure_time': row.get('Departure time'),
            'route_name': row.get('ชื่อเส้นทาง'),
            'route_origin': row.get('ต้นทาง'),
            'route_destination': row.get('ปลายทาง'),
            'stop_order': idx,  # Use row index as stop order within train
        }
        if pd.notna(schedule['station_name_en']):
            schedules.append(schedule)
    
    # Calculate proper stop_order per train
    train_order = {}
    for s in schedules:
        train = s['train_no']
        if train not in train_order:
            train_order[train] = 0
        s['stop_order'] = train_order[train]
        train_order[train] += 1
    
    # Insert into database
    with conn.cursor() as cur:
        # Clear existing data
        cur.execute("TRUNCATE TABLE railway.train_schedules RESTART IDENTITY")
        
        # Insert in batches
        insert_sql = """
            INSERT INTO railway.train_schedules 
            (train_no, station_name_en, station_abbr, arrival_time, departure_time,
             route_name, route_origin, route_destination, stop_order)
            VALUES %s
        """
        
        def convert_time(t):
            if pd.isna(t):
                return None
            if hasattr(t, 'strftime'):
                return t.strftime('%H:%M:%S')
            # Handle malformed strings like "1  0:01:53" (day + time)
            t_str = str(t).strip()
            # Remove any leading day indicator (e.g., "1  " means next day)
            import re
            match = re.search(r'(\d{1,2}:\d{2}(:\d{2})?)', t_str)
            if match:
                return match.group(1)
            return None
        
        values = [
            (s['train_no'], s['station_name_en'], s['station_abbr'],
             convert_time(s['arrival_time']), convert_time(s['departure_time']),
             s['route_name'], s['route_origin'], s['route_destination'], s['stop_order'])
            for s in schedules
        ]
        
        execute_values(cur, insert_sql, values)
    
    conn.commit()
    print(f"✅ Loaded {len(schedules)} schedule entries")
    return len(schedules)

def verify_data(conn):
    """Verify loaded data."""
    print("\n📊 Verification:")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM railway.stations")
        stations = cur.fetchone()[0]
        print(f"  Stations: {stations}")
        
        cur.execute("SELECT COUNT(*) FROM railway.train_schedules")
        schedules = cur.fetchone()[0]
        print(f"  Schedules: {schedules}")
        
        cur.execute("SELECT COUNT(DISTINCT train_no) FROM railway.train_schedules")
        trains = cur.fetchone()[0]
        print(f"  Unique trains: {trains}")
        
        # Test finding trains
        print("\n🔍 Test query: Find trains from Bangkok to Khon Kaen:")
        cur.execute("""
            SELECT * FROM railway.find_trains('Bang Sue', 'Khon Kaen', '08:00:00')
            LIMIT 3
        """)
        results = cur.fetchall()
        for r in results:
            print(f"  Train {r[0]}: {r[2]} ({r[3]}) → {r[4]} ({r[5]})")

def main():
    print("🚂 Railway Data Loader")
    print("=" * 50)
    
    try:
        conn = get_connection()
        print(f"✅ Connected to database: {DB_CONFIG['database']}")
        
        # Initialize schema
        init_schema(conn)
        
        # Load data
        load_stations(conn)
        load_schedules(conn)
        
        # Verify
        verify_data(conn)
        
        conn.close()
        print("\n✅ Data loading complete!")
        
    except psycopg2.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        print("\nMake sure PostgreSQL is running:")
        print("  docker-compose up -d postgres")
    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == '__main__':
    main()

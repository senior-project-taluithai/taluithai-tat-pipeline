#!/usr/bin/env python3
"""
Geocode missing Thai railway station coordinates.
Uses Nominatim (OpenStreetMap) first, falls back to Google Maps via Playwright.
"""

import pandas as pd
import time
import json
import re
import warnings
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

warnings.filterwarnings('ignore')

# Configuration
INPUT_FILE = 'after16.csv'  # Actually an Excel file
OUTPUT_FILE = 'stations_geocoded.xlsx'
PROGRESS_FILE = 'geocode_progress.json'

# Thailand bounding box for validation
THAILAND_BOUNDS = {
    'lat_min': 5.5,
    'lat_max': 21.0,
    'long_min': 97.0,
    'long_max': 106.0
}


def load_stations():
    """Load station data from Excel file."""
    df = pd.read_excel(INPUT_FILE, engine='openpyxl')
    print(f"📊 Loaded {len(df)} stations")
    return df


def is_valid_thailand_coord(lat, long):
    """Check if coordinates are within Thailand."""
    return (THAILAND_BOUNDS['lat_min'] <= lat <= THAILAND_BOUNDS['lat_max'] and
            THAILAND_BOUNDS['long_min'] <= long <= THAILAND_BOUNDS['long_max'])


def load_progress():
    """Load progress from previous run."""
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_progress(progress):
    """Save progress for resume."""
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def get_station_keyword(station_type):
    """Get appropriate search keyword based on station type."""
    if pd.isna(station_type):
        return 'สถานีรถไฟ'
    station_type = str(station_type)
    if 'ป้ายหยุด' in station_type:
        return 'ป้ายหยุดรถไฟ'
    elif 'ที่หยุด' in station_type or 'ทีหยุด' in station_type:
        return 'ที่หยุดรถไฟ'
    elif 'สถานี' in station_type:
        return 'สถานีรถไฟ'
    else:
        return 'สถานีรถไฟ'


def geocode_with_nominatim(station_name, province, district, subdistrict, station_type):
    """Try to geocode using Nominatim (OpenStreetMap)."""
    geolocator = Nominatim(user_agent="thai_railway_geocoder", timeout=10)
    
    # Get appropriate keyword based on station type
    keyword = get_station_keyword(station_type)
    
    # Try different query combinations
    queries = [
        f"{keyword}{station_name} {district} {province} Thailand",
        f"{station_name} railway station {province} Thailand",
        f"{keyword}{station_name} {province}",
        f"{station_name} train station {district} {province} Thailand",
        f"{station_name} {district} {province} Thailand",
    ]
    
    for query in queries:
        try:
            time.sleep(1.1)  # Rate limit: max 1 request per second
            location = geolocator.geocode(query)
            
            if location:
                lat, long = location.latitude, location.longitude
                if is_valid_thailand_coord(lat, long):
                    return lat, long, 'nominatim'
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"  ⚠️ Nominatim error: {e}")
            time.sleep(2)
            continue
    
    return None, None, None


def geocode_with_playwright(station_name, province, district, station_type):
    """Fallback: Use Playwright to search Google Maps."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ⚠️ Playwright not installed. Run: pip install playwright && playwright install")
        return None, None, None
    
    keyword = get_station_keyword(station_type)
    
    # Try multiple search queries
    search_queries = [
        f"{keyword}{station_name} {district} {province}",
        f"สถานีรถไฟ{station_name} {province}",
        f"{station_name} railway station {province} Thailand",
    ]
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            for query in search_queries:
                url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
                page.goto(url, timeout=20000)
                time.sleep(5)  # Wait for map to load and redirect
                
                # Get URL which contains coordinates
                current_url = page.url
                
                # Extract coordinates from URL (format: @lat,long,zoom)
                match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', current_url)
                if match:
                    lat, long = float(match.group(1)), float(match.group(2))
                    if is_valid_thailand_coord(lat, long):
                        browser.close()
                        return lat, long, 'playwright'
            
            browser.close()
    except Exception as e:
        print(f"  ⚠️ Playwright error: {e}")
    
    return None, None, None


def geocode_all_missing(df):
    """Geocode all stations with missing coordinates."""
    lat_col = 'ค่าพิกัด_Lat'
    long_col = 'ค่าพิกัด_Long'
    
    # Find missing
    missing_mask = df[lat_col].isna() | df[long_col].isna()
    missing_indices = df[missing_mask].index.tolist()
    
    print(f"\n🔍 Found {len(missing_indices)} stations with missing coordinates")
    
    # Load previous progress
    progress = load_progress()
    
    nominatim_success = 0
    playwright_success = 0
    failed = 0
    
    for i, idx in enumerate(missing_indices):
        row = df.loc[idx]
        station_name = row['ชื่อสถานีรถไฟ']
        province = row['จังหวัด']
        district = row['อำเภอ'] if pd.notna(row['อำเภอ']) else ''
        subdistrict = row['ตำบล'] if pd.notna(row['ตำบล']) else ''
        station_type = row['รายละเอียดเพิ่มเติม'] if 'รายละเอียดเพิ่มเติม' in row else None
        
        # Skip if already processed and successful
        if station_name in progress:
            cached = progress[station_name]
            if cached['lat'] and cached['long']:
                df.at[idx, lat_col] = cached['lat']
                df.at[idx, long_col] = cached['long']
                continue
            # If previously failed, retry with Playwright
        
        keyword = get_station_keyword(station_type)
        print(f"\n[{i+1}/{len(missing_indices)}] 📍 {station_name} ({province}) - {keyword}")
        
        # Try Nominatim first
        lat, long, source = geocode_with_nominatim(station_name, province, district, subdistrict, station_type)
        
        # Fallback to Playwright if Nominatim fails
        if lat is None:
            print(f"  ↪️ Trying Playwright fallback...")
            lat, long, source = geocode_with_playwright(station_name, province, district, station_type)
        
        if lat and long:
            df.at[idx, lat_col] = lat
            df.at[idx, long_col] = long
            progress[station_name] = {'lat': lat, 'long': long, 'source': source}
            
            if source == 'nominatim':
                nominatim_success += 1
            else:
                playwright_success += 1
            
            print(f"  ✅ Found: {lat:.6f}, {long:.6f} (via {source})")
        else:
            failed += 1
            progress[station_name] = {'lat': None, 'long': None, 'source': None}
            print(f"  ❌ Could not find coordinates")
        
        # Save progress periodically
        if (i + 1) % 10 == 0:
            save_progress(progress)
            print(f"\n💾 Progress saved ({i+1}/{len(missing_indices)})")
    
    # Final save
    save_progress(progress)
    
    print(f"\n" + "="*60)
    print(f"📊 RESULTS:")
    print(f"  ✅ Nominatim: {nominatim_success}")
    print(f"  ✅ Playwright: {playwright_success}")
    print(f"  ❌ Failed: {failed}")
    print(f"="*60)
    
    return df


def main():
    print("🚂 Thai Railway Station Geocoder")
    print("="*60)
    
    # Load data
    df = load_stations()
    
    # Count missing before
    lat_col = 'ค่าพิกัด_Lat'
    missing_before = df[lat_col].isna().sum()
    print(f"📍 Missing coordinates: {missing_before}")
    
    # Geocode
    df = geocode_all_missing(df)
    
    # Count missing after
    missing_after = df[lat_col].isna().sum()
    
    # Save result
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\n💾 Saved to: {OUTPUT_FILE}")
    print(f"📊 Missing before: {missing_before} → after: {missing_after}")
    print(f"✅ Filled: {missing_before - missing_after} coordinates")


if __name__ == '__main__':
    main()

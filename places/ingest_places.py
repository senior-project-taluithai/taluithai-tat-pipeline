#!/usr/bin/env python3
"""
TAT Place Data Ingestion Script

This script reads TAT (Tourism Authority of Thailand) place data from JSON files
and inserts them into a PostgreSQL database.

Usage:
    python ingest_places.py
"""

import json
import os
import glob
from datetime import datetime
from typing import Dict, List, Any, Optional
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Database configuration
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": int(os.environ.get("POSTGRES_PORT", "5434")),
    "database": os.environ.get("POSTGRES_DB", "taluithai"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Data is now in the same directory as the script
RAW_PLACE_DIR = os.path.join(BASE_DIR, "raw_tat_place")
RAW_PROVINCE_FILE = os.path.join(BASE_DIR, "raw_tat_province", "province.json")


def get_connection():
    """Create database connection."""
    return psycopg2.connect(**DB_CONFIG)


def load_json_file(filepath: str) -> Any:
    """Load and parse a JSON file."""
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def load_all_places() -> List[Dict]:
    """Load all place data from JSON files."""
    all_places = []
    json_files = glob.glob(os.path.join(RAW_PLACE_DIR, "*.json"))
    
    for filepath in json_files:
        filename = os.path.basename(filepath)
        # Skip temp files
        if filename == "temp.json":
            continue
        
        try:
            data = load_json_file(filepath)
            # Handle both formats: direct list or {data: [...]}
            if isinstance(data, dict) and "data" in data:
                places = data["data"]
            elif isinstance(data, list):
                places = data
            else:
                print(f"⚠️  Unknown format in {filename}")
                continue
            
            all_places.extend(places)
            print(f"✓ Loaded {len(places)} places from {filename}")
        except Exception as e:
            print(f"✗ Error loading {filename}: {e}")
    
    return all_places


def extract_lookup_data(places: List[Dict]) -> Dict[str, Dict]:
    """Extract categories, provinces, districts, and sub-districts from places."""
    categories = {}
    provinces = {}
    districts = {}
    sub_districts = {}
    sha_types = {}
    sha_categories = {}
    
    for place in places:
        # Extract category
        if cat := place.get("category"):
            cat_id = cat.get("categoryId")
            if cat_id and cat_id not in categories:
                categories[cat_id] = cat.get("name")
        
        # Extract location data
        if loc := place.get("location"):
            # Province
            if prov := loc.get("province"):
                prov_id = prov.get("provinceId")
                if prov_id and prov_id not in provinces:
                    provinces[prov_id] = prov.get("name")
            
            # District
            if dist := loc.get("district"):
                dist_id = dist.get("districtId")
                if dist_id and dist_id not in districts:
                    districts[dist_id] = {
                        "name": dist.get("name"),
                        "province_id": loc.get("province", {}).get("provinceId")
                    }
            
            # Sub-district
            if sub := loc.get("subDistrict"):
                sub_id = sub.get("subDistrictId")
                if sub_id and sub_id not in sub_districts:
                    sub_districts[sub_id] = {
                        "name": sub.get("name"),
                        "district_id": loc.get("district", {}).get("districtId")
                    }
        
        # Extract SHA data
        if sha := place.get("sha"):
            if sha_type := sha.get("type"):
                type_id = sha_type.get("typeId")
                if type_id and type_id not in sha_types:
                    sha_types[type_id] = sha_type.get("name")
            
            if sha_cat := sha.get("category"):
                cat_id = sha_cat.get("categoryId")
                if cat_id and cat_id not in sha_categories:
                    sha_categories[cat_id] = {
                        "name": sha_cat.get("name"),
                        "icon": sha_cat.get("icon")
                    }
    
    return {
        "categories": categories,
        "provinces": provinces,
        "districts": districts,
        "sub_districts": sub_districts,
        "sha_types": sha_types,
        "sha_categories": sha_categories,
    }


def load_province_mapping() -> Dict[int, str]:
    """Load province mapping from province.json."""
    if os.path.exists(RAW_PROVINCE_FILE):
        provinces = load_json_file(RAW_PROVINCE_FILE)
        return {p["id"]: p["name"] for p in provinces}
    return {}


def parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp string to datetime."""
    if not ts:
        return None
    try:
        # Handle ISO format with Z suffix
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except:
        return None


def insert_lookup_tables(conn, lookup_data: Dict):
    """Insert data into lookup tables."""
    with conn.cursor() as cur:
        # Insert categories
        if lookup_data["categories"]:
            categories = [(cat_id, name) for cat_id, name in lookup_data["categories"].items()]
            execute_values(
                cur,
                """
                INSERT INTO tat.categories (category_id, name)
                VALUES %s
                ON CONFLICT (category_id) DO UPDATE SET name = EXCLUDED.name
                """,
                categories
            )
            print(f"✓ Inserted {len(categories)} categories")
        
        # Load and merge province data
        province_mapping = load_province_mapping()
        all_provinces = {**lookup_data["provinces"], **province_mapping}
        
        if all_provinces:
            provinces = [(prov_id, name) for prov_id, name in all_provinces.items()]
            execute_values(
                cur,
                """
                INSERT INTO tat.provinces (province_id, name)
                VALUES %s
                ON CONFLICT (province_id) DO UPDATE SET name = EXCLUDED.name
                """,
                provinces
            )
            print(f"✓ Inserted {len(provinces)} provinces")
        
        # Insert districts
        if lookup_data["districts"]:
            districts = [
                (dist_id, data["province_id"], data["name"])
                for dist_id, data in lookup_data["districts"].items()
                if data["province_id"] in all_provinces
            ]
            execute_values(
                cur,
                """
                INSERT INTO tat.districts (district_id, province_id, name)
                VALUES %s
                ON CONFLICT (district_id) DO UPDATE SET 
                    province_id = EXCLUDED.province_id,
                    name = EXCLUDED.name
                """,
                districts
            )
            print(f"✓ Inserted {len(districts)} districts")
        
        # Insert sub-districts
        if lookup_data["sub_districts"]:
            valid_district_ids = set(lookup_data["districts"].keys())
            sub_districts = [
                (sub_id, data["district_id"], data["name"])
                for sub_id, data in lookup_data["sub_districts"].items()
                if data["district_id"] in valid_district_ids
            ]
            execute_values(
                cur,
                """
                INSERT INTO tat.sub_districts (sub_district_id, district_id, name)
                VALUES %s
                ON CONFLICT (sub_district_id) DO UPDATE SET 
                    district_id = EXCLUDED.district_id,
                    name = EXCLUDED.name
                """,
                sub_districts
            )
            print(f"✓ Inserted {len(sub_districts)} sub-districts")
        
        # Insert SHA types
        if lookup_data["sha_types"]:
            sha_types = [(type_id, name) for type_id, name in lookup_data["sha_types"].items()]
            execute_values(
                cur,
                """
                INSERT INTO tat.sha_types (type_id, name)
                VALUES %s
                ON CONFLICT (type_id) DO UPDATE SET name = EXCLUDED.name
                """,
                sha_types
            )
            print(f"✓ Inserted {len(sha_types)} SHA types")
        
        # Insert SHA categories
        if lookup_data["sha_categories"]:
            sha_cats = [
                (cat_id, data["name"], data["icon"])
                for cat_id, data in lookup_data["sha_categories"].items()
            ]
            execute_values(
                cur,
                """
                INSERT INTO tat.sha_categories (category_id, name, icon)
                VALUES %s
                ON CONFLICT (category_id) DO UPDATE SET 
                    name = EXCLUDED.name,
                    icon = EXCLUDED.icon
                """,
                sha_cats
            )
            print(f"✓ Inserted {len(sha_cats)} SHA categories")
    
    conn.commit()


def insert_places(conn, places: List[Dict]):
    """Insert places into the database."""
    place_records = []
    
    for place in places:
        loc = place.get("location", {})
        sha = place.get("sha")
        
        record = (
            int(place.get("placeId")),
            place.get("status"),
            place.get("name"),
            place.get("introduction"),
            place.get("category", {}).get("categoryId"),
            float(place.get("latitude")) if place.get("latitude") else None,
            float(place.get("longitude")) if place.get("longitude") else None,
            loc.get("address"),
            (loc.get("province") or {}).get("provinceId"),
            (loc.get("district") or {}).get("districtId"),
            (loc.get("subDistrict") or {}).get("subDistrictId"),
            loc.get("postcode"),
            place.get("thumbnailUrl", []),
            place.get("tags", []),
            place.get("viewer"),
            place.get("slug"),
            place.get("migrateId"),
            parse_timestamp(place.get("createdAt")),
            parse_timestamp(place.get("updatedAt")),
            sha.get("name") if sha else None,
            sha.get("detail") if sha else None,
            sha.get("thumbnailUrl") if sha else None,
            sha.get("detailPicture", []) if sha else [],
            (sha.get("type") or {}).get("typeId") if sha else None,
            (sha.get("category") or {}).get("categoryId") if sha else None,
        )
        place_records.append(record)
    
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO tat.places (
                place_id, status, name, introduction, category_id,
                latitude, longitude, address, province_id, district_id,
                sub_district_id, postcode, thumbnail_urls, tags, viewer,
                slug, migrate_id, created_at, updated_at,
                sha_name, sha_detail, sha_thumbnail_url, sha_detail_pictures,
                sha_type_id, sha_category_id
            )
            VALUES %s
            ON CONFLICT (place_id) DO UPDATE SET
                status = EXCLUDED.status,
                name = EXCLUDED.name,
                introduction = EXCLUDED.introduction,
                category_id = EXCLUDED.category_id,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                address = EXCLUDED.address,
                province_id = EXCLUDED.province_id,
                district_id = EXCLUDED.district_id,
                sub_district_id = EXCLUDED.sub_district_id,
                postcode = EXCLUDED.postcode,
                thumbnail_urls = EXCLUDED.thumbnail_urls,
                tags = EXCLUDED.tags,
                viewer = EXCLUDED.viewer,
                slug = EXCLUDED.slug,
                migrate_id = EXCLUDED.migrate_id,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                sha_name = EXCLUDED.sha_name,
                sha_detail = EXCLUDED.sha_detail,
                sha_thumbnail_url = EXCLUDED.sha_thumbnail_url,
                sha_detail_pictures = EXCLUDED.sha_detail_pictures,
                sha_type_id = EXCLUDED.sha_type_id,
                sha_category_id = EXCLUDED.sha_category_id
            """,
            place_records,
            page_size=500
        )
    
    conn.commit()
    print(f"✓ Inserted/Updated {len(place_records)} places")


def main():
    """Main entry point."""
    print("=" * 60)
    print("TAT Place Data Ingestion")
    print("=" * 60)
    
    # Load all places
    print("\n📂 Loading JSON files...")
    places = load_all_places()
    
    if not places:
        print("❌ No places found!")
        return
    
    print(f"\n📊 Total places loaded: {len(places)}")
    
    # Remove duplicates by place_id
    unique_places = {}
    for place in places:
        place_id = place.get("placeId")
        if place_id:
            unique_places[place_id] = place
    
    places = list(unique_places.values())
    print(f"📊 Unique places: {len(places)}")
    
    # Extract lookup data
    print("\n🔍 Extracting lookup data...")
    lookup_data = extract_lookup_data(places)
    
    print(f"   - Categories: {len(lookup_data['categories'])}")
    print(f"   - Provinces: {len(lookup_data['provinces'])}")
    print(f"   - Districts: {len(lookup_data['districts'])}")
    print(f"   - Sub-districts: {len(lookup_data['sub_districts'])}")
    print(f"   - SHA Types: {len(lookup_data['sha_types'])}")
    print(f"   - SHA Categories: {len(lookup_data['sha_categories'])}")
    
    # Connect to database
    print("\n🔌 Connecting to database...")
    try:
        conn = get_connection()
        print("✓ Connected!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n💡 Make sure the database is running:")
        print("   docker-compose up -d")
        return
    
    try:
        # Insert lookup data
        print("\n📝 Inserting lookup tables...")
        insert_lookup_tables(conn, lookup_data)
        
        # Insert places
        print("\n📝 Inserting places...")
        insert_places(conn, places)
        
        print("\n" + "=" * 60)
        print("✅ Ingestion completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

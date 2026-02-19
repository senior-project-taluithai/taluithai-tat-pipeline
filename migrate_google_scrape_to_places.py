#!/usr/bin/env python3
"""
MongoDB → PostgreSQL tat.places Migration
==========================================
1. Match MongoDB docs to existing tat.places by:
   a) google_place_id (ChIJ) exact match
   b) Name + lat/lng proximity (<500m)
2. UPDATE matched places: fill NULL columns (address, introduction, google_place_id, ratings)
3. INSERT unmatched MongoDB docs as new places

Usage:
    # Dry-run (no writes):
    python migrate_google_scrape_to_places.py --dry-run

    # Execute:
    python migrate_google_scrape_to_places.py

    # Limit per collection:
    python migrate_google_scrape_to_places.py --limit 100 --dry-run
"""

import os
import sys
import argparse
import math
import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus

import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────
PG_CONFIG = dict(
    host=os.environ.get("MIGRATE_PG_HOST", "34.87.52.21"),
    port=int(os.environ.get("MIGRATE_PG_PORT", "5432")),
    database=os.environ.get("MIGRATE_PG_DB", "taluithai"),
    user=os.environ.get("MIGRATE_PG_USER", "postgres"),
    password=os.environ.get("MIGRATE_PG_PASSWORD", os.environ.get("POSTGRES_PASSWORD", "")),
)

# Build MongoDB URI with properly encoded credentials
_mongo_user = quote_plus(os.environ.get("MONGODB_USERNAME", ""))
_mongo_pass = quote_plus(os.environ.get("MONGODB_PASSWORD", ""))
_mongo_host = os.environ.get("MONGODB_HOST", "db-taluithai.oswinfalk.xyz")
_mongo_port = os.environ.get("MONGODB_PORT", "27017")
MONGO_URI = f"mongodb://{_mongo_user}:{_mongo_pass}@{_mongo_host}:{_mongo_port}/?authSource=admin"
MONGO_DB = "google-scrape"

# MongoDB Collection → tat category_id
COLLECTION_CATEGORY_MAP = {
    "hotel": 2,       # ที่พัก
    "attraction": 3,  # สถานที่ท่องเที่ยว
    "musuem": 3,      # สถานที่ท่องเที่ยว (typo in original collection name)
    "park": 3,        # สถานที่ท่องเที่ยว
    "temple": 3,      # สถานที่ท่องเที่ยว
    "cafe": 8,        # ร้านอาหาร กาแฟ เบเกอรี่
    "restaurants": 8,  # ร้านอาหาร กาแฟ เบเกอรี่
    "hospital": 13,   # สถานที่อื่นๆ
}

# Variant A collections use longitude/website/descriptions
# Variant B collections use longtitude/web_site/description
VARIANT_A = {"attraction", "hotel"}

MATCH_DISTANCE_M = 500  # Max distance in meters for name+geo matching


# ── Helpers ────────────────────────────────────────────────────

def haversine_m(lat1, lon1, lat2, lon2):
    """Haversine distance in meters between two lat/lng pairs."""
    R = 6_371_000  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _safe_float(val) -> float | None:
    """Safely cast to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> int | None:
    """Safely cast to int, returning None on failure."""
    if val is None:
        return None
    try:
        return int(float(val))  # float() first to handle "4.0" strings
    except (ValueError, TypeError):
        return None


def _normalize_open_hours(oh) -> str | None:
    """Normalize open_hours to a JSON string (or None).
    MongoDB stores variant A as JSON string, variant B as dict.
    """
    import json
    if oh is None:
        return None
    if isinstance(oh, dict):
        return json.dumps(oh, ensure_ascii=False) if oh else None
    if isinstance(oh, str):
        s = oh.strip()
        return s if s else None
    return None


def normalize_doc(doc: dict, collection_name: str) -> dict:
    """Normalize MongoDB doc fields to a consistent schema."""
    is_variant_a = collection_name in VARIANT_A

    lat = doc.get("latitude")
    lng = doc.get("longitude") if is_variant_a else doc.get("longtitude")
    description = doc.get("descriptions") if is_variant_a else doc.get("description")
    website = doc.get("website") if is_variant_a else doc.get("web_site")

    # Parse complete_address for province info
    ca = doc.get("complete_address", {}) or {}
    state = ca.get("state", "") if isinstance(ca, dict) else ""
    postal_code = ca.get("postal_code", "") if isinstance(ca, dict) else ""
    borough = ca.get("borough", "") if isinstance(ca, dict) else ""

    # Parse reviews_per_rating: {"1": N, "2": N, ...}
    rpr = doc.get("reviews_per_rating", {}) or {}
    if not isinstance(rpr, dict):
        rpr = {}

    return {
        "google_place_id": doc.get("place_id", ""),
        "title": (doc.get("title") or "").strip(),
        "lat": _safe_float(lat),
        "lng": _safe_float(lng),
        "address": (doc.get("address") or "").strip() or None,
        "description": (description or "").strip() or None,
        "phone": (doc.get("phone") or "").strip() or None,
        "website": (website or "").strip() or None,
        "rating": _safe_float(doc.get("review_rating")),
        "review_count": _safe_int(doc.get("review_count")),
        "star_5": _safe_int(rpr.get("5") or rpr.get(5)),
        "star_4": _safe_int(rpr.get("4") or rpr.get(4)),
        "star_3": _safe_int(rpr.get("3") or rpr.get(3)),
        "star_2": _safe_int(rpr.get("2") or rpr.get(2)),
        "star_1": _safe_int(rpr.get("1") or rpr.get(1)),
        "open_hours": _normalize_open_hours(doc.get("open_hours")),
        "thumbnail": doc.get("thumbnail"),
        "categories": doc.get("categories") if not is_variant_a else None,
        "state": state,
        "postal_code": postal_code,
        "borough": borough,
        "collection": collection_name,
    }


# ── Province name mapping ──────────────────────────────────────

# English name → Thai name (for MongoDB docs that have English state names)
PROVINCE_EN_TO_TH = {
    "Bangkok": "กรุงเทพมหานคร",
    "Ang Thong": "อ่างทอง",
    "Buri Ram": "บุรีรัมย์",
    "Chachoengsao": "ฉะเชิงเทรา",
    "Chaiyaphum": "ชัยภูมิ",
    "Chiang Mai": "เชียงใหม่",
    "Chiang Rai": "เชียงราย",
    "Chon Buri": "ชลบุรี",
    "Chumpon": "ชุมพร",
    "Chumphon": "ชุมพร",
    "Kalasin": "กาฬสินธุ์",
    "Kanchanaburi": "กาญจนบุรี",
    "Kamphaeng Phet": "กำแพงเพชร",
    "Khon Kaen": "ขอนแก่น",
    "Krabi": "กระบี่",
    "Lampang": "ลำปาง",
    "Lamphun": "ลำพูน",
    "Loei": "เลย",
    "Mae Hong Son": "แม่ฮ่องสอน",
    "Maha Sarakham": "มหาสารคาม",
    "Mukdahan": "มุกดาหาร",
    "Nakhon Nayok": "นครนายก",
    "Nakhon Pathom": "นครปฐม",
    "Nakhon Phanom": "นครพนม",
    "Nakhon Ratchasima": "นครราชสีมา",
    "Nakhon Ratchasima Province": "นครราชสีมา",
    "Nakhon Sawan": "นครสวรรค์",
    "Nakhon Si Thammarat": "นครศรีธรรมราช",
    "Nan": "น่าน",
    "Narathiwat": "นราธิวาส",
    "Nong Bua Lam Phu": "หนองบัวลำภู",
    "Nong Khai": "หนองคาย",
    "Nonthaburi": "นนทบุรี",
    "Pathum Thani": "ปทุมธานี",
    "Pattani": "ปัตตานี",
    "Phang Nga": "พังงา",
    "Phatthalung": "พัทลุง",
    "Phattalung": "พัทลุง",
    "Phayao": "พะเยา",
    "Phetchabun": "เพชรบูรณ์",
    "Phetchaburi": "เพชรบุรี",
    "Phichit": "พิจิตร",
    "Phitsanulok": "พิษณุโลก",
    "Phra Nakhon Si Ayutthaya": "พระนครศรีอยุธยา",
    "Phrae": "แพร่",
    "Phuket": "ภูเก็ต",
    "Prachin Buri": "ปราจีนบุรี",
    "Prachuap Khiri Khan": "ประจวบคีรีขันธ์",
    "Ranong": "ระนอง",
    "Ratchaburi": "ราชบุรี",
    "Rayong": "ระยอง",
    "Roi Et": "ร้อยเอ็ด",
    "Sa Kaeo": "สระแก้ว",
    "Sakon Nakhon": "สกลนคร",
    "Samut Prakan": "สมุทรปราการ",
    "Samut Sakhon": "สมุทรสาคร",
    "Samut Songkhram": "สมุทรสงคราม",
    "Saraburi": "สระบุรี",
    "Satun": "สตูล",
    "Sing Buri": "สิงห์บุรี",
    "Si Sa Ket": "ศรีสะเกษ",
    "Songkhla": "สงขลา",
    "Sukhothai": "สุโขทัย",
    "Suphan Buri": "สุพรรณบุรี",
    "Surat Thani": "สุราษฎร์ธานี",
    "Surin": "สุรินทร์",
    "Tak": "ตาก",
    "Trang": "ตรัง",
    "Trat": "ตราด",
    "Ubon Ratchathani": "อุบลราชธานี",
    "Udon Thani": "อุดรธานี",
    "Uthai Thani": "อุทัยธานี",
    "Uttaradit": "อุตรดิตถ์",
    "Yala": "ยะลา",
    "Yasothon": "ยโสธร",
    "Chai Nat": "ชัยนาท",
    "Chanthaburi": "จันทบุรี",
}


def _strip_th_prefix(name: str) -> str:
    """Strip Thai province prefixes like จังหวัด, จ., จังหวัด (with spaces)."""
    import re
    return re.sub(r'^(จังหวัด\s*|จ\.\s*)', '', name.strip()).strip()


def build_province_lookup(cur) -> dict:
    """Build province name → province_id lookup."""
    cur.execute("SELECT province_id, name FROM tat.provinces")
    lookup = {}
    for pid, name in cur.fetchall():
        lookup[name] = pid
        lookup[name.replace(" ", "")] = pid
    return lookup


def build_province_geo_index(cur) -> list:
    """Build province centroid index from existing tat.places data.
    For each province, compute the median lat/lng of its places.
    Returns: list of (province_id, lat, lng)
    """
    cur.execute("""
        SELECT province_id,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY latitude)  AS med_lat,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY longitude) AS med_lng
        FROM tat.places
        WHERE province_id IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        GROUP BY province_id
        HAVING count(*) >= 3
    """)
    index = []
    for pid, lat, lng in cur.fetchall():
        index.append((pid, float(lat), float(lng)))
    return index


def resolve_province_by_geo(lat: float, lng: float, province_geo: list) -> int | None:
    """Find nearest province by lat/lng using centroid distance.
    Returns province_id or None.
    """
    if lat is None or lng is None or not province_geo:
        return None

    best_pid = None
    best_dist = float('inf')
    for pid, plat, plng in province_geo:
        dist = haversine_m(lat, lng, plat, plng)
        if dist < best_dist:
            best_dist = dist
            best_pid = pid

    # Sanity check: if nearest centroid is >150km away, don't assign
    if best_dist > 150_000:
        return None
    return best_pid


def resolve_province_id(state: str, province_lookup: dict,
                        lat: float = None, lng: float = None,
                        province_geo: list = None) -> int | None:
    """Resolve MongoDB state name to tat.province_id.
    Fallback chain:
      1. Direct Thai name match
      2. Strip จังหวัด/จ. prefix and match
      3. English → Thai name mapping
      4. Nearest province by lat/lng
    """
    if state:
        # 1) Direct Thai match
        if state in province_lookup:
            return province_lookup[state]

        # 2) Strip Thai prefix (จังหวัดพระนครศรีอยุธยา → พระนครศรีอยุธยา)
        stripped = _strip_th_prefix(state)
        if stripped and stripped in province_lookup:
            return province_lookup[stripped]

        # 3) English → Thai
        th_name = PROVINCE_EN_TO_TH.get(state)
        if th_name and th_name in province_lookup:
            return province_lookup[th_name]

    # 4) Fallback: nearest province by lat/lng
    if lat is not None and lng is not None and province_geo:
        return resolve_province_by_geo(lat, lng, province_geo)

    return None


# ── Main ───────────────────────────────────────────────────────

def load_pg_places(cur) -> tuple:
    """Load all existing places for matching.
    Returns:
        gpid_map: {google_place_id: place_id}  (for ChIJ-format entries)
        geo_index: list of (place_id, name, lat, lng)
    """
    cur.execute("""
        SELECT place_id, name, latitude, longitude, google_place_id
        FROM tat.places
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    rows = cur.fetchall()

    gpid_map = {}
    geo_index = []
    for pid, name, lat, lng, gpid in rows:
        if gpid and gpid.startswith("ChIJ"):
            gpid_map[gpid] = pid
        geo_index.append((pid, name, float(lat), float(lng)))

    # Also load all google_place_ids for dedup check
    cur.execute("SELECT google_place_id FROM tat.places WHERE google_place_id IS NOT NULL")
    all_gpids = set(r[0] for r in cur.fetchall())

    return gpid_map, geo_index, all_gpids


def find_match(doc: dict, gpid_map: dict, geo_index: list, all_gpids: set) -> int | None:
    """Try to match a MongoDB doc to an existing tat.place.
    Returns place_id if matched, None otherwise.
    """
    gpid = doc["google_place_id"]

    # Strategy 1: Exact google_place_id match (ChIJ format)
    if gpid and gpid in gpid_map:
        return gpid_map[gpid]

    # Strategy 2: Name + proximity
    if doc["lat"] is None or doc["lng"] is None or not doc["title"]:
        return None

    best_match = None
    best_dist = MATCH_DISTANCE_M

    title_lower = doc["title"].lower().strip()

    for pid, name, plat, plng in geo_index:
        # Quick bounding box filter (approx 0.005 degrees ≈ 500m)
        if abs(plat - doc["lat"]) > 0.005 or abs(plng - doc["lng"]) > 0.005:
            continue

        dist = haversine_m(doc["lat"], doc["lng"], plat, plng)
        if dist < best_dist:
            # Check name similarity (simple: one contains the other, or Thai name match)
            name_lower = name.lower().strip()
            if (name_lower in title_lower or title_lower in name_lower or
                name_lower == title_lower):
                best_dist = dist
                best_match = pid

    return best_match


def update_existing_place(cur, place_id: int, doc: dict, dry_run: bool) -> dict:
    """Update NULL columns of an existing place with MongoDB data.
    Returns dict of what was updated.
    """
    updates = {}

    # Build SET clause for NULL-only updates
    fields = {
        "address": doc["address"],
        "introduction": doc["description"],
        "google_place_id": doc["google_place_id"],
        "google_avg_rating": doc["rating"],
        "google_review_count": doc["review_count"],
        "google_star_5": doc["star_5"],
        "google_star_4": doc["star_4"],
        "google_star_3": doc["star_3"],
        "google_star_2": doc["star_2"],
        "google_star_1": doc["star_1"],
        "phone": doc["phone"],
        "website": doc["website"],
        "open_hours": doc["open_hours"],
    }

    set_clauses = []
    values = []
    for col, val in fields.items():
        if val is not None:
            # Only update if current value is NULL
            set_clauses.append(f'"{col}" = COALESCE("{col}", %s)')
            values.append(val)
            updates[col] = val

    if not set_clauses:
        return updates

    # Also update timestamps
    set_clauses.append('"google_scraped_at" = COALESCE("google_scraped_at", %s)')
    values.append(datetime.now(timezone.utc))
    set_clauses.append('"updated_at" = %s')
    values.append(datetime.now(timezone.utc))

    sql = f'UPDATE tat.places SET {", ".join(set_clauses)} WHERE place_id = %s'
    values.append(place_id)

    if not dry_run:
        cur.execute(sql, values)

    return updates


def insert_new_place(cur, doc: dict, category_id: int, province_lookup: dict,
                     province_geo: list, next_id: int, dry_run: bool) -> int | None:
    """Insert a new place from MongoDB doc. Returns new place_id or None."""
    if not doc["title"]:
        return None
    if doc["lat"] is None or doc["lng"] is None:
        return None

    province_id = resolve_province_id(
        doc["state"], province_lookup,
        lat=doc["lat"], lng=doc["lng"], province_geo=province_geo
    )

    # Extract thumbnail
    thumbnail = doc.get("thumbnail")
    thumbnail_urls = [thumbnail] if thumbnail else []

    now = datetime.now(timezone.utc)

    row = {
        "place_id": next_id,
        "status": "approved",
        "name": doc["title"],
        "introduction": doc["description"],
        "category_id": category_id,
        "latitude": doc["lat"],
        "longitude": doc["lng"],
        "address": doc["address"],
        "province_id": province_id,
        "district_id": None,
        "sub_district_id": None,
        "postcode": doc["postal_code"] or None,
        "thumbnail_urls": thumbnail_urls,
        "tags": [],
        "viewer": 0,
        "slug": doc["title"],
        "migrate_id": f"GS_{doc['collection']}_{doc['google_place_id']}" if doc["google_place_id"] else None,
        "created_at": now,
        "updated_at": now,
        "sha_name": None,
        "sha_detail": None,
        "sha_thumbnail_url": None,
        "sha_detail_pictures": [],
        "sha_type_id": None,
        "sha_category_id": None,
        "google_place_id": doc["google_place_id"] or None,
        "google_avg_rating": doc["rating"],
        "google_review_count": doc["review_count"],
        "google_star_5": doc["star_5"],
        "google_star_4": doc["star_4"],
        "google_star_3": doc["star_3"],
        "google_star_2": doc["star_2"],
        "google_star_1": doc["star_1"],
        "google_scraped_at": now,
        "google_scrape_status": "success_mongo",
        "wongnai_genres": [],
        "wongnai_neighborhoods": [],
        "wongnai_price_range": None,
        "wongnai_url": None,
        "wongnai_scraped_at": None,
        "phone": doc["phone"],
        "website": doc["website"],
        "open_hours": doc["open_hours"],
    }

    cols = list(row.keys())
    vals = [row[c] for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(f'"{c}"' for c in cols)

    sql = f"INSERT INTO tat.places ({col_names}) VALUES ({placeholders})"

    if not dry_run:
        cur.execute(sql, vals)

    return next_id


def main():
    parser = argparse.ArgumentParser(description="Migrate MongoDB google-scrape → tat.places")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--limit", type=int, default=0, help="Limit docs per collection (0=all)")
    parser.add_argument("--collections", nargs="*", help="Only process these collections")
    args = parser.parse_args()

    if args.dry_run:
        log.info("🔍 DRY-RUN MODE — no changes will be written")

    # Connect
    log.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = False
    cur = conn.cursor()

    log.info("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
    db = client[MONGO_DB]

    # Load existing data
    log.info("Loading existing tat.places for matching...")

    # ── Ensure extra columns exist ─────────────────────────────
    log.info("Ensuring phone / website / open_hours columns exist...")
    for ddl in [
        'ALTER TABLE tat.places ADD COLUMN IF NOT EXISTS "phone" text',
        'ALTER TABLE tat.places ADD COLUMN IF NOT EXISTS "website" text',
        'ALTER TABLE tat.places ADD COLUMN IF NOT EXISTS "open_hours" text',
    ]:
        cur.execute(ddl)
    conn.commit()
    log.info("  Columns ready.")

    gpid_map, geo_index, all_gpids = load_pg_places(cur)
    province_lookup = build_province_lookup(cur)
    province_geo = build_province_geo_index(cur)
    log.info(f"  Loaded {len(geo_index)} places, {len(gpid_map)} ChIJ IDs, "
             f"{len(province_lookup)} provinces, {len(province_geo)} province centroids")

    # Get max place_id for new inserts
    cur.execute("SELECT COALESCE(MAX(place_id), 0) FROM tat.places")
    next_id = cur.fetchone()[0] + 1

    # Track inserted google_place_ids to avoid duplicates within this run
    seen_gpids = set(all_gpids)

    # Stats
    stats = {
        "matched_gpid": 0,
        "matched_geo": 0,
        "updated": 0,
        "inserted": 0,
        "skipped_dup": 0,
        "skipped_no_latlng": 0,
        "skipped_no_name": 0,
        "no_province": 0,
        "total_processed": 0,
    }

    collections_to_process = args.collections or sorted(COLLECTION_CATEGORY_MAP.keys())

    for coll_name in collections_to_process:
        if coll_name not in COLLECTION_CATEGORY_MAP:
            log.warning(f"Unknown collection: {coll_name}, skipping")
            continue

        category_id = COLLECTION_CATEGORY_MAP[coll_name]
        coll = db[coll_name]
        total = coll.count_documents({})

        # ── Deduplicate within collection ──────────────────────────
        # Many collections have duplicate place_ids (e.g. hotel ~50%).
        # Use MongoDB aggregation to pick one doc per place_id (server-side).
        log.info(f"\n{'='*60}")
        log.info(f"Dedup & loading: {coll_name} ({total} raw docs)")

        # Aggregation: sort by field count desc, group by place_id, pick first
        pipeline = [
            {"$match": {"place_id": {"$exists": True, "$ne": None, "$ne": ""}}},
            # Score: docs with more fields = better quality
            {"$addFields": {"_field_count": {"$size": {"$objectToArray": "$$ROOT"}}}},
            {"$sort": {"_field_count": -1}},
            {"$group": {"_id": "$place_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
        ]
        if args.limit > 0:
            pipeline.append({"$limit": args.limit})

        deduped_docs = list(coll.aggregate(pipeline, allowDiskUse=True))
        # Also include docs with empty place_id (no dedup possible)
        empty_pid_docs = list(coll.find({"$or": [
            {"place_id": {"$exists": False}}, {"place_id": None}, {"place_id": ""}
        ]}))
        deduped_docs += empty_pid_docs

        log.info(f"  {total} raw → {len(deduped_docs)} deduped docs"
                 f" (removed ~{total - len(deduped_docs)} duplicates)")
        log.info(f"Processing: {coll_name} ({len(deduped_docs)} docs, category={category_id})")
        log.info(f"{'='*60}")

        coll_stats = {"matched": 0, "updated": 0, "inserted": 0, "skipped": 0}

        batch_count = 0

        for doc in deduped_docs:
            stats["total_processed"] += 1
            batch_count += 1

            ndoc = normalize_doc(doc, coll_name)

            # Skip if no title
            if not ndoc["title"]:
                stats["skipped_no_name"] += 1
                coll_stats["skipped"] += 1
                continue

            # Skip if no lat/lng
            if ndoc["lat"] is None or ndoc["lng"] is None:
                stats["skipped_no_latlng"] += 1
                coll_stats["skipped"] += 1
                continue

            # Skip if google_place_id already seen (dedup)
            if ndoc["google_place_id"] and ndoc["google_place_id"] in seen_gpids:
                # But still try to match and update
                match_pid = find_match(ndoc, gpid_map, geo_index, all_gpids)
                if match_pid:
                    updated = update_existing_place(cur, match_pid, ndoc, args.dry_run)
                    if updated:
                        stats["updated"] += 1
                        coll_stats["updated"] += 1
                else:
                    stats["skipped_dup"] += 1
                    coll_stats["skipped"] += 1
                continue

            # Try matching
            match_pid = find_match(ndoc, gpid_map, geo_index, all_gpids)

            if match_pid:
                # Update existing place
                if ndoc["google_place_id"] and ndoc["google_place_id"].startswith("ChIJ"):
                    stats["matched_gpid"] += 1
                else:
                    stats["matched_geo"] += 1
                coll_stats["matched"] += 1

                updated = update_existing_place(cur, match_pid, ndoc, args.dry_run)
                if updated:
                    stats["updated"] += 1
                    coll_stats["updated"] += 1
            else:
                # Insert new place
                province_id = resolve_province_id(
                    ndoc["state"], province_lookup,
                    lat=ndoc["lat"], lng=ndoc["lng"], province_geo=province_geo
                )
                if province_id is None:
                    stats["no_province"] += 1

                new_pid = insert_new_place(cur, ndoc, category_id, province_lookup,
                                           province_geo, next_id, args.dry_run)
                if new_pid:
                    stats["inserted"] += 1
                    coll_stats["inserted"] += 1
                    # Add to geo_index for future matching within this run
                    geo_index.append((next_id, ndoc["title"], ndoc["lat"], ndoc["lng"]))
                    if ndoc["google_place_id"]:
                        seen_gpids.add(ndoc["google_place_id"])
                        if ndoc["google_place_id"].startswith("ChIJ"):
                            gpid_map[ndoc["google_place_id"]] = next_id
                    next_id += 1

            if batch_count % 1000 == 0:
                log.info(f"  ... {batch_count}/{len(deduped_docs)} processed "
                         f"(matched={coll_stats['matched']} updated={coll_stats['updated']} "
                         f"inserted={coll_stats['inserted']} skipped={coll_stats['skipped']})")

        log.info(f"  {coll_name} done: matched={coll_stats['matched']} "
                 f"updated={coll_stats['updated']} inserted={coll_stats['inserted']} "
                 f"skipped={coll_stats['skipped']}")

    # Summary
    log.info(f"\n{'='*60}")
    log.info("FINAL SUMMARY")
    log.info(f"{'='*60}")
    for k, v in stats.items():
        log.info(f"  {k:25s}: {v}")

    if args.dry_run:
        log.info("\n🔍 DRY-RUN — rolling back all changes")
        conn.rollback()
    else:
        log.info("\n💾 Committing changes...")
        conn.commit()
        log.info("✅ Done!")

    cur.close()
    conn.close()
    client.close()


if __name__ == "__main__":
    main()
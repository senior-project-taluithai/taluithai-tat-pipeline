#!/usr/bin/env python3
"""
MongoDB → Qdrant Embedding Pipeline

Reads all collections from the 'google-scrape' MongoDB database,
embeds each document using sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2,
and upserts the vectors + metadata into a Qdrant collection.

Usage:
    # Copy .env.example to .env and adjust values, then:
    python embed_to_qdrant.py

    # Or with explicit env vars:
    MONGODB_URI=mongodb://... QDRANT_HOST=localhost python embed_to_qdrant.py
"""

import os
import sys
import time
import hashlib
import logging
from typing import Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
)
from sentence_transformers import SentenceTransformer

# ── Setup logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Load config ────────────────────────────────────────────────
load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "google-scrape")
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "google_scrape_places")
EMBEDDING_MODEL = os.getenv(
    "EMBEDDING_MODEL",
    "BAAI/bge-m3",
)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))


# ── Helpers ────────────────────────────────────────────────────

def make_point_id(place_id: str, collection_name: str) -> int:
    """
    Generate a deterministic integer ID for a Qdrant point
    from the place_id + collection_name.
    Uses a 63-bit hash so it fits in a signed int64.
    """
    raw = f"{collection_name}:{place_id}"
    digest = hashlib.sha256(raw.encode()).hexdigest()
    return int(digest[:15], 16)  # 60-bit int, safe for Qdrant


def normalize_doc(doc: dict) -> dict:
    """
    Normalize field name differences between collection schema variants.
    Variant A (attraction, hotel):  descriptions, longitude, website
    Variant B (others):             description, longtitude, web_site, categories
    """
    return {
        "title": doc.get("title", ""),
        "category": doc.get("category", ""),
        "categories": doc.get("categories", ""),
        "address": doc.get("address", ""),
        "description": doc.get("descriptions") or doc.get("description") or "",
        "latitude": doc.get("latitude", ""),
        "longitude": doc.get("longitude") or doc.get("longtitude") or "",
        "review_count": doc.get("review_count", ""),
        "review_rating": doc.get("review_rating", ""),
        "website": doc.get("website") or doc.get("web_site") or "",
        "phone": doc.get("phone", ""),
        "place_id": doc.get("place_id", ""),
        "cid": doc.get("cid", ""),
        "thumbnail": doc.get("thumbnail", ""),
        "price_range": doc.get("price_range", ""),
        "open_hours": doc.get("open_hours", ""),
        "link": doc.get("link", ""),
    }


def build_embedding_text(doc: dict) -> str:
    """
    Build the text string that will be embedded.
    Strategy: title | category | address | description
    """
    parts = []
    if doc["title"]:
        parts.append(doc["title"])
    if doc["category"]:
        parts.append(doc["category"])
    if doc["address"]:
        parts.append(doc["address"])
    if doc["description"]:
        parts.append(doc["description"])
    return " | ".join(parts)


def safe_float(val, default: float = 0.0) -> float:
    """Safely parse a float value."""
    try:
        return float(val) if val else default
    except (ValueError, TypeError):
        return default


# ── Main pipeline ──────────────────────────────────────────────

def create_qdrant_collection(client: QdrantClient, vector_dim: int) -> None:
    """Create or recreate the Qdrant collection if dimension changed."""
    collections = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION in collections:
        info = client.get_collection(QDRANT_COLLECTION)
        existing_dim = info.config.params.vectors.size
        if existing_dim == vector_dim:
            log.info(f"Collection '{QDRANT_COLLECTION}' exists (dim={existing_dim}), reusing it.")
            return
        log.warning(
            f"Collection dim mismatch: existing={existing_dim}, needed={vector_dim}. Recreating..."
        )
        client.delete_collection(QDRANT_COLLECTION)

    log.info(f"Creating collection '{QDRANT_COLLECTION}' (dim={vector_dim}, cosine)...")
    client.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
    )
    log.info("✓ Collection created.")


def get_existing_point_ids(client: QdrantClient, collection_name: str) -> set:
    """
    Get set of point IDs already in Qdrant for a given MongoDB collection.
    Used for resumability — skip already-embedded documents.
    """
    existing_ids = set()
    offset = None
    while True:
        result = client.scroll(
            collection_name=QDRANT_COLLECTION,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="source_collection",
                        match=MatchValue(value=collection_name),
                    )
                ]
            ),
            limit=1000,
            offset=offset,
            with_payload=False,
            with_vectors=False,
        )
        points, next_offset = result
        for p in points:
            existing_ids.add(p.id)
        if next_offset is None:
            break
        offset = next_offset

    return existing_ids


def process_collection(
    mongo_db,
    qdrant_client: QdrantClient,
    model: SentenceTransformer,
    collection_name: str,
    skip_existing: bool = True,
) -> dict:
    """Process a single MongoDB collection: embed and upsert to Qdrant."""
    collection = mongo_db[collection_name]
    total_docs = collection.count_documents({})
    log.info(f"  Collection '{collection_name}': {total_docs} documents")

    if total_docs == 0:
        return {"collection": collection_name, "total": 0, "embedded": 0, "skipped": 0}

    # Get existing IDs for resumability
    existing_ids = set()
    if skip_existing:
        log.info(f"  Checking for already-embedded documents...")
        existing_ids = get_existing_point_ids(qdrant_client, collection_name)
        if existing_ids:
            log.info(f"  Found {len(existing_ids)} already-embedded, will skip them.")

    # Process in batches
    batch_texts = []
    batch_points_meta = []
    embedded_count = 0
    skipped_count = 0

    cursor = collection.find({})
    for i, doc in enumerate(cursor):
        normalized = normalize_doc(doc)
        place_id = normalized["place_id"] or str(doc.get("_id", ""))
        point_id = make_point_id(place_id, collection_name)

        # Skip if already embedded
        if point_id in existing_ids:
            skipped_count += 1
            continue

        text = build_embedding_text(normalized)
        if not text.strip():
            skipped_count += 1
            continue

        batch_texts.append(text)
        batch_points_meta.append({
            "point_id": point_id,
            "payload": {
                "title": normalized["title"],
                "category": normalized["category"],
                "categories": normalized["categories"],
                "address": normalized["address"],
                "description": normalized["description"],
                "latitude": safe_float(normalized["latitude"]),
                "longitude": safe_float(normalized["longitude"]),
                "review_count": normalized["review_count"],
                "review_rating": safe_float(normalized["review_rating"]),
                "website": normalized["website"],
                "phone": normalized["phone"],
                "place_id": place_id,
                "cid": normalized["cid"],
                "thumbnail": normalized["thumbnail"],
                "price_range": normalized["price_range"],
                "open_hours": normalized["open_hours"],
                "link": normalized["link"],
                "source_collection": collection_name,
            },
        })

        # When batch is full, encode and upsert
        if len(batch_texts) >= BATCH_SIZE:
            embeddings = model.encode(batch_texts, show_progress_bar=False)
            points = [
                PointStruct(
                    id=meta["point_id"],
                    vector=emb.tolist(),
                    payload=meta["payload"],
                )
                for emb, meta in zip(embeddings, batch_points_meta)
            ]
            qdrant_client.upsert(collection_name=QDRANT_COLLECTION, points=points)
            embedded_count += len(points)
            log.info(
                f"    [{collection_name}] {embedded_count + skipped_count}/{total_docs} "
                f"(embedded: {embedded_count}, skipped: {skipped_count})"
            )
            batch_texts = []
            batch_points_meta = []

    # Final batch
    if batch_texts:
        embeddings = model.encode(batch_texts, show_progress_bar=False)
        points = [
            PointStruct(
                id=meta["point_id"],
                vector=emb.tolist(),
                payload=meta["payload"],
            )
            for emb, meta in zip(embeddings, batch_points_meta)
        ]
        qdrant_client.upsert(collection_name=QDRANT_COLLECTION, points=points)
        embedded_count += len(points)

    log.info(
        f"  ✓ '{collection_name}' done — "
        f"embedded: {embedded_count}, skipped: {skipped_count}"
    )
    return {
        "collection": collection_name,
        "total": total_docs,
        "embedded": embedded_count,
        "skipped": skipped_count,
    }


def main():
    start_time = time.time()

    log.info("=" * 60)
    log.info("MongoDB → Qdrant Embedding Pipeline")
    log.info("=" * 60)

    # ── Connect to MongoDB ─────────────────────────────────
    log.info(f"Connecting to MongoDB ({MONGODB_DATABASE})...")
    mongo_client = MongoClient(MONGODB_URI)
    mongo_db = mongo_client[MONGODB_DATABASE]

    # List all collections
    collection_names = mongo_db.list_collection_names()
    log.info(f"Found {len(collection_names)} collections: {collection_names}")

    if not collection_names:
        log.error("No collections found in database. Exiting.")
        sys.exit(1)

    # ── Connect to Qdrant ──────────────────────────────────
    log.info(f"Connecting to Qdrant ({QDRANT_HOST}:{QDRANT_PORT})...")
    qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

    # Verify connection
    try:
        qdrant_client.get_collections()
        log.info("✓ Qdrant connected.")
    except Exception as e:
        log.error(f"✗ Cannot connect to Qdrant: {e}")
        log.error("Make sure Qdrant is running. Run setup_qdrant.sh first.")
        sys.exit(1)

    # ── Load embedding model ───────────────────────────────
    log.info(f"Loading embedding model: {EMBEDDING_MODEL}")
    model = SentenceTransformer(EMBEDDING_MODEL)
    vector_dim = model.get_sentence_embedding_dimension()
    log.info(f"✓ Model loaded. Vector dimension: {vector_dim}")

    # ── Create collection ──────────────────────────────────
    create_qdrant_collection(qdrant_client, vector_dim)

    # ── Process all collections ────────────────────────────
    results = []
    for col_name in sorted(collection_names):
        result = process_collection(
            mongo_db, qdrant_client, model, col_name, skip_existing=True
        )
        results.append(result)

    # ── Summary ────────────────────────────────────────────
    elapsed = time.time() - start_time
    total_embedded = sum(r["embedded"] for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    total_docs = sum(r["total"] for r in results)

    log.info("")
    log.info("=" * 60)
    log.info("Pipeline Complete!")
    log.info("=" * 60)
    log.info(f"Total documents in MongoDB:  {total_docs}")
    log.info(f"Newly embedded:              {total_embedded}")
    log.info(f"Skipped (already embedded):  {total_skipped}")
    log.info(f"Time elapsed:                {elapsed:.1f}s")
    log.info("")

    # Verify Qdrant collection count
    info = qdrant_client.get_collection(QDRANT_COLLECTION)
    log.info(f"Qdrant collection '{QDRANT_COLLECTION}' now has {info.points_count} points")
    log.info("=" * 60)

    # Cleanup
    mongo_client.close()


if __name__ == "__main__":
    main()

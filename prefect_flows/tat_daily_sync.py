#!/usr/bin/env python3
"""
TAT Daily Sync — Prefect Flow
==============================
Fetches **new/updated** events from TAT Data API daily and upserts into remote PostgreSQL.

Usage (local test):
    python prefect_flows/tat_daily_sync.py
"""

import os
import time
from datetime import datetime

import psycopg2
import requests
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
from psycopg2.extras import execute_values

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# TAT API
API_BASE = "https://tatdataapi.io/api/v2/events"
API_KEY = os.environ.get("TAT_API_KEY")
PAGE_SIZE = 1000

# Remote PostgreSQL (cloud DB) — all values from env, no hardcoded defaults
DB_CONFIG = {
    "host": os.environ["MIGRATE_PG_HOST"],
    "port": int(os.environ.get("MIGRATE_PG_PORT", "5432")),
    "database": os.environ.get("MIGRATE_PG_DB", "taluithai"),
    "user": os.environ["MIGRATE_PG_USER"],
    "password": os.environ["MIGRATE_PG_PASSWORD"],
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ── Tasks ────────────────────────────────────────────────────


@task(name="get-latest-event-timestamp")
def get_latest_event_timestamp() -> str | None:
    """Get the most recent updated_at from DB so we only sync newer events."""
    logger = get_run_logger()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT MAX(updated_at) FROM tat.events")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()

    if result:
        logger.info("Latest event in DB: %s", result.isoformat())
    else:
        logger.info("No existing events — will fetch all")
    return result.isoformat() if result else None


@task(retries=3, retry_delay_seconds=60, name="fetch-tat-events")
def fetch_events(lang: str = "th") -> list:
    """Fetch all events from TAT API with pagination."""
    logger = get_run_logger()
    all_events: list = []
    page = 1

    headers = {
        "accept": "application/json",
        "Accept-Language": lang,
        "x-api-key": API_KEY,
    }

    resp = requests.get(
        API_BASE,
        headers=headers,
        params={"limit": PAGE_SIZE, "page": page},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    total = data["pagination"]["total"]
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    all_events.extend(data["data"])
    logger.info(
        "Page %d/%d — %d events (lang=%s)", page, total_pages, len(data["data"]), lang
    )

    for page in range(2, total_pages + 1):
        time.sleep(0.5)  # rate-limit
        resp = requests.get(
            API_BASE,
            headers=headers,
            params={"limit": PAGE_SIZE, "page": page},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        all_events.extend(data["data"])
        logger.info("Page %d/%d — %d events", page, total_pages, len(data["data"]))

    logger.info("Fetched %d events total (lang=%s)", len(all_events), lang)
    return all_events


@task(retries=2, retry_delay_seconds=30, name="upsert-events-th")
def upsert_events_th(events: list) -> int:
    """Upsert Thai events into PostgreSQL."""
    logger = get_run_logger()
    if not events:
        logger.info("No new Thai events to upsert")
        return 0
    conn = get_connection()
    cur = conn.cursor()

    insert_data = []
    for e in events:
        province_id = None
        if e.get("location") and e["location"].get("province"):
            province_id = e["location"]["province"].get("provinceId")

        insert_data.append(
            (
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
            )
        )

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
    cur.close()
    conn.close()
    logger.info("Upserted %d Thai events", len(insert_data))
    return len(insert_data)


@task(retries=2, retry_delay_seconds=30, name="upsert-events-en")
def upsert_events_en(events: list) -> int:
    """Update English fields for existing events."""
    logger = get_run_logger()
    if not events:
        logger.info("No new English events to update")
        return 0
    conn = get_connection()
    cur = conn.cursor()

    update_data = [
        (e.get("name"), e.get("introduction"), e["eventId"]) for e in events
    ]

    sql = """
        UPDATE tat.events
        SET name_en = %s, introduction_en = %s
        WHERE event_id = %s
    """
    cur.executemany(sql, update_data)
    conn.commit()
    updated = cur.rowcount
    cur.close()
    conn.close()
    logger.info("Updated %d English event translations", updated)
    return updated


# ── Flow ─────────────────────────────────────────────────────


def _parse_iso(ts: str | None) -> datetime | None:
    """Parse ISO timestamp, tolerating Z suffix."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


@task(name="filter-new-events")
def filter_new_events(events: list, since: str | None) -> list:
    """Keep only events created/updated after *since* that have lat/lon."""
    logger = get_run_logger()

    # Drop events without coordinates
    with_coords = [e for e in events if e.get("latitude") and e.get("longitude")]
    dropped = len(events) - len(with_coords)
    if dropped:
        logger.info("Dropped %d events without lat/lon", dropped)

    if not since:
        return with_coords  # first run → take everything with coords

    since_dt = _parse_iso(since)
    if since_dt is None:
        return with_coords

    new_events = []
    for e in with_coords:
        event_ts = _parse_iso(e.get("updatedAt") or e.get("createdAt"))
        if event_ts is None or event_ts > since_dt:
            new_events.append(e)

    logger.info(
        "Filtered %d → %d new/updated events (since %s)",
        len(with_coords), len(new_events), since,
    )
    return new_events


@flow(name="tat-daily-event-sync", log_prints=True)
def tat_daily_event_sync():
    """Daily sync: fetch only NEW/UPDATED TAT events and upsert into cloud PostgreSQL."""
    # 1. Find out what we already have
    latest_ts = get_latest_event_timestamp()

    # 2. Fetch & filter Thai events
    all_events_th = fetch_events(lang="th")
    new_events_th = filter_new_events(all_events_th, latest_ts)
    th_count = upsert_events_th(new_events_th)

    # 3. Fetch & filter English translations
    all_events_en = fetch_events(lang="en")
    new_events_en = filter_new_events(all_events_en, latest_ts)
    en_count = upsert_events_en(new_events_en)

    # Summary
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM tat.events")
    total = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM tat.events WHERE name_en IS NOT NULL")
    with_en = cur.fetchone()[0]
    cur.close()
    conn.close()

    print(f"✅ Sync complete — TH upserted: {th_count}, EN updated: {en_count}")
    print(f"📊 Total events: {total}, With English: {with_en}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        # Long-running mode: serve the flow on a cron schedule
        tat_daily_event_sync.serve(
            name="tat-daily-event-sync",
            cron="0 2 * * *",  # every day at 02:00 Asia/Bangkok
            tags=["tat", "events", "production"],
            description="Daily sync TAT events (TH+EN) into cloud PostgreSQL",
        )
    else:
        # One-shot: run immediately
        tat_daily_event_sync()

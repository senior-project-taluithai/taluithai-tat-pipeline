#!/usr/bin/env python
"""
Parallel Wongnai Scraper Runner (Staggered)
=============================================
Launches multiple workers with staggered start times
to avoid rate limiting while maintaining parallelism.

Usage:
    python run_parallel_wongnai.py --workers 3 --delay-min 8 --delay-max 15
"""

import argparse
import subprocess
import sys
import os
import time

# Adjust path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from tasks.wongnai.loader import get_connection


def get_remaining_count():
    """Get count of places without Wongnai data"""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tat.places WHERE wongnai_genres IS NULL")
            return cur.fetchone()[0]
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Run multiple Wongnai scraper workers in parallel")
    parser.add_argument("--workers", type=int, default=3, help="Number of workers to run")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size for each worker")
    parser.add_argument("--delay-min", type=float, default=8.0, help="Min delay between requests (seconds)")
    parser.add_argument("--delay-max", type=float, default=15.0, help="Max delay between requests (seconds)")
    parser.add_argument("--stagger", type=float, default=5.0, help="Seconds between starting each worker")
    
    args = parser.parse_args()
    
    # 1. Get remaining places count
    remaining = get_remaining_count()
    print(f"📊 Remaining places to scrape: {remaining:,}")
    
    if remaining == 0:
        print("✅ All places already have Wongnai data!")
        return
    
    # Calculate effective rate
    avg_delay = (args.delay_min + args.delay_max) / 2
    effective_rate = 60 / avg_delay * args.workers
    estimated_hours = remaining / effective_rate / 60
    
    print(f"\n⚙️  Configuration:")
    print(f"   Workers: {args.workers}")
    print(f"   Delay per request: {args.delay_min}-{args.delay_max}s")
    print(f"   Stagger between workers: {args.stagger}s")
    print(f"   Estimated rate: ~{effective_rate:.1f} places/min")
    print(f"   Estimated time: ~{estimated_hours:.1f} hours")
    
    print(f"\n🚀 Launching {args.workers} workers (staggered)...")
    
    for i in range(args.workers):
        worker_id = i + 1
        
        cmd = [
            sys.executable,
            os.path.join(BASE_DIR, "run_worker_wongnai_scrape.py"),
            "--worker-id", str(worker_id),
            "--batch-size", str(args.batch_size),
            "--delay-min", str(args.delay_min),
            "--delay-max", str(args.delay_max),
        ]
        
        print(f"   ► Worker {worker_id} starting...")
        
        # Open new Terminal tab on macOS
        if sys.platform == "darwin":
            cmd_str = " ".join(cmd)
            osa_cmd = [
                "osascript",
                "-e",
                f'tell application "Terminal" to do script "{cmd_str}"'
            ]
            subprocess.Popen(osa_cmd)
        else:
            subprocess.Popen(cmd)
        
        # Stagger worker starts to spread out API requests
        if i < args.workers - 1:
            print(f"      (waiting {args.stagger}s before next worker...)")
            time.sleep(args.stagger)
    
    print(f"\n✅ All {args.workers} workers launched!")
    print(f"   Workers are staggered to spread API load.")
    print(f"   Rate-limited requests will be retried later.")


if __name__ == "__main__":
    main()

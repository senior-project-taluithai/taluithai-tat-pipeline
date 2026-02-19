#!/usr/bin/env python
"""
Parallel Google Maps Re-scrape Launcher
========================================
Launches multiple run_google_rescrape.py workers in parallel.

Usage:
    python run_parallel_google_rescrape.py --workers 3 --batch-size 10
"""

import argparse
import subprocess
import sys
import os
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKER_SCRIPT = os.path.join(BASE_DIR, "run_google_rescrape.py")


def main():
    parser = argparse.ArgumentParser(description="Launch parallel Google re-scrape workers")
    parser.add_argument("--workers", type=int, default=3, help="Number of workers")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size per worker")
    parser.add_argument("--delay-min", type=float, default=3.0)
    parser.add_argument("--delay-max", type=float, default=6.0)
    args = parser.parse_args()

    print("=" * 60)
    print(f"Launching {args.workers} Google Maps re-scrape workers")
    print("=" * 60)

    for i in range(1, args.workers + 1):
        cmd = [
            sys.executable,
            WORKER_SCRIPT,
            "--worker-id", str(i),
            "--batch-size", str(args.batch_size),
            "--delay-min", str(args.delay_min),
            "--delay-max", str(args.delay_max),
        ]

        cmd_str = " ".join(cmd)

        if sys.platform == "darwin":
            osa_cmd = [
                "osascript", "-e",
                f'tell application "Terminal" to do script "{cmd_str}"'
            ]
            subprocess.Popen(osa_cmd)
        else:
            log_file = os.path.join(BASE_DIR, f"worker_{i}.log")
            with open(log_file, "w") as lf:
                subprocess.Popen(cmd, stdout=lf, stderr=lf)

        print(f"   Worker {i} launched")
        time.sleep(2)  # Stagger start

    print(f"\n{args.workers} workers launched!")
    print("Check each Terminal tab for progress.")
    print("\nTo monitor remaining count:")
    print('  docker exec taluithai-postgres psql -U postgres -d taluithai -c '
          '"SELECT google_scrape_status, COUNT(*) FROM tat.places '
          "WHERE google_scrape_status IN ('not_found','error','scraping') "
          'GROUP BY google_scrape_status;"')


if __name__ == "__main__":
    main()

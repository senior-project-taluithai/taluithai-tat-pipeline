#!/usr/bin/env python
"""
Parallel Scraper Runner
=======================
Splits work across multiple workers and runs them in parallel.

Usage:
    python places/run_parallel_scrapers.py --workers 3
"""

import argparse
import subprocess
import sys
import os
import math
import json

# Adjust path to find modules
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(BASE_DIR))

# Path to province file
PROVINCE_FILE = os.path.join(BASE_DIR, "raw_tat_province", "province.json")

def load_provinces():
    """Load province IDs from JSON file"""
    with open(PROVINCE_FILE, 'r', encoding='utf-8') as f:
        provinces = json.load(f)
    return [p['id'] for p in provinces]

def chunk_list(lst, n):
    """Yield n successive chunks from lst."""
    # Simple chunking attempts to be even
    k, m = divmod(len(lst), n)
    return (lst[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

def main():
    parser = argparse.ArgumentParser(description="Run multiple scraper workers in parallel")
    parser.add_argument("--workers", type=int, default=3, help="Number of workers to run")
    parser.add_argument("--batch-size", type=int, default=10, help="Batch size for each worker")
    
    args = parser.parse_args()
    
    # 1. Get all provinces
    try:
        all_province_ids = load_provinces()
        print(f"📊 Found {len(all_province_ids)} provinces.")
    except Exception as e:
        print(f"❌ Error loading provinces: {e}")
        return

    # 2. Split into chunks
    chunks = list(chunk_list(all_province_ids, args.workers))
    
    print(f"🚀 Launching {args.workers} workers...")
    
    processes = []
    
    for i, province_chunk in enumerate(chunks):
        worker_id = i + 1
        
        if not province_chunk:
            print(f"⚠️ Worker {worker_id} has no provinces assignments, skipping.")
            continue
            
        provinces_str = ",".join(map(str, province_chunk))
        
        cmd = [
            sys.executable,
            os.path.join(BASE_DIR, "run_worker_google_scrape.py"),
            "--worker-id", str(worker_id),
            "--provinces", provinces_str,
            "--batch-size", str(args.batch_size)
        ]
        
        print(f"   ► Worker {worker_id}: {len(province_chunk)} provinces (IDs: {province_chunk[:3]}...)")
        
        # Use Popen to run in background
        # We can open new terminal windows on macOS specifically for better visualization
        if sys.platform == "darwin":
            # AppleScript to open new terminal tab/window
            # Construct the command string safely
            cmd_str = " ".join(cmd)
            osa_cmd = [
                "osascript",
                "-e",
                f'tell application "Terminal" to do script "{cmd_str}"'
            ]
            subprocess.Popen(osa_cmd)
        else:
            # Fallback for other OS: just run in background and pipe output to separate log files?
            # For simplicity in this environment, let's just use simple Popen
            # But usually user wants to see output. 
            # Since we are on Mac ("The USER's OS version is mac."), the AppleScript method is best.
            p = subprocess.Popen(cmd)
            processes.append(p)
            
    print(f"\n✅ All {len(chunks)} workers launched via Terminal tabs!")

if __name__ == "__main__":
    main()

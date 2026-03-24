#!/usr/bin/env python3
"""
Deploy TikTok Trends Sync to Prefect Cloud
==========================================
Creates a deployment in Prefect Cloud.

Prerequisites:
    1. prefect cloud login
    2. Have a work pool created in Prefect Cloud

Usage:
    python prefect_flows/deploy_tiktok_cloud.py
"""

import os
import sys

from dotenv import load_dotenv

PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_PATH)
load_dotenv(os.path.join(PROJECT_PATH, ".env"))

from tiktok_trends_sync import tiktok_trends_pipeline


def deploy_to_prefect_cloud():
    tiktok_trends_pipeline.deploy(
        name="tiktok-trends-sync",
        work_pool_name="default-agent-pool",  # Change to your work pool name in Prefect Cloud
        cron="0 2,14 * * *",  # Twice daily: 02:00 and 14:00
        tags=["tiktok", "trends", "production"],
        description="Scrapes TikTok for trending tourist places twice daily",
        parameters={"max_videos": 40},
    )
    print("✅ Deployment created/updated in Prefect Cloud")


if __name__ == "__main__":
    deploy_to_prefect_cloud()

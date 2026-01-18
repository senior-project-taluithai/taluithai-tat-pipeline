"""
Google Maps Review Scraper
==========================
ใช้ Playwright ดึงข้อมูลรีวิวจาก Google Maps สำหรับ TAT places

Features:
- Uses Thai locale (th-TH) for aria-label parsing
- Extracts star distribution (1-5 stars)
- Calculates weighted average rating
- Anti-bot measures with random delays
- Batch processing with progress tracking
- Supports Google login session persistence

Reference: User-provided code for aria-label based scraping
"""

import os
import re
import time
import random
import logging
from typing import Dict, Optional, List, Tuple
from datetime import datetime
from dataclasses import dataclass

from playwright.sync_api import sync_playwright, Page, Browser

logger = logging.getLogger(__name__)

# Session file path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SESSION_FILE = os.path.join(BASE_DIR, "google_session.json")

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


@dataclass
class GoogleReviewData:
    """Data class for Google Maps review information"""
    place_id: int
    google_place_id: Optional[str] = None
    google_avg_rating: Optional[float] = None
    google_review_count: Optional[int] = None
    google_star_1: int = 0
    google_star_2: int = 0
    google_star_3: int = 0
    google_star_4: int = 0
    google_star_5: int = 0
    status: str = "pending"  # pending, success, not_found, error, partial
    error_message: Optional[str] = None
    scraped_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        return {
            "place_id": self.place_id,
            "google_place_id": self.google_place_id,
            "google_avg_rating": self.google_avg_rating,
            "google_review_count": self.google_review_count,
            "google_star_1": self.google_star_1,
            "google_star_2": self.google_star_2,
            "google_star_3": self.google_star_3,
            "google_star_4": self.google_star_4,
            "google_star_5": self.google_star_5,
            "status": self.status,
            "error_message": self.error_message,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
        }


def get_google_maps_reviews(
    place_name: str,
    province: str,
    district: str = "",
    sub_district: str = "",
    place_id: int = None,
    headless: bool = True,
    timeout: int = 30000,
) -> GoogleReviewData:
    """
    ดึงข้อมูล Google Maps reviews สำหรับสถานที่
    """
    result = GoogleReviewData(
        place_id=place_id,
        scraped_at=datetime.now()
    )

    # Build search query
    search_parts = [place_name]
    if sub_district:
        search_parts.append(sub_district)
    if district:
        search_parts.append(district)
    search_parts.append(province)
    search_query = " ".join(search_parts)

    logger.info(f"🔍 Searching Google Maps: {search_query}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ]
            )

            context_options = {
                'locale': 'th-TH',
                'timezone_id': 'Asia/Bangkok',
                'user_agent': random.choice(USER_AGENTS),
                'viewport': {'width': 1280, 'height': 800},
                'extra_http_headers': {'Accept-Language': 'th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7'}
            }
            
            if os.path.exists(SESSION_FILE):
                context_options['storage_state'] = SESSION_FILE
                logger.debug("   🔐 Using saved Google session")
            
            context = browser.new_context(**context_options)
            page = context.new_page()

            # Search
            search_url = f"https://www.google.com/maps/search/{search_query}"
            page.goto(search_url, wait_until='domcontentloaded', timeout=60000)
            time.sleep(3 + random.uniform(0, 2))

            # Handle search results
            try:
                page.wait_for_selector('div[role="feed"]', timeout=5000)
                # logger.info("   📋 Found search results, clicking first result...")
                first_result = page.locator('div[role="feed"] > div > div a').first
                first_result.click()
                time.sleep(3 + random.uniform(0, 1))
            except Exception:
                pass
            
            # Extract Google Place ID
            try:
                url = page.url
                if "/place/" in url:
                    match = re.search(r'!1s(0x[0-9a-f]+:0x[0-9a-f]+)', url)
                    if match:
                        result.google_place_id = match.group(1)
            except:
                pass

            # Click Reviews Tab or Rating area to load star distribution
            review_opened = False
            
            # Method 1: Click on the rating/review count area (e.g., "4.2 ★★★★☆ (29)")
            rating_click_selectors = [
                # Click on review count like "(29)"
                'button[aria-label*="รีวิว"]',
                'span[aria-label*="รีวิว"]',
                # Click rating text
                '[class*="F7nice"]',
                'div[class*="fontBodyMedium"] span[aria-hidden="true"]',
                # Click star icons area
                'span[role="img"][aria-label*="ดาว"]',
                'span[role="img"][aria-label*="star"]',
            ]
            
            for selector in rating_click_selectors:
                try:
                    elem = page.locator(selector).first
                    if elem.is_visible(timeout=1500):
                        elem.click()
                        logger.debug(f"   Clicked rating area via: {selector}")
                        review_opened = True
                        time.sleep(2 + random.uniform(0.5, 1))
                        break
                except Exception:
                    continue
            
            # Method 2: Click Reviews Tab directly
            if not review_opened:
                review_tab_selectors = [
                    'button[aria-label*="รีวิว"]',
                    'button[data-tab-index="1"]',
                    '[role="tab"][aria-label*="รีวิว"]',
                    '[role="tab"][aria-label*="Review"]',
                ]

                for selector in review_tab_selectors:
                    try:
                        tab = page.locator(selector).first
                        if tab.is_visible(timeout=2000):
                            tab.click()
                            review_opened = True
                            time.sleep(2 + random.uniform(0, 0.5))
                            break
                    except Exception:
                        continue

            # Wait longer for star distribution to load
            time.sleep(2 + random.uniform(0.5, 1))
            
            # Scroll down a bit to ensure bars are visible
            try:
                page.mouse.wheel(0, 300)
                time.sleep(1)
            except:
                pass

            # Extract Data
            star_data = {}
            total_reviews = 0
            weighted_sum = 0

            # Pattern 1: Thai aria-label
            try:
                rows = page.locator('tr[aria-label*="ดาว"]').all()
                if not rows:
                     rows = page.locator('div[aria-label*="ดาว"]').all()
                
                for row in rows:
                    label = row.get_attribute("aria-label")
                    if not label: continue
                    match = re.search(r'(\d+)\s*ดาว\s*([\d,]+)\s*รีวิว', label)
                    if match:
                        star = int(match.group(1))
                        count = int(match.group(2).replace(',', ''))
                        star_data[star] = count
                        total_reviews += count
                        weighted_sum += (star * count)
                        logger.info(f"   {star}⭐: {count:,} reviews")
            except Exception:
                pass

            # Pattern 2: English aria-label
            if not star_data:
                try:
                    rows = page.locator('tr[aria-label*="star"]').all()
                    if rows:
                        for row in rows:
                            label = row.get_attribute("aria-label")
                            if not label: continue
                            match = re.search(r'(\d+)\s*star[s]?\s*([\d,]+)\s*review', label, re.IGNORECASE)
                            if match:
                                star = int(match.group(1))
                                count = int(match.group(2).replace(',', ''))
                                star_data[star] = count
                                total_reviews += count
                                weighted_sum += (star * count)
                except Exception:
                    pass

            # Pattern 3: Fallback (Overall only)
            if not star_data:
                try:
                    rating_elem = page.locator('[class*="fontDisplayLarge"]').first
                    if rating_elem.is_visible(timeout=3000):
                        rating_text = rating_elem.inner_text()
                        avg_match = re.search(r'([\d.]+)', rating_text)
                        if avg_match:
                            result.google_avg_rating = float(avg_match.group(1))

                    review_count_elem = page.locator('button[jsaction*="reviews"]').first
                    if review_count_elem.is_visible(timeout=2000):
                        count_text = review_count_elem.inner_text()
                        count_match = re.search(r'([\d,]+)', count_text)
                        if count_match:
                            result.google_review_count = int(count_match.group(1).replace(',', ''))

                    if result.google_avg_rating or result.google_review_count:
                        result.status = "partial"
                except Exception:
                    pass

            # Finalize Result
            if total_reviews > 0:
                avg_rating = weighted_sum / total_reviews
                result.google_avg_rating = round(avg_rating, 2)
                result.google_review_count = total_reviews
                result.google_star_1 = star_data.get(1, 0)
                result.google_star_2 = star_data.get(2, 0)
                result.google_star_3 = star_data.get(3, 0)
                result.google_star_4 = star_data.get(4, 0)
                result.google_star_5 = star_data.get(5, 0)
                result.status = "success"
                logger.info(f"   ✅ Average: {avg_rating:.2f}, Total: {total_reviews:,}")
            elif result.status == "partial":
                logger.info(f"   ⚠️ Partial data: {result.google_avg_rating} stars, {result.google_review_count} reviews")
            else:
                result.status = "not_found"
                result.error_message = "No review data found"
                # logger.warning("   ⚠️ No review block found")

            browser.close()

    except Exception as e:
        result.status = "error"
        result.error_message = str(e)
        logger.error(f"   ❌ Error scraping: {e}")

    return result


def start_test():
    logging.basicConfig(level=logging.INFO)
    res = get_google_maps_reviews(
        place_name="สนามหลวง", 
        province="กรุงเทพมหานคร", 
        headless=False
    )
    print(res.to_dict())

if __name__ == "__main__":
    start_test()

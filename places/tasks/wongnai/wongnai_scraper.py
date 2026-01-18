"""
Wongnai Scraper
===============
ใช้ดึงข้อมูลรีวิวจาก Wongnai สำหรับ places ที่ Google หาไม่เจอ

Flow:
1. Search Wongnai API: /_api/suggestions.json?q={place_name}
2. Get first result URL
3. Fetch page and extract business JSON from <script> tag
4. Map to existing Google columns with status = 'success_w'
"""

import os
import re
import json
import time
import random
import logging
import requests
from typing import Dict, Optional, List
from datetime import datetime
from dataclasses import dataclass, field
from urllib.parse import quote

logger = logging.getLogger(__name__)

# Price range mapping based on ฿ symbols
# ฿ -> < 100, ฿฿ -> 100-250, ฿฿฿ -> 251-500, ฿฿฿฿ -> 500+, ฿฿฿฿฿ -> 1000+
PRICE_RANGE_MAP = {
    "ถูกกว่า 100 บาท": 1,
    "100 - 250 บาท": 2,
    "251 - 500 บาท": 3,
    "501 - 1,000 บาท": 4,
    "1,001 - 2,000 บาท": 5,
    "มากกว่า 2,000 บาท": 5,
}

# API endpoints
WONGNAI_SEARCH_API = "https://www.wongnai.com/_api/suggestions.json"
WONGNAI_API_VERSION = "6.126"

# Headers to mimic browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "th-TH,th;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.wongnai.com/",
}


@dataclass
class WongnaiResult:
    """Result from Wongnai scraping"""
    place_id: int
    status: str = "pending"  # success_w, not_found, error
    wongnai_url: Optional[str] = None
    avg_rating: Optional[float] = None
    review_count: Optional[int] = None
    star_5: int = 0
    star_4: int = 0
    star_3: int = 0
    star_2: int = 0
    star_1: int = 0
    # New fields for genre, neighborhoods, price
    genres: List[str] = field(default_factory=list)
    neighborhoods: List[str] = field(default_factory=list)
    price_range: Optional[int] = None  # 1-5 based on ฿ count
    error_message: Optional[str] = None
    scraped_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        return {
            "place_id": self.place_id,
            "status": self.status,
            "wongnai_url": self.wongnai_url,
            "google_avg_rating": self.avg_rating,
            "google_review_count": self.review_count,
            "google_star_5": self.star_5,
            "google_star_4": self.star_4,
            "google_star_3": self.star_3,
            "google_star_2": self.star_2,
            "google_star_1": self.star_1,
            "wongnai_genres": self.genres,
            "wongnai_neighborhoods": self.neighborhoods,
            "wongnai_price_range": self.price_range,
            "error_message": self.error_message,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
        }


def search_wongnai(place_name: str, max_retries: int = 3) -> tuple:
    """
    Search Wongnai API and return the first result URL.
    Includes retry with exponential backoff for rate limiting.
    
    Returns: (url, was_rate_limited)
    - url: The found URL or None
    - was_rate_limited: True if we hit rate limit and exhausted retries
    """
    was_rate_limited = False
    
    for attempt in range(max_retries):
        try:
            params = {
                "_v": WONGNAI_API_VERSION,
                "locale": "th",
                "q": place_name
            }
            
            response = requests.get(
                WONGNAI_SEARCH_API,
                params=params,
                headers=HEADERS,
                timeout=10
            )
            
            if response.status_code == 403:
                # Rate limited - wait and retry
                was_rate_limited = True
                wait_time = (2 ** attempt) * 2 + random.uniform(1, 3)
                logger.warning(f"   ⏳ Rate limited (403), waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            
            if response.status_code != 200:
                logger.warning(f"   Wongnai API returned {response.status_code}")
                return None, False
            
            data = response.json()
            suggestions = data.get("suggestions", [])
            
            # Filter for places (type 1 = business/place)
            for suggestion in suggestions:
                if suggestion.get("type") == 1 and suggestion.get("url"):
                    return suggestion.get("url"), False
            
            return None, False
            
        except Exception as e:
            logger.error(f"   Wongnai search error: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None, False
    
    # If we get here, all retries failed due to rate limiting
    return None, was_rate_limited


def extract_business_json(page_html: str) -> Optional[Dict]:
    """
    Extract business statistic JSON from page HTML
    Pattern: "statistic":{"numberOfBookmarks":30,...,"rating":3.79,...,"ratingDistribution":{"one":0,...}}
    """
    try:
        # Pattern 1: Look for statistic block with ratingDistribution
        # This matches: "statistic":{...,"ratingDistribution":{...}}
        pattern = r'"statistic"\s*:\s*\{[^}]*"rating"\s*:\s*([\d.]+)[^}]*"numberOfReviews"\s*:\s*(\d+)[^}]*"ratingDistribution"\s*:\s*\{\s*"one"\s*:\s*(\d+)\s*,\s*"two"\s*:\s*(\d+)\s*,\s*"three"\s*:\s*(\d+)\s*,\s*"four"\s*:\s*(\d+)\s*,\s*"five"\s*:\s*(\d+)\s*\}'
        
        match = re.search(pattern, page_html)
        if match:
            return {
                "statistic": {
                    "rating": float(match.group(1)),
                    "numberOfReviews": int(match.group(2)),
                    "ratingDistribution": {
                        "one": int(match.group(3)),
                        "two": int(match.group(4)),
                        "three": int(match.group(5)),
                        "four": int(match.group(6)),
                        "five": int(match.group(7)),
                    }
                }
            }
        
        # Pattern 2: Simpler - just get rating and ratingDistribution separately
        rating_match = re.search(r'"rating"\s*:\s*([\d.]+)', page_html)
        reviews_match = re.search(r'"numberOfReviews"\s*:\s*(\d+)', page_html)
        dist_match = re.search(
            r'"ratingDistribution"\s*:\s*\{\s*"one"\s*:\s*(\d+)\s*,\s*"two"\s*:\s*(\d+)\s*,\s*"three"\s*:\s*(\d+)\s*,\s*"four"\s*:\s*(\d+)\s*,\s*"five"\s*:\s*(\d+)\s*\}',
            page_html
        )
        
        if rating_match:
            result = {
                "statistic": {
                    "rating": float(rating_match.group(1)),
                    "numberOfReviews": int(reviews_match.group(1)) if reviews_match else 0,
                    "ratingDistribution": {}
                }
            }
            
            if dist_match:
                result["statistic"]["ratingDistribution"] = {
                    "one": int(dist_match.group(1)),
                    "two": int(dist_match.group(2)),
                    "three": int(dist_match.group(3)),
                    "four": int(dist_match.group(4)),
                    "five": int(dist_match.group(5)),
                }
            
            return result
        
        return None
        
    except Exception as e:
        logger.error(f"   Error extracting business JSON: {e}")
        return None


def extract_wongnai_metadata(page_html: str) -> Dict:
    """
    Extract genres, neighborhoods, and price range from window._wn data
    These are available in the page's JavaScript data
    """
    result = {
        "genres": [],
        "neighborhoods": [],
        "price_range": None,
    }
    
    try:
        # Helper function to decode unicode escapes properly
        def decode_unicode_escapes(text: str) -> str:
            """Decode unicode escapes like \\u002F to / while preserving Thai UTF-8"""
            if '\\u' in text:
                try:
                    # Use raw_unicode_escape to decode \uXXXX sequences
                    return text.encode('utf-8').decode('unicode_escape').encode('latin1').decode('utf-8')
                except (UnicodeDecodeError, UnicodeEncodeError):
                    # Fallback: just replace common escapes
                    return text.replace('\\u002F', '/').replace('\\u0026', '&')
            return text
        
        # Extract categories/genres
        # Pattern: "categories":[{"id":123,"name":"คาเฟ่"},...]
        cat_pattern = r'"categories"\s*:\s*\[(.*?)\]'
        cat_match = re.search(cat_pattern, page_html)
        if cat_match:
            cat_json = cat_match.group(1)
            # Extract all category names
            name_pattern = r'"name"\s*:\s*"([^"]+)"'
            raw_genres = re.findall(name_pattern, cat_json)
            # Filter out internal tags (MAIN, SUB, etc.) and deduplicate
            genres = []
            for g in raw_genres:
                if g not in ('MAIN', 'SUB', 'OTHER', 'ATTRACTION'):
                    decoded = decode_unicode_escapes(g)
                    if decoded not in genres:
                        genres.append(decoded)
            result["genres"] = genres
            
        # Extract neighborhoods
        # Pattern: "neighborhoods":["เขาตะเกียบ",...]
        neighborhood_pattern = r'"neighborhoods"\s*:\s*\[(.*?)\]'
        neighborhood_match = re.search(neighborhood_pattern, page_html)
        if neighborhood_match:
            neighborhood_json = neighborhood_match.group(1)
            # Extract neighborhood names from the array
            raw_neighborhoods = re.findall(r'"([^"]+)"', neighborhood_json)
            neighborhoods = [decode_unicode_escapes(n) for n in raw_neighborhoods]
            result["neighborhoods"] = neighborhoods
        
        # Extract price range
        # Pattern: "priceRange":{"id":3,"name":"251 - 500 บาท"}
        price_pattern = r'"priceRange"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"'
        price_match = re.search(price_pattern, page_html)
        if price_match:
            price_name = price_match.group(1)
            result["price_range"] = PRICE_RANGE_MAP.get(price_name)
            
            # Fallback: count ฿ symbols if available
            if result["price_range"] is None:
                baht_count = price_name.count('฿')
                if baht_count > 0:
                    result["price_range"] = min(baht_count, 5)
                    
    except Exception as e:
        logger.error(f"   Error extracting metadata: {e}")
    
    return result


def get_wongnai_reviews(
    place_name: str,
    place_id: int = 0,
) -> WongnaiResult:
    """
    Get reviews from Wongnai for a place
    """
    result = WongnaiResult(
        place_id=place_id,
        scraped_at=datetime.now()
    )
    
    logger.info(f"🔍 Searching Wongnai: {place_name}")
    
    try:
        # Step 1: Search for place
        wongnai_url, was_rate_limited = search_wongnai(place_name)
        
        if not wongnai_url:
            if was_rate_limited:
                # Rate limited - don't mark as processed so it can be retried
                result.status = "rate_limited"
                result.error_message = "Rate limited by Wongnai API"
                logger.warning("   ⚠️ Skipping due to rate limit (will retry later)")
            else:
                result.status = "not_found"
                result.error_message = "No results from Wongnai search"
                logger.info("   ❌ Not found on Wongnai")
            return result
        
        result.wongnai_url = wongnai_url
        logger.info(f"   📍 Found: {wongnai_url}")
        
        # Step 2: Fetch the page
        time.sleep(random.uniform(0.5, 1.5))  # Polite delay
        
        page_response = requests.get(
            wongnai_url,
            headers=HEADERS,
            timeout=15
        )
        
        if page_response.status_code != 200:
            result.status = "error"
            result.error_message = f"Page returned {page_response.status_code}"
            return result
        
        # Step 3: Extract business data
        business_data = extract_business_json(page_response.text)
        
        if not business_data:
            # Try to extract rating from page directly
            # Look for rating pattern in HTML
            rating_match = re.search(r'(\d+\.?\d*)\s*⭐', page_response.text)
            review_match = re.search(r'\((\d+)\s*รีวิว\)', page_response.text)
            
            if rating_match:
                result.avg_rating = float(rating_match.group(1))
                if review_match:
                    result.review_count = int(review_match.group(1))
                result.status = "success_w"
                logger.info(f"   ✅ Rating from HTML: {result.avg_rating}")
                return result
            
            result.status = "error"
            result.error_message = "Could not extract business data"
            logger.warning("   ⚠️ Could not extract data from page")
            return result
        
        # Step 4: Map data
        statistic = business_data.get("statistic", business_data)
        
        result.avg_rating = statistic.get("rating")
        result.review_count = statistic.get("numberOfReviews", 0)
        
        # Rating distribution
        dist = statistic.get("ratingDistribution", {})
        result.star_5 = dist.get("five", 0)
        result.star_4 = dist.get("four", 0)
        result.star_3 = dist.get("three", 0)
        result.star_2 = dist.get("two", 0)
        result.star_1 = dist.get("one", 0)
        
        # Step 5: Extract metadata (genres, neighborhoods, price)
        metadata = extract_wongnai_metadata(page_response.text)
        result.genres = metadata.get("genres", [])
        result.neighborhoods = metadata.get("neighborhoods", [])
        result.price_range = metadata.get("price_range")
        
        if result.genres:
            logger.info(f"   🏷️ Genres: {', '.join(result.genres)}")
        if result.neighborhoods:
            logger.info(f"   📍 Neighborhoods: {', '.join(result.neighborhoods)}")
        if result.price_range:
            logger.info(f"   💰 Price Range: {'฿' * result.price_range}")
        
        if result.avg_rating is not None:
            result.status = "success_w"
            logger.info(f"   ✅ Rating: {result.avg_rating}, Reviews: {result.review_count}")
        else:
            result.status = "not_found"
            result.error_message = "No rating data in business object"
        
        return result
        
    except Exception as e:
        result.status = "error"
        result.error_message = str(e)
        logger.error(f"   ❌ Error: {e}")
        return result


def test_wongnai():
    """Test the scraper"""
    logging.basicConfig(level=logging.INFO)
    
    result = get_wongnai_reviews(
        place_name="เขื่อนสิรินธร",
        place_id=12345
    )
    
    print("\n=== Result ===")
    print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    test_wongnai()

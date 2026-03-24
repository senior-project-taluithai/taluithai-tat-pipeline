"""
TikTok Scraper - Dynamic Hashtag Configuration
===============================================
Automatically adjusts hashtag priorities based on:
- Current Thai season
- Upcoming festivals (within 30 days)
- Thai public holidays
- Long weekends
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# ==================== THAI CALENDAR CONFIG ====================

THAI_SEASONS = {
    "summer": {
        "months": [2, 3, 4, 5],  # Mid Feb - Mid May
        "description": "ฤดูร้อน",
        "priority_boost": 2.0,
    },
    "rainy": {
        "months": [6, 7, 8, 9, 10],  # Mid May - Oct
        "description": "ฤดูฝน", 
        "priority_boost": 2.0,
    },
    "winter": {
        "months": [11, 12, 1],  # Nov - Mid Feb
        "description": "ฤดูหนาว",
        "priority_boost": 2.0,
    },
}

# Thai Festivals with date ranges (MM-DD format)
THAI_FESTIVALS = {
    "songkran": {
        "name": "สงกรานต์",
        "start": "04-13",
        "end": "04-15",
        "lead_days": 30,  # Start boosting 30 days before
        "hashtags": ["สงกรานต์", "Songkran", "Songkran2026", "เล่นน้ำสงกรานต์", "ถนนข้าวสาร", "สงกรานต์2026"],
    },
    "loy_krathong": {
        "name": "ลอยกระทง",
        "start": "11-15",  # Approximate (full moon in Nov)
        "end": "11-15",
        "lead_days": 21,
        "hashtags": ["ลอยกระทง", "LoyKrathong", "ยี่เป็ง", "เชียงใหม่ลอยกระทง", "กระทง"],
    },
    "new_year": {
        "name": "ปีใหม่",
        "start": "12-25",
        "end": "01-05",
        "lead_days": 30,
        "hashtags": ["ปีใหม่", "ปีใหม่2026", "เคาท์ดาวน์", "Countdown", "NewYear", "HappyNewYear", "Countdown2026"],
    },
    "chinese_new_year": {
        "name": "ตรุษจีน",
        "start": "01-29",  # 2026 date (varies by lunar calendar)
        "end": "01-31",
        "lead_days": 21,
        "hashtags": ["ตรุษจีน", "ChineseNewYear", "เยาวราช", "ไหว้เจ้า", "ตรุษจีน2026"],
    },
    "candle_festival": {
        "name": "แห่เทียนพรรษา",
        "start": "07-10",  # Approximate (varies by lunar calendar)
        "end": "07-12",
        "lead_days": 14,
        "hashtags": ["แห่เทียนพรรษา", "เข้าพรรษา", "อุบลราชธานี", "ทำบุญเข้าพรรษา", "เทียนพรรษา"],
    },
    "visakha_bucha": {
        "name": "วันวิสาขบูชา",
        "start": "05-12",  # 2026 approximate
        "end": "05-12",
        "lead_days": 7,
        "hashtags": ["วิสาขบูชา", "ทำบุญ", "เวียนเทียน", "วันพระใหญ่"],
    },
    "asalha_bucha": {
        "name": "วันอาสาฬหบูชา",
        "start": "07-09",  # 2026 approximate
        "end": "07-09",
        "lead_days": 7,
        "hashtags": ["อาสาฬหบูชา", "วันพระใหญ่", "ทำบุญ", "เวียนเทียน"],
    },
    "mother_day": {
        "name": "วันแม่",
        "start": "08-12",
        "end": "08-12",
        "lead_days": 14,
        "hashtags": ["วันแม่", "12สิงหา", "MothersDay", "พาแม่เที่ยว", "วันแม่แห่งชาติ"],
    },
    "father_day": {
        "name": "วันพ่อ",
        "start": "12-05",
        "end": "12-05",
        "lead_days": 14,
        "hashtags": ["วันพ่อ", "5ธันวา", "FathersDay", "พาพ่อเที่ยว", "วันพ่อแห่งชาติ"],
    },
    "chakri_day": {
        "name": "วันจักรี",
        "start": "04-06",
        "end": "04-06",
        "lead_days": 7,
        "hashtags": ["วันจักรี", "ราชวงศ์จักรี"],
    },
    "coronation_day": {
        "name": "วันฉัตรมงคล",
        "start": "05-04",
        "end": "05-04",
        "lead_days": 7,
        "hashtags": ["วันฉัตรมงคล"],
    },
    "king_birthday": {
        "name": "วันเฉลิมพระชนมพรรษา ร.10",
        "start": "07-28",
        "end": "07-28",
        "lead_days": 7,
        "hashtags": ["วันเฉลิมพระชนมพรรษา", "28กรกฎาคม"],
    },
    "queen_birthday": {
        "name": "วันเฉลิมพระชนมพรรษา พระราชินี",
        "start": "06-03",
        "end": "06-03",
        "lead_days": 7,
        "hashtags": ["วันเฉลิมพระชนมพรรษา", "3มิถุนายน"],
    },
    "chulalongkorn_day": {
        "name": "วันปิยมหาราช",
        "start": "10-23",
        "end": "10-23",
        "lead_days": 7,
        "hashtags": ["วันปิยมหาราช", "23ตุลาคม"],
    },
}

# Long weekends 2026 (approximate - update yearly)
LONG_WEEKENDS = [
    ("01-01", "01-02", "ปีใหม่"),
    ("02-14", "02-16", "วาเลนไทน์+หยุดยาว"),
    ("04-06", "04-06", "วันจักรี"),
    ("04-13", "04-16", "สงกรานต์"),
    ("05-01", "05-04", "แรงงาน+หยุดยาว"),
    ("05-04", "05-04", "วันฉัตรมงคล"),
    ("06-03", "06-03", "วันเฉลิมฯ พระราชินี"),
    ("07-28", "07-28", "วันเฉลิมฯ ร.10"),
    ("08-12", "08-12", "วันแม่"),
    ("10-23", "10-26", "ปิยมหาราช+หยุดยาว"),
    ("12-05", "12-08", "วันพ่อ+หยุดยาว"),
    ("12-31", "01-02", "ปีใหม่"),
]

# ==================== BASE HASHTAG CATEGORIES ====================

HASHTAG_CATEGORIES = {
    # 1. General Travel (Always Active - High Priority)
    "general_travel": {
        "description": "ข้อมูลสถานที่ท่องเที่ยวทั่วไป",
        "priority": 1.0,  # Base priority
        "hashtags": [
            "เที่ยวไทย", "รีวิวที่เที่ยว", "Tiktokพาเที่ยว",
            "บันทึกการเดินทาง", "Vlogท่องเที่ยว", "ที่เที่ยวใหม่",
            "เที่ยวคนเดียว", "ทริปเดย์", "ที่เที่ยวแนะนำ",
            "ที่เที่ยวถ่ายรูปสวย", "จุดเช็คอิน", "ไปไหนดี",
            "เที่ยวไทยไปไหนดี", "fyp", "foryou",
        ]
    },
    
    "regional": {
        "description": "ระบุภาค - Context ภูมิศาสตร์",
        "priority": 0.8,
        "hashtags": [
            "เที่ยวเหนือ", "เที่ยวใต้", "เที่ยวอีสาน",
            "เที่ยวภาคกลาง", "เที่ยวภาคตะวันออก", "เที่ยวภาคตะวันตก",
            "เที่ยวกรุงเทพ", "เที่ยวใกล้กรุงเทพ",
        ]
    },
    
    # 2. Seasonal (Boosted by current season)
    "summer": {
        "description": "หน้าร้อน / ทะเล / เกาะ",
        "priority": 0.5,  # Base, boosted 2x in summer
        "season": "summer",
        "hashtags": [
            # Generic beach/summer
            "ทะเล", "เที่ยวทะเล", "ซัมเมอร์", "SummerVibes", "หนีร้อนไปทะเล",
            "เกาะ", "ดำน้ำ", "Snorkeling", "ดำน้ำตื้น", "ทะเลสวย",
            "ชายหาด", "Beach", "IslandLife", "เล่นน้ำทะเล",
            # Gulf of Thailand
            "ภูเก็ต", "กระบี่", "เกาะล้าน", "เกาะเสม็ด", "เกาะช้าง",
            "หัวหิน", "พัทยา", "ชะอำ", "เกาะพีพี", "เกาะหลีเป๊ะ",
            "เกาะเต่า", "เกาะสมุย", "เกาะพะงัน", "ระยอง", "บางแสน",
            "เกาะกูด", "เกาะหมาก", "เกาะมุก", "เกาะลันตา", "เกาะยาว",
            # Andaman
            "พังงา", "ตรัง", "สตูล", "เกาะสิมิลัน", "เกาะราชา",
            # Activities
            "ปาร์ตี้เกาะ", "FullMoonParty", "ดำน้ำลึก", "Scuba",
        ]
    },
    
    "winter": {
        "description": "หน้าหนาว / ภูเขา / ดอย / แคมป์ปิ้ง",
        "priority": 0.5,  # Base, boosted 2x in winter
        "season": "winter",
        "hashtags": [
            # Generic winter/mountain
            "หน้าหนาว", "เที่ยวหน้าหนาว", "ขึ้นดอย", "ภูเขา", "ทะเลหมอก",
            "ดูพระอาทิตย์ขึ้น", "จุดชมวิว", "อากาศหนาว", "หนาวนี้ไปไหนดี",
            "ทริปหน้าหนาว", "เช้าหมอก", "พระอาทิตย์ขึ้น", "Sunrise",
            # Camping
            "กางเต็นท์", "แคมป์ปิ้ง", "Camp", "Camping", "กางเต็นท์หน้าหนาว",
            "ลานกางเต็นท์", "แคมป์ไฟ", "Campfire", "แคมป์ปิ้งหน้าหนาว",
            "นอนเต็นท์", "เต็นท์", "Glamping",
            # Northern Thailand
            "เชียงใหม่", "เชียงราย", "แม่ฮ่องสอน", "น่าน", "ลำปาง", "ลำพูน",
            "ดอยอินทนนท์", "ภูกระดึง", "ปาย", "มอนแจ่ม", "ภูชี้ฟ้า",
            "ดอยอ่างขาง", "ดอยตุง", "ภูลมโล", "ภูเรือ", "บ้านรักไทย",
            "เลย", "ภูหินร่องกล้า", "เชียงดาว", "ดอยหลวงเชียงดาว",
            "ดอยแม่สลอง", "ดอยผ้าห่มปก", "ดอยม่อนจอง",
            # Central highlands
            "เขาค้อ", "ภูทับเบิก", "เพชรบูรณ์", "วังน้ำเขียว", "เขาใหญ่",
            "ปากช่อง", "นครราชสีมา",
            # Flower season
            "ทุ่งดอกกระเจียว", "ทุ่งดอกบัวตอง", "ซากุระเมืองไทย",
        ]
    },
    
    "rainy": {
        "description": "หน้าฝน / ป่าเขา / น้ำตก / Green Season",
        "priority": 0.5,  # Base, boosted 2x in rainy
        "season": "rainy",
        "hashtags": [
            # Generic rainy/nature
            "หน้าฝน", "เที่ยวหน้าฝน", "GreenSeason", "ป่าเขียว",
            "ป่า", "หลงรักป่า", "ธรรมชาติบำบัด", "ป่าเขา", "ป่าดิบ",
            "อุทยานแห่งชาติ", "NationalPark",
            # Waterfalls
            "น้ำตก", "เล่นน้ำตก", "Waterfall", "น้ำตกสวย",
            "น้ำตกเอราวัณ", "น้ำตกทีลอซู", "น้ำตกห้วยแม่ขมิ้น",
            # Destinations good for rainy
            "น่าน", "เขาใหญ่", "กาญจนบุรี", "นครนายก", "สระบุรี",
            "แก่งกระจาน", "เขาสก", "ตาก", "อุ้มผาง",
            # Activities
            "ล่องแก่ง", "Rafting", "ป่าฝน",
        ]
    },
    
    # 3. Festivals (Boosted near festival dates)
    "festivals": {
        "description": "เทศกาลหลัก",
        "priority": 0.3,  # Low base, heavily boosted near dates
        "hashtags": [
            "สงกรานต์", "Songkran", "งานวัด", "ลอยกระทง",
            "ปีใหม่", "เคาท์ดาวน์", "ตรุษจีน", "แห่เทียนพรรษา",
            "วันวิสาขบูชา", "เข้าพรรษา", "ออกพรรษา", "ยี่เป็ง",
        ]
    },
    
    "holidays": {
        "description": "วันหยุดยาว & กิจกรรมทางศาสนา",
        "priority": 0.5,
        "hashtags": [
            "ทำบุญ", "ไหว้พระ", "สายมู", "วัด", "วัดดัง",
            "วันหยุดยาว", "ทริปวันหยุด", "หยุดยาว", "หยุดยาวไปไหนดี",
            "พาพ่อแม่เที่ยว", "ครอบครัว", "ทริปครอบครัว", "เที่ยวกับครอบครัว",
            "เที่ยววันหยุด", "LongWeekend",
        ]
    },
    
    # 4. Niche Activities / Lifestyle
    "cafe": {
        "description": "Cafe Hopping / ร้านกาแฟ",
        "priority": 0.6,
        "hashtags": [
            "คาเฟ่", "รีวิวคาเฟ่", "CafeHopping", "คาเฟ่น่านั่ง",
            "คาเฟ่สวย", "คาเฟ่ลับ", "ร้านกาแฟ", "คาเฟ่วิวสวย",
            "คาเฟ่ถ่ายรูปสวย", "คาเฟ่เชียงใหม่", "คาเฟ่กรุงเทพ",
            "คาเฟ่ริมทะเล", "คาเฟ่ในสวน",
        ]
    },
    
    "food": {
        "description": "Food Tourism / กินเที่ยว",
        "priority": 0.6,
        "hashtags": [
            "StreetFood", "อร่อยบอกต่อ", "กินเที่ยว", "Foodie",
            "ของกินอร่อย", "ตลาดนัด", "ตลาดน้ำ", "ร้านอร่อย",
            "อาหารท้องถิ่น", "อาหารใต้", "อาหารเหนือ", "อาหารอีสาน",
            "ร้านเด็ด", "ของกินท้องถิ่น", "ชิมอาหาร",
        ]
    },
    
    "adventure": {
        "description": "สถานที่ลับๆ & Road Trip & Adventure",
        "priority": 0.7,
        "hashtags": [
            "UnseenThailand", "RoadTrip", "ที่เที่ยวลับ", "ที่เที่ยวใหม่",
            "Unseen", "ที่ลับ", "ขับรถเที่ยว", "DriveTrip",
            "ผจญภัย", "Adventure", "ธรรมชาติ", "Explore",
            "ที่เที่ยวคนน้อย", "ที่เที่ยวลับๆ",
        ]
    },
    
    "culture": {
        "description": "วัฒนธรรม & ประวัติศาสตร์ & วัด",
        "priority": 0.5,
        "hashtags": [
            "วัด", "วัดสวย", "โบราณสถาน", "อยุธยา", "สุโขทัย",
            "ประวัติศาสตร์", "มรดกโลก", "วัดดัง", "สถาปัตยกรรม",
            "วัดไทย", "Temple", "พระพุทธรูป", "ศิลปะไทย",
        ]
    },
    
    "accommodation": {
        "description": "ที่พัก / รีสอร์ท / โรงแรม",
        "priority": 0.5,
        "hashtags": [
            "ที่พัก", "รีสอร์ท", "โรงแรม", "Hotel", "Resort",
            "ที่พักสวย", "ที่พักวิวดี", "ที่พักติดทะเล", "ที่พักบนดอย",
            "Airbnb", "Hostel", "ที่พักราคาถูก", "ที่พักหรู",
            "Pool Villa", "พูลวิลล่า",
        ]
    },
    
    "photography": {
        "description": "จุดถ่ายรูป / Photography spots",
        "priority": 0.5,
        "hashtags": [
            "ถ่ายรูป", "จุดถ่ายรูป", "มุมถ่ายรูป", "ถ่ายรูปสวย",
            "Photography", "PhotoSpot", "Instagrammable",
            "พิกัดถ่ายรูป", "ที่ถ่ายรูป",
        ]
    },
}

# ==================== DYNAMIC PRIORITY FUNCTIONS ====================

def get_current_thai_season() -> str:
    """Get current Thai season based on month"""
    month = datetime.now().month
    
    if month in [11, 12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "summer"
    else:
        return "rainy"


def get_upcoming_festivals(days_ahead: int = 30) -> List[Dict]:
    """Get festivals happening within the next N days"""
    today = datetime.now()
    year = today.year
    upcoming = []
    
    for festival_id, festival in THAI_FESTIVALS.items():
        # Parse festival dates
        start_month, start_day = map(int, festival["start"].split("-"))
        
        # Handle year wrap (e.g., New Year spans Dec-Jan)
        festival_date = datetime(year, start_month, start_day)
        if festival_date < today:
            festival_date = datetime(year + 1, start_month, start_day)
        
        # Check if within lead time
        days_until = (festival_date - today).days
        if 0 <= days_until <= festival["lead_days"]:
            upcoming.append({
                "id": festival_id,
                "name": festival["name"],
                "days_until": days_until,
                "hashtags": festival["hashtags"],
                # Boost more as festival approaches (max 3x, min 2x)
                "boost": 2.0 + (1.0 - days_until / festival["lead_days"]),
            })
    
    return upcoming


def is_long_weekend_approaching(days_ahead: int = 14) -> Tuple[bool, str]:
    """Check if a long weekend is approaching"""
    today = datetime.now()
    year = today.year
    
    for start, end, name in LONG_WEEKENDS:
        start_month, start_day = map(int, start.split("-"))
        
        # Handle year wrap
        try:
            start_date = datetime(year, start_month, start_day)
        except ValueError:
            continue
            
        if start_date < today:
            start_date = datetime(year + 1, start_month, start_day)
        
        days_until = (start_date - today).days
        if 0 <= days_until <= days_ahead:
            return True, name
    
    return False, ""


def get_dynamic_hashtag_config() -> Dict:
    """
    Generate dynamic hashtag configuration based on current date
    Returns hashtag categories with adjusted priorities
    """
    config = {}
    current_season = get_current_thai_season()
    upcoming_festivals = get_upcoming_festivals()
    is_long_weekend, weekend_name = is_long_weekend_approaching()
    
    # Copy base categories with adjusted priorities
    for category_id, category in HASHTAG_CATEGORIES.items():
        config[category_id] = category.copy()
        config[category_id]["hashtags"] = category["hashtags"].copy()
        
        # Boost seasonal categories
        if "season" in category and category["season"] == current_season:
            config[category_id]["priority"] *= THAI_SEASONS[current_season]["priority_boost"]
            config[category_id]["boosted_reason"] = f"Current season: {current_season}"
    
    # Add festival hashtags with high priority
    for festival in upcoming_festivals:
        festival_category_id = f"festival_{festival['id']}"
        config[festival_category_id] = {
            "description": f"เทศกาล{festival['name']} (อีก {festival['days_until']} วัน)",
            "priority": festival["boost"],
            "hashtags": festival["hashtags"],
            "boosted_reason": f"Upcoming festival: {festival['name']}",
            "temporary": True,
        }
    
    # Boost holidays category if long weekend approaching
    if is_long_weekend:
        config["holidays"]["priority"] *= 1.5
        config["holidays"]["boosted_reason"] = f"Long weekend: {weekend_name}"
    
    return config


def get_hashtags_for_scraping(max_hashtags: int = 60) -> List[str]:
    """
    Get prioritized list of hashtags for scraping
    Higher priority categories get more representation
    """
    config = get_dynamic_hashtag_config()
    
    # Calculate total priority
    total_priority = sum(cat["priority"] for cat in config.values())
    
    # Allocate hashtags proportionally
    all_hashtags = []
    for category_id, category in sorted(config.items(), key=lambda x: x[1]["priority"], reverse=True):
        # Number of hashtags to take from this category
        proportion = category["priority"] / total_priority
        n_hashtags = max(1, int(proportion * max_hashtags))
        
        # Take hashtags (prioritize first ones in list)
        hashtags_to_add = category["hashtags"][:n_hashtags]
        all_hashtags.extend(hashtags_to_add)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_hashtags = []
    for h in all_hashtags:
        h_lower = h.lower()
        if h_lower not in seen:
            seen.add(h_lower)
            unique_hashtags.append(h)
    
    return unique_hashtags[:max_hashtags]


def get_all_hashtags_by_category() -> Dict[str, List[str]]:
    """
    Get all hashtags organized by category (for the DAG)
    Returns dict with category names as keys and hashtag lists as values
    """
    config = get_dynamic_hashtag_config()
    
    result = {}
    for category_id, category in config.items():
        result[category_id] = category["hashtags"]
    
    return result


# ==================== SCRAPING CONFIG ====================

# จำนวน video ต่อ hashtag
VIDEOS_PER_HASHTAG = 25

# Scroll count for Playwright
SCROLL_COUNT = 8

# Max hashtags per run
MAX_HASHTAGS_PER_RUN = 60


# ==================== DEBUG / LOGGING ====================

def print_current_config():
    """Print current dynamic configuration for debugging"""
    print("=" * 60)
    print(f"DYNAMIC HASHTAG CONFIG - {datetime.now().strftime('%Y-%m-%d')}")
    print("=" * 60)
    
    print(f"\n🌤️ Current Season: {get_current_thai_season()}")
    
    festivals = get_upcoming_festivals()
    if festivals:
        print("\n🎉 Upcoming Festivals:")
        for f in festivals:
            print(f"   - {f['name']}: {f['days_until']} days away (boost: {f['boost']:.1f}x)")
    else:
        print("\n🎉 No upcoming festivals in the next 30 days")
    
    is_lw, name = is_long_weekend_approaching()
    if is_lw:
        print(f"\n📅 Long Weekend Approaching: {name}")
    
    config = get_dynamic_hashtag_config()
    print("\n📊 Category Priorities (Top 10):")
    sorted_cats = sorted(config.items(), key=lambda x: x[1]["priority"], reverse=True)
    for cat_id, cat in sorted_cats[:10]:
        boost_info = f" ⬆️ {cat.get('boosted_reason', '')}" if cat.get('boosted_reason') else ""
        print(f"   {cat['priority']:.2f} | {cat_id}: {cat['description']}{boost_info}")
    
    hashtags = get_hashtags_for_scraping(40)
    print(f"\n🏷️ Top {len(hashtags)} Hashtags for Today:")
    print(f"   {', '.join(hashtags[:20])}")
    if len(hashtags) > 20:
        print(f"   {', '.join(hashtags[20:40])}")
    
    print(f"\n📈 Total unique hashtags available: {len(set(h for cat in HASHTAG_CATEGORIES.values() for h in cat['hashtags']))}")


if __name__ == "__main__":
    print_current_config()

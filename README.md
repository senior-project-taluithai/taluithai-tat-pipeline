# TAT Place Data Pipeline

นำข้อมูลสถานที่ท่องเที่ยวจาก TAT (การท่องเที่ยวแห่งประเทศไทย) เข้าสู่ PostgreSQL database

## Prerequisites

- Docker & Docker Compose
- Python 3.8+

## Quick Start

### 1. Start PostgreSQL

```bash
docker-compose up -d
```

Database จะพร้อมใช้งานที่ `localhost:5434`

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run Ingestion

```bash
python ingest_places.py
```

### 4. Run Google Maps Scraper

**A. Generate Session (One-time setup)**
ต้อง login Google เพื่อให้ session file สำหรับ scraper:
```bash
python places/generate_google_session.py
```
*Browser จะเด้งขึ้นมา ให้ login ให้เสร็จแล้วกลับมากด Enter ที่ terminal*

**B. Run Scraper Worker**
ระบุ Worker ID และ Province IDs ที่ต้องการ scrape (comma-separated):
```bash
python places/run_worker_google_scrape.py --worker-id 1 --provinces "10,11" --batch-size 10
```

**C. Run Parallel Scrapers (Recommended)**
รัน 3 workers พร้อมกัน โดยแบ่งงานตามจังหวัดอัตโนมัติ:
```bash
python places/run_parallel_scrapers.py --workers 3
```
*จะเปิด Terminal ใหม่ 3 หน้าต่าง แยกกันทำงาน*

## Database Schema

| Table | Description |
|-------|-------------|
| `tat.places` | สถานที่ท่องเที่ยวหลัก (Includes Google Ratings) |
| `tat.categories` | ประเภทสถานที่ |
| `tat.provinces` | จังหวัด |
| `tat.districts` | อำเภอ/เขต |
| `tat.sub_districts` | ตำบล/แขวง |
| `tat.sha_types` | ประเภท SHA |
| `tat.sha_categories` | หมวดหมู่ SHA |

## Verification

```bash
# เช็คจำนวน places
docker exec -it taluithai-postgres psql -U postgres -d taluithai \
  -c "SELECT COUNT(*) FROM tat.places;"

# ดูตัวอย่าง 5 records
docker exec -it taluithai-postgres psql -U postgres -d taluithai \
  -c "SELECT place_id, name, p.name as province FROM tat.places pl JOIN tat.provinces p USING(province_id) LIMIT 5;"
```

## Project Structure

```
├── docker-compose.yml     # PostgreSQL container config
├── init.sql               # Database schema initialization
├── ingest_places.py       # Ingestion script
├── requirements.txt       # Python dependencies
├── raw_tat_place/         # Raw JSON data (places)
└── raw_tat_province/      # Province reference data
```

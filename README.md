# TAT Data Pipeline

ระบบจัดการข้อมูลท่องเที่ยวจาก TAT (การท่องเที่ยวแห่งประเทศไทย) รวมถึงข้อมูลรถไฟและขนส่งสาธารณะ

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

---

## Data Modules

### 📍 Places (สถานที่)

```bash
cd places
python ingest_places.py
```

**Google Maps Scraper:**
```bash
# Generate session (one-time)
python places/generate_google_session.py

# Run scraper
python places/run_worker_google_scrape.py --worker-id 1 --provinces "10,11" --batch-size 10

# Or run parallel
python places/run_parallel_scrapers.py --workers 3
```

---

### 🎉 Events (กิจกรรม)

```bash
cd events
python ingest_events.py --lang both
```

Options: `--lang th`, `--lang en`, `--lang both`

---

### 🚂 Railway (ข้อมูลรถไฟ)

```bash
cd railway/scripts
python load_railway_data.py
```

**Data Files:**
- `railway/data/stations_geocoded.xlsx` - สถานีรถไฟพร้อมพิกัด
- `railway/data/after32.csv` - ตารางเดินรถ
- `railway/data/roadinfrastation.xlsx` - ข้อมูล Bus Terminals

**Test Query:**
```sql
SELECT * FROM railway.find_trains('Bang Sue', 'Khon Kaen', '08:00:00');
```

---

## Database Schemas

| Schema | Tables | Description |
|--------|--------|-------------|
| `tat` | places, events, provinces, categories | ข้อมูล TAT หลัก |
| `railway` | stations, train_schedules | ตารางรถไฟ |
| `public_transport` | bus_terminals, bus_routes | ขนส่งสาธารณะ |

---

## Verification

```bash
# Check all schemas
docker exec -it taluithai-postgres psql -U postgres -d taluithai \
  -c "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('tat', 'railway', 'public_transport');"

# Check places count
docker exec -it taluithai-postgres psql -U postgres -d taluithai \
  -c "SELECT COUNT(*) FROM tat.places;"

# Check railway data
docker exec -it taluithai-postgres psql -U postgres -d taluithai \
  -c "SELECT COUNT(*) FROM railway.stations;"
```

---

## Project Structure

```
├── docker-compose.yml          # PostgreSQL container
├── init.sql                    # TAT base schema
├── 02-railway-schema.sql       # Railway schema
├── 03-public-transport-schema.sql
├── requirements.txt
│
├── places/                     # Places data & scrapers
│   ├── ingest_places.py
│   ├── raw_tat_place/          # JSON data
│   ├── raw_tat_province/
│   ├── tasks/                  # google_maps, wongnai
│   └── run_*.py                # Worker scripts
│
├── events/                     # Events ingestion
│   └── ingest_events.py
│
└── railway/                    # Railway data
    ├── data/                   # XLS, CSV files
    └── scripts/                # Python loaders
```

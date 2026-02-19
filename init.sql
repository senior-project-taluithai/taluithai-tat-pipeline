-- TAT Place Database Schema
-- Initialize schema for Tourism Authority of Thailand (TAT) data

-- Create schema
CREATE SCHEMA IF NOT EXISTS tat;

-- Categories table
CREATE TABLE IF NOT EXISTS tat.categories (
    category_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- Provinces table  
CREATE TABLE IF NOT EXISTS tat.provinces (
    province_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- Districts table
CREATE TABLE IF NOT EXISTS tat.districts (
    district_id INTEGER PRIMARY KEY,
    province_id INTEGER REFERENCES tat.provinces(province_id),
    name TEXT NOT NULL
);

-- Sub-districts table
CREATE TABLE IF NOT EXISTS tat.sub_districts (
    sub_district_id INTEGER PRIMARY KEY,
    district_id INTEGER REFERENCES tat.districts(district_id),
    name TEXT NOT NULL
);

-- SHA Types table (Amazing Thailand Safety & Health Administration)
CREATE TABLE IF NOT EXISTS tat.sha_types (
    type_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

-- SHA Categories table
CREATE TABLE IF NOT EXISTS tat.sha_categories (
    category_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    icon TEXT
);

-- Main places table
CREATE TABLE IF NOT EXISTS tat.places (
    place_id INTEGER PRIMARY KEY,
    status TEXT,
    name TEXT NOT NULL,
    introduction TEXT,
    category_id INTEGER REFERENCES tat.categories(category_id),
    latitude DECIMAL(10, 5),
    longitude DECIMAL(10, 5),
    address TEXT,
    province_id INTEGER REFERENCES tat.provinces(province_id),
    district_id INTEGER REFERENCES tat.districts(district_id),
    sub_district_id INTEGER REFERENCES tat.sub_districts(sub_district_id),
    postcode TEXT,
    thumbnail_urls TEXT[],
    tags TEXT[],
    viewer INTEGER,
    slug TEXT,
    migrate_id TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    -- SHA data
    sha_name TEXT,
    sha_detail TEXT,
    sha_thumbnail_url TEXT,
    sha_detail_pictures TEXT[],
    sha_type_id INTEGER REFERENCES tat.sha_types(type_id),
    sha_category_id INTEGER REFERENCES tat.sha_categories(category_id)
);

-- Events table
CREATE TABLE IF NOT EXISTS tat.events (
    event_id INTEGER PRIMARY KEY,
    name TEXT,
    introduction TEXT,
    name_en TEXT,
    introduction_en TEXT,
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    latitude DECIMAL(10, 5),
    longitude DECIMAL(10, 5),
    province_id INTEGER REFERENCES tat.provinces(province_id),
    thumbnail_url TEXT,
    tags TEXT[],
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_places_province ON tat.places(province_id);
CREATE INDEX IF NOT EXISTS idx_places_category ON tat.places(category_id);
CREATE INDEX IF NOT EXISTS idx_places_status ON tat.places(status);
CREATE INDEX IF NOT EXISTS idx_places_name ON tat.places USING gin(to_tsvector('simple', name));
CREATE INDEX IF NOT EXISTS idx_districts_province ON tat.districts(province_id);
CREATE INDEX IF NOT EXISTS idx_sub_districts_district ON tat.sub_districts(district_id);

-- Grant permissions (optional, for production use)
-- GRANT SELECT ON ALL TABLES IN SCHEMA tat TO readonly_user;

COMMENT ON SCHEMA tat IS 'Tourism Authority of Thailand (TAT) data schema';
COMMENT ON TABLE tat.places IS 'Main table containing tourist attractions and places from TAT';
COMMENT ON TABLE tat.provinces IS 'Thai provinces reference table';
COMMENT ON TABLE tat.categories IS 'Place categories (e.g., สถานที่ท่องเที่ยว, ร้านอาหาร)';

-- Add Google Maps columns to places table
-- Includes detailed star rating distribution and average rating

ALTER TABLE tat.places 
ADD COLUMN IF NOT EXISTS google_place_id TEXT,
ADD COLUMN IF NOT EXISTS google_avg_rating DECIMAL(3, 1),
ADD COLUMN IF NOT EXISTS google_review_count INTEGER,
ADD COLUMN IF NOT EXISTS google_star_5 INTEGER,
ADD COLUMN IF NOT EXISTS google_star_4 INTEGER,
ADD COLUMN IF NOT EXISTS google_star_3 INTEGER,
ADD COLUMN IF NOT EXISTS google_star_2 INTEGER,
ADD COLUMN IF NOT EXISTS google_star_1 INTEGER,
ADD COLUMN IF NOT EXISTS google_scraped_at TIMESTAMPTZ;

-- Index for querying scraped vs unscraped places
CREATE INDEX IF NOT EXISTS idx_places_google_scraped_at ON tat.places(google_scraped_at);

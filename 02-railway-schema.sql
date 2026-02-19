-- Railway Schema for Train Route Planning
-- Add this to init.sql or run separately

-- Create schema
CREATE SCHEMA IF NOT EXISTS railway;

-- Enable PostGIS extension (optional, for advanced spatial queries)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- =====================================================
-- STATIONS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS railway.stations (
    id SERIAL PRIMARY KEY,
    name_th VARCHAR(200) NOT NULL UNIQUE,
    name_en VARCHAR(200),
    province VARCHAR(100),
    district VARCHAR(100),
    subdistrict VARCHAR(100),
    postal_code VARCHAR(10),
    lat DECIMAL(10, 7),
    lng DECIMAL(10, 7),
    station_type VARCHAR(50),  -- สถานีรถไฟ, ป้ายหยุดรถไฟ, ที่หยุดรถไฟ
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- TRAIN SCHEDULES TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS railway.train_schedules (
    id SERIAL PRIMARY KEY,
    train_no VARCHAR(20) NOT NULL,
    station_name_en VARCHAR(200) NOT NULL,
    station_abbr VARCHAR(20),
    arrival_time TIME,
    departure_time TIME,
    route_name VARCHAR(200),
    route_origin VARCHAR(200),
    route_destination VARCHAR(200),
    stop_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ROUTES VIEW (aggregated train info)
-- =====================================================
CREATE OR REPLACE VIEW railway.routes AS
SELECT 
    train_no,
    route_name,
    route_origin,
    route_destination,
    MIN(departure_time) as first_departure,
    MAX(departure_time) as last_arrival,
    COUNT(*) as total_stops
FROM railway.train_schedules
GROUP BY train_no, route_name, route_origin, route_destination;

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_stations_name ON railway.stations(name_th);
CREATE INDEX IF NOT EXISTS idx_stations_coords ON railway.stations(lat, lng);
CREATE INDEX IF NOT EXISTS idx_schedules_train ON railway.train_schedules(train_no);
CREATE INDEX IF NOT EXISTS idx_schedules_station ON railway.train_schedules(station_name_en);

-- =====================================================
-- HELPER FUNCTION: Find route between stations
-- =====================================================
CREATE OR REPLACE FUNCTION railway.find_trains(
    p_origin VARCHAR,
    p_destination VARCHAR,
    p_depart_after TIME DEFAULT '00:00:00'
)
RETURNS TABLE (
    train_no VARCHAR,
    route_name VARCHAR,
    origin_station VARCHAR,
    origin_time TIME,
    dest_station VARCHAR,
    dest_time TIME,
    total_stops INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        o.train_no,
        o.route_name,
        o.station_name_en as origin_station,
        o.departure_time as origin_time,
        d.station_name_en as dest_station,
        d.departure_time as dest_time,
        (d.stop_order - o.stop_order) as total_stops
    FROM railway.train_schedules o
    JOIN railway.train_schedules d ON o.train_no = d.train_no
    WHERE o.station_name_en ILIKE '%' || p_origin || '%'
      AND d.station_name_en ILIKE '%' || p_destination || '%'
      AND o.stop_order < d.stop_order
      AND o.departure_time >= p_depart_after
    ORDER BY o.departure_time;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- HELPER FUNCTION: Get route GeoJSON
-- =====================================================
CREATE OR REPLACE FUNCTION railway.get_route_geojson(p_train_no VARCHAR)
RETURNS JSON AS $$
DECLARE
    result JSON;
BEGIN
    SELECT json_build_object(
        'type', 'FeatureCollection',
        'properties', json_build_object(
            'train_no', p_train_no,
            'stations', (SELECT COUNT(*) FROM railway.train_schedules WHERE train_no = p_train_no)
        ),
        'features', json_agg(
            json_build_object(
                'type', 'Feature',
                'geometry', json_build_object(
                    'type', 'Point',
                    'coordinates', json_build_array(s.lng, s.lat)
                ),
                'properties', json_build_object(
                    'name', t.station_name_en,
                    'time', t.departure_time,
                    'order', t.stop_order
                )
            ) ORDER BY t.stop_order
        )
    ) INTO result
    FROM railway.train_schedules t
    LEFT JOIN railway.stations s ON s.name_en ILIKE '%' || SPLIT_PART(t.station_name_en, ' ', 1) || '%'
    WHERE t.train_no = p_train_no;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT USAGE ON SCHEMA railway TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA railway TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA railway TO postgres;

-- =====================================================
-- TRAIN ROUTES TABLE (derived from schedules)
-- =====================================================
CREATE TABLE IF NOT EXISTS railway.train_routes (
    train_no VARCHAR(20) PRIMARY KEY,
    origin_station VARCHAR(200),
    departure_time TIME,
    destination_station VARCHAR(200),
    arrival_time TIME,
    total_stops INTEGER
);

-- =====================================================
-- STATION NAME MAPPING (for schedule -> station matching)
-- =====================================================
CREATE TABLE IF NOT EXISTS railway.station_name_mapping (
    id SERIAL PRIMARY KEY,
    name_en VARCHAR(200) NOT NULL UNIQUE,
    lat DECIMAL(10, 7),
    lng DECIMAL(10, 7)
);

CREATE INDEX IF NOT EXISTS idx_mapping_name ON railway.station_name_mapping(name_en);

-- =====================================================
-- HELPER FUNCTION: Search Journey (with transfer support)
-- =====================================================
CREATE OR REPLACE FUNCTION railway.search_journey(
    p_origin TEXT,
    p_destination TEXT,
    p_time TEXT DEFAULT '08:00'
)
RETURNS JSON AS $$
DECLARE
    result JSON;
    direct_routes JSON;
    leg1_routes JSON;
    leg2_routes JSON;
BEGIN
    -- Check for direct routes
    SELECT json_agg(route ORDER BY departure_time)
    INTO direct_routes
    FROM (
        SELECT json_build_object(
            'train_no', t1.train_no,
            'from_station', t1.station_name_en,
            'departure_time', to_char(t1.departure_time, 'HH24:MI'),
            'to_station', t2.station_name_en,
            'arrival_time', to_char(t2.arrival_time, 'HH24:MI')
        ) as route,
        t1.departure_time
        FROM railway.train_schedules t1
        JOIN railway.train_schedules t2 ON t1.train_no = t2.train_no
        WHERE t1.station_name_en ILIKE '%' || p_origin || '%'
          AND t2.station_name_en ILIKE '%' || p_destination || '%'
          AND t1.stop_order < t2.stop_order
          AND t1.departure_time >= p_time::TIME
        LIMIT 5
    ) sub;
    
    IF direct_routes IS NOT NULL THEN
        result := json_build_object(
            'type', 'direct',
            'origin', p_origin,
            'destination', p_destination,
            'routes', direct_routes
        );
    ELSE
        -- Search via Bangkok (transfer)
        SELECT json_agg(route ORDER BY departure_time)
        INTO leg1_routes
        FROM (
            SELECT json_build_object(
                'train_no', t1.train_no,
                'from_station', t1.station_name_en,
                'departure_time', to_char(t1.departure_time, 'HH24:MI'),
                'to_station', t2.station_name_en,
                'arrival_time', to_char(t2.arrival_time, 'HH24:MI')
            ) as route,
            t1.departure_time
            FROM railway.train_schedules t1
            JOIN railway.train_schedules t2 ON t1.train_no = t2.train_no
            WHERE t1.station_name_en ILIKE '%' || p_origin || '%'
              AND t2.station_name_en ILIKE '%Bang Sue%'
              AND t1.stop_order < t2.stop_order
            LIMIT 3
        ) sub;
        
        SELECT json_agg(route ORDER BY departure_time)
        INTO leg2_routes
        FROM (
            SELECT json_build_object(
                'train_no', t1.train_no,
                'from_station', t1.station_name_en,
                'departure_time', to_char(t1.departure_time, 'HH24:MI'),
                'to_station', t2.station_name_en,
                'arrival_time', to_char(t2.arrival_time, 'HH24:MI')
            ) as route,
            t1.departure_time
            FROM railway.train_schedules t1
            JOIN railway.train_schedules t2 ON t1.train_no = t2.train_no
            WHERE t1.station_name_en ILIKE '%Bang Sue%'
              AND t2.station_name_en ILIKE '%' || p_destination || '%'
              AND t1.stop_order < t2.stop_order
              AND t1.departure_time >= p_time::TIME
            LIMIT 3
        ) sub;
        
        result := json_build_object(
            'type', 'transfer',
            'origin', p_origin,
            'destination', p_destination,
            'transfer_at', 'Bang Sue Junction',
            'leg1', leg1_routes,
            'leg2', leg2_routes
        );
    END IF;
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

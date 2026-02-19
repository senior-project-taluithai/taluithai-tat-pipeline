-- Public Transport Schema for Bus Terminals
-- Add this to init.sql or run separately

-- =====================================================
-- PUBLIC TRANSPORT SCHEMA
-- =====================================================
CREATE SCHEMA IF NOT EXISTS public_transport;

-- =====================================================
-- BUS TERMINALS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS public_transport.bus_terminals (
    id SERIAL PRIMARY KEY,
    name_th VARCHAR(300) NOT NULL,
    name_en VARCHAR(300),
    location_th VARCHAR(200),
    owner VARCHAR(200),
    province VARCHAR(100),
    district VARCHAR(100),
    subdistrict VARCHAR(100),
    lat DECIMAL(10, 7),
    lng DECIMAL(10, 7),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- BUS ROUTES TABLE (for future use)
-- =====================================================
CREATE TABLE IF NOT EXISTS public_transport.bus_routes (
    id SERIAL PRIMARY KEY,
    route_code VARCHAR(50),
    route_name VARCHAR(300),
    origin_terminal_id INTEGER REFERENCES public_transport.bus_terminals(id),
    destination_terminal_id INTEGER REFERENCES public_transport.bus_terminals(id),
    operator VARCHAR(200),
    route_type VARCHAR(50), -- intercity, local, express
    estimated_duration_minutes INTEGER,
    fare_thb DECIMAL(10, 2),
    frequency VARCHAR(100), -- e.g., "every 30 mins", "daily at 8:00, 12:00"
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_bus_terminals_province 
ON public_transport.bus_terminals(province);

CREATE INDEX IF NOT EXISTS idx_bus_terminals_coords 
ON public_transport.bus_terminals(lat, lng);

CREATE INDEX IF NOT EXISTS idx_bus_routes_terminals 
ON public_transport.bus_routes(origin_terminal_id, destination_terminal_id);

-- =====================================================
-- HELPER FUNCTION: Find nearest bus terminals
-- =====================================================
CREATE OR REPLACE FUNCTION public_transport.find_nearest_terminals(
    p_lat DECIMAL,
    p_lng DECIMAL,
    p_limit INTEGER DEFAULT 5
)
RETURNS TABLE (
    id INTEGER,
    name_th VARCHAR,
    name_en VARCHAR,
    province VARCHAR,
    lat DECIMAL,
    lng DECIMAL,
    distance_km DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.name_th,
        t.name_en,
        t.province,
        t.lat,
        t.lng,
        -- Haversine formula approximation (simple Euclidean * 111km)
        ROUND(CAST(SQRT(POWER(t.lat - p_lat, 2) + POWER(t.lng - p_lng, 2)) * 111 AS NUMERIC), 2) as distance_km
    FROM public_transport.bus_terminals t
    ORDER BY SQRT(POWER(t.lat - p_lat, 2) + POWER(t.lng - p_lng, 2))
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- HELPER FUNCTION: Find terminals by province
-- =====================================================
CREATE OR REPLACE FUNCTION public_transport.find_terminals_by_province(
    p_province TEXT
)
RETURNS TABLE (
    id INTEGER,
    name_th VARCHAR,
    name_en VARCHAR,
    district VARCHAR,
    lat DECIMAL,
    lng DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.id,
        t.name_th,
        t.name_en,
        t.district,
        t.lat,
        t.lng
    FROM public_transport.bus_terminals t
    WHERE t.province ILIKE '%' || p_province || '%'
    ORDER BY t.name_th;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- HELPER FUNCTION: Search multimodal journey
-- Combines train and bus options
-- =====================================================
CREATE OR REPLACE FUNCTION public_transport.search_multimodal(
    p_origin TEXT,
    p_destination TEXT
)
RETURNS JSON AS $$
DECLARE
    result JSON;
    train_options JSON;
    bus_terminals_origin JSON;
    bus_terminals_dest JSON;
    nearest_train_origin JSON;
    nearest_train_dest JSON;
BEGIN
    -- Get train options (if any)
    SELECT railway.search_journey(p_origin, p_destination, '06:00')
    INTO train_options;
    
    -- Get bus terminals near origin
    SELECT json_agg(json_build_object(
        'name_th', t.name_th,
        'name_en', t.name_en,
        'province', t.province,
        'lat', t.lat,
        'lng', t.lng
    ))
    INTO bus_terminals_origin
    FROM public_transport.bus_terminals t
    WHERE t.province ILIKE '%' || p_origin || '%'
       OR t.name_en ILIKE '%' || p_origin || '%';
    
    -- Get bus terminals near destination
    SELECT json_agg(json_build_object(
        'name_th', t.name_th,
        'name_en', t.name_en,
        'province', t.province,
        'lat', t.lat,
        'lng', t.lng
    ))
    INTO bus_terminals_dest
    FROM public_transport.bus_terminals t
    WHERE t.province ILIKE '%' || p_destination || '%'
       OR t.name_en ILIKE '%' || p_destination || '%';
    
    result := json_build_object(
        'origin', p_origin,
        'destination', p_destination,
        'train_options', train_options,
        'bus_terminals_at_origin', bus_terminals_origin,
        'bus_terminals_at_destination', bus_terminals_dest,
        'recommendation', CASE 
            WHEN train_options->>'type' = 'direct' THEN 'train_direct'
            WHEN bus_terminals_origin IS NOT NULL AND bus_terminals_dest IS NOT NULL THEN 'bus_option_available'
            WHEN train_options->>'type' = 'transfer' THEN 'train_with_transfer'
            ELSE 'limited_options'
        END
    );
    
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT USAGE ON SCHEMA public_transport TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public_transport TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public_transport TO postgres;

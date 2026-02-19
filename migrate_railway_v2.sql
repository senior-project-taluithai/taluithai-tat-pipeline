-- ============================================================
-- Railway Schema v2 Migration
-- ============================================================
-- Redesigns railway tables into:
--   1. stations       — master station data (add station_code)
--   2. trains         — train metadata
--   3. train_schedules — per-station schedule entries
--   4. route_topology — parsed Train route (block/signal data)
-- ============================================================

BEGIN;

-- --------------------------------------------------------
-- 1. Alter stations: add station_code (unique, nullable for now)
-- --------------------------------------------------------
ALTER TABLE railway.stations
    ADD COLUMN IF NOT EXISTS station_code VARCHAR(20) UNIQUE;

-- Index for fast lookup by code
CREATE INDEX IF NOT EXISTS idx_stations_code ON railway.stations (station_code);

-- --------------------------------------------------------
-- 2. Create trains table
-- --------------------------------------------------------
DROP TABLE IF EXISTS railway.trains CASCADE;
CREATE TABLE railway.trains (
    train_number     VARCHAR(20) PRIMARY KEY,
    route_group_id   VARCHAR(20),
    train_type       VARCHAR(100),
    train_length     INTEGER,
    traction_unit    VARCHAR(100)
);

CREATE INDEX idx_trains_route_group ON railway.trains (route_group_id);

-- --------------------------------------------------------
-- 3. Recreate train_schedules
-- --------------------------------------------------------
-- Drop dependent objects first
DROP VIEW IF EXISTS railway.routes CASCADE;
DROP TABLE IF EXISTS railway.route_topology CASCADE;
DROP TABLE IF EXISTS railway.train_schedules CASCADE;

CREATE TABLE railway.train_schedules (
    schedule_id      SERIAL PRIMARY KEY,
    train_number     VARCHAR(20) NOT NULL REFERENCES railway.trains(train_number),
    station_code     VARCHAR(20) NOT NULL,
    arrival_time     TIME,
    departure_time   TIME,
    platform         VARCHAR(10),
    stop_type        VARCHAR(30),
    sequence         INTEGER NOT NULL,
    train_route_raw  TEXT
);

CREATE INDEX idx_schedules_train ON railway.train_schedules (train_number);
CREATE INDEX idx_schedules_station ON railway.train_schedules (station_code);
CREATE INDEX idx_schedules_seq ON railway.train_schedules (train_number, sequence);

-- --------------------------------------------------------
-- 4. Create route_topology
-- --------------------------------------------------------
CREATE TABLE railway.route_topology (
    topology_id    SERIAL PRIMARY KEY,
    schedule_id    INTEGER NOT NULL REFERENCES railway.train_schedules(schedule_id) ON DELETE CASCADE,
    node_from      VARCHAR(20),
    link_in        VARCHAR(30),
    track_used     VARCHAR(20),
    node_to        VARCHAR(20),
    link_out       VARCHAR(30),
    variant_index  INTEGER
);

CREATE INDEX idx_topology_schedule ON railway.route_topology (schedule_id);
CREATE INDEX idx_topology_nodes ON railway.route_topology (node_from, node_to);

-- --------------------------------------------------------
-- Drop old unused tables
-- --------------------------------------------------------
DROP TABLE IF EXISTS railway.train_routes CASCADE;
DROP TABLE IF EXISTS railway.station_name_mapping CASCADE;

COMMIT;

-- Schema definition for BKK Transit Analytics

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- ==========================================
-- Staging Tables (Raw Data)
-- ==========================================

-- Vehicle positions (from BKK API)
CREATE TABLE IF NOT EXISTS staging.stg_vehicles (
    vehicle_id TEXT,
    trip_id TEXT,
    route_id TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    bearing INTEGER,
    speed DOUBLE PRECISION,
    license_plate TEXT,
    label TEXT,
    model TEXT,
    status TEXT,
    timestamp TIMESTAMP,
    service_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather data (from OpenWeather API)
CREATE TABLE IF NOT EXISTS staging.stg_weather (
    timestamp TIMESTAMP,
    temp DOUBLE PRECISION,
    pressure INTEGER,
    humidity INTEGER,
    wind_speed DOUBLE PRECISION,
    wind_deg INTEGER,
    rain DOUBLE PRECISION,
    cloudiness INTEGER,
    weather_main TEXT,
    weather_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GTFS Feed Registry
-- Each loaded GTFS snapshot has a validity period from its feed_info.txt.
-- Vehicle observations are matched to the feed whose window covers service_date.
CREATE TABLE IF NOT EXISTS staging.gtfs_feeds (
    id SERIAL PRIMARY KEY,
    feed_version TEXT UNIQUE,
    feed_start_date DATE NOT NULL,
    feed_end_date DATE NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- GTFS Stops
CREATE TABLE IF NOT EXISTS staging.stg_gtfs_stops (
    feed_id INTEGER REFERENCES staging.gtfs_feeds(id) ON DELETE CASCADE,
    stop_id TEXT,
    stop_name TEXT,
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    location_type INTEGER,
    parent_station TEXT,
    wheelchair_boarding INTEGER
);

-- GTFS Routes
CREATE TABLE IF NOT EXISTS staging.stg_gtfs_routes (
    feed_id INTEGER REFERENCES staging.gtfs_feeds(id) ON DELETE CASCADE,
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    route_color TEXT,
    route_text_color TEXT,
    route_desc TEXT
);

-- GTFS Trips
CREATE TABLE IF NOT EXISTS staging.stg_gtfs_trips (
    feed_id INTEGER REFERENCES staging.gtfs_feeds(id) ON DELETE CASCADE,
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    trip_headsign TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER
);

-- GTFS Stop Times
CREATE TABLE IF NOT EXISTS staging.stg_gtfs_stop_times (
    feed_id INTEGER REFERENCES staging.gtfs_feeds(id) ON DELETE CASCADE,
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled DOUBLE PRECISION
);

-- ==========================================
-- DWH Tables (Dimensional Model)
-- ==========================================

-- Time Dimension
CREATE TABLE IF NOT EXISTS dwh.dim_time (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    hour INTEGER,
    minute INTEGER,
    weekday INTEGER, -- 0=Monday, 6=Sunday
    is_weekend BOOLEAN,
    date_iso DATE
);

-- Weather Dimension
CREATE TABLE IF NOT EXISTS dwh.dim_weather (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP UNIQUE,
    temp DOUBLE PRECISION,
    rain DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    condition TEXT,
    humidity INTEGER,
    cloudiness INTEGER
);

-- Route Dimension
CREATE TABLE IF NOT EXISTS dwh.dim_route (
    route_id TEXT PRIMARY KEY,
    short_name TEXT,
    type TEXT
);

-- Stop Dimension
CREATE TABLE IF NOT EXISTS dwh.dim_stop (
    stop_id TEXT PRIMARY KEY,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    location_type TEXT
);

-- Vehicle Dimension
CREATE TABLE IF NOT EXISTS dwh.dim_vehicle (
    id SERIAL PRIMARY KEY,
    vehicle_id TEXT UNIQUE, -- Natural Key (BKK ID)
    model TEXT,
    label TEXT,
    license_plate TEXT
);

-- Vehicle Event Fact Table
-- Captures vehicle positions or arrival events
CREATE TABLE IF NOT EXISTS dwh.fact_vehicle_event (
    id SERIAL PRIMARY KEY,
    time_id INTEGER REFERENCES dwh.dim_time(id),
    weather_id INTEGER REFERENCES dwh.dim_weather(id),
    route_id TEXT REFERENCES dwh.dim_route(route_id),
    stop_id TEXT REFERENCES dwh.dim_stop(stop_id), -- Nullable if not at a stop
    vehicle_id INTEGER REFERENCES dwh.dim_vehicle(id),

    trip_id TEXT,

    delay_seconds INTEGER,
    scheduled_time TIMESTAMP,
    actual_time TIMESTAMP,

    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,

    status TEXT
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_stg_vehicles_timestamp ON staging.stg_vehicles(timestamp);
CREATE INDEX IF NOT EXISTS idx_stg_vehicles_service_date ON staging.stg_vehicles(service_date);
CREATE INDEX IF NOT EXISTS idx_stg_weather_timestamp ON staging.stg_weather(timestamp);
CREATE INDEX IF NOT EXISTS idx_stg_gtfs_stop_times_feed_trip ON staging.stg_gtfs_stop_times(feed_id, trip_id);
CREATE INDEX IF NOT EXISTS idx_stg_gtfs_stops_feed ON staging.stg_gtfs_stops(feed_id, stop_id);
CREATE INDEX IF NOT EXISTS idx_fact_vehicle_event_time ON dwh.fact_vehicle_event(time_id);
CREATE INDEX IF NOT EXISTS idx_fact_vehicle_event_route ON dwh.fact_vehicle_event(route_id);
CREATE INDEX IF NOT EXISTS idx_fact_vehicle_event_vehicle_id ON dwh.fact_vehicle_event(vehicle_id);

-- Unique constraint for upsert-based deduplication on re-runs
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_vehicle_event_trip_stop_sched
    ON dwh.fact_vehicle_event(trip_id, stop_id, scheduled_time);

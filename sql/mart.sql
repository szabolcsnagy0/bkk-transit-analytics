-- Create the mart schema
CREATE SCHEMA IF NOT EXISTS mart;

-- Master Analytics View
-- Joins the Fact table with all Dimensions and adds business logic columns
CREATE OR REPLACE VIEW mart.v_analytics_master AS
SELECT
    -- Metrics
    f.id AS event_id,
    f.delay_seconds,
    f.status,
    f.lat,
    f.lon,
    f.scheduled_time,
    f.actual_time,

    -- Time Dimension
    t.date_iso,
    t.year,
    t.month,
    t.day,
    t.hour,
    t.minute,
    t.weekday, -- 0=Monday
    t.is_weekend,
    CASE
        WHEN t.hour >= 5 AND t.hour < 12 THEN 'Morning'
        WHEN t.hour >= 12 AND t.hour < 17 THEN 'Afternoon'
        WHEN t.hour >= 17 AND t.hour < 21 THEN 'Evening'
        ELSE 'Night'
    END AS part_of_day,

    -- Weather Dimension
    w.temp,
    w.rain,
    w.wind_speed,
    w.condition,
    w.cloudiness,
    w.humidity,
    CASE
        WHEN w.rain > 0 OR w.condition ILIKE '%rain%' OR w.condition ILIKE '%drizzle%' OR w.condition ILIKE '%thunderstorm%' THEN 'Rainy'
        WHEN w.cloudiness < 20 THEN 'Sunny'
        ELSE 'Cloudy'
    END AS weather_category,

    -- Route Dimension
    r.route_id,
    r.short_name AS route_name,
    r.type AS route_type,

    -- Stop Dimension
    s.stop_id,
    s.name AS stop_name,
    s.location_type AS stop_type,

    -- Vehicle Dimension
    v.vehicle_id AS vehicle_natural_id,
    v.model AS vehicle_model,
    v.label AS vehicle_label,
    v.license_plate

FROM dwh.fact_vehicle_event f
JOIN dwh.dim_time t ON f.time_id = t.id
JOIN dwh.dim_weather w ON f.weather_id = w.id
JOIN dwh.dim_route r ON f.route_id = r.route_id
LEFT JOIN dwh.dim_stop s ON f.stop_id = s.stop_id
LEFT JOIN dwh.dim_vehicle v ON f.vehicle_id = v.id;

"""
ETL: Stage to DWH
Transforms data from Staging to DWH tables.
Handles data cleaning, dimension population, and delay calculations.
"""

import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logger = logging.getLogger('ETLStageToDWH')

class ETLStageToDWH:
    def __init__(self, config_path="config/config.yaml"):
        load_dotenv()
        self.config = self._load_config(config_path)

        # Load DB config
        db_conf = self.config['database']
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')

        if not user or not password:
            raise ValueError("Database credentials not found in environment variables")

        host = os.getenv('DB_HOST', db_conf['host'])
        self.db_url = f"postgresql://{user}:{password}@{host}:{db_conf['port']}/{db_conf['dbname']}"
        self.engine = create_engine(self.db_url)

    def _load_config(self, config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def populate_dim_time(self, start_year=2025, end_year=2026):
        """Generate and populate Time Dimension"""
        logger.info("Populating dim_time...")

        start_date = datetime(start_year, 1, 1)
        end_date = datetime(end_year, 12, 31, 23, 59)

        freq = '1min'
        timestamps = pd.date_range(start=start_date, end=end_date, freq=freq)

        df = pd.DataFrame({'timestamp': timestamps})
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['day'] = df['timestamp'].dt.day
        df['hour'] = df['timestamp'].dt.hour
        df['minute'] = df['timestamp'].dt.minute
        df['weekday'] = df['timestamp'].dt.dayofweek # 0=Monday
        df['is_weekend'] = df['weekday'].isin([5, 6])
        df['date_iso'] = df['timestamp'].dt.date

        # Bulk insert using chunking to avoid memory issues
        chunk_size = 100000
        total_rows = len(df)

        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE TEMP TABLE temp_dim_time (LIKE dwh.dim_time INCLUDING ALL)"))

                for i in range(0, total_rows, chunk_size):
                    chunk = df.iloc[i:i+chunk_size]
                    chunk.to_sql('temp_dim_time', conn, if_exists='append', index=False)
                    logger.info(f"Staged {min(i+chunk_size, total_rows)}/{total_rows} time rows")

                # Move from temp to real table
                conn.execute(text("""
                    INSERT INTO dwh.dim_time (timestamp, year, month, day, hour, minute, weekday, is_weekend, date_iso)
                    SELECT timestamp, year, month, day, hour, minute, weekday, is_weekend, date_iso
                    FROM temp_dim_time
                    ON CONFLICT (timestamp) DO NOTHING
                """))
                conn.commit()

        except Exception as e:
            logger.error(f"Failed to populate dim_time: {e}")

    def populate_dim_weather(self):
        """Clean and populate Weather Dimension"""
        logger.info("Populating dim_weather...")

        # Read raw weather
        query = "SELECT * FROM staging.stg_weather ORDER BY timestamp"
        df = pd.read_sql(query, self.engine)

        if df.empty:
            logger.warning("No weather data in staging.")
            return

        # 1. Deduplicate: Keep first record per hour (or timestamp)
        # Round to nearest hour for the dimension to reduce noise
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['timestamp_hour'] = df['timestamp'].dt.round('h')

        # Group by hour and take the mean for metrics, mode for text
        # Or simpler: just take the first record for that hour
        df_clean = df.drop_duplicates(subset=['timestamp_hour'], keep='first').copy()

        if 'timestamp' in df_clean.columns:
            df_clean = df_clean.drop(columns=['timestamp'])

        df_clean = df_clean.rename(columns={'timestamp_hour': 'timestamp'})

        # Select columns for DWH
        cols = ['timestamp', 'temp', 'rain', 'wind_speed', 'weather_main', 'humidity', 'cloudiness']
        df_load = df_clean[cols].rename(columns={'weather_main': 'condition'})

        # Load to DWH
        try:
            with self.engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS temp_dim_weather"))
                conn.execute(text("CREATE TEMP TABLE temp_dim_weather (LIKE dwh.dim_weather INCLUDING ALL)"))

                df_load.to_sql('temp_dim_weather', conn, if_exists='append', index=False)

                conn.execute(text("""
                    INSERT INTO dwh.dim_weather (timestamp, temp, rain, wind_speed, condition, humidity, cloudiness)
                    SELECT timestamp, temp, rain, wind_speed, condition, humidity, cloudiness
                    FROM temp_dim_weather
                    ON CONFLICT (timestamp) DO UPDATE
                    SET temp = EXCLUDED.temp,
                        rain = EXCLUDED.rain,
                        condition = EXCLUDED.condition
                """))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to populate dim_weather: {e}")

    def populate_dim_vehicle(self):
        """Populate Vehicle Dimension from Staging"""
        logger.info("Populating dim_vehicle...")

        # Get unique vehicles with their latest attributes
        # We use DISTINCT ON (vehicle_id) ORDER BY vehicle_id, timestamp DESC to get the most recent metadata
        query = """
            SELECT DISTINCT ON (vehicle_id)
                vehicle_id,
                model,
                label,
                license_plate
            FROM staging.stg_vehicles
            WHERE vehicle_id IS NOT NULL
            ORDER BY vehicle_id, timestamp DESC
        """

        try:
            df = pd.read_sql(query, self.engine)

            if df.empty:
                logger.warning("No vehicle data found in staging to populate dim_vehicle.")
                return

            with self.engine.connect() as conn:
                conn.execute(text("CREATE TEMP TABLE temp_dim_vehicle (LIKE dwh.dim_vehicle INCLUDING ALL)"))

                df.to_sql('temp_dim_vehicle', conn, if_exists='append', index=False)

                # Upsert
                conn.execute(text("""
                    INSERT INTO dwh.dim_vehicle (vehicle_id, model, label, license_plate)
                    SELECT vehicle_id, model, label, license_plate
                    FROM temp_dim_vehicle
                    ON CONFLICT (vehicle_id) DO UPDATE
                    SET model = EXCLUDED.model,
                        label = EXCLUDED.label,
                        license_plate = EXCLUDED.license_plate
                """))
                conn.execute(text("DROP TABLE temp_dim_vehicle"))
                conn.commit()

            logger.info(f"Populated dim_vehicle with {len(df)} vehicles.")

        except Exception as e:
            logger.error(f"Failed to populate dim_vehicle: {e}")

    def calculate_delays_and_load_facts(self):
        """
        Core Logic:
        1. Stage cleaned vehicles (stripped IDs + feed_id) to a temp table
        2. SQL-join vehicles × gtfs_feeds × stop_times × stops with a bbox filter
           — Postgres can use indexes here, far faster than a pandas merge of
           millions × millions of rows.
        3. Precise Haversine + delay calc in pandas on the small candidate set.
        4. Upsert to fact table.
        """
        logger.info("Calculating delays and loading facts...")

        # 1. Stage cleaned vehicles to a temp table.
        # Strip agency prefix from trip_id / route_id (e.g. "BKK_1234" -> "1234").
        # Fall back to observation date when service_date is missing (legacy rows).
        logger.info("Staging cleaned vehicles to temp table...")
        with self.engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS tmp_clean_vehicles"))
            conn.execute(text("""
                CREATE TEMP TABLE tmp_clean_vehicles AS
                SELECT
                    vehicle_id AS vehicle_natural_id,
                    substring(trip_id FROM position('_' IN trip_id) + 1) AS trip_id,
                    substring(route_id FROM position('_' IN route_id) + 1) AS route_id,
                    lat,
                    lon,
                    timestamp,
                    status,
                    COALESCE(service_date, timestamp::date) AS service_date
                FROM staging.stg_vehicles
                WHERE lat != 0 AND lon != 0
                  AND trip_id IS NOT NULL AND trip_id != ''
                  AND route_id IS NOT NULL
            """))
            conn.execute(text("CREATE INDEX ON tmp_clean_vehicles (service_date, trip_id)"))

            # Warn about vehicles whose service_date falls outside any loaded feed.
            uncovered = conn.execute(text("""
                SELECT count(*) FROM tmp_clean_vehicles v
                WHERE NOT EXISTS (
                    SELECT 1 FROM staging.gtfs_feeds f
                    WHERE v.service_date BETWEEN f.feed_start_date AND f.feed_end_date
                )
            """)).scalar()
            if uncovered:
                logger.warning(
                    f"{uncovered} vehicle observations have no GTFS feed covering their "
                    f"service_date and will be skipped."
                )

        # 2. SQL candidate query: join through the feed whose window covers service_date
        # (most recent wins on ties), bbox-filter to ~200m around the scheduled stop.
        # This returns a small dataframe — only vehicles observed near a scheduled stop.
        logger.info("Finding candidate stop-matches via SQL (bbox filter)...")
        candidates_query = """
            WITH vehicle_with_feed AS (
                SELECT DISTINCT ON (v.ctid)
                    v.vehicle_natural_id, v.trip_id, v.route_id,
                    v.lat, v.lon, v.timestamp, v.status, v.service_date,
                    f.id AS feed_id
                FROM tmp_clean_vehicles v
                JOIN staging.gtfs_feeds f
                  ON v.service_date BETWEEN f.feed_start_date AND f.feed_end_date
                ORDER BY v.ctid, f.loaded_at DESC
            )
            SELECT
                vf.vehicle_natural_id, vf.trip_id, vf.route_id,
                vf.lat, vf.lon, vf.timestamp, vf.status, vf.service_date,
                st.stop_id, st.arrival_time, s.stop_lat, s.stop_lon
            FROM vehicle_with_feed vf
            JOIN staging.stg_gtfs_stop_times st
              ON st.feed_id = vf.feed_id AND st.trip_id = vf.trip_id
            JOIN staging.stg_gtfs_stops s
              ON s.feed_id = vf.feed_id AND s.stop_id = st.stop_id
            WHERE abs(vf.lat - s.stop_lat) < 0.002
              AND abs(vf.lon - s.stop_lon) < 0.003
        """
        candidates = pd.read_sql(candidates_query, self.engine)
        logger.info(f"SQL returned {len(candidates)} candidate (vehicle × stop) rows.")

        if candidates.empty:
            logger.warning("No candidate vehicle-stop pairs found.")
            return

        candidates['timestamp'] = pd.to_datetime(candidates['timestamp'])
        candidates['service_date'] = pd.to_datetime(candidates['service_date'])
        candidates['stop_lat'] = candidates['stop_lat'].astype(float)
        candidates['stop_lon'] = candidates['stop_lon'].astype(float)
        # Rename to match downstream code expecting 'vehicle_id' as the natural key
        candidates = candidates.rename(columns={'vehicle_natural_id': 'vehicle_id'})

        # Precise distance calculation (Haversine)
        def haversine_np(lon1, lat1, lon2, lat2):
            lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
            c = 2 * np.arcsin(np.sqrt(a))
            km = 6367 * c
            return km * 1000 # meters

        candidates['distance_m'] = haversine_np(
            candidates['lon'], candidates['lat'],
            candidates['stop_lon'], candidates['stop_lat']
        )

        # Filter strictly < 50m
        arrivals = candidates[candidates['distance_m'] < 50].copy()

        # 5. Calculate Delay
        # Parse GTFS time (HH:MM:SS)
        # Handle hours >= 24 (GTFS spec)
        def parse_gtfs_time(t_str):
            parts = list(map(int, t_str.split(':')))
            hours = parts[0]
            if hours >= 24:
                hours -= 24
                return timedelta(days=1, hours=hours, minutes=parts[1], seconds=parts[2])
            return timedelta(hours=hours, minutes=parts[1], seconds=parts[2])

        # Use the trip's service_date (from the BKK API) as the base, NOT the observation date.
        # For overnight trips the vehicle may be observed on service_date + 1 (after midnight),
        # and GTFS encodes that with arrival_time >= 24:00:00 relative to service_date.
        arrivals['sched_timedelta'] = arrivals['arrival_time'].apply(parse_gtfs_time)
        arrivals['scheduled_ts'] = arrivals['service_date'] + arrivals['sched_timedelta']

        # Calculate delay in seconds
        arrivals['delay_seconds'] = (arrivals['timestamp'] - arrivals['scheduled_ts']).dt.total_seconds()

        # Deduplicate: a vehicle sits near a stop across multiple collection cycles.
        # Key on scheduled_ts (not just trip_id+stop_id) so that the same trip_id
        # observed on different service days produces separate events.
        # Keep the closest observation.
        final_facts = arrivals.sort_values('distance_m').drop_duplicates(
            subset=['trip_id', 'stop_id', 'scheduled_ts']
        )

        logger.info(f"Identified {len(final_facts)} stop arrival events.")

        # 6. Prepare for Loading
        min_ts = final_facts['timestamp'].min()
        max_ts = final_facts['timestamp'].max()

        time_map_query = f"""
            SELECT id as time_id, timestamp
            FROM dwh.dim_time
            WHERE timestamp >= '{min_ts}' AND timestamp <= '{max_ts}'
        """
        df_time = pd.read_sql(time_map_query, self.engine)
        df_time['timestamp'] = pd.to_datetime(df_time['timestamp'])

        # Merge time_id
        final_facts['ts_minute'] = final_facts['timestamp'].dt.floor('min')
        final_facts = pd.merge(final_facts, df_time, left_on='ts_minute', right_on='timestamp', how='left')

        # Lookup weather_id
        # Match on nearest hour
        final_facts['ts_hour'] = final_facts['timestamp_x'].dt.round('h')

        weather_map_query = f"""
            SELECT id as weather_id, timestamp
            FROM dwh.dim_weather
            WHERE timestamp >= '{min_ts - timedelta(hours=1)}' AND timestamp <= '{max_ts + timedelta(hours=1)}'
        """
        df_weather = pd.read_sql(weather_map_query, self.engine)
        df_weather['timestamp'] = pd.to_datetime(df_weather['timestamp'])

        final_facts = pd.merge(final_facts, df_weather, left_on='ts_hour', right_on='timestamp', how='left')

        # Lookup vehicle_id
        logger.info("Looking up vehicle keys...")
        df_vehicles_dim = pd.read_sql("SELECT id, vehicle_id FROM dwh.dim_vehicle", self.engine)
        final_facts = pd.merge(final_facts, df_vehicles_dim, on='vehicle_id', how='left')

        # Filter out records with missing weather data
        initial_count = len(final_facts)
        final_facts = final_facts.dropna(subset=['weather_id'])
        dropped_count = initial_count - len(final_facts)
        if dropped_count > 0:
            logger.warning(f"Dropped {dropped_count} records due to missing weather data.")

        if final_facts.empty:
            logger.warning("No facts to load after filtering missing weather.")
            return

        # Prepare final columns
        load_df = pd.DataFrame({
            'time_id': final_facts['time_id'],
            'weather_id': final_facts['weather_id'],
            'route_id': final_facts['route_id'],
            'stop_id': final_facts['stop_id'],
            'vehicle_id': final_facts['id'],
            'trip_id': final_facts['trip_id'],
            'delay_seconds': final_facts['delay_seconds'],
            'scheduled_time': final_facts['scheduled_ts'],
            'actual_time': final_facts['timestamp_x'],
            'lat': final_facts['lat'],
            'lon': final_facts['lon'],
            'status': final_facts['status']
        })

        # Upsert via staging table to dedupe on re-runs.
        # The unique index on (trip_id, stop_id, scheduled_time) makes each real-world
        # arrival event idempotent regardless of how many times this ETL runs.
        logger.info(f"Upserting {len(load_df)} facts into DWH...")
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TEMP TABLE temp_fact_vehicle_event
                (LIKE dwh.fact_vehicle_event INCLUDING DEFAULTS)
                ON COMMIT DROP
            """))
            load_df.to_sql('temp_fact_vehicle_event', conn, if_exists='append', index=False)
            conn.execute(text("""
                INSERT INTO dwh.fact_vehicle_event (
                    time_id, weather_id, route_id, stop_id, vehicle_id,
                    trip_id, delay_seconds, scheduled_time, actual_time,
                    lat, lon, status
                )
                SELECT time_id, weather_id, route_id, stop_id, vehicle_id,
                       trip_id, delay_seconds, scheduled_time, actual_time,
                       lat, lon, status
                FROM temp_fact_vehicle_event
                ON CONFLICT (trip_id, stop_id, scheduled_time) DO UPDATE
                SET delay_seconds = EXCLUDED.delay_seconds,
                    actual_time   = EXCLUDED.actual_time,
                    time_id       = EXCLUDED.time_id,
                    weather_id    = EXCLUDED.weather_id,
                    lat           = EXCLUDED.lat,
                    lon           = EXCLUDED.lon,
                    status        = EXCLUDED.status
            """))
        logger.info("Done.")

    def run(self):
        self.populate_dim_time()
        self.populate_dim_weather()
        self.populate_dim_vehicle()
        self.calculate_delays_and_load_facts()

if __name__ == "__main__":
    etl = ETLStageToDWH()
    etl.run()

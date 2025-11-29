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

        self.db_url = f"postgresql://{user}:{password}@{db_conf['host']}:{db_conf['port']}/{db_conf['dbname']}"
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
        1. Load Vehicles
        2. Load GTFS Schedule (Stop Times + Stops)
        3. Match Vehicle -> Trip -> Stop
        4. Calculate Delay
        5. Load to Fact Table
        """
        logger.info("Calculating delays and loading facts...")

        # 1. Load Vehicles
        # Filter invalid lat/lon and missing identifiers
        logger.info("Reading vehicles...")
        vehicles_query = """
            SELECT vehicle_id, trip_id, route_id, lat, lon, timestamp, status
            FROM staging.stg_vehicles
            WHERE lat != 0
              AND lon != 0
              AND trip_id IS NOT NULL
              AND trip_id != ''
              AND route_id IS NOT NULL
        """
        df_vehicles = pd.read_sql(vehicles_query, self.engine)
        df_vehicles['timestamp'] = pd.to_datetime(df_vehicles['timestamp'])

        # Clean trip_id and route_id to match GTFS format
        # Remove agency prefix (e.g. "BKK_", "volan_") by taking everything after the first underscore
        df_vehicles['trip_id'] = df_vehicles['trip_id'].astype(str).str.split('_', n=1).str[-1]
        df_vehicles['route_id'] = df_vehicles['route_id'].astype(str).str.split('_', n=1).str[-1]

        if df_vehicles.empty:
            logger.warning("No vehicle data found after cleaning.")
            return

        # 2. Load GTFS Schedule
        # We need: trip_id, stop_id, arrival_time, stop_lat, stop_lon
        logger.info("Reading GTFS schedule...")
        gtfs_query = """
            SELECT st.trip_id, st.stop_id, st.arrival_time, s.stop_lat, s.stop_lon
            FROM staging.stg_gtfs_stop_times st
            JOIN staging.stg_gtfs_stops s ON st.stop_id = s.stop_id
        """
        df_schedule = pd.read_sql(gtfs_query, self.engine)

        # Optimize types
        df_schedule['stop_lat'] = df_schedule['stop_lat'].astype(float)
        df_schedule['stop_lon'] = df_schedule['stop_lon'].astype(float)

        # 3. Merge Vehicles with Schedule on trip_id
        # This creates a row for every (vehicle_pos, scheduled_stop) pair in the trip
        logger.info("Merging vehicles with schedule...")
        merged = pd.merge(df_vehicles, df_schedule, on='trip_id', how='inner')

        # 4. Calculate Distance
        # Vectorized filter first to reduce heavy calculations
        # Simple bounding box check
        merged['lat_diff'] = (merged['lat'] - merged['stop_lat']).abs()
        merged['lon_diff'] = (merged['lon'] - merged['stop_lon']).abs()

        # Filter candidates roughly within ~100m
        candidates = merged[
            (merged['lat_diff'] < 0.002) &
            (merged['lon_diff'] < 0.003)
        ].copy()

        if candidates.empty:
            logger.warning("No vehicles found near scheduled stops (within ~200m bounding box).")
            # Debug info
            if not merged.empty:
                logger.info(f"Min lat_diff: {merged['lat_diff'].min()}, Min lon_diff: {merged['lon_diff'].min()}")
            return

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

        arrivals['trip_date'] = arrivals['timestamp'].dt.normalize()

        # Convert arrival_time string to timedelta
        arrivals['sched_timedelta'] = arrivals['arrival_time'].apply(parse_gtfs_time)

        arrivals['scheduled_ts'] = arrivals['trip_date'] + arrivals['sched_timedelta']

        # Calculate delay in seconds
        arrivals['delay_seconds'] = (arrivals['timestamp'] - arrivals['scheduled_ts']).dt.total_seconds()

        # Deduplicate: A vehicle might be at the stop for multiple minutes.
        # We want one event per (trip_id, stop_id).
        final_facts = arrivals.sort_values('distance_m').drop_duplicates(
            subset=['trip_id', 'stop_id']
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

        # Insert
        logger.info("Inserting facts into DWH...")
        load_df.to_sql('fact_vehicle_event', self.engine, schema='dwh', if_exists='append', index=False)
        logger.info("Done.")

    def run(self):
        self.populate_dim_time()
        self.populate_dim_weather()
        self.populate_dim_vehicle()
        self.calculate_delays_and_load_facts()

if __name__ == "__main__":
    etl = ETLStageToDWH()
    etl.run()

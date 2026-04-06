"""
GTFS Data Loader
Loads BKK GTFS data into the staging and DWH tables.
"""

import zipfile
import pandas as pd
import logging
import shutil
import os
from pathlib import Path
from sqlalchemy import create_engine, text
import yaml
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

logger = logging.getLogger('GTFSLoader')

class GTFSLoader:
    def __init__(self, config_path="config/config.yaml"):
        load_dotenv()
        self.config = self._load_config(config_path)

        # Load DB config
        db_conf = self.config['database']
        user = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')

        if not user or not password:
            raise ValueError("Database credentials not found in environment variables (POSTGRES_USER, POSTGRES_PASSWORD)")

        host = os.getenv('DB_HOST', db_conf['host'])
        self.db_url = f"postgresql://{user}:{password}@{host}:{db_conf['port']}/{db_conf['dbname']}"

        self.engine = create_engine(self.db_url)
        self.gtfs_dir = Path("data/raw/gtfs")
    def _load_config(self, config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def run(self):
        if not self.gtfs_dir.exists():
            logger.error(f"GTFS directory not found at {self.gtfs_dir}. Please extract GTFS data there.")
            return

        self.load_staging_tables()
        self.populate_dimensions()

    def load_staging_tables(self):
        """Load GTFS files into staging tables"""
        table_columns = {
            'stg_gtfs_stops': [
                'stop_id', 'stop_name', 'stop_lat', 'stop_lon',
                'location_type', 'parent_station', 'wheelchair_boarding'
            ],
            'stg_gtfs_routes': [
                'route_id', 'agency_id', 'route_short_name', 'route_long_name',
                'route_type', 'route_color', 'route_text_color', 'route_desc'
            ],
            'stg_gtfs_trips': [
                'route_id', 'service_id', 'trip_id', 'trip_headsign',
                'direction_id', 'block_id', 'shape_id',
                'wheelchair_accessible', 'bikes_allowed'
            ],
            'stg_gtfs_stop_times': [
                'trip_id', 'arrival_time', 'departure_time', 'stop_id',
                'stop_sequence', 'pickup_type', 'drop_off_type',
                'shape_dist_traveled'
            ]
        }

        files_to_load = {
            'stops.txt': 'stg_gtfs_stops',
            'routes.txt': 'stg_gtfs_routes',
            'trips.txt': 'stg_gtfs_trips',
            'stop_times.txt': 'stg_gtfs_stop_times'
        }

        for filename, table_name in files_to_load.items():
            file_path = self.gtfs_dir / filename
            if not file_path.exists():
                logger.warning(f"File {filename} not found in {self.gtfs_dir}, skipping...")
                continue

            logger.info(f"Loading {filename} into {table_name}...")

            # Read CSV
            chunk_size = 100000
            first_chunk = True

            try:
                # Truncate table first
                with self.engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE staging.{table_name}"))
                    conn.commit()

                for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=str):
                    # Clean column names (remove whitespace)
                    chunk.columns = chunk.columns.str.strip()

                    # Filter columns to match database schema
                    if table_name in table_columns:
                        expected_cols = table_columns[table_name]
                        # Keep only columns that exist in both DataFrame and expected list
                        valid_cols = [c for c in expected_cols if c in chunk.columns]
                        chunk = chunk[valid_cols]

                    # Write to DB
                    chunk.to_sql(
                        table_name,
                        self.engine,
                        schema='staging',
                        if_exists='append',
                        index=False,
                        method='multi' # Faster insert
                    )

                    if first_chunk:
                        logger.info(f"Started loading {table_name}...")
                        first_chunk = False

                logger.info(f"Finished loading {table_name}")

            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")

    def populate_dimensions(self):
        """Populate DWH dimensions from staging"""
        logger.info("Populating DWH dimensions...")

        ROUTE_TYPE_MAP = {
            '0': 'Tram',
            '1': 'Metro',
            '2': 'Rail',
            '3': 'Bus',
            '4': 'Ferry',
            '11': 'Trolleybus',
            '109': 'Suburban Railway'
        }

        LOCATION_TYPE_MAP = {
            '0': 'Stop',
            '1': 'Station',
            '2': 'Entrance/Exit',
            '3': 'Generic Node',
            '4': 'Boarding Area'
        }

        with self.engine.begin() as conn:
            # 1. Dim Route
            logger.info("Populating dim_route...")
            df_routes = pd.read_sql("SELECT DISTINCT route_id, route_short_name, route_type FROM staging.stg_gtfs_routes", conn)

            # Map types
            df_routes['type'] = df_routes['route_type'].astype(str).map(ROUTE_TYPE_MAP).fillna('Other')

            # Prepare for load
            df_routes = df_routes[['route_id', 'route_short_name', 'type']].rename(columns={'route_short_name': 'short_name'})

            # Load to temp table
            conn.execute(text("CREATE TEMP TABLE temp_dim_route (LIKE dwh.dim_route INCLUDING ALL)"))
            df_routes.to_sql('temp_dim_route', conn, if_exists='append', index=False)

            # Upsert
            conn.execute(text("""
                INSERT INTO dwh.dim_route (route_id, short_name, type)
                SELECT route_id, short_name, type FROM temp_dim_route
                ON CONFLICT (route_id) DO UPDATE
                SET short_name = EXCLUDED.short_name,
                    type = EXCLUDED.type;
            """))
            conn.execute(text("DROP TABLE temp_dim_route"))

            # 2. Dim Stop
            logger.info("Populating dim_stop...")
            df_stops = pd.read_sql("SELECT DISTINCT stop_id, stop_name, stop_lat, stop_lon, location_type FROM staging.stg_gtfs_stops", conn)

            # Map location types (handle NaN/None)
            df_stops['location_type'] = df_stops['location_type'].fillna('0').astype(int).astype(str).map(LOCATION_TYPE_MAP).fillna('Unknown')

            # Prepare for load
            df_stops = df_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'location_type']].rename(columns={'stop_name': 'name', 'stop_lat': 'lat', 'stop_lon': 'lon'})

            # Load to temp table
            conn.execute(text("CREATE TEMP TABLE temp_dim_stop (LIKE dwh.dim_stop INCLUDING ALL)"))
            df_stops.to_sql('temp_dim_stop', conn, if_exists='append', index=False)

            # Upsert
            conn.execute(text("""
                INSERT INTO dwh.dim_stop (stop_id, name, lat, lon, location_type)
                SELECT stop_id, name, lat, lon, location_type FROM temp_dim_stop
                ON CONFLICT (stop_id) DO UPDATE
                SET name = EXCLUDED.name,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    location_type = EXCLUDED.location_type;
            """))
            conn.execute(text("DROP TABLE temp_dim_stop"))

            logger.info("Dimensions populated successfully.")

    def run(self):
        if not self.gtfs_dir.exists():
            logger.error(f"GTFS directory not found at {self.gtfs_dir}. Please extract GTFS data there.")
            return

        self.load_staging_tables()
        self.populate_dimensions()

if __name__ == "__main__":
    loader = GTFSLoader()
    loader.run()

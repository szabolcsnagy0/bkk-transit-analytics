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

        self.db_url = f"postgresql://{user}:{password}@{db_conf['host']}:{db_conf['port']}/{db_conf['dbname']}"

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

        with self.engine.connect() as conn:
            # Dim Route
            logger.info("Populating dim_route...")
            conn.execute(text("""
                INSERT INTO dwh.dim_route (route_id, short_name, long_name, type, color, text_color)
                SELECT DISTINCT
                    route_id,
                    route_short_name,
                    route_long_name,
                    CAST(route_type AS INTEGER),
                    route_color,
                    route_text_color
                FROM staging.stg_gtfs_routes
                ON CONFLICT (route_id) DO UPDATE
                SET short_name = EXCLUDED.short_name,
                    long_name = EXCLUDED.long_name,
                    type = EXCLUDED.type,
                    color = EXCLUDED.color,
                    text_color = EXCLUDED.text_color;
            """))

            # Dim Stop
            logger.info("Populating dim_stop...")
            conn.execute(text("""
                INSERT INTO dwh.dim_stop (stop_id, name, lat, lon, location_type)
                SELECT DISTINCT
                    stop_id,
                    stop_name,
                    CAST(stop_lat AS DOUBLE PRECISION),
                    CAST(stop_lon AS DOUBLE PRECISION),
                    CAST(location_type AS INTEGER)
                FROM staging.stg_gtfs_stops
                ON CONFLICT (stop_id) DO UPDATE
                SET name = EXCLUDED.name,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    location_type = EXCLUDED.location_type;
            """))

            conn.commit()
            logger.info("Dimensions populated successfully.")

    def cleanup(self):
        """Remove temp files"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info("Cleaned up temp files.")

    def run(self):
        self.load_staging_tables()
    def cleanup(self):
        """Cleanup not needed for raw directory"""
        pass

    def run(self):
        # Ensure GTFS directory exists
        if not self.gtfs_dir.exists():
            logger.error(f"GTFS directory not found at {self.gtfs_dir}. Please extract GTFS data there.")
            return

        self.load_staging_tables()
        self.populate_dimensions()

if __name__ == "__main__":
    loader = GTFSLoader()
    loader.run()

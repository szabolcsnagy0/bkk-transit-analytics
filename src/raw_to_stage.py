"""
ETL: Raw to Stage
Loads raw JSON data from data/raw into staging tables in PostgreSQL.
"""

import json
import os
import pandas as pd
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

logger = logging.getLogger('ETLRawToStage')

class ETLRawToStage:
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

        self.raw_path = Path(self.config['storage']['base_path']).parent # data/raw
        self.bkk_path = self.raw_path / "bkk"
        self.weather_path = self.raw_path / "weather"

    def _load_config(self, config_path):
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def truncate_staging_tables(self):
        """Clear staging tables before loading"""
        logger.info("Truncating staging tables...")
        with self.engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE staging.stg_vehicles"))
            conn.execute(text("TRUNCATE TABLE staging.stg_weather"))
            conn.commit()

    def load_vehicles(self):
        """Load BKK vehicle data"""
        logger.info("Loading BKK vehicle data...")

        files = sorted(self.bkk_path.rglob("*.json"))
        total_files = len(files)
        logger.info(f"Found {total_files} BKK files")

        batch_size = 10000
        batch_data = []

        for i, file_path in enumerate(files):
            try:
                with open(file_path, 'r') as f:
                    content = json.load(f)

                metadata = content.get('metadata', {})
                timestamp = metadata.get('timestamp')

                if 'data' in content and 'data' in content['data'] and 'list' in content['data']['data']:
                    vehicles = content['data']['data']['list']
                elif 'data' in content and 'list' in content['data']:
                     # Fallback for potential different structure
                    vehicles = content['data']['list']
                else:
                    continue

                for v in vehicles:
                    # Extract fields matching stg_vehicles schema
                    vehicle_row = {
                        'vehicle_id': v.get('vehicleId'),
                        'trip_id': v.get('tripId'),
                        'route_id': v.get('routeId'),
                        'lat': v.get('location', {}).get('lat'),
                        'lon': v.get('location', {}).get('lon'),
                        'bearing': v.get('bearing'),
                        'speed': v.get('speed'), # Might be null
                        'license_plate': v.get('licensePlate'),
                        'label': v.get('label'),
                        'model': v.get('model'),
                        'status': v.get('status'),
                        'timestamp': timestamp
                    }
                    batch_data.append(vehicle_row)

                # Insert batch
                if len(batch_data) >= batch_size or (i == total_files - 1 and batch_data):
                    self._insert_batch('stg_vehicles', batch_data)
                    batch_data = []
                    logger.info(f"Processed {i+1}/{total_files} files")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")

    def load_weather(self):
        """Load Weather data"""
        logger.info("Loading Weather data...")

        files = sorted(self.weather_path.rglob("*.json"))
        total_files = len(files)
        logger.info(f"Found {total_files} Weather files")

        batch_data = []

        for i, file_path in enumerate(files):
            try:
                with open(file_path, 'r') as f:
                    content = json.load(f)

                data = content.get('data', {})
                metadata = content.get('metadata', {})
                weather_main = data.get('main', {})
                wind = data.get('wind', {})
                clouds = data.get('clouds', {})
                weather_desc_list = data.get('weather', [])
                weather_desc = weather_desc_list[0] if weather_desc_list else {}
                rain = data.get('rain', {})

                rain_val = 0
                if isinstance(rain, dict):
                    rain_val = rain.get('1h', 0)

                ts = metadata.get('timestamp')

                weather_row = {
                    'timestamp': ts,
                    'temp': weather_main.get('temp'),
                    'pressure': weather_main.get('pressure'),
                    'humidity': weather_main.get('humidity'),
                    'wind_speed': wind.get('speed'),
                    'wind_deg': wind.get('deg'),
                    'rain': rain_val,
                    'cloudiness': clouds.get('all'),
                    'weather_main': weather_desc.get('main'),
                    'weather_description': weather_desc.get('description')
                }
                batch_data.append(weather_row)

                # Insert batch
                if len(batch_data) >= 1000 or (i == total_files - 1 and batch_data):
                    self._insert_batch('stg_weather', batch_data)
                    batch_data = []
                    logger.info(f"Processed {i+1}/{total_files} files")

            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")

    def _insert_batch(self, table_name, data):
        """Insert a batch of data into PostgreSQL"""
        if not data:
            return

        df = pd.DataFrame(data)
        try:
            df.to_sql(
                table_name,
                self.engine,
                schema='staging',
                if_exists='append',
                index=False,
                method='multi'
            )
        except Exception as e:
            logger.error(f"Failed to insert batch into {table_name}: {e}")

    def run(self):
        self.truncate_staging_tables()
        self.load_vehicles()
        self.load_weather()
        logger.info("Raw to Stage ETL completed successfully.")

if __name__ == "__main__":
    etl = ETLRawToStage()
    etl.run()
